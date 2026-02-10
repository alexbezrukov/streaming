use crate::{
    domain::quality::StreamQuality, state::app_state::AppState,
    streaming::webrtc::signaling::WebRTCSignal,
};
use axum::extract::ws::{Message, WebSocket};
use axum::response::IntoResponse;
use axum::{
    extract::{Path, State, ws::WebSocketUpgrade},
    response::Response,
};
use bytes::Bytes;
use futures::{sink::SinkExt, stream::StreamExt};
use tokio::io::AsyncWriteExt;
use tokio::process::Command;
use tokio::sync::mpsc;

// ============================================================================
// LIVE STREAM VIEWING VIA WEBSOCKET
// ============================================================================

/// WebSocket endpoint for watching live streams
/// Sends video segments in real-time to viewers
pub async fn watch_stream(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
    Path(stream_id): Path<String>,
) -> Response {
    ws.on_upgrade(move |socket| handle_viewer_socket(socket, state, stream_id))
}

pub async fn ingest_stream(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
    Path(stream_id): Path<String>,
) -> Response {
    tracing::info!("📹 Ingest connection for stream {}", stream_id);

    if state
        .stream_manager
        .get_stream_metadata(&stream_id)
        .is_none()
    {
        tracing::warn!("❌ Stream {} not found", stream_id);
        return (axum::http::StatusCode::NOT_FOUND, "Stream not found").into_response();
    }

    ws.on_upgrade(move |socket| handle_media_ingest(socket, state, stream_id))
}

async fn handle_media_ingest(socket: WebSocket, state: AppState, stream_id: String) {
    let (mut sender, mut receiver) = socket.split();

    tracing::info!("✅ WebSocket connected: {}", stream_id);

    let (tx, rx) = mpsc::channel::<Bytes>(200);

    // Запускаем FFmpeg + watcher для сегментов
    let state_clone = state.clone();
    let stream_id_clone = stream_id.clone();
    let ffmpeg_task = tokio::spawn(async move {
        if let Err(e) = spawn_ffmpeg_and_watch(&stream_id_clone, &state_clone, rx).await {
            tracing::error!("💥 FFmpeg error: {}", e);
        }
    });

    let mut chunks = 0u64;
    let start = std::time::Instant::now();

    while let Some(msg) = receiver.next().await {
        match msg {
            Ok(Message::Binary(data)) => {
                chunks += 1;

                if chunks == 1 {
                    tracing::info!("🎯 First chunk: {} bytes", data.len());
                }

                if chunks % 100 == 0 {
                    tracing::info!("📊 Received {} chunks", chunks);
                }

                if tx.send(Bytes::from(data)).await.is_err() {
                    tracing::error!("⚠️ FFmpeg channel closed");
                    break;
                }
            }
            Ok(Message::Close(_)) => {
                tracing::info!("👋 Client disconnected");
                break;
            }
            Ok(Message::Ping(data)) => {
                let _ = sender.send(Message::Pong(data)).await;
            }
            Ok(_) => {}
            Err(e) => {
                tracing::error!("❌ WebSocket error: {}", e);
                break;
            }
        }
    }

    tracing::info!(
        "🏁 Received {} chunks in {:.1}s",
        chunks,
        start.elapsed().as_secs_f64()
    );

    drop(tx);
    let _ = ffmpeg_task.await;
}

/// Запускает FFmpeg И следит за созданными сегментами
async fn spawn_ffmpeg_and_watch(
    stream_id: &str,
    state: &AppState,
    mut input_rx: mpsc::Receiver<Bytes>,
) -> Result<(), Box<dyn std::error::Error>> {
    let hls_dir = format!("../hls_output/{}", stream_id);
    let quality_dir = format!("{}/720p", hls_dir);
    tokio::fs::create_dir_all(&quality_dir).await?;

    tracing::info!("🚀 Starting FFmpeg for {}", stream_id);

    // Запускаем FFmpeg
    let mut child = Command::new("ffmpeg")
        .args([
            "-loglevel",
            "error",
            "-re",
            "-i",
            "pipe:0",
            "-c:v",
            "libx264",
            "-preset",
            "ultrafast",
            "-tune",
            "zerolatency",
            "-profile:v",
            "baseline",
            "-level",
            "3.0",
            "-pix_fmt",
            "yuv420p",
            "-g",
            "60",
            "-sc_threshold",
            "0",
            "-b:v",
            "2500k",
            "-maxrate",
            "2800k",
            "-bufsize",
            "5600k",
            "-s",
            "1280x720",
            "-r",
            "30",
            "-c:a",
            "aac",
            "-b:a",
            "128k",
            "-ar",
            "48000",
            "-ac",
            "2",
            "-f",
            "hls",
            "-hls_time",
            "2",
            "-hls_list_size",
            "5",
            "-hls_flags",
            "delete_segments+append_list",
            "-hls_segment_type",
            "mpegts",
            "-hls_segment_filename",
            &format!("{}/segment_%01d.ts", quality_dir),
        ])
        .arg(&format!("{}/index.m3u8", quality_dir))
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()?;

    let mut stdin = child.stdin.take().unwrap();

    // Пишем данные в FFmpeg
    let write_task = tokio::spawn(async move {
        while let Some(chunk) = input_rx.recv().await {
            if stdin.write_all(&chunk).await.is_err() {
                break;
            }
        }
        let _ = stdin.shutdown().await;
    });

    // ВОТ КЛЮЧЕВАЯ ЧАСТЬ: следим за созданными файлами
    let state_clone = state.clone();
    let stream_id_owned = stream_id.to_string();
    let quality_dir_clone = quality_dir.clone();

    let watcher_task = tokio::spawn(async move {
        watch_and_ingest_segments(stream_id_owned, quality_dir_clone, state_clone).await;
    });

    // Ждём завершения FFmpeg
    let output = child.wait_with_output().await?;
    write_task.await?;

    // Останавливаем watcher
    watcher_task.abort();

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        tracing::error!("FFmpeg stderr: {}", stderr);
        return Err(stderr.into());
    }

    Ok(())
}

/// Следит за директорией и добавляет новые сегменты в adaptive_streams
async fn watch_and_ingest_segments(stream_id: String, quality_dir: String, state: AppState) {
    use std::collections::HashSet;
    use tokio::time::{Duration, sleep};

    let mut seen_segments = HashSet::new();
    let mut sequence = 0u64;

    tracing::info!("👀 Starting segment watcher for {}", stream_id);

    loop {
        sleep(Duration::from_millis(500)).await;

        // Читаем директорию
        let mut entries = match tokio::fs::read_dir(&quality_dir).await {
            Ok(e) => e,
            Err(_) => continue,
        };

        while let Ok(Some(entry)) = entries.next_entry().await {
            let path = entry.path();
            let filename = match path.file_name() {
                Some(name) => name.to_string_lossy().to_string(),
                None => continue,
            };

            // Только .ts файлы
            if !filename.ends_with(".ts") {
                continue;
            }

            // Пропускаем уже обработанные
            if seen_segments.contains(&filename) {
                continue;
            }

            // Читаем сегмент
            let data = match tokio::fs::read(&path).await {
                Ok(d) => d,
                Err(e) => {
                    tracing::warn!("Failed to read {}: {}", filename, e);
                    continue;
                }
            };

            tracing::info!("📦 New segment: {} ({} bytes)", filename, data.len());

            // Создаём VideoSegment
            let segment = crate::domain::VideoSegment {
                sequence,
                data: Bytes::from(data),
                duration_ms: 2000, // 2 seconds
                keyframe: true,
                timestamp: sequence * 2000,
                quality: crate::domain::StreamQuality::Medium,
            };

            tracing::info!("segment {:?}", segment);

            // ДОБАВЛЯЕМ В ADAPTIVE_STREAMS!
            if let Err(e) = state
                .stream_manager
                .ingest_segment(&stream_id, segment)
                .await
            {
                tracing::error!("Failed to ingest segment: {}", e);
            } else {
                tracing::info!("✅ Ingested segment {} for {}", sequence, stream_id);
            }

            seen_segments.insert(filename);
            sequence += 1;
        }
    }
}

/// Handle individual viewer WebSocket connection
async fn handle_viewer_socket(socket: WebSocket, state: AppState, stream_id: String) {
    let (mut sender, mut receiver) = socket.split();

    // Subscribe to the stream with default quality
    let mut rx = match state
        .stream_manager
        .subscribe_to_stream(&stream_id, StreamQuality::Medium)
    {
        Ok(rx) => rx,
        Err(e) => {
            let error_msg = format!("{{\"error\": \"{}\"}}", e);
            let _ = sender.send(Message::Text(error_msg)).await;
            let _ = sender.close().await;
            return;
        }
    };

    // Send initial stream metadata
    if let Some(metadata) = state.stream_manager.get_stream_metadata(&stream_id) {
        if let Ok(json) = serde_json::to_string(&metadata) {
            let _ = sender.send(Message::Text(json)).await;
        }
    }

    let stream_id_clone = stream_id.clone();
    let state_clone = state.clone();

    // Spawn task to send video segments
    let mut send_task = tokio::spawn(async move {
        while let Ok(segment) = rx.recv().await {
            // Send segment as binary message
            if sender
                .send(Message::Binary(segment.data.to_vec()))
                .await
                .is_err()
            {
                break;
            }
        }

        // Unsubscribe when connection closes
        state_clone
            .stream_manager
            .unsubscribe_from_stream(&stream_id_clone);
    });

    // Handle incoming messages from client (quality changes, etc.)
    let mut recv_task = tokio::spawn(async move {
        while let Some(Ok(msg)) = receiver.next().await {
            match msg {
                Message::Text(text) => {
                    // Handle quality change requests
                    if text.starts_with("quality:") {
                        if let Some(quality_str) = text.strip_prefix("quality:") {
                            let _quality = parse_quality(quality_str);
                            // In production: switch to different quality stream
                            // state.stream_manager.change_quality(&stream_id, quality);
                        }
                    }
                }
                Message::Close(_) => {
                    break;
                }
                Message::Ping(data) => {
                    // Respond to ping with pong
                    // Note: Axum handles this automatically, but we can log it
                    tracing::debug!("Received ping: {:?}", data);
                }
                Message::Pong(_) => {
                    // Heartbeat response
                }
                _ => {}
            }
        }
    });

    // Wait for either task to complete
    tokio::select! {
        _ = (&mut send_task) => {
            recv_task.abort();
        }
        _ = (&mut recv_task) => {
            send_task.abort();
        }
    }
}

// ============================================================================
// WEBRTC SIGNALING
// ============================================================================

/// WebSocket endpoint for WebRTC signaling
/// Handles SDP offer/answer and ICE candidate exchange
pub async fn webrtc_signaling(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
    Path(stream_id): Path<String>,
) -> Response {
    ws.on_upgrade(move |socket| handle_webrtc_signaling(socket, state, stream_id))
}

/// Handle WebRTC signaling over WebSocket
async fn handle_webrtc_signaling(socket: WebSocket, state: AppState, stream_id: String) {
    let (mut sender, mut receiver) = socket.split();

    // Create unique peer ID for this connection
    let peer_id = uuid::Uuid::new_v4().to_string();

    // Register WebRTC connection
    state
        .stream_manager
        .webrtc_manager
        .create_connection(peer_id.clone(), stream_id.clone())
        .await;

    tracing::info!(
        "WebRTC signaling started for peer {} on stream {}",
        peer_id,
        stream_id
    );

    // Handle incoming signaling messages
    while let Some(msg_result) = receiver.next().await {
        match msg_result {
            Ok(Message::Text(text)) => {
                if let Err(e) = handle_signaling_message(&text, &peer_id, &state, &mut sender).await
                {
                    tracing::error!("Signaling error: {}", e);
                    break;
                }
            }
            Ok(Message::Close(_)) => {
                tracing::info!("WebRTC signaling closed for peer {}", peer_id);
                break;
            }
            Ok(Message::Ping(data)) => {
                // Respond to ping
                let _ = sender.send(Message::Pong(data)).await;
            }
            Ok(Message::Pong(_)) => {
                // Heartbeat received
            }
            Ok(_) => {
                // Ignore other message types
            }
            Err(e) => {
                tracing::error!("WebSocket error: {}", e);
                break;
            }
        }
    }

    // Cleanup on disconnect
    // In production: remove peer connection, cleanup resources
    tracing::info!("Cleaning up WebRTC connection for peer {}", peer_id);
}

/// Process individual signaling messages
async fn handle_signaling_message(
    text: &str,
    peer_id: &str,
    state: &AppState,
    sender: &mut futures::stream::SplitSink<WebSocket, Message>,
) -> Result<(), String> {
    // Parse signaling message
    let signal: WebRTCSignal =
        serde_json::from_str(text).map_err(|e| format!("Invalid signal format: {}", e))?;

    match signal.signal_type.as_str() {
        "offer" => {
            // Handle SDP offer from client
            tracing::debug!("Received SDP offer from peer {}", peer_id);

            if let Some(answer) = state
                .stream_manager
                .webrtc_manager
                .set_offer(peer_id, signal.payload)
                .await
            {
                // Send SDP answer back to client
                let response = WebRTCSignal {
                    signal_type: "answer".to_string(),
                    payload: answer,
                };

                let json = serde_json::to_string(&response)
                    .map_err(|e| format!("Failed to serialize answer: {}", e))?;

                sender
                    .send(Message::Text(json))
                    .await
                    .map_err(|e| format!("Failed to send answer: {}", e))?;

                tracing::debug!("Sent SDP answer to peer {}", peer_id);
            } else {
                return Err(format!("Failed to generate answer for peer {}", peer_id));
            }
        }

        "ice" => {
            // Handle ICE candidate
            tracing::debug!("Received ICE candidate from peer {}", peer_id);

            state
                .stream_manager
                .webrtc_manager
                .add_ice_candidate(peer_id, signal.payload)
                .await;
        }

        "answer" => {
            // Handle SDP answer (if server initiates connection)
            tracing::debug!("Received SDP answer from peer {}", peer_id);
            // In production: process answer if server is the offerer
        }

        _ => {
            tracing::warn!(
                "Unknown signal type '{}' from peer {}",
                signal.signal_type,
                peer_id
            );
        }
    }

    Ok(())
}

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

/// Parse quality string to StreamQuality enum
fn parse_quality(quality_str: &str) -> Option<StreamQuality> {
    match quality_str.to_lowercase().as_str() {
        "low" | "360p" => Some(StreamQuality::Low),
        "medium" | "720p" => Some(StreamQuality::Medium),
        "high" | "1080p" => Some(StreamQuality::High),
        "ultra" | "4k" | "2160p" => Some(StreamQuality::Ultra),
        _ => None,
    }
}

// ============================================================================
// TESTS
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_quality() {
        assert_eq!(parse_quality("low"), Some(StreamQuality::Low));
        assert_eq!(parse_quality("360p"), Some(StreamQuality::Low));
        assert_eq!(parse_quality("medium"), Some(StreamQuality::Medium));
        assert_eq!(parse_quality("720p"), Some(StreamQuality::Medium));
        assert_eq!(parse_quality("high"), Some(StreamQuality::High));
        assert_eq!(parse_quality("1080p"), Some(StreamQuality::High));
        assert_eq!(parse_quality("ultra"), Some(StreamQuality::Ultra));
        assert_eq!(parse_quality("4k"), Some(StreamQuality::Ultra));
        assert_eq!(parse_quality("invalid"), None);
    }
}

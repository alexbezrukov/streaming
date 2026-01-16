use axum::{
    extract::{ws::WebSocketUpgrade, Path, State},
    response::Response,
};
use axum::extract::ws::{Message, WebSocket};
use futures::{sink::SinkExt, stream::StreamExt};
use crate::{
    domain::{quality::StreamQuality},
    state::app_state::AppState,
    streaming::webrtc::signaling::WebRTCSignal,
};

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
                if let Err(e) = handle_signaling_message(
                    &text,
                    &peer_id,
                    &state,
                    &mut sender,
                )
                .await
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
    let signal: WebRTCSignal = serde_json::from_str(text)
        .map_err(|e| format!("Invalid signal format: {}", e))?;

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
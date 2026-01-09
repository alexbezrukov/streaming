use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Path, State,
    },
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use bytes::Bytes;
use dashmap::DashMap;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, SystemTime},
};
use tokio::{
    sync::{broadcast, mpsc, RwLock as TokioRwLock},
    time::interval,
};
use tower_http::cors::CorsLayer;

// ============================================================================
// DOMAIN MODELS
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamMetadata {
    pub stream_id: String,
    pub broadcaster_id: String,
    pub title: String,
    pub viewers: usize,
    pub started_at: SystemTime,
    pub bitrate_kbps: u32,
    pub resolution: String,
    pub codec: String,
}

#[derive(Debug, Clone)]
pub struct VideoSegment {
    pub sequence: u64,
    pub data: Bytes,
    pub duration_ms: u32,
    pub keyframe: bool,
    pub timestamp: u64,
}

#[derive(Debug, Clone)]
pub struct AdaptiveStream {
    pub quality: StreamQuality,
    pub bitrate_kbps: u32,
    pub resolution: (u32, u32),
    pub segments: Vec<VideoSegment>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum StreamQuality {
    Low,    // 360p, ~1 Mbps
    Medium, // 720p, ~3 Mbps
    High,   // 1080p, ~6 Mbps
    Ultra,  // 4K, ~12 Mbps
}

// ============================================================================
// STREAM MANAGER - Core business logic
// ============================================================================

pub struct StreamManager {
    // Active streams with their metadata
    streams: DashMap<String, Arc<RwLock<StreamMetadata>>>,
    
    // Broadcast channels for each stream (multiple viewers per stream)
    broadcast_channels: DashMap<String, broadcast::Sender<VideoSegment>>,
    
    // Adaptive bitrate variants for each stream
    adaptive_streams: DashMap<String, HashMap<StreamQuality, AdaptiveStream>>,
    
    // Viewer counts
    viewer_counts: DashMap<String, usize>,
    
    // Redis client for distributed state (optional)
    redis_client: Option<redis::Client>,
}

impl StreamManager {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            streams: DashMap::new(),
            broadcast_channels: DashMap::new(),
            adaptive_streams: DashMap::new(),
            viewer_counts: DashMap::new(),
            redis_client: None,
        })
    }

    pub async fn create_stream(
        &self,
        stream_id: String,
        broadcaster_id: String,
        title: String,
    ) -> Result<(), String> {
        if self.streams.contains_key(&stream_id) {
            return Err("Stream already exists".to_string());
        }

        let metadata = StreamMetadata {
            stream_id: stream_id.clone(),
            broadcaster_id,
            title,
            viewers: 0,
            started_at: SystemTime::now(),
            bitrate_kbps: 3000,
            resolution: "1280x720".to_string(),
            codec: "H264".to_string(),
        };

        // Create broadcast channel with buffer for 100 segments
        let (tx, _) = broadcast::channel(100);
        
        self.streams.insert(stream_id.clone(), Arc::new(RwLock::new(metadata)));
        self.broadcast_channels.insert(stream_id.clone(), tx);
        self.viewer_counts.insert(stream_id.clone(), 0);

        // Initialize adaptive streams
        let mut variants = HashMap::new();
        variants.insert(
            StreamQuality::Low,
            AdaptiveStream {
                quality: StreamQuality::Low,
                bitrate_kbps: 1000,
                resolution: (640, 360),
                segments: Vec::new(),
            },
        );
        variants.insert(
            StreamQuality::Medium,
            AdaptiveStream {
                quality: StreamQuality::Medium,
                bitrate_kbps: 3000,
                resolution: (1280, 720),
                segments: Vec::new(),
            },
        );
        variants.insert(
            StreamQuality::High,
            AdaptiveStream {
                quality: StreamQuality::High,
                bitrate_kbps: 6000,
                resolution: (1920, 1080),
                segments: Vec::new(),
            },
        );
        
        self.adaptive_streams.insert(stream_id, variants);

        Ok(())
    }

    pub async fn ingest_segment(
        &self,
        stream_id: &str,
        segment: VideoSegment,
    ) -> Result<(), String> {
        let tx = self
            .broadcast_channels
            .get(stream_id)
            .ok_or("Stream not found")?;

        // Store segment in adaptive streams (in production, transcode here)
        if let Some(mut variants) = self.adaptive_streams.get_mut(stream_id) {
            for (quality, adaptive_stream) in variants.iter_mut() {
                // In production: transcode segment to different qualities
                // For now, we simulate by just storing the original
                let mut transcoded_segment = segment.clone();
                transcoded_segment.data = self.simulate_transcode(&segment.data, *quality);
                
                adaptive_stream.segments.push(transcoded_segment);
                
                // Keep only last 30 segments (DVR window)
                if adaptive_stream.segments.len() > 30 {
                    adaptive_stream.segments.remove(0);
                }
            }
        }

        // Broadcast to all viewers
        let _ = tx.send(segment);

        Ok(())
    }

    pub fn subscribe_to_stream(
        &self,
        stream_id: &str,
        quality: StreamQuality,
    ) -> Result<broadcast::Receiver<VideoSegment>, String> {
        let tx = self
            .broadcast_channels
            .get(stream_id)
            .ok_or("Stream not found")?;

        // Increment viewer count
        self.viewer_counts
            .entry(stream_id.to_string())
            .and_modify(|count| *count += 1)
            .or_insert(1);

        // Update metadata
        if let Some(metadata) = self.streams.get(stream_id) {
            let count = self.viewer_counts.get(stream_id).map(|c| *c).unwrap_or(0);
            metadata.write().viewers = count;
        }

        Ok(tx.subscribe())
    }

    pub fn unsubscribe_from_stream(&self, stream_id: &str) {
        self.viewer_counts
            .entry(stream_id.to_string())
            .and_modify(|count| {
                if *count > 0 {
                    *count -= 1;
                }
            });

        if let Some(metadata) = self.streams.get(stream_id) {
            let count = self.viewer_counts.get(stream_id).map(|c| *c).unwrap_or(0);
            metadata.write().viewers = count;
        }
    }

    pub fn get_stream_metadata(&self, stream_id: &str) -> Option<StreamMetadata> {
        self.streams.get(stream_id).map(|m| m.read().clone())
    }

    pub fn list_streams(&self) -> Vec<StreamMetadata> {
        self.streams
            .iter()
            .map(|entry| entry.value().read().clone())
            .collect()
    }

    pub async fn end_stream(&self, stream_id: &str) -> Result<(), String> {
        self.streams.remove(stream_id);
        self.broadcast_channels.remove(stream_id);
        self.adaptive_streams.remove(stream_id);
        self.viewer_counts.remove(stream_id);
        Ok(())
    }

    // Simulate transcoding (in production, use FFmpeg/GStreamer)
    fn simulate_transcode(&self, data: &Bytes, quality: StreamQuality) -> Bytes {
        // In production: actual transcoding with different bitrates/resolutions
        // For now, just return the same data (simulated)
        let scale_factor = match quality {
            StreamQuality::Low => 0.3,
            StreamQuality::Medium => 0.5,
            StreamQuality::High => 0.8,
            StreamQuality::Ultra => 1.0,
        };
        
        let new_size = (data.len() as f32 * scale_factor) as usize;
        data.slice(0..new_size.min(data.len()))
    }
}

// ============================================================================
// HTTP API HANDLERS
// ============================================================================

#[derive(Clone)]
struct AppState {
    stream_manager: Arc<StreamManager>,
}

#[derive(Deserialize)]
struct CreateStreamRequest {
    broadcaster_id: String,
    title: String,
}

#[derive(Serialize)]
struct CreateStreamResponse {
    stream_id: String,
    rtmp_url: String,
    stream_key: String,
}

async fn create_stream(
    State(state): State<AppState>,
    Json(req): Json<CreateStreamRequest>,
) -> Result<Json<CreateStreamResponse>, (axum::http::StatusCode, String)> {
    let stream_id = uuid::Uuid::new_v4().to_string();
    
    state
        .stream_manager
        .create_stream(stream_id.clone(), req.broadcaster_id, req.title)
        .await
        .map_err(|e| (axum::http::StatusCode::BAD_REQUEST, e))?;

    Ok(Json(CreateStreamResponse {
        stream_id: stream_id.clone(),
        rtmp_url: format!("rtmp://localhost:1935/live"),
        stream_key: stream_id,
    }))
}

async fn list_streams(
    State(state): State<AppState>,
) -> Json<Vec<StreamMetadata>> {
    Json(state.stream_manager.list_streams())
}

async fn get_stream(
    State(state): State<AppState>,
    Path(stream_id): Path<String>,
) -> Result<Json<StreamMetadata>, axum::http::StatusCode> {
    state
        .stream_manager
        .get_stream_metadata(&stream_id)
        .map(Json)
        .ok_or(axum::http::StatusCode::NOT_FOUND)
}

async fn end_stream(
    State(state): State<AppState>,
    Path(stream_id): Path<String>,
) -> Result<Json<String>, (axum::http::StatusCode, String)> {
    state
        .stream_manager
        .end_stream(&stream_id)
        .await
        .map_err(|e| (axum::http::StatusCode::BAD_REQUEST, e))?;
    
    Ok(Json("Stream ended".to_string()))
}

// ============================================================================
// WEBSOCKET HANDLER - For viewers
// ============================================================================

async fn watch_stream(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
    Path(stream_id): Path<String>,
) -> Response {
    ws.on_upgrade(move |socket| handle_viewer_socket(socket, state, stream_id))
}

async fn handle_viewer_socket(socket: WebSocket, state: AppState, stream_id: String) {
    let (mut sender, mut receiver) = socket.split();
    
    // Subscribe to stream
    let mut rx = match state.stream_manager.subscribe_to_stream(&stream_id, StreamQuality::Medium) {
        Ok(rx) => rx,
        Err(e) => {
            let _ = sender.send(Message::Text(format!("Error: {}", e))).await;
            return;
        }
    };

    // Send initial metadata
    if let Some(metadata) = state.stream_manager.get_stream_metadata(&stream_id) {
        let json = serde_json::to_string(&metadata).unwrap();
        let _ = sender.send(Message::Text(json)).await;
    }

    // Spawn task to forward video segments
    let stream_id_clone = stream_id.clone();
    let state_clone = state.clone();
    
    tokio::spawn(async move {
        while let Ok(segment) = rx.recv().await {
            // Send binary video data
            if sender.send(Message::Binary(segment.data.to_vec())).await.is_err() {
                break;
            }
        }
        
        // Unsubscribe on disconnect
        state_clone.stream_manager.unsubscribe_from_stream(&stream_id_clone);
    });

    // Handle incoming messages (quality changes, etc.)
    while let Some(Ok(msg)) = receiver.next().await {
        if let Message::Text(text) = msg {
            // Handle quality change requests, etc.
            if text.starts_with("quality:") {
                // In production: switch to different quality stream
            }
        }
    }
}

// ============================================================================
// RTMP INGEST HANDLER
// ============================================================================

async fn handle_rtmp_ingest(state: AppState) {
    let listener = tokio::net::TcpListener::bind("0.0.0.0:1935")
        .await
        .expect("Failed to bind RTMP port");

    println!("RTMP server listening on 0.0.0.0:1935");

    while let Ok((stream, addr)) = listener.accept().await {
        let state = state.clone();
        tokio::spawn(async move {
            if let Err(e) = process_rtmp_connection(stream, addr, state).await {
                eprintln!("RTMP connection error: {}", e);
            }
        });
    }
}

async fn process_rtmp_connection(
    mut stream: tokio::net::TcpStream,
    addr: SocketAddr,
    state: AppState,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("New RTMP connection from {}", addr);
    
    // In production: Full RTMP handshake and parsing
    // For this demo, we simulate receiving video segments
    
    let mut buffer = vec![0u8; 4096];
    let mut sequence = 0u64;
    
    loop {
        let n = stream.readable().await?;
        if n == 0 {
            break;
        }

        // Simulate receiving a video segment
        let segment = VideoSegment {
            sequence,
            data: Bytes::from(buffer.clone()),
            duration_ms: 1000,
            keyframe: sequence % 30 == 0,
            timestamp: sequence * 1000,
        };

        // Extract stream_id from RTMP stream key (simplified)
        let stream_id = "demo-stream-id"; // In production: parse from RTMP handshake
        
        if let Err(e) = state.stream_manager.ingest_segment(stream_id, segment).await {
            eprintln!("Failed to ingest segment: {}", e);
        }

        sequence += 1;
        tokio::time::sleep(Duration::from_millis(33)).await; // ~30 FPS
    }

    Ok(())
}

// ============================================================================
// METRICS & MONITORING
// ============================================================================

#[derive(Serialize)]
struct SystemMetrics {
    active_streams: usize,
    total_viewers: usize,
    uptime_seconds: u64,
    memory_usage_mb: u64,
}

async fn get_metrics(State(state): State<AppState>) -> Json<SystemMetrics> {
    let active_streams = state.stream_manager.streams.len();
    let total_viewers: usize = state
        .stream_manager
        .viewer_counts
        .iter()
        .map(|entry| *entry.value())
        .sum();

    Json(SystemMetrics {
        active_streams,
        total_viewers,
        uptime_seconds: 0, // Track actual uptime in production
        memory_usage_mb: 0, // Use proper memory tracking
    })
}

// ============================================================================
// MAIN APPLICATION
// ============================================================================

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let stream_manager = StreamManager::new();
    let state = AppState {
        stream_manager: stream_manager.clone(),
    };

    // Spawn RTMP ingest server
    let rtmp_state = state.clone();
    tokio::spawn(async move {
        handle_rtmp_ingest(rtmp_state).await;
    });

    // Build HTTP API
    let app = Router::new()
        .route("/api/streams", post(create_stream))
        .route("/api/streams", get(list_streams))
        .route("/api/streams/:stream_id", get(get_stream))
        .route("/api/streams/:stream_id", axum::routing::delete(end_stream))
        .route("/api/streams/:stream_id/watch", get(watch_stream))
        .route("/api/metrics", get(get_metrics))
        .layer(CorsLayer::permissive())
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
    println!("HTTP API listening on {}", addr);
    
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("Failed to bind HTTP port");

    axum::serve(listener, app)
        .await
        .expect("Server failed");
}

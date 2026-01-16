use bytes::Bytes;
use std::net::SocketAddr;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::Duration,
};
use tracing::{debug, error, info, warn};

use crate::{
    domain::{quality::StreamQuality, stream::VideoSegment},
    state::app_state::AppState,
};

/// RTMP server error types
#[derive(Debug, thiserror::Error)]
pub enum RtmpError {
    #[error("Failed to bind RTMP port: {0}")]
    BindError(String),
    
    #[error("Connection error: {0}")]
    ConnectionError(String),
    
    #[error("Invalid RTMP handshake")]
    InvalidHandshake,
    
    #[error("Stream not found: {0}")]
    StreamNotFound(String),
    
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
}

/// RTMP server
pub struct RtmpServer {
    state: AppState,
    bind_addr: SocketAddr,
}

impl RtmpServer {
    pub fn new(state: AppState, bind_addr: SocketAddr) -> Self {
        Self { state, bind_addr }
    }

    /// Start RTMP server
    pub async fn run(self) -> Result<(), RtmpError> {
        let listener = TcpListener::bind(self.bind_addr)
            .await
            .map_err(|e| RtmpError::BindError(e.to_string()))?;

        info!("🎥 RTMP server listening on {}", self.bind_addr);

        loop {
            match listener.accept().await {
                Ok((stream, addr)) => {
                    let state = self.state.clone();
                    
                    tokio::spawn(async move {
                        info!("New RTMP connection from {}", addr);
                        
                        if let Err(e) = handle_rtmp_connection(stream, addr, state).await {
                            error!("RTMP connection error from {}: {}", addr, e);
                        } else {
                            info!("RTMP connection from {} closed gracefully", addr);
                        }
                    });
                }
                Err(e) => {
                    error!("Failed to accept RTMP connection: {}", e);
                }
            }
        }
    }
}

/// Main entry point for RTMP ingest
pub async fn handle_rtmp_ingest(
    state: AppState,
    bind_addr: SocketAddr,
) -> Result<(), RtmpError> {
    let server = RtmpServer::new(state, bind_addr);
    server.run().await
}

/// Handle individual RTMP connection
async fn handle_rtmp_connection(
    mut stream: TcpStream,
    addr: SocketAddr,
    state: AppState,
) -> Result<(), RtmpError> {
    // Set TCP options for better streaming performance
    stream.set_nodelay(true)?;
    
    // Perform RTMP handshake
    let stream_key = perform_rtmp_handshake(&mut stream).await?;
    info!("RTMP handshake completed for stream: {}", stream_key);

    // Verify stream exists
    if !state.stream_manager.streams.contains_key(&stream_key) {
        warn!("Stream key not found: {}", stream_key);
        return Err(RtmpError::StreamNotFound(stream_key));
    }

    // Process video stream
    process_video_stream(&mut stream, &stream_key, state).await?;

    Ok(())
}

/// Perform RTMP handshake (simplified version)
/// In production, use a proper RTMP library like `rml_rtmp`
async fn perform_rtmp_handshake(stream: &mut TcpStream) -> Result<String, RtmpError> {
    // RTMP Handshake consists of 3 phases: C0+C1, S0+S1+S2, C2
    
    // Phase 1: Receive C0 (1 byte) + C1 (1536 bytes)
    let mut c0c1 = vec![0u8; 1537];
    stream
        .read_exact(&mut c0c1)
        .await
        .map_err(|_| RtmpError::InvalidHandshake)?;

    // Verify C0 (RTMP version 3)
    if c0c1[0] != 3 {
        return Err(RtmpError::InvalidHandshake);
    }

    debug!("Received C0+C1 from client");

    // Phase 2: Send S0 (version 3) + S1 (1536 bytes) + S2 (echo C1)
    let mut s0s1s2 = Vec::with_capacity(3073);
    s0s1s2.push(3); // S0

    // S1: timestamp (4) + zero (4) + random data (1528)
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as u32;
    
    s0s1s2.extend_from_slice(&timestamp.to_be_bytes());
    s0s1s2.extend_from_slice(&[0u8; 4]); // Zero
    s0s1s2.extend_from_slice(&vec![0u8; 1528]); // Random data (simplified)

    // S2: Echo C1
    s0s1s2.extend_from_slice(&c0c1[1..]);

    stream
        .write_all(&s0s1s2)
        .await
        .map_err(|_| RtmpError::InvalidHandshake)?;
    stream.flush().await?;

    debug!("Sent S0+S1+S2 to client");

    // Phase 3: Receive C2 (echo of S1)
    let mut c2 = vec![0u8; 1536];
    stream
        .read_exact(&mut c2)
        .await
        .map_err(|_| RtmpError::InvalidHandshake)?;

    debug!("Received C2, handshake complete");

    // Parse RTMP connect and publish commands to get stream key
    // In a real implementation, this would parse the actual RTMP messages
    // For now, we'll use a demo stream ID
    let stream_key = parse_stream_key(stream).await?;

    Ok(stream_key)
}

/// Parse stream key from RTMP publish command
/// Simplified version - in production use proper RTMP parsing
async fn parse_stream_key(stream: &mut TcpStream) -> Result<String, RtmpError> {
    // Read RTMP messages until we find the publish command
    let mut buffer = vec![0u8; 4096];
    
    // Set a timeout for receiving the stream key
    let timeout = tokio::time::sleep(Duration::from_secs(10));
    tokio::pin!(timeout);

    loop {
        tokio::select! {
            result = stream.read(&mut buffer) => {
                match result {
                    Ok(n) if n > 0 => {
                        // In production: Parse RTMP chunks and extract stream key
                        // For now, search for "publish" command in the data
                        let data = String::from_utf8_lossy(&buffer[..n]);
                        
                        if data.contains("publish") || data.contains("rtmp://") {
                            // Extract stream key (simplified)
                            // In real implementation: proper AMF0 parsing
                            debug!("Found publish command in RTMP stream");
                            
                            // For demo purposes, return a default stream key
                            // In production: extract from actual publish command
                            return Ok("demo-stream-id".to_string());
                        }
                    }
                    Ok(_) => {
                        return Err(RtmpError::ConnectionError("Connection closed".into()));
                    }
                    Err(e) => {
                        return Err(RtmpError::IoError(e));
                    }
                }
            }
            _ = &mut timeout => {
                return Err(RtmpError::ConnectionError("Timeout waiting for stream key".into()));
            }
        }
    }
}

/// Process incoming video stream and ingest segments
async fn process_video_stream(
    stream: &mut TcpStream,
    stream_key: &str,
    state: AppState,
) -> Result<(), RtmpError> {
    info!("Starting video ingest for stream: {}", stream_key);

    let mut buffer = vec![0u8; 65536]; // 64KB buffer
    let mut sequence = 0u64;
    let mut last_keyframe = std::time::Instant::now();

    loop {
        // Read data with timeout
        let n = tokio::time::timeout(
            Duration::from_secs(30),
            stream.read(&mut buffer)
        )
        .await
        .map_err(|_| RtmpError::ConnectionError("Read timeout".into()))?
        .map_err(RtmpError::IoError)?;

        if n == 0 {
            info!("RTMP stream ended for: {}", stream_key);
            break;
        }

        debug!("Received {} bytes for stream {}", n, stream_key);

        // In production: Parse FLV tags and extract video frames
        // For now, we simulate video segments
        
        // Check if this is a keyframe (every ~2 seconds)
        let is_keyframe = last_keyframe.elapsed() >= Duration::from_secs(2);
        if is_keyframe {
            last_keyframe = std::time::Instant::now();
        }

        // Create video segment
        let segment = VideoSegment {
            sequence,
            data: Bytes::copy_from_slice(&buffer[..n]),
            duration_ms: 33, // ~30 FPS
            keyframe: is_keyframe,
            timestamp: sequence * 33,
            quality: StreamQuality::Medium, // Source quality
        };

        // Ingest segment into stream manager
        if let Err(e) = state
            .stream_manager
            .ingest_segment(stream_key, segment)
            .await
        {
            error!("Failed to ingest segment for stream {}: {}", stream_key, e);
            // Don't break - continue processing
        }

        sequence += 1;

        // Small delay to simulate real-time streaming (~30 FPS)
        tokio::time::sleep(Duration::from_millis(33)).await;
    }

    Ok(())
}

// ============================================================================
// PRODUCTION-READY RTMP IMPLEMENTATION NOTES
// ============================================================================

/*
For production deployment, consider using these libraries:

1. rml_rtmp - Pure Rust RTMP implementation
   Cargo.toml: rml_rtmp = "0.6"
   
   Benefits:
   - Full RTMP spec compliance
   - Proper handshake handling
   - AMF0/AMF3 parsing
   - Chunk parsing
   - FLV demuxing

2. rtmp-rs - Alternative RTMP library
   Cargo.toml: rtmp-rs = "0.1"

Example with rml_rtmp:

use rml_rtmp::sessions::{
    ServerSession, 
    ServerSessionConfig, 
    ServerSessionEvent
};

async fn handle_rtmp_connection_production(
    mut stream: TcpStream,
    stream_key: &str,
    state: AppState,
) -> Result<(), RtmpError> {
    let config = ServerSessionConfig::new();
    let (mut session, mut results) = ServerSession::new(config)?;

    loop {
        // Read from TCP stream
        let mut buffer = [0u8; 4096];
        let bytes_read = stream.read(&mut buffer).await?;
        
        if bytes_read == 0 {
            break;
        }

        // Process RTMP data
        results = session.handle_input(&buffer[..bytes_read])?;

        for result in results {
            match result {
                ServerSessionEvent::ConnectionRequested { app_name } => {
                    session.accept_request()?;
                }
                ServerSessionEvent::PublishStreamRequested { stream_key, .. } => {
                    // Verify stream exists
                    session.accept_request()?;
                }
                ServerSessionEvent::VideoDataReceived { data, timestamp, .. } => {
                    // Process video data
                    let segment = VideoSegment {
                        sequence: timestamp as u64,
                        data: Bytes::from(data),
                        duration_ms: 33,
                        keyframe: is_keyframe(&data),
                        timestamp: timestamp as u64,
                        quality: StreamQuality::Medium,
                    };
                    
                    state.stream_manager.ingest_segment(stream_key, segment).await?;
                }
                ServerSessionEvent::AudioDataReceived { data, timestamp, .. } => {
                    // Process audio data
                }
                _ => {}
            }
        }

        // Send outbound data
        let outbound = session.get_outbound_bytes()?;
        if !outbound.is_empty() {
            stream.write_all(&outbound).await?;
        }
    }

    Ok(())
}
*/

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

/// Check if data contains a keyframe marker
/// In production: properly parse FLV video tag
fn is_keyframe_data(data: &[u8]) -> bool {
    // FLV video tag format check (simplified)
    if data.len() < 5 {
        return false;
    }

    // FLV video tag: [frame_type(4bits) | codec_id(4bits)]
    // frame_type: 1 = keyframe, 2 = inter frame
    let frame_type = (data[0] & 0xF0) >> 4;
    frame_type == 1
}

// ============================================================================
// RTMP SERVER BUILDER
// ============================================================================

/// Builder for RTMP server configuration
pub struct RtmpServerBuilder {
    bind_addr: Option<SocketAddr>,
    max_connections: usize,
    connection_timeout: Duration,
    buffer_size: usize,
}

impl RtmpServerBuilder {
    pub fn new() -> Self {
        Self {
            bind_addr: None,
            max_connections: 1000,
            connection_timeout: Duration::from_secs(30),
            buffer_size: 65536,
        }
    }

    pub fn bind_addr(mut self, addr: SocketAddr) -> Self {
        self.bind_addr = Some(addr);
        self
    }

    pub fn max_connections(mut self, max: usize) -> Self {
        self.max_connections = max;
        self
    }

    pub fn connection_timeout(mut self, timeout: Duration) -> Self {
        self.connection_timeout = timeout;
        self
    }

    pub fn buffer_size(mut self, size: usize) -> Self {
        self.buffer_size = size;
        self
    }

    pub fn build(self, state: AppState) -> Result<RtmpServer, RtmpError> {
        let bind_addr = self
            .bind_addr
            .ok_or_else(|| RtmpError::BindError("No bind address specified".into()))?;

        Ok(RtmpServer::new(state, bind_addr))
    }
}

impl Default for RtmpServerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// TESTS
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_keyframe_detection() {
        // FLV keyframe tag
        let keyframe_data = vec![0x17, 0x00, 0x00, 0x00, 0x00];
        assert!(is_keyframe_data(&keyframe_data));

        // FLV inter frame tag
        let inter_frame_data = vec![0x27, 0x00, 0x00, 0x00, 0x00];
        assert!(!is_keyframe_data(&inter_frame_data));
    }

    #[test]
    fn test_rtmp_server_builder() {
        let addr = "127.0.0.1:1935".parse().unwrap();
        
        let builder = RtmpServerBuilder::new()
            .bind_addr(addr)
            .max_connections(500)
            .connection_timeout(Duration::from_secs(60))
            .buffer_size(131072);

        assert_eq!(builder.max_connections, 500);
        assert_eq!(builder.buffer_size, 131072);
    }
}
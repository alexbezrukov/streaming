use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

/// Main application configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub server: ServerConfig,
    pub redis: RedisConfig,
    pub cdn: CdnConfig,
    pub streaming: StreamingConfig,
}

/// HTTP/RTMP server configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    /// HTTP API port
    pub http_port: u16,
    
    /// RTMP ingest port
    pub rtmp_port: u16,
    
    /// Bind address (0.0.0.0 for all interfaces)
    pub bind_addr: String,
    
    /// Server node ID (unique identifier)
    pub node_id: String,
    
    /// Node type: "origin" or "edge"
    pub node_type: NodeType,
    
    /// Geographic location (e.g., "EU-WEST", "US-EAST")
    pub location: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NodeType {
    Origin,
    Edge,
}

/// Redis configuration for distributed cache
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedisConfig {
    /// Redis connection URL
    pub url: String,
    
    /// Connection pool size
    pub pool_size: u32,
    
    /// Connection timeout in seconds
    pub timeout_seconds: u64,
    
    /// Enable Redis cluster mode
    pub cluster_mode: bool,
    
    /// Redis key prefix for namespacing
    pub key_prefix: String,
}

/// CDN-specific configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdnConfig {
    /// In-memory cache size (number of segments)
    pub memory_cache_size: usize,
    
    /// Segment TTL in Redis (seconds)
    pub segment_ttl: u64,
    
    /// Maximum edge node capacity (concurrent viewers)
    pub max_node_capacity: usize,
    
    /// Enable edge caching
    pub enable_edge_cache: bool,
    
    /// Origin server URL (for edge nodes)
    pub origin_url: Option<String>,
    
    /// Enable CDN statistics
    pub enable_stats: bool,
    
    /// Load balancing strategy for edge nodes
    /// Options: "least-connections", "geographic", "weighted", "round-robin", "random"
    pub load_balancing_strategy: String,
    
    /// Maximum heartbeat age before node is unhealthy (seconds)
    pub heartbeat_timeout_seconds: u64,
    
    /// Maximum load percentage before node is overloaded
    pub max_load_percentage: f64,
}

/// Streaming protocol configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamingConfig {
    /// HLS segment duration in seconds
    pub hls_segment_duration: u32,
    
    /// HLS playlist window size (number of segments)
    pub hls_playlist_size: usize,
    
    /// Enable DASH streaming
    pub enable_dash: bool,
    
    /// Enable WebRTC streaming
    pub enable_webrtc: bool,
    
    /// DVR window duration in seconds
    pub dvr_window_seconds: u64,
    
    /// Default video codec
    pub default_codec: VideoCodec,
    
    /// Enable adaptive bitrate
    pub enable_abr: bool,
    
    /// Transcoding profiles
    pub profiles: Vec<TranscodingProfile>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum VideoCodec {
    H264,
    H265,
    VP9,
    AV1,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TranscodingProfile {
    pub name: String,
    pub width: u32,
    pub height: u32,
    pub bitrate_kbps: u32,
    pub fps: u32,
}

impl Config {
    /// Load configuration from environment variables
    pub fn load_from_env() -> Self {
        Self {
            server: ServerConfig::from_env(),
            redis: RedisConfig::from_env(),
            cdn: CdnConfig::from_env(),
            streaming: StreamingConfig::from_env(),
        }
    }

    /// Load configuration from TOML file
    pub fn load_from_file(path: &str) -> Result<Self, ConfigError> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| ConfigError::FileReadError(e.to_string()))?;
        
        toml::from_str(&content)
            .map_err(|e| ConfigError::ParseError(e.to_string()))
    }

    /// Validate configuration
    pub fn validate(&self) -> Result<(), ConfigError> {
        // Validate ports
        if self.server.http_port == 0 {
            return Err(ConfigError::InvalidValue("http_port cannot be 0".into()));
        }
        if self.server.rtmp_port == 0 {
            return Err(ConfigError::InvalidValue("rtmp_port cannot be 0".into()));
        }

        // Validate node ID
        if self.server.node_id.is_empty() {
            return Err(ConfigError::InvalidValue("node_id cannot be empty".into()));
        }

        // Validate Redis URL
        if self.redis.url.is_empty() {
            return Err(ConfigError::InvalidValue("redis_url cannot be empty".into()));
        }

        // Validate cache size
        if self.cdn.memory_cache_size == 0 {
            return Err(ConfigError::InvalidValue("memory_cache_size must be > 0".into()));
        }

        // Validate edge node has origin URL
        if self.server.node_type == NodeType::Edge && self.cdn.origin_url.is_none() {
            return Err(ConfigError::InvalidValue(
                "Edge nodes must have origin_url configured".into()
            ));
        }

        Ok(())
    }

    /// Get HTTP server address
    pub fn http_addr(&self) -> SocketAddr {
        format!("{}:{}", self.server.bind_addr, self.server.http_port)
            .parse()
            .expect("Invalid HTTP address")
    }

    /// Get RTMP server address
    pub fn rtmp_addr(&self) -> SocketAddr {
        format!("{}:{}", self.server.bind_addr, self.server.rtmp_port)
            .parse()
            .expect("Invalid RTMP address")
    }

    /// Check if this is an origin node
    pub fn is_origin(&self) -> bool {
        self.server.node_type == NodeType::Origin
    }

    /// Check if this is an edge node
    pub fn is_edge(&self) -> bool {
        self.server.node_type == NodeType::Edge
    }
}

impl ServerConfig {
    fn from_env() -> Self {
        Self {
            http_port: env_var_or("HTTP_PORT", "3000").parse().unwrap(),
            rtmp_port: env_var_or("RTMP_PORT", "1935").parse().unwrap(),
            bind_addr: env_var_or("BIND_ADDR", "0.0.0.0"),
            node_id: env_var_or("NODE_ID", &format!("node-{}", uuid::Uuid::new_v4())),
            node_type: match env_var_or("NODE_TYPE", "origin").as_str() {
                "edge" => NodeType::Edge,
                _ => NodeType::Origin,
            },
            location: env_var_or("NODE_LOCATION", "UNKNOWN"),
        }
    }
}

impl RedisConfig {
    fn from_env() -> Self {
        Self {
            url: env_var_or("REDIS_URL", "redis://127.0.0.1:6379"),
            pool_size: env_var_or("REDIS_POOL_SIZE", "10").parse().unwrap(),
            timeout_seconds: env_var_or("REDIS_TIMEOUT", "5").parse().unwrap(),
            cluster_mode: env_var_or("REDIS_CLUSTER", "false").parse().unwrap(),
            key_prefix: env_var_or("REDIS_KEY_PREFIX", "stream:"),
        }
    }
}

impl CdnConfig {
    fn from_env() -> Self {
        Self {
            memory_cache_size: env_var_or("CACHE_SIZE", "10000").parse().unwrap(),
            segment_ttl: env_var_or("SEGMENT_TTL", "300").parse().unwrap(),
            max_node_capacity: env_var_or("MAX_NODE_CAPACITY", "5000").parse().unwrap(),
            enable_edge_cache: env_var_or("ENABLE_EDGE_CACHE", "true").parse().unwrap(),
            origin_url: std::env::var("ORIGIN_URL").ok(),
            enable_stats: env_var_or("ENABLE_STATS", "true").parse().unwrap(),
            load_balancing_strategy: env_var_or("LOAD_BALANCING_STRATEGY", "least-connections"),
            heartbeat_timeout_seconds: env_var_or("HEARTBEAT_TIMEOUT", "30").parse().unwrap(),
            max_load_percentage: env_var_or("MAX_LOAD_PERCENTAGE", "90.0").parse().unwrap(),
        }
    }
}

impl StreamingConfig {
    fn from_env() -> Self {
        Self {
            hls_segment_duration: env_var_or("HLS_SEGMENT_DURATION", "6").parse().unwrap(),
            hls_playlist_size: env_var_or("HLS_PLAYLIST_SIZE", "10").parse().unwrap(),
            enable_dash: env_var_or("ENABLE_DASH", "true").parse().unwrap(),
            enable_webrtc: env_var_or("ENABLE_WEBRTC", "true").parse().unwrap(),
            dvr_window_seconds: env_var_or("DVR_WINDOW", "60").parse().unwrap(),
            default_codec: VideoCodec::H264,
            enable_abr: env_var_or("ENABLE_ABR", "true").parse().unwrap(),
            profiles: Self::default_profiles(),
        }
    }

    fn default_profiles() -> Vec<TranscodingProfile> {
        vec![
            TranscodingProfile {
                name: "360p".to_string(),
                width: 640,
                height: 360,
                bitrate_kbps: 1000,
                fps: 30,
            },
            TranscodingProfile {
                name: "720p".to_string(),
                width: 1280,
                height: 720,
                bitrate_kbps: 3000,
                fps: 30,
            },
            TranscodingProfile {
                name: "1080p".to_string(),
                width: 1920,
                height: 1080,
                bitrate_kbps: 6000,
                fps: 30,
            },
            TranscodingProfile {
                name: "2160p".to_string(),
                width: 3840,
                height: 2160,
                bitrate_kbps: 12000,
                fps: 30,
            },
        ]
    }
}

/// Configuration errors
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("Failed to read config file: {0}")]
    FileReadError(String),
    
    #[error("Failed to parse config: {0}")]
    ParseError(String),
    
    #[error("Invalid configuration value: {0}")]
    InvalidValue(String),
    
    #[error("Missing required field: {0}")]
    MissingField(String),
}

/// Helper function to get environment variable or default
fn env_var_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

// ============================================================================
// DEFAULT CONFIGURATION (for examples/testing)
// ============================================================================

impl Default for Config {
    fn default() -> Self {
        Self {
            server: ServerConfig::default(),
            redis: RedisConfig::default(),
            cdn: CdnConfig::default(),
            streaming: StreamingConfig::default(),
        }
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            http_port: 3000,
            rtmp_port: 1935,
            bind_addr: "0.0.0.0".to_string(),
            node_id: format!("node-{}", uuid::Uuid::new_v4()),
            node_type: NodeType::Origin,
            location: "UNKNOWN".to_string(),
        }
    }
}

impl Default for RedisConfig {
    fn default() -> Self {
        Self {
            url: "redis://127.0.0.1:6379".to_string(),
            pool_size: 10,
            timeout_seconds: 5,
            cluster_mode: false,
            key_prefix: "stream:".to_string(),
        }
    }
}

impl Default for CdnConfig {
    fn default() -> Self {
        Self {
            memory_cache_size: 10000,
            segment_ttl: 300,
            max_node_capacity: 5000,
            enable_edge_cache: true,
            origin_url: None,
            enable_stats: true,
            load_balancing_strategy: "least-connections".to_string(),
            heartbeat_timeout_seconds: 30,
            max_load_percentage: 90.0,
        }
    }
}

impl Default for StreamingConfig {
    fn default() -> Self {
        Self {
            hls_segment_duration: 6,
            hls_playlist_size: 10,
            enable_dash: true,
            enable_webrtc: true,
            dvr_window_seconds: 60,
            default_codec: VideoCodec::H264,
            enable_abr: true,
            profiles: vec![
                TranscodingProfile {
                    name: "360p".to_string(),
                    width: 640,
                    height: 360,
                    bitrate_kbps: 1000,
                    fps: 30,
                },
                TranscodingProfile {
                    name: "720p".to_string(),
                    width: 1280,
                    height: 720,
                    bitrate_kbps: 3000,
                    fps: 30,
                },
                TranscodingProfile {
                    name: "1080p".to_string(),
                    width: 1920,
                    height: 1080,
                    bitrate_kbps: 6000,
                    fps: 30,
                },
            ],
        }
    }
}

// ============================================================================
// TESTS
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert_eq!(config.server.http_port, 3000);
        assert_eq!(config.server.rtmp_port, 1935);
        assert_eq!(config.server.node_type, NodeType::Origin);
    }

    #[test]
    fn test_config_validation() {
        let mut config = Config::default();
        assert!(config.validate().is_ok());

        // Invalid port
        config.server.http_port = 0;
        assert!(config.validate().is_err());

        // Edge without origin
        config.server.http_port = 3000;
        config.server.node_type = NodeType::Edge;
        assert!(config.validate().is_err());

        // Edge with origin
        config.cdn.origin_url = Some("http://origin:3000".to_string());
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_addresses() {
        let config = Config::default();
        let http_addr = config.http_addr();
        assert_eq!(http_addr.port(), 3000);

        let rtmp_addr = config.rtmp_addr();
        assert_eq!(rtmp_addr.port(), 1935);
    }

    #[test]
    fn test_node_type_helpers() {
        let mut config = Config::default();
        
        config.server.node_type = NodeType::Origin;
        assert!(config.is_origin());
        assert!(!config.is_edge());

        config.server.node_type = NodeType::Edge;
        assert!(!config.is_origin());
        assert!(config.is_edge());
    }
}
pub mod streams;
pub mod hls;
pub mod dash;
pub mod websocket;
pub mod cdn;
pub mod metrics;
pub mod health;

// Re-export commonly used items
pub use streams::{create_stream, list_streams, get_stream, end_stream};
pub use hls::{hls_master_playlist, hls_media_playlist, hls_segment};
pub use dash::dash_manifest;
pub use websocket::{watch_stream, webrtc_signaling};
pub use cdn::{register_edge_node, select_edge_node, cache_stats};
pub use metrics::get_metrics;
pub use health::health_check;
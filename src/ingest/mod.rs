
pub mod rtmp;
pub mod transcoder;

// Optional advanced features
#[cfg(feature = "transcoder-pool")]
pub mod transcoder_pool;

#[cfg(feature = "transcoder-pool")]
pub mod segment_buffer;

pub use transcoder::{
    TranscoderService, 
    TranscoderError, 
    SimpleTranscoder,
    HardwareAcceleration,
    AccelType,
    TranscodingStats,
};

#[cfg(feature = "transcoder-pool")]
pub use transcoder_pool::{TranscoderPool, PoolStats};

#[cfg(feature = "transcoder-pool")]
pub use segment_buffer::SegmentBuffer;
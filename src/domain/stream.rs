use std::{collections::VecDeque, time::SystemTime};

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::domain::quality::StreamQuality;

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
    pub quality: StreamQuality,
}

#[derive(Debug, Clone)]
pub struct AdaptiveStream {
    pub quality: StreamQuality,
    pub bitrate_kbps: u32,
    pub resolution: (u32, u32),
    pub segments: VecDeque<VideoSegment>,
    pub max_segments: usize,
}


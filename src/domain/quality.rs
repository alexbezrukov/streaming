use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum StreamQuality {
    Low,    // 360p
    Medium, // 720p
    High,   // 1080p
    Ultra,  // 4K
}

impl StreamQuality {
    pub fn to_string(&self) -> &'static str {
        match self {
            StreamQuality::Low => "360p",
            StreamQuality::Medium => "720p",
            StreamQuality::High => "1080p",
            StreamQuality::Ultra => "2160p",
        }
    }

    pub fn bitrate_kbps(&self) -> u32 {
        match self {
            StreamQuality::Low => 1000,
            StreamQuality::Medium => 3000,
            StreamQuality::High => 6000,
            StreamQuality::Ultra => 12000,
        }
    }

    pub fn resolution(&self) -> (u32, u32) {
        match self {
            StreamQuality::Low => (640, 360),
            StreamQuality::Medium => (1280, 720),
            StreamQuality::High => (1920, 1080),
            StreamQuality::Ultra => (3840, 2160),
        }
    }
}

impl std::fmt::Display for StreamQuality {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_string())
    }
}
use std::collections::VecDeque;

use crate::domain::{StreamQuality, VideoSegment};

pub struct HLSManifestGenerator {
    target_duration: u32,
    playlist_type: String,
}

impl HLSManifestGenerator {
    pub fn new() -> Self {
        Self {
            target_duration: 6,
            playlist_type: "EVENT".to_string(),
        }
    }

    pub fn generate_master_playlist(&self, stream_id: &str, qualities: &[StreamQuality]) -> String {
        let mut m3u8 = String::from("#EXTM3U\n#EXT-X-VERSION:3\n\n");

        for quality in qualities {
            let (width, height) = match quality {
                StreamQuality::Low => (640, 360),
                StreamQuality::Medium => (1280, 720),
                StreamQuality::High => (1920, 1080),
                StreamQuality::Ultra => (3840, 2160),
            };

            let bandwidth = match quality {
                StreamQuality::Low => 1000000,
                StreamQuality::Medium => 3000000,
                StreamQuality::High => 6000000,
                StreamQuality::Ultra => 12000000,
            };

            m3u8.push_str(&format!(
                "#EXT-X-STREAM-INF:BANDWIDTH={},RESOLUTION={}x{},NAME=\"{}\"\n",
                bandwidth,
                width,
                height,
                quality.to_string()
            ));
            m3u8.push_str(&format!("{}/index.m3u8\n\n", quality.to_string()));
        }

        m3u8
    }

    pub fn generate_media_playlist(
        &self,
        stream_id: &str,
        quality: StreamQuality,
        segments: &VecDeque<VideoSegment>,
    ) -> String {
        let mut m3u8 = format!(
            "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:{}\n#EXT-X-MEDIA-SEQUENCE:{}\n\n",
            self.target_duration,
            segments.front().map(|s| s.sequence).unwrap_or(0)
        );

        for segment in segments {
            m3u8.push_str(&format!(
                "#EXTINF:{:.3},\n",
                segment.duration_ms as f32 / 1000.0
            ));
            m3u8.push_str(&format!("segment_{}.ts\n", segment.sequence));
        }

        m3u8
    }
}

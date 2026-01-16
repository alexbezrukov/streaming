use std::collections::{HashMap, VecDeque};

use crate::domain::{StreamQuality, VideoSegment};

pub struct DASHManifestGenerator;

impl DASHManifestGenerator {
    pub fn new() -> Self {
        Self
    }

    pub fn generate_mpd(
        &self,
        stream_id: &str,
        qualities: &[StreamQuality],
        segments: &HashMap<StreamQuality, VecDeque<VideoSegment>>,
    ) -> String {
        let mut mpd = String::from(r#"<?xml version="1.0" encoding="UTF-8"?>
<MPD xmlns="urn:mpeg:dash:schema:mpd:2011" type="dynamic" minBufferTime="PT2S">
  <Period id="0">
    <AdaptationSet mimeType="video/mp4" codecs="avc1.4d401f" startWithSAP="1">
"#);

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

            mpd.push_str(&format!(
                r#"      <Representation id="{}" bandwidth="{}" width="{}" height="{}">
        <SegmentTemplate media="{}/{}/segment_$Number$.m4s" startNumber="1"/>
      </Representation>
"#,
                quality.to_string(),
                bandwidth,
                width,
                height,
                stream_id,
                quality.to_string()
            ));
        }

        mpd.push_str(r#"    </AdaptationSet>
  </Period>
</MPD>"#);

        mpd
    }
}
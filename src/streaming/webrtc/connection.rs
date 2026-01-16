pub struct WebRTCConnection {
    pub peer_id: String,
    pub stream_id: String,
    pub ice_candidates: Vec<String>,
    pub sdp_offer: Option<String>,
    pub sdp_answer: Option<String>,
}

use std::collections::{HashMap, VecDeque};

use crate::domain::{StreamQuality, VideoSegment};



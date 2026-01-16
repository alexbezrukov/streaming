use std::{sync::Arc, time::{SystemTime, UNIX_EPOCH}};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use tokio::{
    sync::{RwLock as TokioRwLock},
};
use crate::streaming::webrtc::connection::WebRTCConnection;
use sha2::{Sha256, Digest};

#[derive(Deserialize, Serialize)]
pub struct WebRTCSignal {
    pub signal_type: String,
    pub payload: String,
}

pub struct WebRTCManager {
    connections: DashMap<String, Arc<TokioRwLock<WebRTCConnection>>>,
}

impl WebRTCManager {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            connections: DashMap::new(),
        })
    }

    pub async fn create_connection(&self, peer_id: String, stream_id: String) -> String {
        let connection = Arc::new(TokioRwLock::new(WebRTCConnection {
            peer_id: peer_id.clone(),
            stream_id,
            ice_candidates: Vec::new(),
            sdp_offer: None,
            sdp_answer: None,
        }));

        self.connections.insert(peer_id.clone(), connection);
        peer_id
    }

    pub async fn set_offer(&self, peer_id: &str, offer: String) -> Option<String> {
        if let Some(conn) = self.connections.get(peer_id) {
            let mut conn = conn.write().await;
            conn.sdp_offer = Some(offer);

            // Generate SDP answer (simplified - in production use webrtc crate)
            let answer = self.generate_sdp_answer(&conn.sdp_offer.as_ref().unwrap());
            conn.sdp_answer = Some(answer.clone());
            
            return Some(answer);
        }
        None
    }

    pub async fn add_ice_candidate(&self, peer_id: &str, candidate: String) {
        if let Some(conn) = self.connections.get(peer_id) {
            conn.write().await.ice_candidates.push(candidate);
        }
    }

    fn generate_sdp_answer(&self, offer: &str) -> String {
        // Simplified SDP answer generation
        // In production: use webrtc crate for proper negotiation
        format!(
            r#"v=0
o=- {} 2 IN IP4 127.0.0.1
s=-
t=0 0
a=group:BUNDLE 0
m=video 9 UDP/TLS/RTP/SAVPF 96
c=IN IP4 0.0.0.0
a=rtcp:9 IN IP4 0.0.0.0
a=ice-ufrag:{}
a=ice-pwd:{}
a=fingerprint:sha-256 {}
a=setup:active
a=mid:0
a=sendrecv
a=rtcp-mux
a=rtpmap:96 VP8/90000
"#,
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            uuid::Uuid::new_v4().to_string()[..8].to_string(),
            uuid::Uuid::new_v4().to_string(),
            hex::encode(Sha256::digest(offer.as_bytes()))
        )
    }
}
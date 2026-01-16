use axum::{extract::State, http::StatusCode, Json};
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};
use crate::state::app_state::AppState;

#[derive(Debug, Serialize, Deserialize)]
pub struct HealthResponse {
    pub status: HealthStatus,
    pub node_id: String,
    pub node_type: String,
    pub location: String,
    pub uptime_seconds: u64,
    pub version: String,
    pub redis_connected: bool,
    pub active_streams: usize,
    pub total_viewers: usize,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum HealthStatus {
    Healthy,
    Degraded,
    Unhealthy,
}

/// Health check endpoint
/// Returns current node status and basic metrics
pub async fn health_check(State(state): State<AppState>) -> (StatusCode, Json<HealthResponse>) {
    let config = &state.config;
    
    // Check Redis connection
    let redis_connected = check_redis_connection(&state).await;
    
    // Get basic metrics
    let active_streams = state.stream_manager.streams.len();
    let total_viewers: usize = state
        .stream_manager
        .viewer_counts
        .iter()
        .map(|entry| *entry.value())
        .sum();
    
    // Determine health status
    let status = if !redis_connected {
        HealthStatus::Unhealthy
    } else if total_viewers > config.cdn.max_node_capacity {
        HealthStatus::Degraded
    } else {
        HealthStatus::Healthy
    };
    
    let http_status = match status {
        HealthStatus::Healthy => StatusCode::OK,
        HealthStatus::Degraded => StatusCode::OK,
        HealthStatus::Unhealthy => StatusCode::SERVICE_UNAVAILABLE,
    };
    
    let response = HealthResponse {
        status,
        node_id: config.server.node_id.clone(),
        node_type: format!("{:?}", config.server.node_type).to_lowercase(),
        location: config.server.location.clone(),
        uptime_seconds: get_uptime_seconds(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        redis_connected,
        active_streams,
        total_viewers,
    };
    
    (http_status, Json(response))
}

/// Check if Redis is reachable
async fn check_redis_connection(state: &AppState) -> bool {
    if let Ok(mut conn) = state
        .stream_manager
        .edge_cache
        .redis_client
        .get_multiplexed_async_connection()
        .await
    {
        redis::cmd("PING")
            .query_async::<_, String>(&mut conn)
            .await
            .is_ok()
    } else {
        false
    }
}

/// Get server uptime in seconds
fn get_uptime_seconds() -> u64 {
    static START_TIME: std::sync::OnceLock<SystemTime> = std::sync::OnceLock::new();
    
    let start = START_TIME.get_or_init(|| SystemTime::now());
    
    SystemTime::now()
        .duration_since(*start)
        .unwrap_or_default()
        .as_secs()
}
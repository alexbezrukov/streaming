use axum::{Json, extract::State};
use serde::Serialize;
use crate::state::app_state::AppState;

#[derive(Serialize)]
pub struct SystemMetrics {
    pub active_streams: usize,
    pub total_viewers: usize,
    pub edge_nodes: usize,
    pub cache_hit_rate: f64,
    pub uptime_seconds: u64,
}

pub async fn get_metrics(State(state): State<AppState>) -> Json<SystemMetrics> {
    let active_streams = state.stream_manager.streams.len();
    let total_viewers: usize = state
        .stream_manager
        .viewer_counts
        .iter()
        .map(|entry| *entry.value())
        .sum();

    let cache_stats = state.stream_manager.edge_cache.get_stats();
    let total_hits: u64 = cache_stats.values().map(|s| s.hits).sum();
    let total_misses: u64 = cache_stats.values().map(|s| s.misses).sum();
    let cache_hit_rate = if total_hits + total_misses > 0 {
        total_hits as f64 / (total_hits + total_misses) as f64
    } else {
        0.0
    };

    Json(SystemMetrics {
        active_streams,
        total_viewers,
        edge_nodes: state.stream_manager.edge_nodes.len(),
        cache_hit_rate,
        uptime_seconds: 0,
    })
}
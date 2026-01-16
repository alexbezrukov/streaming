use axum::{
    extract::{Json, State},
    http::StatusCode,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::{
    cdn::cache::CacheStats,
    domain::edge_node::EdgeNode,
    state::app_state::AppState,
};

// ============================================================================
// REQUEST/RESPONSE TYPES
// ============================================================================

#[derive(Debug, Deserialize)]
pub struct RegisterEdgeNodeRequest {
    pub node_id: String,
    pub location: String,
    pub capacity: usize,
}

#[derive(Debug, Serialize)]
pub struct RegisterEdgeNodeResponse {
    pub success: bool,
    pub message: String,
    pub node_id: String,
}

#[derive(Debug, Deserialize)]
pub struct SelectEdgeNodeRequest {
    pub viewer_location: String,
}

#[derive(Debug, Serialize)]
pub struct SelectEdgeNodeResponse {
    pub node_id: String,
    pub location: String,
    pub endpoint: String,
    pub load_percentage: f64,
}

#[derive(Debug, Serialize)]
pub struct CacheStatsResponse {
    pub stats: HashMap<String, CacheStats>,
    pub total_hits: u64,
    pub total_misses: u64,
    pub hit_rate: f64,
}

// ============================================================================
// EDGE NODE MANAGEMENT
// ============================================================================

/// Register a new edge node
pub async fn register_edge_node(
    State(state): State<AppState>,
    Json(req): Json<RegisterEdgeNodeRequest>,
) -> Result<Json<RegisterEdgeNodeResponse>, StatusCode> {
    let node = EdgeNode::new(
        req.node_id.clone(),
        req.location.clone(),
        req.capacity,
    );

    state.stream_manager.register_edge_node(node);

    tracing::info!(
        "Registered edge node: {} at {} (capacity: {})",
        req.node_id,
        req.location,
        req.capacity
    );

    Ok(Json(RegisterEdgeNodeResponse {
        success: true,
        message: "Edge node registered successfully".to_string(),
        node_id: req.node_id,
    }))
}

/// Select best edge node for viewer
pub async fn select_edge_node(
    State(state): State<AppState>,
    Json(req): Json<SelectEdgeNodeRequest>,
) -> Result<Json<SelectEdgeNodeResponse>, StatusCode> {
    let node = state
        .stream_manager
        .select_edge_node(&req.viewer_location)
        .ok_or(StatusCode::SERVICE_UNAVAILABLE)?;

    let endpoint = format!("https://{}.cdn.example.com", node.node_id);
    let load_percentage = node.load_percentage();

    tracing::debug!(
        "Selected edge node {} for location {} (load: {:.1}%)",
        node.node_id,
        req.viewer_location,
        load_percentage
    );

    Ok(Json(SelectEdgeNodeResponse {
        node_id: node.node_id,
        location: node.location,
        endpoint,
        load_percentage,
    }))
}

// ============================================================================
// CACHE STATISTICS
// ============================================================================

/// Get cache statistics
pub async fn cache_stats(
    State(state): State<AppState>,
) -> Json<CacheStatsResponse> {
    let stats = state.stream_manager.get_cache_stats().await;
    
    let total_hits: u64 = stats.values().map(|s| s.hits).sum();
    let total_misses: u64 = stats.values().map(|s| s.misses).sum();
    let total_requests = total_hits + total_misses;
    
    let hit_rate = if total_requests > 0 {
        (total_hits as f64 / total_requests as f64) * 100.0
    } else {
        0.0
    };

    tracing::debug!(
        "Cache stats: hits={}, misses={}, hit_rate={:.2}%",
        total_hits,
        total_misses,
        hit_rate
    );

    Json(CacheStatsResponse {
        stats,
        total_hits,
        total_misses,
        hit_rate,
    })
}

/// Get detailed cache statistics for specific stream
pub async fn stream_cache_stats(
    State(state): State<AppState>,
    axum::extract::Path(stream_id): axum::extract::Path<String>,
) -> Result<Json<HashMap<String, CacheStats>>, StatusCode> {
    let all_stats = state.stream_manager.get_cache_stats().await;
    
    // Filter stats for this stream
    let stream_stats: HashMap<String, CacheStats> = all_stats
        .into_iter()
        .filter(|(key, _)| key.starts_with(&format!("segment:{}", stream_id)))
        .collect();

    if stream_stats.is_empty() {
        return Err(StatusCode::NOT_FOUND);
    }

    Ok(Json(stream_stats))
}

/// Clear cache for specific stream
pub async fn clear_stream_cache(
    State(state): State<AppState>,
    axum::extract::Path(stream_id): axum::extract::Path<String>,
) -> Result<Json<String>, StatusCode> {
    state
        .stream_manager
        .invalidate_cache(&stream_id)
        .await;

    tracing::info!("Cleared cache for stream: {}", stream_id);

    Ok(Json(format!("Cache cleared for stream: {}", stream_id)))
}

// ============================================================================
// EDGE NODE HEALTH
// ============================================================================

#[derive(Debug, Serialize)]
pub struct EdgeNodeHealthResponse {
    pub healthy_nodes: usize,
    pub total_nodes: usize,
    pub nodes: Vec<EdgeNodeStatus>,
}

#[derive(Debug, Serialize)]
pub struct EdgeNodeStatus {
    pub node_id: String,
    pub location: String,
    pub healthy: bool,
    pub load_percentage: f64,
    pub last_heartbeat_seconds: u64,
}

/// Get health status of all edge nodes
pub async fn edge_nodes_health(
    State(state): State<AppState>,
) -> Json<EdgeNodeHealthResponse> {
    let nodes = state.stream_manager.get_all_edge_nodes();
    let now = std::time::SystemTime::now();
    
    let nodes_status: Vec<EdgeNodeStatus> = nodes
        .iter()
        .map(|node| {
            let last_heartbeat_seconds = now
                .duration_since(node.last_heartbeat)
                .unwrap_or_default()
                .as_secs();
            
            // Node is healthy if heartbeat within last 30 seconds
            let healthy = last_heartbeat_seconds < 30;
            
            EdgeNodeStatus {
                node_id: node.node_id.clone(),
                location: node.location.clone(),
                healthy,
                load_percentage: node.load_percentage(),
                last_heartbeat_seconds,
            }
        })
        .collect();

    let healthy_count = nodes_status.iter().filter(|n| n.healthy).count();

    Json(EdgeNodeHealthResponse {
        healthy_nodes: healthy_count,
        total_nodes: nodes_status.len(),
        nodes: nodes_status,
    })
}

// ============================================================================
// CDN METRICS
// ============================================================================

#[derive(Debug, Serialize)]
pub struct CdnMetrics {
    pub total_edge_nodes: usize,
    pub healthy_edge_nodes: usize,
    pub total_cache_hits: u64,
    pub total_cache_misses: u64,
    pub cache_hit_rate: f64,
    pub total_bandwidth_mbps: f64,
    pub top_streams: Vec<StreamBandwidth>,
}

#[derive(Debug, Serialize)]
pub struct StreamBandwidth {
    pub stream_id: String,
    pub viewers: usize,
    pub bandwidth_mbps: f64,
}

/// Get overall CDN metrics
pub async fn cdn_metrics(
    State(state): State<AppState>,
) -> Json<CdnMetrics> {
    let nodes = state.stream_manager.get_all_edge_nodes();
    let now = std::time::SystemTime::now();
    
    let healthy_nodes = nodes
        .iter()
        .filter(|node| {
            now.duration_since(node.last_heartbeat)
                .unwrap_or_default()
                .as_secs() < 30
        })
        .count();

    let cache_stats = state.stream_manager.get_cache_stats().await;
    let total_hits: u64 = cache_stats.values().map(|s| s.hits).sum();
    let total_misses: u64 = cache_stats.values().map(|s| s.misses).sum();
    let total_requests = total_hits + total_misses;
    
    let hit_rate = if total_requests > 0 {
        (total_hits as f64 / total_requests as f64) * 100.0
    } else {
        0.0
    };

    // Calculate bandwidth (simplified)
    let streams = state.stream_manager.list_streams();
    let mut top_streams: Vec<StreamBandwidth> = streams
        .iter()
        .map(|stream| {
            let bandwidth_mbps = (stream.bitrate_kbps as f64 / 1000.0) * stream.viewers as f64;
            StreamBandwidth {
                stream_id: stream.stream_id.clone(),
                viewers: stream.viewers,
                bandwidth_mbps,
            }
        })
        .collect();
    
    top_streams.sort_by(|a, b| b.bandwidth_mbps.partial_cmp(&a.bandwidth_mbps).unwrap());
    top_streams.truncate(10);

    let total_bandwidth: f64 = top_streams.iter().map(|s| s.bandwidth_mbps).sum();

    Json(CdnMetrics {
        total_edge_nodes: nodes.len(),
        healthy_edge_nodes: healthy_nodes,
        total_cache_hits: total_hits,
        total_cache_misses: total_misses,
        cache_hit_rate: hit_rate,
        total_bandwidth_mbps: total_bandwidth,
        top_streams,
    })
}
use axum::{
    Router,
    routing::{delete, get, post},
};
use std::sync::Arc;
use tower_http::{
    compression::CompressionLayer,
    cors::CorsLayer,
    trace::{DefaultMakeSpan, DefaultOnResponse, TraceLayer},
};
use tracing::Level;

mod api;
mod cdn;
mod config;
mod domain;
mod ingest;
mod manager;
mod state;
mod streaming;

use config::settings::Config;
use manager::stream_manager::StreamManager;
use state::app_state::AppState;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_target(false)
        .with_thread_ids(true)
        .with_level(true)
        .init();

    // Load environment variables from .env file
    dotenvy::dotenv().ok();

    // Load configuration
    let config = load_config()?;

    // Validate configuration
    config.validate()?;

    tracing::info!(
        "Starting {} node: {}",
        if config.is_origin() { "origin" } else { "edge" },
        config.server.node_id
    );
    tracing::info!("Location: {}", config.server.location);
    tracing::info!("HTTP: {}", config.http_addr());
    tracing::info!("RTMP: {}", config.rtmp_addr());

    // Initialize stream manager
    let stream_manager = StreamManager::new(&config).await?;

    let state = AppState {
        stream_manager: stream_manager.clone(),
        config: Arc::new(config.clone()),
    };

    // Spawn RTMP ingest server (only for origin nodes)
    if config.is_origin() {
        let rtmp_state = state.clone();
        let rtmp_addr = config.rtmp_addr();

        tokio::spawn(async move {
            tracing::info!("Starting RTMP server on {}", rtmp_addr);

            match ingest::rtmp::handle_rtmp_ingest(rtmp_state, rtmp_addr).await {
                Ok(_) => {
                    tracing::info!("RTMP server stopped gracefully");
                }
                Err(e) => {
                    tracing::error!("RTMP server error: {}", e);
                    std::process::exit(1);
                }
            }
        });
    } else {
        tracing::info!("Running as edge node - RTMP server disabled");
    }

    // Spawn periodic tasks
    spawn_background_tasks(state.clone());

    // Build HTTP router
    let app = build_router(state);

    // Start HTTP server
    let http_addr = config.http_addr();
    tracing::info!("🚀 Server listening on {}", http_addr);

    let listener = tokio::net::TcpListener::bind(http_addr).await?;

    axum::serve(listener, app)
        .await
        .map_err(|e| format!("Server error: {}", e))?;

    Ok(())
}

/// Load configuration from file or environment
fn load_config() -> Result<Config, Box<dyn std::error::Error>> {
    // Try to load from config file first
    if let Ok(config_path) = std::env::var("CONFIG_FILE") {
        tracing::info!("Loading configuration from file: {}", config_path);
        return Ok(Config::load_from_file(&config_path)?);
    }

    // Fall back to environment variables
    tracing::info!("Loading configuration from environment variables");
    Ok(Config::load_from_env())
}

/// Build the HTTP router with all endpoints
fn build_router(state: AppState) -> Router {
    Router::new()
        // Health check
        .route("/health", get(api::health::health_check))
        // Stream management
        .route("/api/streams", post(api::streams::create_stream))
        .route("/api/streams", get(api::streams::list_streams))
        .route("/api/streams/:stream_id", get(api::streams::get_stream))
        .route("/api/streams/:stream_id", delete(api::streams::end_stream))
        // HLS endpoints
        // .route(
        //     "/hls/:stream_id/master.m3u8",
        //     get(api::hls::hls_master_playlist),
        // )
        .route(
            "/hls/:stream_id/:quality/index.m3u8",
            get(api::hls::hls_media_playlist),
        )
        // .route(
        //     "/hls/:stream_id/:quality/:segment",
        //     get(api::hls::hls_segment),
        // )
        .route(
            "/hls/:stream_id/master.m3u8",
            get(api::hls::hls_master_playlist),
        )
        .route(
            "/hls/:stream_id/:quality/:filename",
            get(api::hls::hls_segment),
        )
        // DASH endpoints
        .route(
            "/dash/:stream_id/manifest.mpd",
            get(api::dash::dash_manifest),
        )
        // WebSocket endpoints
        .route("/ws/:stream_id/watch", get(api::websocket::watch_stream))
        .route(
            "/ws/:stream_id/webrtc",
            get(api::websocket::webrtc_signaling),
        )
        .route("/ws/ingest/:stream_id", get(api::websocket::ingest_stream))
        // CDN management
        .route("/api/cdn/edge-nodes", post(api::cdn::register_edge_node))
        .route("/api/cdn/select-edge", post(api::cdn::select_edge_node))
        .route("/api/cdn/cache-stats", get(api::cdn::cache_stats))
        // Metrics & monitoring
        .route("/api/metrics", get(api::metrics::get_metrics))
        // .route("/api/metrics/cache", get(api::metrics::cache_metrics))
        // .route("/api/metrics/streams", get(api::metrics::stream_metrics))
        // Middleware
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(DefaultMakeSpan::new().level(Level::INFO))
                .on_response(DefaultOnResponse::new().level(Level::INFO)),
        )
        .layer(CorsLayer::permissive())
        .layer(CompressionLayer::new())
        // State
        .with_state(state)
}

/// Spawn background tasks
fn spawn_background_tasks(state: AppState) {
    let config = state.config.clone();

    // Cache cleanup task
    if config.cdn.enable_edge_cache {
        let state_clone = state.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));

            loop {
                interval.tick().await;

                // Cleanup expired cache entries
                if let Err(e) = state_clone.stream_manager.cleanup_cache().await {
                    tracing::error!("Cache cleanup error: {}", e);
                }
            }
        });
    }

    // Edge node heartbeat (for edge nodes)
    if config.is_edge() {
        let state_clone = state.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(10));

            loop {
                interval.tick().await;

                // Send heartbeat to origin
                if let Err(e) = send_heartbeat(&state_clone).await {
                    tracing::error!("Heartbeat error: {}", e);
                }
            }
        });
    }

    // Metrics collection
    if config.cdn.enable_stats {
        let state_clone = state.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));

            loop {
                interval.tick().await;

                // Collect and log metrics
                collect_metrics(&state_clone).await;
            }
        });
    }
}

/// Send heartbeat to origin server (for edge nodes)
async fn send_heartbeat(state: &AppState) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(origin_url) = &state.config.cdn.origin_url {
        let node_info = serde_json::json!({
            "node_id": state.config.server.node_id,
            "location": state.config.server.location,
            "capacity": state.config.cdn.max_node_capacity,
            "current_load": state.stream_manager.get_current_load().await,
        });

        let client = reqwest::Client::new();
        let url = format!("{}/api/cdn/heartbeat", origin_url);

        client.post(&url).json(&node_info).send().await?;

        tracing::debug!("Heartbeat sent to origin");
    }

    Ok(())
}

/// Collect and log metrics
async fn collect_metrics(state: &AppState) {
    let metrics = state.stream_manager.get_metrics().await;

    tracing::info!(
        "Metrics - Active streams: {}, Total viewers: {}, Cache hit rate: {:.2}%",
        metrics.active_streams,
        metrics.total_viewers,
        metrics.cache_hit_rate * 100.0
    );
}

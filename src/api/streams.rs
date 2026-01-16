use axum::{Json, extract::{Path, State}, http::StatusCode};
use serde::{Deserialize, Serialize};

use crate::{domain::StreamMetadata, state::app_state::AppState};

#[derive(Deserialize)]
pub struct CreateStreamRequest {
    pub broadcaster_id: String,
    pub title: String,
}

#[derive(Serialize)]
pub struct CreateStreamResponse {
    pub stream_id: String,
    pub rtmp_url: String,
    pub stream_key: String,
    pub hls_url: String,
    pub dash_url: String,
    pub webrtc_url: String,
}

pub async fn create_stream(
    State(state): State<AppState>,
    Json(req): Json<CreateStreamRequest>,
) -> Result<Json<CreateStreamResponse>, (StatusCode, String)> {
    let stream_id = uuid::Uuid::new_v4().to_string();
    
    state
        .stream_manager
        .create_stream(stream_id.clone(), req.broadcaster_id, req.title)
        .await
        .map_err(|e| (StatusCode::BAD_REQUEST, e))?;

    Ok(Json(CreateStreamResponse {
        stream_id: stream_id.clone(),
        rtmp_url: format!("rtmp://localhost:1935/live"),
        stream_key: stream_id.clone(),
        hls_url: format!("http://localhost:3000/hls/{}/master.m3u8", stream_id),
        dash_url: format!("http://localhost:3000/dash/{}/manifest.mpd", stream_id),
        webrtc_url: format!("ws://localhost:3000/webrtc/{}", stream_id),
    }))
}

pub async fn list_streams(State(state): State<AppState>) -> Json<Vec<StreamMetadata>> {
    Json(state.stream_manager.list_streams())
}

pub async fn get_stream(
    State(state): State<AppState>,
    Path(stream_id): Path<String>,
) -> Result<Json<StreamMetadata>, StatusCode> {
    state
        .stream_manager
        .get_stream_metadata(&stream_id)
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

pub async fn end_stream(
    State(state): State<AppState>,
    Path(stream_id): Path<String>,
) -> Result<Json<String>, (StatusCode, String)> {
    state
        .stream_manager
        .end_stream(&stream_id)
        .await
        .map_err(|e| (StatusCode::BAD_REQUEST, e))?;
    
    Ok(Json("Stream ended".to_string()))
}
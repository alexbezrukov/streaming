use axum::{
    extract::{
        Path, State,
    },
    response::{IntoResponse, Response},
    http::{header, StatusCode},
};
use crate::{domain::StreamQuality, state::app_state::AppState};

// HLS master playlist
pub async fn hls_master_playlist(
    State(state): State<AppState>,
    Path(stream_id): Path<String>,
) -> Result<Response, StatusCode> {
    let playlist = state
        .stream_manager
        .get_hls_master_playlist(&stream_id)
        .ok_or(StatusCode::NOT_FOUND)?;

    Ok((
        [(header::CONTENT_TYPE, "application/vnd.apple.mpegurl")],
        playlist,
    )
        .into_response())
}

// HLS media playlist
pub async fn hls_media_playlist(
    State(state): State<AppState>,
    Path((stream_id, quality_str)): Path<(String, String)>,
) -> Result<Response, StatusCode> {
    let quality = match quality_str.as_str() {
        "360p" => StreamQuality::Low,
        "720p" => StreamQuality::Medium,
        "1080p" => StreamQuality::High,
        "2160p" => StreamQuality::Ultra,
        _ => return Err(StatusCode::BAD_REQUEST),
    };

    let playlist = state
        .stream_manager
        .get_hls_media_playlist(&stream_id, quality)
        .ok_or(StatusCode::NOT_FOUND)?;

    Ok((
        [(header::CONTENT_TYPE, "application/vnd.apple.mpegurl")],
        playlist,
    )
        .into_response())
}

// HLS segment
pub async fn hls_segment(
    State(state): State<AppState>,
    Path((stream_id, quality_str, segment_name)): Path<(String, String, String)>,
) -> Result<Response, StatusCode> {
    let quality = match quality_str.as_str() {
        "360p" => StreamQuality::Low,
        "720p" => StreamQuality::Medium,
        "1080p" => StreamQuality::High,
        "2160p" => StreamQuality::Ultra,
        _ => return Err(StatusCode::BAD_REQUEST),
    };

    // Extract sequence number from segment_N.ts
    let sequence: u64 = segment_name
        .trim_start_matches("segment_")
        .trim_end_matches(".ts")
        .parse()
        .map_err(|_| StatusCode::BAD_REQUEST)?;

    let data = state
        .stream_manager
        .get_segment(&stream_id, quality, sequence)
        .await
        .ok_or(StatusCode::NOT_FOUND)?;

    Ok((
        [
            (header::CONTENT_TYPE, "video/mp2t"),
            (header::CACHE_CONTROL, "public, max-age=300"),
        ],
        data,
    )
        .into_response())
}
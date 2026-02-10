use crate::{domain::StreamQuality, state::app_state::AppState};
use axum::{
    extract::{Path, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};
use tokio::fs;

//// Serve master playlist
pub async fn hls_master_playlist(
    State(state): State<AppState>,
    Path(stream_id): Path<String>,
) -> Result<Response, StatusCode> {
    // Проверяем существование стрима
    if state
        .stream_manager
        .get_stream_metadata(&stream_id)
        .is_none()
    {
        return Err(StatusCode::NOT_FOUND);
    }

    let master_path = format!("./hls_output/{}/master.m3u8", stream_id);

    // Если master.m3u8 уже создан FFmpeg, отдаём его
    if let Ok(content) = fs::read_to_string(&master_path).await {
        return Ok((
            [
                (header::CONTENT_TYPE, "application/vnd.apple.mpegurl"),
                (header::CACHE_CONTROL, "no-cache"),
                (header::ACCESS_CONTROL_ALLOW_ORIGIN, "*"),
            ],
            content,
        )
            .into_response());
    }

    // Иначе генерируем простой master playlist
    let master_playlist = format!(
        "#EXTM3U\n\
         #EXT-X-VERSION:3\n\
         #EXT-X-STREAM-INF:BANDWIDTH=2800000,RESOLUTION=1280x720,NAME=\"720p\"\n\
         720p/index.m3u8\n"
    );

    Ok((
        [
            (header::CONTENT_TYPE, "application/vnd.apple.mpegurl"),
            (header::CACHE_CONTROL, "no-cache"),
            (header::ACCESS_CONTROL_ALLOW_ORIGIN, "*"),
        ],
        master_playlist,
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

/// Serve HLS segment files
pub async fn hls_segment(
    Path((stream_id, quality, filename)): Path<(String, String, String)>,
) -> Result<Response, StatusCode> {
    // Проверка безопасности пути
    if filename.contains("..") || filename.contains("/") {
        return Err(StatusCode::BAD_REQUEST);
    }

    let file_path = format!("./hls_output/{}/{}/{}", stream_id, quality, filename);

    tracing::debug!("Serving HLS file: {}", file_path);

    // Читаем файл
    let content = fs::read(&file_path).await.map_err(|e| {
        tracing::warn!("File not found: {} - {}", file_path, e);
        StatusCode::NOT_FOUND
    })?;

    // Определяем Content-Type
    let content_type = if filename.ends_with(".m3u8") {
        "application/vnd.apple.mpegurl"
    } else if filename.ends_with(".ts") {
        "video/MP2T"
    } else {
        "application/octet-stream"
    };

    Ok((
        [
            (header::CONTENT_TYPE, content_type),
            (header::CACHE_CONTROL, "no-cache"),
            (header::ACCESS_CONTROL_ALLOW_ORIGIN, "*"),
        ],
        content,
    )
        .into_response())
}

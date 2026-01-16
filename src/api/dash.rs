use axum::{
    extract::{
        Path, State,
    },
    response::{IntoResponse, Response},
    http::{header, StatusCode},
};

use crate::state::app_state::AppState;

// DASH manifest
pub async fn dash_manifest(
    State(state): State<AppState>,
    Path(stream_id): Path<String>,
) -> Result<Response, StatusCode> {
    let manifest = state
        .stream_manager
        .get_dash_manifest(&stream_id)
        .ok_or(StatusCode::NOT_FOUND)?;

    Ok((
        [(header::CONTENT_TYPE, "application/dash+xml")],
        manifest,
    )
        .into_response())
}
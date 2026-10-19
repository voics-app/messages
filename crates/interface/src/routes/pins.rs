//! Pin endpoints.

use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use contracts::MessageDto;

use crate::dto::dtos;
use crate::error::ApiError;
use crate::extract::{ApiPath, Caller, Id};
use crate::state::AppState;

pub async fn list(
    State(state): State<AppState>,
    Caller(caller): Caller,
    ApiPath(Id(channel_id)): ApiPath<Id>,
) -> Result<Json<Vec<MessageDto>>, ApiError> {
    let views = state.use_cases.list_pins.execute(channel_id, caller.user_id).await?;
    Ok(Json(dtos(&state, &views).await))
}

pub async fn pin(
    State(state): State<AppState>,
    Caller(caller): Caller,
    ApiPath((Id(channel_id), Id(message_id))): ApiPath<(Id, Id)>,
) -> Result<StatusCode, ApiError> {
    state
        .use_cases
        .pin
        .execute(channel_id, message_id, caller.user_id)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

pub async fn unpin(
    State(state): State<AppState>,
    Caller(caller): Caller,
    ApiPath((Id(channel_id), Id(message_id))): ApiPath<(Id, Id)>,
) -> Result<StatusCode, ApiError> {
    state
        .use_cases
        .unpin
        .execute(channel_id, message_id, caller.user_id)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

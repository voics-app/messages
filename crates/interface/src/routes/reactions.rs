//! Reaction endpoints.

use axum::extract::State;
use axum::http::StatusCode;
use discordy_messages_domain::Emoji;

use crate::error::ApiError;
use crate::extract::{ApiPath, Caller, Id};
use crate::state::AppState;

/// The `{emoji}` segment is percent-decoded by the router; `@me` is the only
/// supported user target.
type ReactionPath = (Id, Id, String);

pub async fn add(
    State(state): State<AppState>,
    Caller(caller): Caller,
    ApiPath((Id(channel_id), Id(message_id), emoji)): ApiPath<ReactionPath>,
) -> Result<StatusCode, ApiError> {
    let emoji = Emoji::parse(&emoji)?;
    state
        .use_cases
        .add_reaction
        .execute(channel_id, message_id, emoji, caller.user_id)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

pub async fn remove_own(
    State(state): State<AppState>,
    Caller(caller): Caller,
    ApiPath((Id(channel_id), Id(message_id), emoji)): ApiPath<ReactionPath>,
) -> Result<StatusCode, ApiError> {
    let emoji = Emoji::parse(&emoji)?;
    state
        .use_cases
        .remove_own_reaction
        .execute(channel_id, message_id, emoji, caller.user_id)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

pub async fn remove_all(
    State(state): State<AppState>,
    Caller(caller): Caller,
    ApiPath((Id(channel_id), Id(message_id))): ApiPath<(Id, Id)>,
) -> Result<StatusCode, ApiError> {
    state
        .use_cases
        .remove_all_reactions
        .execute(channel_id, message_id, caller.user_id)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

//! Reaction endpoints.

use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use contracts::AuthorDto;
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

pub async fn list_users(
    State(state): State<AppState>,
    Caller(caller): Caller,
    ApiPath((Id(channel_id), Id(message_id), emoji)): ApiPath<ReactionPath>,
) -> Result<Json<Vec<AuthorDto>>, ApiError> {
    let emoji = Emoji::parse(&emoji)?;
    let ids = state
        .use_cases
        .list_reaction_users
        .execute(channel_id, message_id, &emoji, caller.user_id)
        .await?;
    let profiles = if ids.is_empty() {
        Default::default()
    } else {
        state.users.lookup(&ids).await
    };
    let users = ids
        .iter()
        .map(|id| {
            let mut dto = AuthorDto {
                id: id.to_string(),
                username: None,
                discriminator: None,
                global_name: None,
                avatar: None,
                bot: None,
                system: None,
                public_flags: None,
            };
            if let Some(profile) = profiles.get(id) {
                dto.username = Some(profile.username.clone());
                dto.discriminator = Some("0".to_owned());
                dto.global_name = profile.global_name.clone();
                dto.avatar = profile.avatar.clone();
                dto.bot = Some(profile.bot);
                dto.system = Some(profile.system);
                dto.public_flags = Some(profile.flags);
            }
            dto
        })
        .collect();
    Ok(Json(users))
}

pub async fn remove_emoji(
    State(state): State<AppState>,
    Caller(caller): Caller,
    ApiPath((Id(channel_id), Id(message_id), emoji)): ApiPath<ReactionPath>,
) -> Result<StatusCode, ApiError> {
    let emoji = Emoji::parse(&emoji)?;
    state
        .use_cases
        .remove_reactions
        .remove_emoji(channel_id, message_id, emoji, caller.user_id)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

pub async fn remove_user(
    State(state): State<AppState>,
    Caller(caller): Caller,
    ApiPath((Id(channel_id), Id(message_id), emoji, Id(user_id))): ApiPath<(Id, Id, String, Id)>,
) -> Result<StatusCode, ApiError> {
    let emoji = Emoji::parse(&emoji)?;
    state
        .use_cases
        .remove_reactions
        .remove_user(channel_id, message_id, emoji, user_id, caller.user_id)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

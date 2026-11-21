use axum::{Json, extract::State};
use contracts::MessageDto;
use contracts::poll::{PollSnapshot, PollVersion, PollVoteRequest};
use discordy_messages_application::MessageView;
use discordy_messages_application::polls::Polls;
use discordy_messages_application::ports::{PollKey, VoteCommand};
use serde::Deserialize;

use crate::error::ApiError;
use crate::extract::{ApiJson, ApiPath, ApiQuery, Caller, Id};
use crate::poll_dto::{self, PollViewer};
use crate::state::AppState;

fn service(state: &AppState) -> Result<&Polls, ApiError> {
    state
        .use_cases
        .polls
        .as_ref()
        .ok_or_else(|| ApiError::Unavailable("poll storage is unavailable".into()))
}

fn revision(raw: &str) -> Result<i64, ApiError> {
    let version = raw
        .parse::<PollVersion>()
        .map_err(|_| ApiError::invalid("expected_vote_version", "must be a canonical decimal revision"))?;
    version
        .to_string()
        .parse()
        .map_err(|_| ApiError::invalid("expected_vote_version", "revision exceeds the supported range"))
}

pub async fn get(
    State(state): State<AppState>,
    Caller(actor): Caller,
    ApiPath((Id(channel_id), Id(message_id))): ApiPath<(Id, Id)>,
) -> Result<Json<PollSnapshot>, ApiError> {
    let view = service(&state)?.get(PollKey { channel_id, message_id }, actor).await?;
    Ok(Json(
        poll_dto::snapshot(&view).map_err(|error| ApiError::Unavailable(error.to_string()))?,
    ))
}

pub async fn select(
    State(state): State<AppState>,
    Caller(actor): Caller,
    ApiPath((Id(channel_id), Id(message_id), answer_id)): ApiPath<(Id, Id, u8)>,
    ApiJson(body): ApiJson<PollVoteRequest>,
) -> Result<Json<PollSnapshot>, ApiError> {
    let command = VoteCommand {
        key: PollKey { channel_id, message_id },
        user_id: actor.user_id,
        answer_id,
        selected: true,
        expected_vote_version: revision(&body.expected_vote_version.to_string())?,
    };
    let view = service(&state)?.vote(actor, command).await?;
    Ok(Json(
        poll_dto::snapshot(&view).map_err(|error| ApiError::Unavailable(error.to_string()))?,
    ))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoveQuery {
    expected_vote_version: String,
}

pub async fn remove(
    State(state): State<AppState>,
    Caller(actor): Caller,
    ApiPath((Id(channel_id), Id(message_id), answer_id)): ApiPath<(Id, Id, u8)>,
    ApiQuery(query): ApiQuery<RemoveQuery>,
) -> Result<Json<PollSnapshot>, ApiError> {
    let command = VoteCommand {
        key: PollKey { channel_id, message_id },
        user_id: actor.user_id,
        answer_id,
        selected: false,
        expected_vote_version: revision(&query.expected_vote_version)?,
    };
    let view = service(&state)?.vote(actor, command).await?;
    Ok(Json(
        poll_dto::snapshot(&view).map_err(|error| ApiError::Unavailable(error.to_string()))?,
    ))
}

pub async fn expire(
    State(state): State<AppState>,
    Caller(actor): Caller,
    ApiPath((Id(channel_id), Id(message_id))): ApiPath<(Id, Id)>,
) -> Result<Json<MessageDto>, ApiError> {
    let view = service(&state)?
        .expire(PollKey { channel_id, message_id }, actor)
        .await?;
    let mut message = crate::dto::single(&state, &MessageView::without_reactions(view.message)).await;
    if let Some(poll) = &mut message.poll {
        poll["results"] = poll_dto::results(&view.state.aggregate, PollViewer::Known(view.state.voter.as_ref()));
    }
    Ok(Json(message))
}

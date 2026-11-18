//! Author-private scheduling REST; ordinary message creation remains a separate path.
#[path = "scheduled_dto.rs"]
mod dto;

use axum::{
    Json,
    extract::{FromRequestParts, State},
    http::{StatusCode, header::AUTHORIZATION, request::Parts},
};
use contracts::scheduled::{ScheduledMessage, ScheduledMessageList};
use discordy_messages_application::{
    ports::scheduled::{ScheduleKey, ScheduleListQuery},
    scheduled::{ScheduleAddress, ScheduledMessages},
};
use discordy_messages_domain::{DomainError, UserId};
use serde::Deserialize;
use serde_json::Value;
use std::sync::Arc;

use crate::{
    error::ApiError,
    extract::{ApiJson, ApiPath, ApiQuery, Caller, Id},
    state::AppState,
};

pub struct ScheduleCaller(UserId);
impl FromRequestParts<AppState> for ScheduleCaller {
    type Rejection = ApiError;
    async fn from_request_parts(parts: &mut Parts, state: &AppState) -> Result<Self, ApiError> {
        let Caller(principal) = Caller::from_request_parts(parts, state).await?;
        let bearer = parts
            .headers
            .get(AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.starts_with("Bearer "));
        if principal.is_bot || !bearer {
            return Err(DomainError::MissingAccess.into());
        }
        Ok(Self(principal.user_id))
    }
}

fn service(state: &AppState) -> Result<&Arc<ScheduledMessages>, ApiError> {
    state
        .scheduled
        .as_ref()
        .ok_or_else(|| ApiError::Unavailable("scheduled messages are disabled".into()))
}

fn address(author_id: UserId, channel: Id, schedule: &str) -> Result<ScheduleAddress, ApiError> {
    Ok(ScheduleAddress {
        key: ScheduleKey {
            author_id,
            id: dto::id(schedule)?,
        },
        channel_id: channel.0,
    })
}

pub async fn create(
    State(state): State<AppState>,
    ScheduleCaller(author): ScheduleCaller,
    ApiPath(Id(channel)): ApiPath<Id>,
    ApiJson(value): ApiJson<Value>,
) -> Result<(StatusCode, Json<ScheduledMessage>), ApiError> {
    let accepted = service(&state)?.create(dto::create(value, author, channel)?).await?;
    Ok((
        if accepted.created {
            StatusCode::CREATED
        } else {
            StatusCode::OK
        },
        Json(dto::resource(accepted.job)?),
    ))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ListQuery {
    after: Option<String>,
    limit: Option<u16>,
}

pub async fn list(
    State(state): State<AppState>,
    ScheduleCaller(author): ScheduleCaller,
    ApiPath(Id(channel)): ApiPath<Id>,
    ApiQuery(query): ApiQuery<ListQuery>,
) -> Result<Json<ScheduledMessageList>, ApiError> {
    let page = service(&state)?
        .list(ScheduleListQuery {
            author_id: author,
            channel_id: channel,
            after: query.after.as_deref().map(dto::id).transpose()?,
            limit: query.limit.unwrap_or(50),
            now_ms: 0,
        })
        .await?;
    let next_after = page
        .next_after
        .map(|id| id.to_string().parse())
        .transpose()
        .map_err(|_| ApiError::Unavailable("invalid stored scheduled cursor".into()))?;
    Ok(Json(ScheduledMessageList {
        items: page.items.into_iter().map(dto::resource).collect::<Result<_, _>>()?,
        next_after,
    }))
}

pub async fn get(
    State(state): State<AppState>,
    ScheduleCaller(author): ScheduleCaller,
    ApiPath((channel, schedule)): ApiPath<(Id, String)>,
) -> Result<Json<ScheduledMessage>, ApiError> {
    Ok(Json(dto::resource(
        service(&state)?.get(address(author, channel, &schedule)?).await?,
    )?))
}

pub async fn edit(
    State(state): State<AppState>,
    ScheduleCaller(author): ScheduleCaller,
    ApiPath((channel, schedule)): ApiPath<(Id, String)>,
    ApiJson(value): ApiJson<Value>,
) -> Result<Json<ScheduledMessage>, ApiError> {
    let command = dto::edit(value, address(author, channel, &schedule)?)?;
    Ok(Json(dto::resource(service(&state)?.edit(command).await?)?))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CancelQuery {
    expected_revision: u64,
}

pub async fn cancel(
    State(state): State<AppState>,
    ScheduleCaller(author): ScheduleCaller,
    ApiPath((channel, schedule)): ApiPath<(Id, String)>,
    ApiQuery(query): ApiQuery<CancelQuery>,
) -> Result<Json<ScheduledMessage>, ApiError> {
    let job = service(&state)?
        .cancel(
            address(author, channel, &schedule)?,
            dto::revision(query.expected_revision)?,
        )
        .await?;
    Ok(Json(dto::resource(job)?))
}

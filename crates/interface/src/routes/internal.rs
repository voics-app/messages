//! Internal endpoints (pinned interface P4), guarded by `X-Internal-Token`.

use axum::Json;
use axum::extract::State;
use contracts::{
    AdminStatsResponse, InternalCreateMessageRequest, LastMessageIdResponse, MessageDto, MessagesByMode, ReadStateDto,
};
use discordy_messages_application::use_cases::CreateMessageCommand;

use crate::dto::{read_state_dto, view_dto};
use crate::error::ApiError;
use crate::extract::{ApiJson, ApiPath, Id, InternalCaller, parse_id};
use crate::routes::messages::json_texts;
use crate::state::AppState;

pub async fn create_message(
    State(state): State<AppState>,
    _: InternalCaller,
    ApiPath(Id(channel_id)): ApiPath<Id>,
    ApiJson(body): ApiJson<InternalCreateMessageRequest>,
) -> Result<Json<MessageDto>, ApiError> {
    let command = CreateMessageCommand {
        channel_id,
        author_id: parse_id("author_id", &body.author_id)?,
        content: body.content.unwrap_or_default(),
        embeds: json_texts(body.embeds),
        components: json_texts(body.components),
        ..CreateMessageCommand::default()
    };
    let view = state.use_cases.create_message.execute_internal(command).await?;
    Ok(Json(view_dto(&view, &state.urls)))
}

pub async fn last_message_id(
    State(state): State<AppState>,
    _: InternalCaller,
    ApiPath(Id(channel_id)): ApiPath<Id>,
) -> Result<Json<LastMessageIdResponse>, ApiError> {
    let last = state.use_cases.last_message_id.execute(channel_id).await?;
    Ok(Json(LastMessageIdResponse {
        last_message_id: last.map(|id| id.to_string()),
    }))
}

pub async fn read_states(
    State(state): State<AppState>,
    _: InternalCaller,
    ApiPath(Id(user_id)): ApiPath<Id>,
) -> Result<Json<Vec<ReadStateDto>>, ApiError> {
    let states = state.use_cases.read_states.execute(user_id).await?;
    Ok(Json(states.iter().map(read_state_dto).collect()))
}

pub async fn admin_stats(
    State(state): State<AppState>,
    _: InternalCaller,
) -> Result<Json<AdminStatsResponse>, ApiError> {
    let stats = state.use_cases.stats.execute().await?;
    Ok(Json(AdminStatsResponse {
        messages_24h: stats.messages_24h,
        messages_by_mode: MessagesByMode {
            standard: stats.standard,
            ephemeral_relayed: stats.ephemeral_relayed,
            p2p_signaled: stats.p2p_signaled,
        },
    }))
}

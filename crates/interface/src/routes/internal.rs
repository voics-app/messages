//! Internal endpoints (pinned interface P4), guarded by `X-Internal-Token`.

use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use contracts::{
    AdminStatsResponse, CallDto, InternalCreateMessageRequest, LastMessageIdResponse, MessageDto, MessagesByMode,
    ReadStateDto, UpdateCallRequest,
};
use discordy_messages_application::MessageView;
use discordy_messages_application::use_cases::{AttachmentReference, CreateMessageCommand};
use discordy_messages_domain::{Call, InteractionLink, MessageKind, WebhookAuthor};
use serde_json::{Value, json};

use crate::dto::{parse_rfc3339, read_state_dto, single};
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
    let command = internal_command(channel_id, body)?;
    let view = state.use_cases.create_message.execute_internal(command).await?;
    Ok(Json(single(&state, &view).await))
}

fn internal_command(
    channel_id: discordy_messages_domain::ChannelId,
    body: InternalCreateMessageRequest,
) -> Result<CreateMessageCommand, ApiError> {
    if body.poll.is_some() {
        return Err(ApiError::invalid(
            "poll",
            "internal/webhook/interaction poll creation is unsupported",
        ));
    }
    let author_id = parse_id("author_id", &body.author_id)?;
    let webhook = match body.webhook_id {
        Some(raw) => Some(WebhookAuthor {
            webhook_id: parse_id("webhook_id", &raw)?,
            username: body.username,
            avatar_url: body.avatar_url,
        }),
        None => None,
    };
    let interaction = match (body.interaction_id, body.application_id) {
        (Some(interaction), Some(application)) => Some(InteractionLink {
            interaction_id: parse_id("interaction_id", &interaction)?,
            application_id: parse_id("application_id", &application)?,
        }),
        (None, None) => None,
        _ => {
            return Err(ApiError::invalid(
                "application_id",
                "interaction_id and application_id go together",
            ));
        }
    };
    let reply_to = body
        .message_reference
        .map(|reference| parse_id("message_reference", &reference.message_id))
        .transpose()?;
    let target_user_ids = body
        .target_user_ids
        .unwrap_or_default()
        .iter()
        .map(|raw| parse_id("target_user_ids", raw))
        .collect::<Result<Vec<_>, _>>()?;
    let system_kind = match body.message_type {
        None | Some(0) => None,
        Some(code) => {
            let kind = MessageKind::from_code(code);
            if !kind.is_system() {
                return Err(ApiError::invalid("type", "unsupported system message type"));
            }
            Some(kind)
        }
    };
    let system_target = body
        .target_user_id
        .as_deref()
        .map(|raw| parse_id("target_user_id", raw))
        .transpose()?;
    let call = body.call.as_ref().map(call_from_dto).transpose()?;
    let thread_id = body
        .thread_id
        .as_deref()
        .map(|raw| parse_id("thread_id", raw))
        .transpose()?;
    Ok(CreateMessageCommand {
        channel_id,
        author_id,
        content: body.content.unwrap_or_default(),
        tts: body.tts,
        attachments: body
            .attachments
            .unwrap_or_default()
            .into_iter()
            .map(|a| AttachmentReference {
                uploaded_filename: a.uploaded_filename,
                description: a.description,
                flags: a.flags.unwrap_or(0),
            })
            .collect(),
        embeds: json_texts(body.embeds),
        components: json_texts(body.components),
        reply_to,
        suppress_reply_ping: false,
        flags: body.flags.unwrap_or(0),
        webhook,
        interaction,
        target_user_ids,
        system_kind,
        system_target,
        call,
        thread_id,
        ..CreateMessageCommand::default()
    })
}

fn call_from_dto(call: &CallDto) -> Result<Call, ApiError> {
    Ok(Call {
        participants: parse_ids("call.participants", &call.participants)?,
        ended_at_ms: call
            .ended_timestamp
            .as_deref()
            .map(|raw| parse_timestamp("call.ended_timestamp", raw))
            .transpose()?,
    })
}

fn parse_ids(field: &str, raw: &[String]) -> Result<Vec<discordy_messages_domain::UserId>, ApiError> {
    raw.iter().map(|id| parse_id(field, id)).collect()
}

fn parse_timestamp(field: &str, raw: &str) -> Result<u64, ApiError> {
    parse_rfc3339(raw).ok_or_else(|| ApiError::invalid(field, "must be an RFC 3339 timestamp"))
}

/// Records the end of a call on its system message.
pub async fn update_call(
    State(state): State<AppState>,
    _: InternalCaller,
    ApiPath((Id(channel_id), Id(message_id))): ApiPath<(Id, Id)>,
    ApiJson(body): ApiJson<UpdateCallRequest>,
) -> Result<Json<MessageDto>, ApiError> {
    let ended_at_ms = body
        .ended_timestamp
        .as_deref()
        .map(|raw| parse_timestamp("ended_timestamp", raw))
        .transpose()?;
    let participants = body
        .participants
        .as_deref()
        .map(|raw| parse_ids("participants", raw))
        .transpose()?;
    let view = state
        .use_cases
        .update_call
        .execute(channel_id, message_id, ended_at_ms, participants)
        .await?;
    Ok(Json(single(&state, &view).await))
}

pub async fn get_message(
    State(state): State<AppState>,
    _: InternalCaller,
    ApiPath((Id(channel_id), Id(message_id))): ApiPath<(Id, Id)>,
) -> Result<Json<MessageDto>, ApiError> {
    let message = state.use_cases.get_message.execute(channel_id, message_id).await?;
    let view = MessageView::without_reactions(message);
    Ok(Json(single(&state, &view).await))
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
) -> Result<Response, ApiError> {
    let states = state.use_cases.read_states.execute(user_id).await?;
    let body: Vec<ReadStateDto> = states.iter().map(read_state_dto).collect();
    Ok(([("x-read-state-versioned", "1")], Json(body)).into_response())
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

/// Starts a rebuild of the search index from stored history in the background
/// (`202`), or answers `409`-free `{"started": false}` when one is running.
pub async fn reindex_search(State(state): State<AppState>, _: InternalCaller) -> (StatusCode, Json<Value>) {
    let task_state = state.clone();
    let started = !task_state.use_cases.reindex.is_running();
    if started {
        tokio::spawn(async move {
            if let Err(error) = task_state.use_cases.reindex.execute().await {
                tracing::error!(%error, "search reindex failed");
            }
        });
    }
    (StatusCode::ACCEPTED, Json(json!({ "started": started })))
}

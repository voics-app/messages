//! Internal endpoints (pinned interface P4), guarded by `X-Internal-Token`.

use axum::Json;
use axum::extract::State;
use contracts::{
    AdminStatsResponse, InternalCreateMessageRequest, LastMessageIdResponse, MessageDto, MessagesByMode, ReadStateDto,
};
use discordy_messages_application::use_cases::{AttachmentReference, CreateMessageCommand};
use discordy_messages_domain::{InteractionLink, WebhookAuthor};

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
    let command = internal_command(channel_id, body)?;
    let view = state.use_cases.create_message.execute_internal(command).await?;
    Ok(Json(view_dto(&view, &state.urls)))
}

fn internal_command(
    channel_id: discordy_messages_domain::ChannelId,
    body: InternalCreateMessageRequest,
) -> Result<CreateMessageCommand, ApiError> {
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
    Ok(CreateMessageCommand {
        channel_id,
        author_id,
        content: body.content.unwrap_or_default(),
        attachments: body
            .attachments
            .unwrap_or_default()
            .into_iter()
            .map(|a| AttachmentReference {
                uploaded_filename: a.uploaded_filename,
            })
            .collect(),
        embeds: json_texts(body.embeds),
        components: json_texts(body.components),
        reply_to,
        flags: body.flags.unwrap_or(0),
        webhook,
        interaction,
        target_user_ids,
        ..CreateMessageCommand::default()
    })
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

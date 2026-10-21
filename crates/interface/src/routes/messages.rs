//! `/channels/{id}/messages` endpoints.

use axum::Json;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::StatusCode;
use contracts::{AckRequest, AckResponse, BulkDeleteRequest, CreateMessageRequest, EditMessageRequest, MessageDto};
use discordy_messages_application::use_cases::{AttachmentReference, CreateMessageCommand};
use discordy_messages_domain::{HistoryCursor, MessageEdit, PageLimit};
use serde::Deserialize;
use serde_json::Value;

use crate::dto::{dtos, single};
use crate::error::ApiError;
use crate::extract::{ApiJson, ApiPath, ApiQuery, Caller, Id, parse_id};
use crate::state::AppState;

#[derive(Debug, Deserialize)]
pub struct HistoryQuery {
    before: Option<String>,
    after: Option<String>,
    around: Option<String>,
    limit: Option<u32>,
}

impl HistoryQuery {
    fn cursor(&self) -> Result<HistoryCursor, ApiError> {
        let cursors = [&self.before, &self.after, &self.around];
        if cursors.iter().filter(|c| c.is_some()).count() > 1 {
            return Err(ApiError::invalid(
                "before",
                "only one of before, after and around may be set",
            ));
        }
        if let Some(raw) = &self.before {
            return Ok(HistoryCursor::Before(parse_id("before", raw)?));
        }
        if let Some(raw) = &self.after {
            return Ok(HistoryCursor::After(parse_id("after", raw)?));
        }
        if let Some(raw) = &self.around {
            return Ok(HistoryCursor::Around(parse_id("around", raw)?));
        }
        Ok(HistoryCursor::Latest)
    }
}

pub async fn history(
    State(state): State<AppState>,
    Caller(caller): Caller,
    ApiPath(Id(channel_id)): ApiPath<Id>,
    ApiQuery(query): ApiQuery<HistoryQuery>,
) -> Result<Json<Vec<MessageDto>>, ApiError> {
    let limit = PageLimit::parse(query.limit)?;
    let views = state
        .use_cases
        .history
        .execute(channel_id, caller.user_id, query.cursor()?, limit)
        .await?;
    Ok(Json(dtos(&state, &views).await))
}

/// Nonces are echoed back in the gateway event; clients send strings or integers.
fn nonce_text(nonce: Option<Value>) -> Result<Option<String>, ApiError> {
    match nonce {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(text)) => Ok(Some(text)),
        Some(Value::Number(number)) => Ok(Some(number.to_string())),
        Some(_) => Err(ApiError::invalid("nonce", "must be a string or an integer")),
    }
}

pub(crate) fn json_texts(values: Option<Vec<Value>>) -> Vec<String> {
    values.unwrap_or_default().iter().map(Value::to_string).collect()
}

pub async fn create(
    State(state): State<AppState>,
    Caller(caller): Caller,
    ApiPath(Id(channel_id)): ApiPath<Id>,
    ApiJson(body): ApiJson<CreateMessageRequest>,
) -> Result<Json<MessageDto>, ApiError> {
    let reply_to = body
        .message_reference
        .map(|reference| parse_id("message_reference", &reference.message_id))
        .transpose()?;
    let command = CreateMessageCommand {
        channel_id,
        author_id: caller.user_id,
        content: body.content.unwrap_or_default(),
        nonce: nonce_text(body.nonce)?,
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
        flags: 0,
        webhook: None,
        interaction: None,
        target_user_ids: Vec::new(),
        system_kind: None,
        system_target: None,
    };
    let view = state.use_cases.create_message.execute(command).await?;
    Ok(Json(single(&state, &view).await))
}

pub async fn edit(
    State(state): State<AppState>,
    Caller(caller): Caller,
    ApiPath((Id(channel_id), Id(message_id))): ApiPath<(Id, Id)>,
    ApiJson(body): ApiJson<EditMessageRequest>,
) -> Result<Json<MessageDto>, ApiError> {
    let edit = MessageEdit {
        content: body.content,
        embeds: body.embeds.map(|embeds| embeds.iter().map(Value::to_string).collect()),
    };
    let view = state
        .use_cases
        .edit_message
        .execute(channel_id, message_id, caller.user_id, edit)
        .await?;
    Ok(Json(single(&state, &view).await))
}

pub async fn delete(
    State(state): State<AppState>,
    Caller(caller): Caller,
    ApiPath((Id(channel_id), Id(message_id))): ApiPath<(Id, Id)>,
) -> Result<StatusCode, ApiError> {
    state
        .use_cases
        .delete_message
        .execute(channel_id, message_id, caller.user_id)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

pub async fn bulk_delete(
    State(state): State<AppState>,
    Caller(caller): Caller,
    ApiPath(Id(channel_id)): ApiPath<Id>,
    ApiJson(body): ApiJson<BulkDeleteRequest>,
) -> Result<StatusCode, ApiError> {
    let ids = body
        .messages
        .iter()
        .map(|raw| parse_id("messages", raw))
        .collect::<Result<Vec<_>, _>>()?;
    state
        .use_cases
        .bulk_delete
        .execute(channel_id, ids, caller.user_id)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

pub async fn acknowledge(
    State(state): State<AppState>,
    Caller(caller): Caller,
    ApiPath((Id(channel_id), Id(message_id))): ApiPath<(Id, Id)>,
    body: Bytes,
) -> Result<Json<AckResponse>, ApiError> {
    let request: AckRequest = if body.is_empty() {
        AckRequest::default()
    } else {
        serde_json::from_slice(&body).map_err(|e| ApiError::invalid("body", &e.to_string()))?
    };
    if request.manual {
        state
            .use_cases
            .acknowledge
            .execute_manual(channel_id, message_id, caller.user_id, request.mention_count)
            .await?;
    } else {
        state
            .use_cases
            .acknowledge
            .execute(channel_id, message_id, caller.user_id)
            .await?;
    }
    Ok(Json(AckResponse { token: None }))
}

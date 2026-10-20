//! `POST /channels/{id}/attachments`: presigned upload slots.

use axum::Json;
use axum::extract::State;
use contracts::{
    RefreshUrlsRequest, RefreshUrlsResponse, RefreshedUrlDto, UploadRequestBody, UploadSlotDto, UploadSlotsResponse,
};
use discordy_messages_application::use_cases::UploadRequest;
use serde_json::Value;

use crate::error::ApiError;
use crate::extract::{ApiJson, ApiPath, Caller, Id};
use crate::state::AppState;

/// The client's correlation id is echoed back as text; a missing one falls
/// back to the file's position.
fn client_id(id: Option<Value>, position: usize) -> String {
    match id {
        Some(Value::String(text)) => text,
        Some(Value::Number(number)) => number.to_string(),
        _ => position.to_string(),
    }
}

pub async fn create_slots(
    State(state): State<AppState>,
    Caller(caller): Caller,
    ApiPath(Id(channel_id)): ApiPath<Id>,
    ApiJson(body): ApiJson<UploadRequestBody>,
) -> Result<Json<UploadSlotsResponse>, ApiError> {
    let files = body
        .files
        .into_iter()
        .enumerate()
        .map(|(position, file)| UploadRequest {
            client_id: client_id(file.id, position),
            filename: file.filename,
            file_size: file.file_size,
        })
        .collect();
    let slots = state
        .use_cases
        .upload_slots
        .execute(channel_id, caller.user_id, files)
        .await?;
    Ok(Json(UploadSlotsResponse {
        attachments: slots
            .into_iter()
            .map(|slot| UploadSlotDto {
                id: slot.client_id,
                upload_url: slot.upload_url,
                upload_filename: slot.upload_filename,
            })
            .collect(),
    }))
}

/// `POST /channels/{id}/attachments/refresh-urls`: fresh signed URLs for
/// attachments of this channel, given their object keys or old URLs. Ephemeral
/// attachments are not stored with any message, so recipients obtain their
/// download links here; expired links of stored messages can use it too.
pub async fn refresh_urls(
    State(state): State<AppState>,
    Caller(caller): Caller,
    ApiPath(Id(channel_id)): ApiPath<Id>,
    ApiJson(body): ApiJson<RefreshUrlsRequest>,
) -> Result<Json<RefreshUrlsResponse>, ApiError> {
    let resolved = state
        .use_cases
        .refresh_urls
        .execute(channel_id, caller.user_id, body.attachment_urls)
        .await?;
    Ok(Json(RefreshUrlsResponse {
        refreshed_urls: resolved
            .into_iter()
            .map(|(original, key)| RefreshedUrlDto {
                refreshed: state.urls.url_for(&key),
                original,
            })
            .collect(),
    }))
}

//! `GET /attachments/{channel}/{attachment}/{filename}?ex&is&hm`: the signed
//! download endpoint behind the CDN host. The bucket itself is private; this
//! route is the only way to read an object, and only with a valid signature.

use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::header::{
    ACCEPT_RANGES, CACHE_CONTROL, CONTENT_DISPOSITION, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_SECURITY_POLICY,
    CONTENT_TYPE, ETAG, RANGE, X_CONTENT_TYPE_OPTIONS,
};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use discordy_messages_domain::{AttachmentFilename, Snowflake, UploadKey};
use serde::Deserialize;

use crate::attachment_urls::{SignedParams, VerifyError, encode_segment};
use crate::error::ApiError;
use crate::state::AppState;

#[derive(Deserialize)]
pub struct SignatureQuery {
    ex: Option<String>,
    is: Option<String>,
    hm: Option<String>,
}

fn refusal(status: StatusCode, message: &'static str) -> Response {
    (status, message).into_response()
}

pub async fn download(
    State(state): State<AppState>,
    Path((channel, attachment, filename)): Path<(String, String, String)>,
    Query(query): Query<SignatureQuery>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let verified =
        SignedParams::parse(query.ex.as_deref(), query.is.as_deref(), query.hm.as_deref()).and_then(|params| {
            state
                .urls
                .verify(&channel, &attachment, &filename, &params)
                .map(|()| params)
        });
    let params = match verified {
        Ok(params) => params,
        Err(VerifyError::Expired) => return Ok(refusal(StatusCode::GONE, "This link has expired.")),
        Err(VerifyError::Malformed | VerifyError::BadSignature) => {
            return Ok(refusal(StatusCode::FORBIDDEN, "Invalid link signature."));
        }
    };

    // The signature covers the path, so these only fail for hand-signed junk.
    let (Ok(channel_id), Ok(attachment_id), Ok(name)) = (
        channel.parse::<Snowflake>(),
        attachment.parse::<Snowflake>(),
        AttachmentFilename::parse(&filename),
    ) else {
        return Ok(refusal(StatusCode::NOT_FOUND, "Not found."));
    };
    if name.as_str() != filename {
        return Ok(refusal(StatusCode::NOT_FOUND, "Not found."));
    }
    let key = UploadKey::new(channel_id, attachment_id, name);
    let range = headers.get(RANGE).and_then(|v| v.to_str().ok());
    let Some(object) = state
        .storage
        .open(&key, range)
        .await
        .map_err(|error| ApiError::Unavailable(error.to_string()))?
    else {
        return Ok(refusal(StatusCode::NOT_FOUND, "Not found."));
    };

    let content_type = object
        .content_type
        .clone()
        .unwrap_or_else(|| "application/octet-stream".to_owned());
    let renders_inline = ["image/", "video/", "audio/"]
        .iter()
        .any(|p| content_type.starts_with(p))
        && !content_type.starts_with("image/svg");
    let disposition = format!(
        "{}; filename*=UTF-8''{}",
        if renders_inline { "inline" } else { "attachment" },
        encode_segment(&filename)
    );
    let remaining = params.expires.saturating_sub(state.urls.now_s());

    let mut response = Response::builder()
        .status(StatusCode::from_u16(object.status).unwrap_or(StatusCode::OK))
        .header(CONTENT_TYPE, content_type)
        .header(CONTENT_DISPOSITION, disposition)
        .header(ACCEPT_RANGES, "bytes")
        .header(CACHE_CONTROL, format!("private, max-age={remaining}"))
        .header(X_CONTENT_TYPE_OPTIONS, "nosniff")
        .header(CONTENT_SECURITY_POLICY, "sandbox; default-src 'none'");
    if let Some(length) = object.content_length {
        response = response.header(CONTENT_LENGTH, length);
    }
    if let Some(range) = &object.content_range {
        response = response.header(CONTENT_RANGE, range);
    }
    if let Some(etag) = &object.etag {
        response = response.header(ETAG, etag);
    }
    Ok(response
        .body(Body::from_stream(object.body))
        .unwrap_or_else(|_| refusal(StatusCode::INTERNAL_SERVER_ERROR, "Internal error.")))
}

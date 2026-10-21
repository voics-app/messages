//! `GET /external/{token}?ex&is&hm`: the media proxy. The token is the
//! base64url of a remote image or video URL; the signature (same HMAC key as
//! attachments, own domain tag) proves this service issued it, so the endpoint
//! is not an open proxy. The upstream fetch has the unfurler's SSRF guard and
//! size limits.

use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::header::{
    ACCEPT_RANGES, CACHE_CONTROL, CONTENT_DISPOSITION, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_SECURITY_POLICY,
    CONTENT_TYPE, RANGE, X_CONTENT_TYPE_OPTIONS,
};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use discordy_messages_application::ports::MediaError;
use serde::Deserialize;

use crate::attachment_urls::{SignedParams, VerifyError};
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

pub async fn proxy(
    State(state): State<AppState>,
    Path(token): Path<String>,
    Query(query): Query<SignatureQuery>,
    headers: HeaderMap,
) -> Response {
    let verified = SignedParams::parse(query.ex.as_deref(), query.is.as_deref(), query.hm.as_deref())
        .and_then(|params| state.urls.verify_proxy(&token, &params).map(|remote| (remote, params)));
    let (remote, params) = match verified {
        Ok(ok) => ok,
        Err(VerifyError::Expired) => return refusal(StatusCode::GONE, "This link has expired."),
        Err(VerifyError::Malformed | VerifyError::BadSignature) => {
            return refusal(StatusCode::FORBIDDEN, "Invalid link signature.");
        }
    };
    let range = headers.get(RANGE).and_then(|v| v.to_str().ok());
    let media = match state.media.open(&remote, range).await {
        Ok(media) => media,
        Err(MediaError::Forbidden) => return refusal(StatusCode::FORBIDDEN, "This address cannot be proxied."),
        Err(MediaError::UnsupportedType) => {
            return refusal(
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "Only images and videos are proxied.",
            );
        }
        Err(MediaError::TooLarge) => return refusal(StatusCode::PAYLOAD_TOO_LARGE, "The file is too large."),
        Err(MediaError::Unavailable(_)) => return refusal(StatusCode::BAD_GATEWAY, "The remote site is unavailable."),
    };
    let mut response = Response::builder()
        .status(StatusCode::from_u16(media.status).unwrap_or(StatusCode::OK))
        .header(CONTENT_TYPE, media.content_type)
        .header(CONTENT_DISPOSITION, "inline")
        .header(ACCEPT_RANGES, "bytes")
        .header(
            CACHE_CONTROL,
            format!("private, max-age={}", state.urls.remaining_s(&params)),
        )
        .header(X_CONTENT_TYPE_OPTIONS, "nosniff")
        .header(CONTENT_SECURITY_POLICY, "sandbox; default-src 'none'");
    if let Some(length) = media.content_length {
        response = response.header(CONTENT_LENGTH, length);
    }
    if let Some(range) = &media.content_range {
        response = response.header(CONTENT_RANGE, range);
    }
    response
        .body(Body::from_stream(media.body))
        .unwrap_or_else(|_| refusal(StatusCode::INTERNAL_SERVER_ERROR, "Internal error."))
}

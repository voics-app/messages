//! Request extractors that fail with the Discord-style error envelope.

use axum::extract::{FromRequest, FromRequestParts, Path, Query, Request};
use axum::http::header::AUTHORIZATION;
use axum::http::request::Parts;
use discordy_messages_application::ports::Principal;
use discordy_messages_domain::Snowflake;
use serde::Deserialize;
use serde::de::DeserializeOwned;

use crate::error::ApiError;
use crate::state::AppState;

pub const INTERNAL_TOKEN_HEADER: &str = "x-internal-token";

/// The authenticated caller of a public endpoint.
pub struct Caller(pub Principal);

impl FromRequestParts<AppState> for Caller {
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, state: &AppState) -> Result<Self, ApiError> {
        let header = parts
            .headers
            .get(AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .ok_or(ApiError::Unauthorized)?;
        Ok(Self(state.authenticator.authenticate(header).await?))
    }
}

/// Proof that the request carried the shared internal token.
pub struct InternalCaller;

impl FromRequestParts<AppState> for InternalCaller {
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, state: &AppState) -> Result<Self, ApiError> {
        let presented = parts
            .headers
            .get(INTERNAL_TOKEN_HEADER)
            .and_then(|value| value.to_str().ok())
            .ok_or(ApiError::Unauthorized)?;
        if state.internal_token.matches(presented) {
            Ok(Self)
        } else {
            Err(ApiError::Unauthorized)
        }
    }
}

/// A snowflake path or query value, validated while deserializing.
#[derive(Debug, Clone, Copy)]
pub struct Id(pub Snowflake);

impl<'de> Deserialize<'de> for Id {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(deserializer)?;
        raw.parse().map(Id).map_err(serde::de::Error::custom)
    }
}

pub struct ApiPath<T>(pub T);

impl<T: DeserializeOwned + Send> FromRequestParts<AppState> for ApiPath<T> {
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, state: &AppState) -> Result<Self, ApiError> {
        Path::<T>::from_request_parts(parts, state)
            .await
            .map(|Path(value)| Self(value))
            .map_err(|_| ApiError::invalid("id", "must be a decimal snowflake"))
    }
}

pub struct ApiQuery<T>(pub T);

impl<T: DeserializeOwned> FromRequestParts<AppState> for ApiQuery<T> {
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, state: &AppState) -> Result<Self, ApiError> {
        Query::<T>::from_request_parts(parts, state)
            .await
            .map(|Query(value)| Self(value))
            .map_err(|error| ApiError::invalid("query", &error.body_text()))
    }
}

pub struct ApiJson<T>(pub T);

impl<T: DeserializeOwned> FromRequest<AppState> for ApiJson<T> {
    type Rejection = ApiError;

    async fn from_request(request: Request, state: &AppState) -> Result<Self, ApiError> {
        axum::Json::<T>::from_request(request, state)
            .await
            .map(|axum::Json(value)| Self(value))
            .map_err(|error| ApiError::invalid("body", &error.body_text()))
    }
}

/// Parses an optional decimal-string snowflake from a request body.
pub fn parse_id(field: &str, raw: &str) -> Result<Snowflake, ApiError> {
    raw.parse()
        .map_err(|_| ApiError::invalid(field, "must be a decimal snowflake"))
}

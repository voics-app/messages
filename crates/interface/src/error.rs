//! HTTP error mapping. Every failure is rendered as the Discord-style
//! `{code, message, errors?}` envelope.

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use contracts::{ErrorBody, error_code};
use discordy_messages_application::AppError;
use discordy_messages_application::ports::AuthError;
use discordy_messages_domain::{DomainError, FieldError};
use serde_json::{Value, json};

#[derive(Debug)]
pub enum ApiError {
    App(AppError),
    Unauthorized,
    Unavailable(String),
}

impl ApiError {
    /// A malformed request (bad path id, query or body).
    pub fn invalid(field: &str, reason: &str) -> Self {
        Self::App(DomainError::invalid(field, reason).into())
    }
}

impl From<AppError> for ApiError {
    fn from(error: AppError) -> Self {
        Self::App(error)
    }
}

impl From<DomainError> for ApiError {
    fn from(error: DomainError) -> Self {
        Self::App(error.into())
    }
}

impl From<AuthError> for ApiError {
    fn from(error: AuthError) -> Self {
        match error {
            AuthError::Invalid => Self::Unauthorized,
            AuthError::Unavailable(port) => Self::Unavailable(port.to_string()),
        }
    }
}

/// Discord's nested field-error shape: `{field: {_errors: [{code, message}]}}`.
fn field_errors(errors: &[FieldError]) -> Value {
    let mut map = serde_json::Map::new();
    for error in errors {
        let entry = map
            .entry(error.field.clone())
            .or_insert_with(|| json!({ "_errors": [] }));
        if let Some(list) = entry["_errors"].as_array_mut() {
            list.push(json!({ "code": "INVALID", "message": error.reason }));
        }
    }
    Value::Object(map)
}

fn domain_status(error: &DomainError) -> (StatusCode, u32, Option<Value>) {
    match error {
        DomainError::InvalidForm(fields) => (
            StatusCode::BAD_REQUEST,
            error_code::INVALID_FORM_BODY,
            Some(field_errors(fields)),
        ),
        DomainError::ScheduleConflict(fields) => (
            StatusCode::CONFLICT,
            error_code::INVALID_FORM_BODY,
            Some(field_errors(fields)),
        ),
        DomainError::UnknownSchedule => (StatusCode::NOT_FOUND, error_code::UNKNOWN_MESSAGE, None),
        DomainError::MissingAccess => (StatusCode::FORBIDDEN, error_code::MISSING_ACCESS, None),
        DomainError::MissingPermissions => (StatusCode::FORBIDDEN, error_code::MISSING_PERMISSIONS, None),
        DomainError::DeliveryModeConflict(_) => (StatusCode::CONFLICT, error_code::DELIVERY_MODE_CONFLICT, None),
        DomainError::UnknownChannel => (StatusCode::NOT_FOUND, error_code::UNKNOWN_CHANNEL, None),
        DomainError::UnknownMessage => (StatusCode::NOT_FOUND, error_code::UNKNOWN_MESSAGE, None),
        DomainError::MaxPinsReached => (StatusCode::BAD_REQUEST, error_code::MAX_PINS, None),
    }
}

fn unavailable(detail: &str) -> (StatusCode, u32, String, Option<Value>) {
    tracing::error!(error = %detail, "dependency failure");
    (
        StatusCode::SERVICE_UNAVAILABLE,
        error_code::SERVICE_UNAVAILABLE,
        "Service temporarily unavailable".to_owned(),
        None,
    )
}

fn poll_error(error: discordy_messages_domain::poll::PollError) -> (StatusCode, u32, String, Option<Value>) {
    use discordy_messages_domain::poll::PollError;
    let field = match &error {
        PollError::Invalid(error) => {
            let (status, code, fields) = domain_status(error);
            return (status, code, error.to_string(), fields);
        }
        PollError::Deleted => {
            return (
                StatusCode::NOT_FOUND,
                error_code::UNKNOWN_MESSAGE,
                "Unknown message".into(),
                None,
            );
        }
        PollError::Corrupt | PollError::Exhausted => return unavailable(&error.to_string()),
        PollError::Closed => "poll",
        PollError::Conflict => "expected_vote_version",
        PollError::Capacity => "poll.participants",
    };
    let fields = field_errors(&[FieldError {
        field: field.into(),
        reason: error.to_string(),
    }]);
    (
        StatusCode::CONFLICT,
        error_code::INVALID_FORM_BODY,
        error.to_string(),
        Some(fields),
    )
}

/// Discord's 429 body, plus the `Retry-After` header (whole seconds, rounded up).
fn rate_limited(retry_after_ms: u64) -> Response {
    let seconds = retry_after_ms.div_ceil(1000).max(1);
    let body = json!({
        "code": 0,
        "message": "You are being rate limited.",
        "retry_after": retry_after_ms as f64 / 1000.0,
        "global": false,
    });
    (
        StatusCode::TOO_MANY_REQUESTS,
        [(axum::http::header::RETRY_AFTER, seconds.to_string())],
        Json(body),
    )
        .into_response()
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, code, message, errors) = match self {
            Self::App(AppError::Poll(error)) => poll_error(error),
            Self::App(AppError::Domain(error)) => {
                let (status, code, errors) = domain_status(&error);
                (status, code, error.to_string(), errors)
            }
            Self::App(AppError::Unavailable(port)) => unavailable(&port.to_string()),
            Self::App(AppError::RateLimited { retry_after_ms }) => return rate_limited(retry_after_ms),
            Self::Unavailable(detail) => unavailable(&detail),
            Self::Unauthorized => (
                StatusCode::UNAUTHORIZED,
                error_code::UNAUTHORIZED,
                "401: Unauthorized".to_owned(),
                None,
            ),
        };
        (status, Json(ErrorBody { code, message, errors })).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use discordy_messages_application::PortError;
    use discordy_messages_domain::DeliveryMode;

    async fn render(error: ApiError) -> (StatusCode, Value) {
        let response = error.into_response();
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        (status, serde_json::from_slice(&bytes).unwrap())
    }

    #[tokio::test]
    async fn delivery_mode_conflicts_are_409_with_code_50160() {
        let error = DomainError::DeliveryModeConflict(DeliveryMode::Ephemeral);
        let (status, body) = render(error.into()).await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(body["code"], 50160);
        assert_eq!(body["message"], "channel uses ephemeral delivery");
    }

    #[tokio::test]
    async fn invalid_forms_list_their_fields() {
        let (status, body) = render(ApiError::invalid("content", "too long")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["code"], 50035);
        assert_eq!(body["errors"]["content"]["_errors"][0]["message"], "too long");
    }

    #[tokio::test]
    async fn status_codes_follow_the_error_catalogue() {
        let cases = [
            (DomainError::MissingAccess, StatusCode::FORBIDDEN, 50001),
            (DomainError::MissingPermissions, StatusCode::FORBIDDEN, 50013),
            (DomainError::UnknownChannel, StatusCode::NOT_FOUND, 10003),
            (DomainError::UnknownMessage, StatusCode::NOT_FOUND, 10008),
            (DomainError::MaxPinsReached, StatusCode::BAD_REQUEST, 30003),
        ];
        for (error, expected_status, expected_code) in cases {
            let (status, body) = render(error.into()).await;
            assert_eq!(status, expected_status);
            assert_eq!(body["code"], expected_code);
        }
    }

    #[tokio::test]
    async fn infrastructure_failures_do_not_leak_details() {
        let error = ApiError::App(AppError::Unavailable(PortError::new("scylla: secret host")));
        let (status, body) = render(error).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert!(!body["message"].as_str().unwrap().contains("scylla"));
    }

    #[tokio::test]
    async fn invalid_credentials_are_401() {
        let (status, body) = render(AuthError::Invalid.into()).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(body["code"], 40001);
    }
}

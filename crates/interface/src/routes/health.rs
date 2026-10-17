//! Liveness, readiness and metrics endpoints.

use axum::Json;
use axum::extract::State;
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use serde_json::json;

use crate::state::AppState;

pub async fn live() -> &'static str {
    "ok"
}

/// Ready when every dependency probe succeeds; failures name the dependency
/// but never its error text.
pub async fn ready(State(state): State<AppState>) -> Response {
    let mut failing = Vec::new();
    for probe in &state.readiness {
        if let Err(error) = probe.check().await {
            tracing::warn!(dependency = probe.name(), %error, "readiness probe failed");
            failing.push(probe.name());
        }
    }
    if failing.is_empty() {
        (StatusCode::OK, Json(json!({ "status": "ready" }))).into_response()
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "status": "unavailable", "failing": failing })),
        )
            .into_response()
    }
}

pub async fn metrics(State(state): State<AppState>) -> Response {
    let mut response = state.metrics.render().into_response();
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; version=0.0.4"),
    );
    response
}

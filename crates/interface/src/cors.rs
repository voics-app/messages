//! CORS policy for the desktop client, which loads the api cross-origin.
//! Bearer tokens only: credentials are never allowed.

use std::time::Duration;

use axum::http::{HeaderName, HeaderValue, Method, header};
use tower_http::cors::{AllowOrigin, CorsLayer};

/// Failure to build the policy from configured origins.
#[derive(Debug, thiserror::Error)]
#[error("CORS_ALLOWED_ORIGINS contains an invalid origin {origin:?}")]
pub struct InvalidOrigin {
    pub origin: String,
}

/// Builds the CORS layer that allows exactly `origins`.
pub fn layer(origins: &[String]) -> Result<CorsLayer, InvalidOrigin> {
    let allowed = origins
        .iter()
        .map(|origin| HeaderValue::from_str(origin).map_err(|_| InvalidOrigin { origin: origin.clone() }))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(CorsLayer::new()
        .allow_origin(AllowOrigin::list(allowed))
        .allow_methods([
            Method::GET,
            Method::POST,
            Method::PUT,
            Method::PATCH,
            Method::DELETE,
            Method::OPTIONS,
        ])
        .allow_headers([
            header::AUTHORIZATION,
            header::CONTENT_TYPE,
            HeaderName::from_static("x-audit-log-reason"),
        ])
        .expose_headers([
            HeaderName::from_static("x-ratelimit-limit"),
            HeaderName::from_static("x-ratelimit-remaining"),
            HeaderName::from_static("x-ratelimit-reset"),
            HeaderName::from_static("x-ratelimit-reset-after"),
            HeaderName::from_static("x-ratelimit-bucket"),
            header::RETRY_AFTER,
        ])
        .max_age(Duration::from_secs(600)))
}

#[cfg(test)]
mod tests {
    use axum::Router;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use axum::routing::get;
    use tower::ServiceExt;

    use super::layer;

    const ALLOWED: &str = "https://app.discordy.example";

    fn app() -> Router {
        let origins = vec!["null".to_owned(), ALLOWED.to_owned()];
        Router::new()
            .route("/api/v1/users/@me", get(|| async { "me" }))
            .layer(layer(&origins).expect("valid origins"))
    }

    async fn send(request: Request<Body>) -> axum::response::Response {
        app().oneshot(request).await.expect("infallible service")
    }

    fn preflight(origin: &str) -> Request<Body> {
        Request::builder()
            .method("OPTIONS")
            .uri("/api/v1/users/@me")
            .header("origin", origin)
            .header("access-control-request-method", "PATCH")
            .header("access-control-request-headers", "authorization,x-audit-log-reason")
            .body(Body::empty())
            .expect("request")
    }

    #[tokio::test]
    async fn preflight_from_an_allowed_origin_echoes_the_origin() {
        let response = send(preflight(ALLOWED)).await;
        assert!(matches!(response.status(), StatusCode::NO_CONTENT | StatusCode::OK));
        assert_eq!(response.headers().get("access-control-allow-origin").unwrap(), ALLOWED);
        assert!(response.headers().get("access-control-allow-credentials").is_none());
    }

    #[tokio::test]
    async fn preflight_from_a_disallowed_origin_gets_no_allow_origin_header() {
        let response = send(preflight("https://evil.example")).await;
        assert!(response.headers().get("access-control-allow-origin").is_none());
    }

    #[tokio::test]
    async fn origin_null_is_allowed_when_configured() {
        let response = send(preflight("null")).await;
        assert_eq!(response.headers().get("access-control-allow-origin").unwrap(), "null");
    }

    #[tokio::test]
    async fn actual_response_exposes_rate_limit_headers() {
        let request = Request::builder()
            .uri("/api/v1/users/@me")
            .header("origin", ALLOWED)
            .body(Body::empty())
            .expect("request");
        let response = send(request).await;
        let exposed = response
            .headers()
            .get("access-control-expose-headers")
            .map(|value| value.to_str().expect("ascii").to_ascii_lowercase())
            .unwrap_or_default();
        for name in [
            "x-ratelimit-limit",
            "x-ratelimit-remaining",
            "x-ratelimit-reset",
            "x-ratelimit-reset-after",
            "x-ratelimit-bucket",
            "retry-after",
        ] {
            assert!(exposed.contains(name), "{name} not exposed: {exposed}");
        }
    }

    #[test]
    fn invalid_origin_is_rejected_at_startup() {
        assert!(layer(&["bad\norigin".to_owned()]).is_err());
    }
}

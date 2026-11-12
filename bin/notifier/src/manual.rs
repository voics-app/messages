//! `POST /internal/v1/manual-push`: the admin service asks for a notification by hand. Cluster
//! internal only (no ingress route, a NetworkPolicy lets only the admin pods in) and guarded by
//! the same `X-Internal-Token` the services use between each other.

use std::sync::Arc;

use axum::Json;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use notifier::application::{ManualError, ManualPush, ManualRequest, ManualTarget};
use serde::Deserialize;
use serde_json::{Value, json};

#[derive(Clone)]
pub struct ManualState {
    pub push: Arc<ManualPush>,
    pub internal_token: Arc<str>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Body {
    target: BodyTarget,
    title: String,
    body: String,
    #[serde(default)]
    dry_run: bool,
    /// The admin's email, for the log.
    actor: Option<String>,
}

/// `{"user_id": "123"}` or the string `"all"`.
#[derive(Deserialize)]
#[serde(untagged)]
enum BodyTarget {
    All(AllMarker),
    User { user_id: String },
}

#[derive(Deserialize)]
enum AllMarker {
    #[serde(rename = "all")]
    All,
}

fn same(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |diff, (x, y)| diff | (x ^ y)) == 0
}

fn reply(status: StatusCode, code: &str, message: &str) -> (StatusCode, Json<Value>) {
    (status, Json(json!({"code": code, "message": message})))
}

pub async fn manual_push(
    State(state): State<ManualState>,
    headers: HeaderMap,
    Json(body): Json<Body>,
) -> (StatusCode, Json<Value>) {
    let presented = headers
        .get("x-internal-token")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    if state.internal_token.is_empty() || !same(presented, &state.internal_token) {
        return reply(
            StatusCode::UNAUTHORIZED,
            "UNAUTHORIZED",
            "A valid internal token is required.",
        );
    }
    let request = ManualRequest {
        target: match body.target {
            BodyTarget::All(_) => ManualTarget::All,
            BodyTarget::User { user_id } => ManualTarget::User(user_id),
        },
        title: body.title,
        body: body.body,
        dry_run: body.dry_run,
        actor: body.actor,
    };
    match state.push.send(request).await {
        Ok(report) => (
            StatusCode::OK,
            Json(json!({
                "devices": report.devices,
                "queued": report.queued,
                "dropped": report.dropped,
                "truncated": report.truncated,
            })),
        ),
        Err(ManualError::Invalid(message)) => reply(StatusCode::UNPROCESSABLE_ENTITY, "MANUAL_PUSH_INVALID", &message),
        Err(ManualError::Busy) => reply(
            StatusCode::CONFLICT,
            "MANUAL_PUSH_BUSY",
            "Another notification to everyone is still being sent.",
        ),
        Err(ManualError::Directory(message)) => {
            tracing::warn!(%message, "manual push could not resolve devices");
            reply(
                StatusCode::BAD_GATEWAY,
                "MANUAL_PUSH_DIRECTORY",
                "The api could not list the devices.",
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::time::Duration;

    use async_trait::async_trait;
    use axum::Router;
    use axum::body::Body as HttpBody;
    use axum::http::Request;
    use axum::routing::post;
    use notifier::application::{
        DeviceDirectory, DevicePage, DeviceToken, DispatchConfig, Dispatcher, Environment, Platform, PortError,
        PushSender, SendError, TokenRegistry,
    };
    use notifier::domain::OutboundPush;
    use tower::ServiceExt;

    use super::*;

    struct Directory;

    fn phone(token: &str) -> DeviceToken {
        DeviceToken {
            platform: Platform::Fcm,
            token: token.into(),
            environment: Environment::Production,
            previews: true,
            only_mentions: false,
        }
    }

    #[async_trait]
    impl DeviceDirectory for Directory {
        async fn user_devices(&self, user_id: &str) -> Result<Vec<DeviceToken>, PortError> {
            Ok(if user_id == "7" {
                vec![phone("p1"), phone("p2")]
            } else {
                Vec::new()
            })
        }

        async fn device_page(&self, _after: Option<&str>) -> Result<DevicePage, PortError> {
            Ok(DevicePage {
                devices: vec![phone("p1"), phone("p2"), phone("p3")],
                next_after: None,
            })
        }
    }

    #[derive(Default)]
    struct Sender(Mutex<Vec<String>>);

    #[async_trait]
    impl PushSender for Sender {
        async fn send(&self, token: &DeviceToken, _push: &OutboundPush) -> Result<(), SendError> {
            self.0.lock().unwrap().push(token.token.clone());
            Ok(())
        }
    }

    struct Registry;

    #[async_trait]
    impl TokenRegistry for Registry {
        async fn forget(&self, _token: &DeviceToken) -> Result<(), PortError> {
            Ok(())
        }
    }

    fn app() -> (Router, Arc<Sender>) {
        let sender = Arc::new(Sender::default());
        let dispatcher = Dispatcher::spawn(
            DispatchConfig {
                queue_capacity: 10,
                max_in_flight: 2,
                max_attempts: 1,
                base_backoff: Duration::from_millis(1),
                max_backoff: Duration::from_millis(1),
            },
            sender.clone(),
            Arc::new(Registry),
        );
        let state = ManualState {
            push: Arc::new(ManualPush::new(Arc::new(Directory), dispatcher)),
            internal_token: Arc::from("secret"),
        };
        (
            Router::new()
                .route("/internal/v1/manual-push", post(manual_push))
                .with_state(state),
            sender,
        )
    }

    async fn call(app: &Router, token: Option<&str>, body: Value) -> (StatusCode, Value) {
        let mut request = Request::post("/internal/v1/manual-push").header("content-type", "application/json");
        if let Some(token) = token {
            request = request.header("x-internal-token", token);
        }
        let response = app
            .clone()
            .oneshot(request.body(HttpBody::from(body.to_string())).unwrap())
            .await
            .unwrap();
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), 1 << 20).await.unwrap();
        (status, serde_json::from_slice(&bytes).unwrap_or(Value::Null))
    }

    fn payload(target: Value) -> Value {
        json!({"target": target, "title": "Hello", "body": "World", "actor": "owner@example.com"})
    }

    #[tokio::test]
    async fn the_internal_token_is_required() {
        let (app, sender) = app();
        for token in [None, Some("wrong"), Some("")] {
            let (status, _) = call(&app, token, payload(json!("all"))).await;
            assert_eq!(status, StatusCode::UNAUTHORIZED);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(sender.0.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn one_user_and_everyone_are_accepted_and_reported() {
        let (app, sender) = app();
        let (status, body) = call(&app, Some("secret"), payload(json!({"user_id": "7"}))).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(
            body,
            json!({"devices": 2, "queued": 2, "dropped": 0, "truncated": false})
        );
        let (status, body) = call(&app, Some("secret"), payload(json!("all"))).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["devices"], 3);
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(sender.0.lock().unwrap().len(), 5);
    }

    #[tokio::test]
    async fn a_dry_run_only_counts() {
        let (app, sender) = app();
        let mut request = payload(json!("all"));
        request["dry_run"] = json!(true);
        let (status, body) = call(&app, Some("secret"), request).await;
        assert_eq!(
            (status, body["devices"].clone(), body["queued"].clone()),
            (StatusCode::OK, json!(3), json!(0))
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(sender.0.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn bad_text_is_a_422_with_the_platform_error_shape() {
        let (app, _) = app();
        let mut request = payload(json!("all"));
        request["title"] = json!("t".repeat(65));
        let (status, body) = call(&app, Some("secret"), request).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(body["code"], "MANUAL_PUSH_INVALID");
    }

    #[tokio::test]
    async fn an_unknown_target_shape_is_rejected() {
        let (app, _) = app();
        for target in [json!("everyone"), json!({"username": "bob"}), json!(null)] {
            let (status, _) = call(&app, Some("secret"), payload(target)).await;
            assert!(status.is_client_error(), "{status}");
        }
    }
}

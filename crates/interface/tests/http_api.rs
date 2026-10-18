//! End-to-end tests of the HTTP surface over in-memory ports.

use std::sync::Arc;

use async_trait::async_trait;
use axum::Router;
use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use discordy_messages_application::PortError;
use discordy_messages_application::fakes::{ALICE, BOB, CHANNEL, Harness, MEMBER, dm_access};
use discordy_messages_application::ports::{AuthError, Authenticator, Principal};
use discordy_messages_domain::{DeliveryMode, Permissions};
use discordy_messages_interface::{AppState, AttachmentUrls, InternalToken, ReadinessCheck, router};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tower::ServiceExt;

const ALICE_TOKEN: &str = "Bearer alice";
const INTERNAL: &str = "internal-secret";

struct FakeAuth;

#[async_trait]
impl Authenticator for FakeAuth {
    async fn authenticate(&self, authorization: &str) -> Result<Principal, AuthError> {
        match authorization {
            ALICE_TOKEN => Ok(Principal {
                user_id: ALICE,
                is_bot: false,
            }),
            "Bearer bob" => Ok(Principal {
                user_id: BOB,
                is_bot: false,
            }),
            _ => Err(AuthError::Invalid),
        }
    }
}

struct Probe(Result<(), PortError>);

#[async_trait]
impl ReadinessCheck for Probe {
    fn name(&self) -> &'static str {
        "probe"
    }

    async fn check(&self) -> Result<(), PortError> {
        self.0.clone()
    }
}

fn app_with(harness: &Harness, probes: Vec<Arc<dyn ReadinessCheck>>) -> Router {
    router(AppState::new(
        &harness.ports,
        Arc::new(FakeAuth),
        InternalToken::new(INTERNAL),
        AttachmentUrls::new("http://cdn/cdn", "attachments"),
        probes,
    ))
}

fn app(harness: &Harness) -> Router {
    app_with(harness, vec![])
}

async fn call(app: &Router, method: Method, uri: &str, auth: Option<&str>, body: Option<Value>) -> (StatusCode, Value) {
    let mut request = Request::builder().method(method).uri(uri);
    if let Some(auth) = auth {
        request = request.header("authorization", auth);
    }
    let body = match body {
        Some(json) => {
            request = request.header("content-type", "application/json");
            Body::from(json.to_string())
        }
        None => Body::empty(),
    };
    send(app, request.body(body).unwrap()).await
}

async fn post_internal(app: &Router, uri: &str, token: Option<&str>, body: Value) -> (StatusCode, Value) {
    let mut request = Request::builder()
        .method(Method::POST)
        .uri(uri)
        .header("content-type", "application/json");
    if let Some(token) = token {
        request = request.header("x-internal-token", token);
    }
    send(app, request.body(Body::from(body.to_string())).unwrap()).await
}

async fn send(app: &Router, request: Request<Body>) -> (StatusCode, Value) {
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(Value::Null)
    };
    (status, value)
}

fn messages_uri() -> String {
    format!("/api/v1/channels/{CHANNEL}/messages")
}

#[tokio::test]
async fn requests_without_valid_credentials_are_rejected() {
    let harness = Harness::new();
    let app = app(&harness);
    let (status, body) = call(&app, Method::GET, &messages_uri(), None, None).await;
    assert_eq!((status, body["code"].as_u64()), (StatusCode::UNAUTHORIZED, Some(40001)));
    let (status, _) = call(&app, Method::GET, &messages_uri(), Some("Bearer nobody"), None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn a_message_can_be_created_edited_read_and_deleted() {
    let harness = Harness::new();
    let app = app(&harness);

    let (status, created) = call(
        &app,
        Method::POST,
        &messages_uri(),
        Some(ALICE_TOKEN),
        Some(json!({"content": "hello <@11>", "nonce": 42})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(created["content"], "hello <@11>");
    assert_eq!(created["author"]["id"], ALICE.to_string());
    assert_eq!(created["mentions"][0]["id"], "11");
    let id = created["id"].as_str().unwrap().to_owned();

    let (status, edited) = call(
        &app,
        Method::PATCH,
        &format!("{}/{id}", messages_uri()),
        Some(ALICE_TOKEN),
        Some(json!({"content": "edited"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(edited["edited_timestamp"].is_string());

    let (status, page) = call(
        &app,
        Method::GET,
        &format!("{}?limit=10", messages_uri()),
        Some(ALICE_TOKEN),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(page.as_array().unwrap().len(), 1);
    assert_eq!(page[0]["content"], "edited");

    let (status, _) = call(
        &app,
        Method::DELETE,
        &format!("{}/{id}", messages_uri()),
        Some(ALICE_TOKEN),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (_, page) = call(&app, Method::GET, &messages_uri(), Some(ALICE_TOKEN), None).await;
    assert!(page.as_array().unwrap().is_empty());
}

#[tokio::test]
async fn non_standard_channels_answer_409_with_code_50160() {
    let harness = Harness::new();
    harness.access.set(Ok(dm_access(DeliveryMode::Ephemeral)));
    let app = app(&harness);
    let (status, body) = call(
        &app,
        Method::POST,
        &messages_uri(),
        Some(ALICE_TOKEN),
        Some(json!({"content": "secret"})),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["code"], 50160);
}

#[tokio::test]
async fn malformed_input_is_a_50035_form_error() {
    let harness = Harness::new();
    let app = app(&harness);
    let (status, body) = call(
        &app,
        Method::GET,
        "/api/v1/channels/not-a-number/messages",
        Some(ALICE_TOKEN),
        None,
    )
    .await;
    assert_eq!((status, body["code"].as_u64()), (StatusCode::BAD_REQUEST, Some(50035)));
    let uri = format!("{}?limit=500", messages_uri());
    let (status, body) = call(&app, Method::GET, &uri, Some(ALICE_TOKEN), None).await;
    assert_eq!((status, body["code"].as_u64()), (StatusCode::BAD_REQUEST, Some(50035)));
    let uri = format!("{}?before=1&after=2", messages_uri());
    let (status, _) = call(&app, Method::GET, &uri, Some(ALICE_TOKEN), None).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, body) = call(
        &app,
        Method::POST,
        &messages_uri(),
        Some(ALICE_TOKEN),
        Some(json!({"content": 12})),
    )
    .await;
    assert_eq!((status, body["code"].as_u64()), (StatusCode::BAD_REQUEST, Some(50035)));
}

#[tokio::test]
async fn reactions_pins_and_acks_round_trip() {
    let harness = Harness::new();
    harness.access.set_permissions(MEMBER | Permissions::MANAGE_MESSAGES);
    let message = harness.seed(BOB, "react to me").await;
    let app = app(&harness);
    let base = format!("{}/{}", messages_uri(), message.id);

    let emoji = "%F0%9F%91%8D";
    let (status, _) = call(
        &app,
        Method::PUT,
        &format!("{base}/reactions/{emoji}/@me"),
        Some(ALICE_TOKEN),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (_, page) = call(&app, Method::GET, &messages_uri(), Some(ALICE_TOKEN), None).await;
    assert_eq!(page[0]["reactions"][0]["count"], 1);
    assert_eq!(page[0]["reactions"][0]["me"], true);
    assert_eq!(page[0]["reactions"][0]["emoji"]["name"], "👍");
    let (status, _) = call(
        &app,
        Method::DELETE,
        &format!("{base}/reactions/{emoji}/@me"),
        Some(ALICE_TOKEN),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    let pins = format!("/api/v1/channels/{CHANNEL}/pins");
    let (status, _) = call(
        &app,
        Method::PUT,
        &format!("{pins}/{}", message.id),
        Some(ALICE_TOKEN),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, pinned) = call(&app, Method::GET, &pins, Some(ALICE_TOKEN), None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(pinned[0]["pinned"], true);

    let (status, ack) = call(&app, Method::POST, &format!("{base}/ack"), Some(ALICE_TOKEN), None).await;
    assert_eq!((status, &ack["token"]), (StatusCode::OK, &Value::Null));
}

#[tokio::test]
async fn upload_slots_point_at_the_attachment_store() {
    let harness = Harness::new();
    let app = app(&harness);
    let (status, body) = call(
        &app,
        Method::POST,
        &format!("/api/v1/channels/{CHANNEL}/attachments"),
        Some(ALICE_TOKEN),
        Some(json!({"files": [{"id": 3, "filename": "cat.png", "file_size": 1024}]})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let slot = &body["attachments"][0];
    assert_eq!(slot["id"], "3");
    assert!(slot["upload_filename"].as_str().unwrap().ends_with("/cat.png"));
    assert!(slot["upload_url"].is_string());
}

#[tokio::test]
async fn internal_create_posts_as_the_author_override_without_a_user_check() {
    let harness = Harness::new();
    harness.access.set_permissions(Permissions::VIEW_CHANNEL);
    let app = app(&harness);
    let uri = format!("/internal/v1/channels/{CHANNEL}/messages");

    let (status, _) = post_internal(&app, &uri, None, json!({"author_id": BOB.to_string(), "content": "x"})).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    let (status, created) = post_internal(
        &app,
        &uri,
        Some(INTERNAL),
        json!({"author_id": BOB.to_string(), "content": "from a bot", "embeds": [{"title": "t"}]}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(created["author"]["id"], BOB.to_string());
    assert_eq!(created["content"], "from a bot");
    assert_eq!(created["embeds"][0]["title"], "t");
}

#[tokio::test]
async fn internal_create_refuses_non_standard_delivery_and_bad_bodies() {
    let harness = Harness::new();
    harness.access.set(Ok(dm_access(DeliveryMode::Ephemeral)));
    let app = app(&harness);
    let uri = format!("/internal/v1/channels/{CHANNEL}/messages");

    let (status, body) = post_internal(
        &app,
        &uri,
        Some(INTERNAL),
        json!({"author_id": BOB.to_string(), "content": "secret"}),
    )
    .await;
    assert_eq!((status, body["code"].as_u64()), (StatusCode::CONFLICT, Some(50160)));

    let (status, body) = post_internal(&app, &uri, Some(INTERNAL), json!({"content": "no author"})).await;
    assert_eq!((status, body["code"].as_u64()), (StatusCode::BAD_REQUEST, Some(50035)));
}

#[tokio::test]
async fn internal_endpoints_require_the_shared_token() {
    let harness = Harness::new();
    let message = harness.seed(BOB, "latest").await;
    let app = app(&harness);
    let uri = format!("/internal/v1/channels/{CHANNEL}/last-message-id");

    let (status, _) = call(&app, Method::GET, &uri, None, None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    let request = |path: &str, token: &str| {
        Request::builder()
            .uri(path.to_owned())
            .header("x-internal-token", token)
            .body(Body::empty())
            .unwrap()
    };
    let wrong = app.clone().oneshot(request(&uri, "wrong")).await.unwrap();
    assert_eq!(wrong.status(), StatusCode::UNAUTHORIZED);

    let response = app.clone().oneshot(request(&uri, INTERNAL)).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body: Value = serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(body["last_message_id"], message.id.to_string());

    let stats = app
        .clone()
        .oneshot(request("/internal/v1/admin/stats", INTERNAL))
        .await
        .unwrap();
    assert_eq!(stats.status(), StatusCode::OK);
    let body: Value = serde_json::from_slice(&stats.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert!(body["messages_by_mode"]["standard"].is_u64());
}

#[tokio::test]
async fn readiness_reports_failing_dependencies_without_details() {
    let harness = Harness::new();
    let failing: Vec<Arc<dyn ReadinessCheck>> = vec![Arc::new(Probe(Err(PortError::new("password=hunter2"))))];
    let app = app_with(&harness, failing);
    let (status, body) = call(&app, Method::GET, "/readyz", None, None).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["failing"][0], "probe");
    assert!(!body.to_string().contains("hunter2"));

    let healthy = app_with(&harness, vec![Arc::new(Probe(Ok(())))]);
    assert_eq!(
        call(&healthy, Method::GET, "/readyz", None, None).await.0,
        StatusCode::OK
    );
    assert_eq!(
        call(&healthy, Method::GET, "/healthz", None, None).await.0,
        StatusCode::OK
    );
}

#[tokio::test]
async fn metrics_count_responses_by_status_class() {
    let harness = Harness::new();
    let app = app(&harness);
    call(&app, Method::GET, "/healthz", None, None).await;
    call(&app, Method::GET, &messages_uri(), None, None).await;
    let response = app
        .clone()
        .oneshot(Request::builder().uri("/metrics").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let text = String::from_utf8(response.into_body().collect().await.unwrap().to_bytes().to_vec()).unwrap();
    assert!(text.contains("messages_http_requests_total{status=\"2xx\"} 1"));
    assert!(text.contains("messages_http_requests_total{status=\"4xx\"} 1"));
}

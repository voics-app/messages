//! End-to-end tests of the HTTP surface over in-memory ports.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use axum::Router;
use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use discordy_messages_application::PortError;
use discordy_messages_application::fakes::{ALICE, BOB, CHANNEL, Harness, MEMBER, dm_access, guild_access};
use discordy_messages_application::ports::{AccessError, AuthError, Authenticator, MessageRepository, Principal};
use discordy_messages_domain::{Audience, DeliveryMode, Permissions};
use discordy_messages_interface::metrics::{LATENCY_BUCKETS, Metrics};
use discordy_messages_interface::{AppState, AttachmentUrls, InternalToken, ReadinessCheck, router};
use http_body_util::BodyExt;
use metrics_exporter_prometheus::PrometheusBuilder;
use serde_json::{Value, json};
use tower::ServiceExt;

const ALICE_TOKEN: &str = "Bearer alice";
const INTERNAL: &str = "internal-secret";
const SIGNING_KEY: &str = "integration-signing-key-0123456789abcdef";

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
        AttachmentUrls::new(
            "http://cdn/cdn",
            SIGNING_KEY,
            Duration::from_secs(3600),
            harness.clock.clone(),
        ),
        harness.media.clone(),
        probes,
        test_metrics(),
    ))
}

/// Per-test recorder, installed as this thread's default (tests run on current-thread runtimes).
fn test_metrics() -> Metrics {
    let recorder: &'static _ = Box::leak(Box::new(
        PrometheusBuilder::new()
            .set_buckets(&LATENCY_BUCKETS)
            .unwrap()
            .build_recorder(),
    ));
    std::mem::forget(metrics::set_default_local_recorder(recorder));
    Metrics::new(recorder.handle())
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

async fn patch_internal(app: &Router, uri: &str, token: Option<&str>, body: Value) -> (StatusCode, Value) {
    let mut request = Request::builder()
        .method(Method::PATCH)
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
async fn tts_is_persisted_returned_and_gated_by_permission() {
    let harness = Harness::new();
    harness.access.set_permissions(MEMBER | (1 << 12));
    let app = app(&harness);
    let (status, created) = call(
        &app,
        Method::POST,
        &messages_uri(),
        Some(ALICE_TOKEN),
        Some(json!({"content": "read this aloud", "tts": true})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(created["tts"], true);
    assert!(matches!(&harness.events.kinds()[0],
        discordy_messages_application::ports::MessageEventKind::Created { message, .. } if message.tts));
    let (_, history) = call(&app, Method::GET, &messages_uri(), Some(ALICE_TOKEN), None).await;
    assert_eq!(history[0]["tts"], true);

    harness.access.set_permissions(MEMBER);
    let (status, denied) = call(
        &app,
        Method::POST,
        &messages_uri(),
        Some(ALICE_TOKEN),
        Some(json!({"content": "not permitted", "tts": true})),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{denied}");
    let (_, normal) = call(
        &app,
        Method::POST,
        &messages_uri(),
        Some(ALICE_TOKEN),
        Some(json!({"content": "normal message"})),
    )
    .await;
    assert_eq!(normal["tts"], false);
}

#[tokio::test]
async fn voice_message_permission_validation_and_history() {
    let harness = Harness::new();
    harness
        .storage
        .put_body(&format!("attachments/{CHANNEL}/77/voice.ogg"), b"audio");
    let app = app(&harness);
    let payload = json!({"flags": 8192, "attachments": [{
        "uploaded_filename": format!("attachments/{CHANNEL}/77/voice.ogg"),
        "duration_secs": 1.5, "waveform": "AQID"
    }]});
    harness.access.set_permissions(MEMBER);
    let (status, _) = call(
        &app,
        Method::POST,
        &messages_uri(),
        Some(ALICE_TOKEN),
        Some(payload.clone()),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    harness
        .access
        .set_permissions(MEMBER | discordy_messages_domain::Permissions::SEND_VOICE_MESSAGES);
    let (status, created) = call(&app, Method::POST, &messages_uri(), Some(ALICE_TOKEN), Some(payload)).await;
    assert_eq!(status, StatusCode::OK, "{created}");
    assert_eq!(created["flags"], 8192);
    assert_eq!(created["attachments"][0]["duration_secs"], 1.5);
    assert_eq!(created["attachments"][0]["waveform"], "AQID");
    let (_, history) = call(&app, Method::GET, &messages_uri(), Some(ALICE_TOKEN), None).await;
    assert_eq!(history[0]["attachments"][0]["waveform"], "AQID");
    let (status, _) = call(
        &app,
        Method::POST,
        &messages_uri(),
        Some(ALICE_TOKEN),
        Some(json!({"attachments": [{"uploaded_filename": format!("attachments/{CHANNEL}/77/voice.ogg"), "duration_secs": 1.5, "waveform": "AQID"}]})),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, _) = call(
        &app,
        Method::POST,
        &messages_uri(),
        Some(ALICE_TOKEN),
        Some(json!({"flags":8192,"content":"invalid"})),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn attachment_metadata_edits_preserve_files_and_can_clear_a_spoiler() {
    let harness = Harness::new();
    harness
        .storage
        .put_body(&format!("attachments/{CHANNEL}/77/SPOILER_cat.png"), b"meow");
    let app = app(&harness);
    let (status, created) = call(
        &app,
        Method::POST,
        &messages_uri(),
        Some(ALICE_TOKEN),
        Some(json!({"content": "look", "attachments": [{
            "uploaded_filename": format!("attachments/{CHANNEL}/77/SPOILER_cat.png"),
            "description": "old alt text"
        }]})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let uri = format!("{}/{}", messages_uri(), created["id"].as_str().unwrap());
    let (status, edited) = call(
        &app,
        Method::PATCH,
        &uri,
        Some(ALICE_TOKEN),
        Some(json!({"attachments": [{"id": "77", "description": " a sleeping cat ", "flags": 0}]})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(edited["attachments"][0]["description"], "a sleeping cat");
    assert_eq!(edited["attachments"][0]["flags"], 0);
    assert_eq!(edited["attachments"][0]["url"], created["attachments"][0]["url"]);
    assert!(edited["edited_timestamp"].is_string());
    let (_, history) = call(&app, Method::GET, &messages_uri(), Some(ALICE_TOKEN), None).await;
    assert_eq!(history[0]["attachments"], edited["attachments"]);
    let (_, cleared) = call(
        &app,
        Method::PATCH,
        &uri,
        Some(ALICE_TOKEN),
        Some(json!({"attachments": [{"id": "77", "description": null, "flags": 8}]})),
    )
    .await;
    assert!(cleared["attachments"][0]["description"].is_null());
    assert_eq!(cleared["attachments"][0]["flags"], 8);
    assert!(matches!(&harness.events.kinds().last().unwrap(),
        discordy_messages_application::ports::MessageEventKind::Updated { message }
        if message.attachments[0].description.is_none() && message.attachments[0].flags == 8));
    for invalid in [
        json!({"attachments": [{"id": "77", "filename": "replacement.png"}]}),
        json!({"content": "must not change", "attachments": [{"id": "999", "description": "unknown"}]}),
        json!({"attachments": [{"id": "77", "flags": 0}, {"id": "77", "flags": 8}]}),
    ] {
        let (status, _) = call(&app, Method::PATCH, &uri, Some(ALICE_TOKEN), Some(invalid)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }
    let (_, unchanged) = call(&app, Method::GET, &messages_uri(), Some(ALICE_TOKEN), None).await;
    assert_eq!(unchanged[0]["content"], "look");
    assert_eq!(unchanged[0]["attachments"], cleared["attachments"]);
    let (status, _) = call(
        &app,
        Method::PATCH,
        &uri,
        Some("Bearer bob"),
        Some(json!({"attachments": [{"id": "77", "flags": 0}]})),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn expired_id_issuer_returns_503_without_storing_or_announcing() {
    struct Expired;
    impl discordy_messages_application::ports::IdGenerator for Expired {
        fn next_id(&self) -> Result<discordy_messages_domain::Snowflake, PortError> {
            Err(PortError::new("ID reservation expired"))
        }
        fn is_ready(&self) -> bool {
            false
        }
    }
    let mut harness = Harness::new();
    harness.ports.ids = Arc::new(Expired);
    let app = app(&harness);
    let (status, body) = call(
        &app,
        Method::POST,
        &messages_uri(),
        Some(ALICE_TOKEN),
        Some(json!({"content":"hello"})),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    assert_eq!(harness.messages.last_message_id(CHANNEL).await.unwrap(), None);
    assert!(harness.events.kinds().is_empty());
    let (status, body) = call(
        &app,
        Method::POST,
        &format!("/api/v1/channels/{CHANNEL}/attachments"),
        Some(ALICE_TOKEN),
        Some(json!({"files":[{"id":"0","filename":"image.png","file_size":1}]})),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
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
async fn silent_flag_is_kept_and_replied_user_false_drops_the_reply_ping() {
    let harness = Harness::new();
    let app = app(&harness);
    let (_, original) = call(
        &app,
        Method::POST,
        &messages_uri(),
        Some(ALICE_TOKEN),
        Some(json!({"content": "first"})),
    )
    .await;
    let reference = json!({"message_id": original["id"]});

    let (status, silent) = call(
        &app,
        Method::POST,
        &messages_uri(),
        Some(ALICE_TOKEN),
        // EPHEMERAL (64) is not client-settable: only SUPPRESS_NOTIFICATIONS survives.
        Some(json!({"content": "shh <@11>", "flags": 4096 | 64 | 1 | 2})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(silent["flags"], 4096);

    let (status, quiet) = call(
        &app,
        Method::POST,
        &messages_uri(),
        Some(ALICE_TOKEN),
        Some(json!({"content": "re", "message_reference": reference, "allowed_mentions": {"replied_user": false}})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(quiet["mentions"].as_array().unwrap().is_empty());
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

    let (status, _) = call(
        &app,
        Method::POST,
        &format!("{base}/ack"),
        Some(ALICE_TOKEN),
        Some(json!({"manual": true, "mention_count": 1})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
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
async fn older_ack_preserves_versioned_mentions_in_dispatch_and_internal_reads() {
    use discordy_messages_application::ports::MessageEventKind;
    let harness = Harness::new();
    let app = app(&harness);
    let mut first = String::new();
    for index in 0..2 {
        let (status, message) = call(
            &app,
            Method::POST,
            &messages_uri(),
            Some(ALICE_TOKEN),
            Some(json!({"content": format!("mention {index} <@{BOB}>")})),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        if index == 0 {
            first = message["id"].as_str().unwrap().to_owned();
        }
    }
    let ack_uri = format!("{}/{first}/ack", messages_uri());
    for _ in 0..2 {
        let (status, _) = call(&app, Method::POST, &ack_uri, Some("Bearer bob"), None).await;
        assert_eq!(status, StatusCode::OK);
        let event = harness.events.events.lock().unwrap().last().unwrap().clone();
        assert_eq!(event.audience, Audience::Users(vec![BOB]));
        assert!(matches!(
            event.kind,
            MessageEventKind::Acknowledged {
                mention_count: 1,
                version: 3,
                ..
            }
        ));
    }
    let request = Request::builder()
        .uri(format!("/internal/v1/users/{BOB}/read-states"))
        .header("x-internal-token", INTERNAL)
        .body(Body::empty())
        .unwrap();
    let (status, reads) = send(&app, request).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(reads[0]["last_message_id"], first);
    assert_eq!(reads[0]["mention_count"], 1);
    assert_eq!(reads[0]["read_state_version"], "3");
}

#[tokio::test]
async fn internal_read_state_header_certifies_an_empty_account() {
    let harness = Harness::new();
    let request = Request::builder()
        .uri(format!("/internal/v1/users/{BOB}/read-states"))
        .header("x-internal-token", INTERNAL)
        .body(Body::empty())
        .unwrap();
    let response = app(&harness).oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["x-read-state-versioned"], "1");
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(serde_json::from_slice::<Value>(&bytes).unwrap(), json!([]));
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
async fn internal_webhook_posts_are_authored_by_the_webhook_with_overrides() {
    let harness = Harness::new();
    harness.access.set(Err(AccessError::NotMember));
    *harness.access.channel.lock().unwrap() = Ok(guild_access(MEMBER));
    let app = app(&harness);
    let uri = format!("/internal/v1/channels/{CHANNEL}/messages");

    let (status, created) = post_internal(
        &app,
        &uri,
        Some(INTERNAL),
        json!({"author_id": "900", "webhook_id": "900", "username": "Relay",
               "avatar_url": "https://cdn.test/a.png", "content": "relayed"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(created["author"]["id"], "900");
    assert_eq!(created["webhook_id"], "900");
    assert_eq!(created["author_override"]["username"], "Relay");
    assert_eq!(created["author_override"]["avatar_url"], "https://cdn.test/a.png");
}

#[tokio::test]
async fn publication_marker_requires_internal_authentication() {
    let harness = Harness::new();
    let mut access = guild_access(MEMBER);
    access.channel_type = 5;
    *harness.access.channel.lock().unwrap() = Ok(access);
    let original = harness.seed(ALICE, "news").await;
    let app = app(&harness);
    let uri = format!("/internal/v1/channels/{CHANNEL}/messages/{}/crossposted", original.id);
    let (status, _) = post_internal(&app, &uri, None, json!({})).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, marked) = post_internal(&app, &uri, Some(INTERNAL), json!({})).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(marked["flags"].as_u64().unwrap() & 1, 1);
}

#[tokio::test]
async fn trusted_crosspost_delivers_snapshot_with_reserved_identity() {
    let harness = Harness::new();
    *harness.access.channel.lock().unwrap() = Ok(guild_access(MEMBER));
    harness.storage.put_body("attachments/900/777/news.png", b"image");
    let app = app(&harness);
    let uri = format!("/internal/v1/channels/{CHANNEL}/crossposts");
    let payload = json!({"source_channel_id":"900", "message_id":"901", "webhook_id":"902", "webhook_name":"News", "snapshot":{"content":"hello @everyone", "embeds":[], "attachments":[{"id":"777", "filename":"news.png", "size":5}]}});
    let (status, _) = post_internal(&app, &uri, None, payload.clone()).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, delivered) = post_internal(&app, &uri, Some(INTERNAL), payload.clone()).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(delivered["id"], "901");
    assert_eq!(delivered["webhook_id"], "902");
    assert_eq!(delivered["flags"].as_u64().unwrap() & 2, 2);
    assert_eq!(delivered["mention_everyone"], false);
    assert_eq!(delivered["attachments"][0]["id"], "777");
    assert!(
        harness
            .storage
            .objects
            .lock()
            .unwrap()
            .contains_key(&format!("attachments/{CHANNEL}/777/news.png"))
    );
    let (status, repeated) = post_internal(&app, &uri, Some(INTERNAL), payload).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(repeated["id"], delivered["id"]);
    assert_eq!(harness.events.kinds().len(), 1);
}

#[tokio::test]
async fn internal_interaction_responses_link_their_interaction() {
    let harness = Harness::new();
    harness.access.set_permissions(Permissions::VIEW_CHANNEL);
    let app = app(&harness);
    let uri = format!("/internal/v1/channels/{CHANNEL}/messages");

    let (status, created) = post_internal(
        &app,
        &uri,
        Some(INTERNAL),
        json!({"author_id": BOB.to_string(), "interaction_id": "31", "application_id": "32",
               "content": "answer"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(created["interaction_metadata"]["id"], "31");
    assert_eq!(created["application_id"], "32");
    assert!(created.get("webhook_id").is_none());
}

#[tokio::test]
async fn internal_system_messages_cover_join_thread_and_call() {
    let harness = Harness::new();
    harness.access.set_permissions(Permissions::VIEW_CHANNEL);
    let app = app(&harness);
    let uri = format!("/internal/v1/channels/{CHANNEL}/messages");

    let (status, join) = post_internal(
        &app,
        &uri,
        Some(INTERNAL),
        json!({"author_id": BOB.to_string(), "type": 7}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(join["type"], 7);
    assert_eq!(join["content"], "");

    let (status, thread) = post_internal(
        &app,
        &uri,
        Some(INTERNAL),
        json!({"author_id": BOB.to_string(), "type": 18, "content": "plans", "thread_id": "777"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(thread["type"], 18);
    assert_eq!(thread["message_reference"]["channel_id"], "777");
    assert!(thread["message_reference"].get("message_id").is_none());

    let (status, body) = post_internal(
        &app,
        &uri,
        Some(INTERNAL),
        json!({"author_id": BOB.to_string(), "type": 18, "content": "no thread"}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["code"], 50035);

    let (status, call_message) = post_internal(
        &app,
        &uri,
        Some(INTERNAL),
        json!({"author_id": BOB.to_string(), "type": 3, "call": {"participants": [BOB.to_string()]}}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(call_message["call"]["participants"], json!([BOB.to_string()]));
    assert!(call_message["call"]["ended_timestamp"].is_null());

    let update = format!(
        "/internal/v1/channels/{CHANNEL}/messages/{}/call",
        call_message["id"].as_str().unwrap()
    );
    let ended =
        json!({"ended_timestamp": "2026-01-01T00:00:00.123Z", "participants": [BOB.to_string(), ALICE.to_string()]});
    let (status, _) = patch_internal(&app, &update, None, ended.clone()).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, updated) = patch_internal(&app, &update, Some(INTERNAL), ended).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(updated["call"]["ended_timestamp"], "2026-01-01T00:00:00.123Z");
    assert_eq!(updated["call"]["participants"].as_array().unwrap().len(), 2);

    let (status, _) = patch_internal(&app, &update, Some(INTERNAL), json!({"ended_timestamp": "yesterday"})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let join_update = format!(
        "/internal/v1/channels/{CHANNEL}/messages/{}/call",
        join["id"].as_str().unwrap()
    );
    let (status, _) = patch_internal(
        &app,
        &join_update,
        Some(INTERNAL),
        json!({"ended_timestamp": "2026-01-01T00:00:00Z"}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn pinning_posts_a_system_message_that_references_the_pinned_one() {
    let harness = Harness::new();
    harness.access.set_permissions(MEMBER | Permissions::MANAGE_MESSAGES);
    let app = app(&harness);
    let message = harness.seed(ALICE, "rules").await;
    let (status, _) = call(
        &app,
        Method::PUT,
        &format!("/api/v1/channels/{CHANNEL}/pins/{}", message.id),
        Some(ALICE_TOKEN),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (_, history) = call(
        &app,
        Method::GET,
        &format!("/api/v1/channels/{CHANNEL}/messages?limit=1"),
        Some(ALICE_TOKEN),
        None,
    )
    .await;
    assert_eq!(history[0]["type"], 6);
    assert_eq!(history[0]["message_reference"]["message_id"], message.id.to_string());
}

#[tokio::test]
async fn internal_ephemeral_replies_go_to_the_invoker_and_are_not_stored() {
    let harness = Harness::new();
    harness.access.set_permissions(Permissions::VIEW_CHANNEL);
    let app = app(&harness);
    let uri = format!("/internal/v1/channels/{CHANNEL}/messages");

    let (status, body) = post_internal(
        &app,
        &uri,
        Some(INTERNAL),
        json!({"author_id": BOB.to_string(), "content": "only you", "flags": 64}),
    )
    .await;
    assert_eq!((status, body["code"].as_u64()), (StatusCode::BAD_REQUEST, Some(50035)));

    let (status, created) = post_internal(
        &app,
        &uri,
        Some(INTERNAL),
        json!({"author_id": BOB.to_string(), "content": "only you", "flags": 64,
               "target_user_ids": [ALICE.to_string()]}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(created["flags"], 64);
    let audiences: Vec<_> = harness
        .events
        .events
        .lock()
        .unwrap()
        .iter()
        .map(|e| e.audience.clone())
        .collect();
    assert_eq!(audiences, vec![Audience::Users(vec![ALICE])]);
    assert_eq!(harness.messages.last_message_id(CHANNEL).await.unwrap(), None);
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
    assert!(
        text.contains("http_requests_total{method=\"GET\",route=\"/healthz\",status=\"2xx\"} 1"),
        "{text}"
    );
    assert!(text.contains("status=\"4xx\"} 1"), "{text}");
    assert!(text.contains("http_request_duration_seconds_bucket{"), "{text}");
}

#[tokio::test]
async fn history_hydrates_authors_with_one_lookup_per_page() {
    let harness = Harness::new();
    let app = app(&harness);
    for (author, text) in [(ALICE, "one"), (BOB, "two"), (ALICE, "three")] {
        harness.seed(author, text).await;
    }

    let (status, page) = call(&app, Method::GET, &messages_uri(), Some(ALICE_TOKEN), None).await;
    assert_eq!(status, StatusCode::OK);
    let names: Vec<&str> = page
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["author"]["username"].as_str().unwrap())
        .collect();
    assert_eq!(
        names,
        ["alice", "bob", "alice"].iter().rev().copied().collect::<Vec<_>>()
    );
    assert_eq!(page[0]["author"]["discriminator"], "0");
    assert_eq!(harness.users.calls(), 1, "one batched lookup for the whole page");
}

#[tokio::test]
async fn created_edited_and_pinned_messages_carry_the_full_author() {
    let harness = Harness::new();
    harness.access.set_permissions(MEMBER | Permissions::MANAGE_MESSAGES);
    let app = app(&harness);
    let (_, created) = call(
        &app,
        Method::POST,
        &messages_uri(),
        Some(ALICE_TOKEN),
        Some(json!({"content": "hello"})),
    )
    .await;
    assert_eq!(created["author"]["username"], "alice");
    let id = created["id"].as_str().unwrap().to_owned();

    let (_, edited) = call(
        &app,
        Method::PATCH,
        &format!("{}/{id}", messages_uri()),
        Some(ALICE_TOKEN),
        Some(json!({"content": "edited"})),
    )
    .await;
    assert_eq!(edited["author"]["username"], "alice");

    let pin_uri = format!("/api/v1/channels/{CHANNEL}/pins");
    let (status, _) = call(&app, Method::PUT, &format!("{pin_uri}/{id}"), Some(ALICE_TOKEN), None).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (_, pins) = call(&app, Method::GET, &pin_uri, Some(ALICE_TOKEN), None).await;
    assert_eq!(pins[0]["author"]["username"], "alice");
}

#[tokio::test]
async fn an_unavailable_api_degrades_authors_to_ids_without_failing() {
    let harness = Harness::new();
    let app = app(&harness);
    harness.seed(ALICE, "one").await;
    harness.users.set_down(true);

    let (status, page) = call(&app, Method::GET, &messages_uri(), Some(ALICE_TOKEN), None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(page[0]["author"], json!({"id": ALICE.to_string()}));

    let (status, created) = call(
        &app,
        Method::POST,
        &messages_uri(),
        Some(ALICE_TOKEN),
        Some(json!({"content": "still works"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(created["author"], json!({"id": ALICE.to_string()}));
}

#[tokio::test]
async fn internal_webhook_posts_keep_their_stored_username() {
    let harness = Harness::new();
    let app = app(&harness);
    let uri = format!("/internal/v1/channels/{CHANNEL}/messages");
    let (status, posted) = post_internal(
        &app,
        &uri,
        Some(INTERNAL),
        json!({"author_id": BOB.to_string(), "content": "deployed", "webhook_id": "900",
               "username": "Deploy bot"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{posted}");
    assert_eq!(posted["author"]["username"], "Deploy bot");
    assert_eq!(posted["author"]["bot"], true);

    let (_, plain) = post_internal(
        &app,
        &uri,
        Some(INTERNAL),
        json!({"author_id": BOB.to_string(), "content": "from a bot user"}),
    )
    .await;
    assert_eq!(plain["author"]["username"], "bob");
}

// ---- signed attachment downloads -------------------------------------------------

const CDN_PREFIX: &str = "http://cdn/cdn";

async fn fetch(app: &Router, uri: &str, range: Option<&str>) -> (StatusCode, axum::http::HeaderMap, Vec<u8>) {
    let mut request = Request::builder().method(Method::GET).uri(uri);
    if let Some(range) = range {
        request = request.header("range", range);
    }
    let response = app.clone().oneshot(request.body(Body::empty()).unwrap()).await.unwrap();
    let (status, headers) = (response.status(), response.headers().clone());
    let bytes = response.into_body().collect().await.unwrap().to_bytes().to_vec();
    (status, headers, bytes)
}

/// Posts a message with one uploaded `cat.png` and returns its signed URL as a local path.
async fn post_with_attachment(harness: &Harness, app: &Router) -> String {
    harness
        .storage
        .put_body(&format!("attachments/{CHANNEL}/77/cat.png"), b"meow-meow");
    let (status, created) = call(
        app,
        Method::POST,
        &messages_uri(),
        Some(ALICE_TOKEN),
        Some(json!({"content": "look", "attachments": [
            {"id": 0, "uploaded_filename": format!("attachments/{CHANNEL}/77/cat.png")}]})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{created}");
    let url = created["attachments"][0]["url"].as_str().unwrap();
    assert!(
        url.starts_with(&format!("{CDN_PREFIX}/attachments/{CHANNEL}/77/cat.png?ex=")),
        "{url}"
    );
    assert_eq!(created["attachments"][0]["proxy_url"], created["attachments"][0]["url"]);
    url.strip_prefix(CDN_PREFIX).unwrap().to_owned()
}

#[tokio::test]
async fn a_signed_attachment_url_downloads_the_file_without_credentials() {
    let harness = Harness::new();
    harness
        .access
        .set_permissions(Permissions::VIEW_CHANNEL | Permissions::SEND_MESSAGES | Permissions::ATTACH_FILES);
    let app = app(&harness);
    let path = post_with_attachment(&harness, &app).await;

    let (status, headers, body) = fetch(&app, &path, None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, b"meow-meow");
    assert_eq!(headers["content-type"], "image/png");
    assert_eq!(headers["x-content-type-options"], "nosniff");
    assert!(headers["content-disposition"].to_str().unwrap().starts_with("inline;"));
    assert_eq!(headers["cache-control"], "private, max-age=3600");

    let (status, headers, body) = fetch(&app, &path, Some("bytes=5-")).await;
    assert_eq!(status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(body, b"meow");
    assert_eq!(headers["content-range"], "bytes 5-8/9");
}

#[tokio::test]
async fn unsigned_tampered_and_foreign_urls_are_forbidden() {
    let harness = Harness::new();
    harness
        .access
        .set_permissions(Permissions::VIEW_CHANNEL | Permissions::SEND_MESSAGES | Permissions::ATTACH_FILES);
    let app = app(&harness);
    let path = post_with_attachment(&harness, &app).await;
    let (base, query) = path.split_once('?').unwrap();

    for uri in [
        base.to_owned(),
        format!("{base}?ex=ffffffffff&is=0&hm={}", "00".repeat(32)),
        path.replace("hm=", "hm=0"),
        path.replace("/cat.png?", "/dog.png?"),
        path.replacen(&format!("/{CHANNEL}/"), "/1/", 1),
        format!("{base}?{}", query.replacen("ex=", "ex=1", 1)),
    ] {
        let (status, _, _) = fetch(&app, &uri, None).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{uri}");
    }
}

#[tokio::test]
async fn expired_urls_are_gone_and_a_refetched_message_carries_a_fresh_one() {
    let harness = Harness::new();
    harness.access.set_permissions(
        Permissions::VIEW_CHANNEL
            | Permissions::SEND_MESSAGES
            | Permissions::ATTACH_FILES
            | Permissions::READ_MESSAGE_HISTORY,
    );
    let app = app(&harness);
    let old = post_with_attachment(&harness, &app).await;

    harness.clock.advance_ms(3_601_000);
    let (status, _, _) = fetch(&app, &old, None).await;
    assert_eq!(status, StatusCode::GONE);

    let (status, history) = call(&app, Method::GET, &messages_uri(), Some(ALICE_TOKEN), None).await;
    assert_eq!(status, StatusCode::OK, "{history}");
    let fresh = history[0]["attachments"][0]["url"]
        .as_str()
        .unwrap()
        .strip_prefix(CDN_PREFIX)
        .unwrap()
        .to_owned();
    assert_ne!(fresh, old);
    let (status, _, body) = fetch(&app, &fresh, None).await;
    assert_eq!((status, body), (StatusCode::OK, b"meow-meow".to_vec()));
}

#[tokio::test]
async fn a_validly_signed_url_for_a_missing_object_is_not_found() {
    let harness = Harness::new();
    harness
        .access
        .set_permissions(Permissions::VIEW_CHANNEL | Permissions::SEND_MESSAGES | Permissions::ATTACH_FILES);
    let app = app(&harness);
    let path = post_with_attachment(&harness, &app).await;
    harness.storage.bodies.lock().unwrap().clear();
    let (status, _, _) = fetch(&app, &path, None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn refresh_urls_signs_keys_for_members_and_refuses_foreign_channels() {
    let harness = Harness::new();
    harness
        .access
        .set_permissions(Permissions::VIEW_CHANNEL | Permissions::READ_MESSAGE_HISTORY);
    let app = app(&harness);
    let uri = format!("/api/v1/channels/{CHANNEL}/attachments/refresh-urls");
    let key = format!("attachments/{CHANNEL}/77/blob.bin");
    harness.storage.put_body(&key, b"ciphertext");

    let (status, _) = call(&app, Method::POST, &uri, None, Some(json!({"attachment_urls": [key]}))).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    let (status, body) = call(
        &app,
        Method::POST,
        &uri,
        Some(ALICE_TOKEN),
        Some(json!({"attachment_urls": [key.clone()]})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["refreshed_urls"][0]["original"], key);
    let fresh = body["refreshed_urls"][0]["refreshed"].as_str().unwrap();
    let path = fresh.strip_prefix(CDN_PREFIX).expect("under the CDN");
    let (status, _, bytes) = fetch(&app, path, None).await;
    assert_eq!((status, bytes), (StatusCode::OK, b"ciphertext".to_vec()));

    let (status, _) = call(
        &app,
        Method::POST,
        &uri,
        Some(ALICE_TOKEN),
        Some(json!({"attachment_urls": ["attachments/12345/77/blob.bin"]})),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

fn search_uri(query: &str) -> String {
    format!("/api/v1/channels/{CHANNEL}/messages/search?{query}")
}

#[tokio::test]
async fn search_returns_groups_with_the_hit_flag_and_authors() {
    let harness = Harness::new();
    let app = app(&harness);
    for text in ["before it", "needle in a haystack", "after it"] {
        harness.clock.advance_ms(5);
        let message = harness.seed(ALICE, text).await;
        harness
            .search
            .index_message(&message, Some(discordy_messages_application::fakes::GUILD));
    }
    let (status, body) = call(
        &app,
        Method::GET,
        &search_uri("content=needle"),
        Some("Bearer bob"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["total_results"], 1);
    let group = body["messages"][0].as_array().unwrap();
    assert_eq!(group.len(), 3);
    assert_eq!(group[1]["content"], "needle in a haystack");
    assert_eq!(group[1]["hit"], true);
    assert!(group[0].get("hit").is_none());
    assert_eq!(group[1]["author"]["username"], "alice");
    assert!(body.get("index_building").is_none());
}

#[tokio::test]
async fn search_validates_parameters_and_credentials() {
    let harness = Harness::new();
    let app = app(&harness);
    let (status, _) = call(&app, Method::GET, &search_uri("content=x"), None, None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, body) = call(&app, Method::GET, &search_uri("limit=99"), Some("Bearer bob"), None).await;
    assert_eq!((status, body["code"].as_u64()), (StatusCode::BAD_REQUEST, Some(50035)));
    let (status, body) = call(&app, Method::GET, &search_uri("has=gif"), Some("Bearer bob"), None).await;
    assert_eq!((status, body["code"].as_u64()), (StatusCode::BAD_REQUEST, Some(50035)));
}

#[tokio::test]
async fn search_respects_permissions_and_delivery_mode() {
    let harness = Harness::new();
    let app = app(&harness);
    harness.access.set_permissions(Permissions::VIEW_CHANNEL);
    let (status, body) = call(&app, Method::GET, &search_uri("content=x"), Some("Bearer bob"), None).await;
    assert_eq!((status, body["code"].as_u64()), (StatusCode::FORBIDDEN, Some(50001)));
    harness.access.set(Ok(dm_access(DeliveryMode::Ephemeral)));
    let (status, body) = call(&app, Method::GET, &search_uri("content=x"), Some("Bearer bob"), None).await;
    assert_eq!((status, body["code"].as_u64()), (StatusCode::CONFLICT, Some(50160)));
}

#[tokio::test]
async fn guild_search_is_scoped_to_readable_channels_and_rate_limited() {
    let harness = Harness::new();
    let app = app(&harness);
    harness.clock.advance_ms(5);
    let message = harness.seed(ALICE, "guild wide hello").await;
    harness
        .search
        .index_message(&message, Some(discordy_messages_application::fakes::GUILD));
    let uri = format!(
        "/api/v1/guilds/{}/messages/search?content=hello",
        discordy_messages_application::fakes::GUILD
    );
    let (status, body) = call(&app, Method::GET, &uri, Some("Bearer bob"), None).await;
    assert_eq!((status, body["total_results"].as_u64()), (StatusCode::OK, Some(1)));
    for _ in 1..10 {
        call(&app, Method::GET, &uri, Some("Bearer bob"), None).await;
    }
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(&uri)
                .header("authorization", "Bearer bob")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    assert!(response.headers().contains_key("retry-after"));
}

#[tokio::test]
async fn reindex_is_internal_only_and_fills_the_index() {
    let harness = Harness::new();
    let app = app(&harness);
    harness.seed(ALICE, "old history").await;
    let (status, _) = post_internal(&app, "/internal/v1/search/reindex", None, json!({})).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, body) = post_internal(&app, "/internal/v1/search/reindex", Some(INTERNAL), json!({})).await;
    assert_eq!((status, body["started"].as_bool()), (StatusCode::ACCEPTED, Some(true)));
    for _ in 0..50 {
        if harness.search.len() == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(harness.search.len(), 1);
}

#[tokio::test]
async fn attachments_carry_alt_text_and_the_spoiler_flag() {
    let harness = Harness::new();
    harness
        .access
        .set_permissions(Permissions::VIEW_CHANNEL | Permissions::SEND_MESSAGES | Permissions::ATTACH_FILES);
    let app = app(&harness);
    harness
        .storage
        .put_body(&format!("attachments/{CHANNEL}/78/pic.png"), b"x");
    let (status, created) = call(
        &app,
        Method::POST,
        &messages_uri(),
        Some(ALICE_TOKEN),
        Some(json!({"content": "", "attachments": [
            {"id": 0, "uploaded_filename": format!("attachments/{CHANNEL}/78/pic.png"),
             "description": "a red pic", "flags": 8}]})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{created}");
    assert_eq!(created["attachments"][0]["description"], "a red pic");
    assert_eq!(created["attachments"][0]["flags"], 8);
}

#[tokio::test]
async fn patching_flags_suppresses_and_restores_embeds() {
    let harness = Harness::new();
    let app = app(&harness);
    let (_, created) = call(
        &app,
        Method::POST,
        &messages_uri(),
        Some(ALICE_TOKEN),
        Some(json!({"content": "hi"})),
    )
    .await;
    let uri = format!("{}/{}", messages_uri(), created["id"].as_str().unwrap());
    let (status, patched) = call(&app, Method::PATCH, &uri, Some(ALICE_TOKEN), Some(json!({"flags": 4}))).await;
    assert_eq!(status, StatusCode::OK, "{patched}");
    assert_eq!(patched["flags"], 4);
    assert_eq!(patched["edited_timestamp"], Value::Null);
    let (_, restored) = call(&app, Method::PATCH, &uri, Some(ALICE_TOKEN), Some(json!({"flags": 0}))).await;
    assert_eq!(restored["flags"], 0);
    let (status, _) = call(&app, Method::PATCH, &uri, Some("Bearer bob"), Some(json!({"flags": 4}))).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn embeds_reach_clients_with_media_proxy_urls_and_the_proxy_serves_them() {
    let harness = Harness::new();
    let app = app(&harness);
    harness.media.put("https://img.example/a.png", "image/png", b"PNGDATA");
    let (status, created) = call(
        &app,
        Method::POST,
        &messages_uri(),
        Some(ALICE_TOKEN),
        Some(json!({"content": "hi", "embeds": [
            {"type": "link", "url": "https://site.example/", "thumbnail": {"url": "https://img.example/a.png"}}]})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{created}");
    let thumb = &created["embeds"][0]["thumbnail"];
    assert_eq!(thumb["url"], "https://img.example/a.png");
    let proxy = thumb["proxy_url"].as_str().unwrap();
    assert!(proxy.starts_with(&format!("{CDN_PREFIX}/external/")), "{proxy}");

    let path = proxy.strip_prefix(CDN_PREFIX).unwrap();
    let (status, headers, body) = fetch(&app, path, None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, b"PNGDATA");
    assert_eq!(headers["content-type"], "image/png");
    assert_eq!(headers["x-content-type-options"], "nosniff");
    assert!(headers["content-security-policy"].to_str().unwrap().contains("sandbox"));
}

#[tokio::test]
async fn the_media_proxy_refuses_unsigned_tampered_and_forbidden_requests() {
    let harness = Harness::new();
    let app = app(&harness);
    let token = |url: &str| {
        use base64::Engine;
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(url)
    };
    let (status, _, _) = fetch(&app, &format!("/external/{}", token("https://img.example/a.png")), None).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "unsigned");

    // A signed URL retargeted at the metadata address keeps its old signature.
    let signed = AttachmentUrls::new(
        "http://cdn/cdn",
        SIGNING_KEY,
        Duration::from_secs(3600),
        harness.clock.clone(),
    )
    .proxy_url("https://img.example/a.png");
    let (_, query) = signed.split_once('?').unwrap();
    let forged = format!(
        "/external/{}?{query}",
        token("http://169.254.169.254/latest/meta-data/")
    );
    let (status, _, _) = fetch(&app, &forged, None).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "tampered");
    assert!(
        harness.media.opened.lock().unwrap().is_empty(),
        "nothing was fetched upstream"
    );

    // A properly signed request for a forbidden address is still refused by the guard.
    let signed_bad = AttachmentUrls::new(
        "http://cdn/cdn",
        SIGNING_KEY,
        Duration::from_secs(3600),
        harness.clock.clone(),
    )
    .proxy_url("http://169.254.169.254/latest/meta-data/");
    let (status, _, _) = fetch(&app, signed_bad.strip_prefix("http://cdn/cdn").unwrap(), None).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "guard");
    let signed_missing = AttachmentUrls::new(
        "http://cdn/cdn",
        SIGNING_KEY,
        Duration::from_secs(3600),
        harness.clock.clone(),
    )
    .proxy_url("https://img.example/gone.png");
    let (status, _, _) = fetch(&app, signed_missing.strip_prefix("http://cdn/cdn").unwrap(), None).await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
}

#[tokio::test]
async fn reaction_users_can_be_listed_and_moderated() {
    let harness = Harness::new();
    harness.access.set_permissions(MEMBER | Permissions::MANAGE_MESSAGES);
    let message = harness.seed(BOB, "react to me").await;
    let app = app(&harness);
    let base = format!("{}/{}/reactions/%F0%9F%91%8D", messages_uri(), message.id);

    let (status, _) = call(&app, Method::PUT, &format!("{base}/@me"), Some(ALICE_TOKEN), None).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, users) = call(&app, Method::GET, &base, Some(ALICE_TOKEN), None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(users.as_array().unwrap().len(), 1);

    let (status, _) = call(
        &app,
        Method::DELETE,
        &format!("{base}/{}", BOB),
        Some(ALICE_TOKEN),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, _) = call(&app, Method::DELETE, &base, Some(ALICE_TOKEN), None).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (_, users) = call(&app, Method::GET, &base, Some(ALICE_TOKEN), None).await;
    assert!(users.as_array().unwrap().is_empty());
}

#[tokio::test]
async fn forwarding_sends_a_snapshot_with_a_forward_reference_and_downloadable_copies() {
    let harness = Harness::new();
    let app = app(&harness);
    let original = post_with_attachment(&harness, &app).await;
    assert!(original.contains("/77/"));
    let (_, history) = call(&app, Method::GET, &messages_uri(), Some(ALICE_TOKEN), None).await;
    let source_id = history[0]["id"].as_str().unwrap().to_owned();
    let target = discordy_messages_domain::Snowflake::new(2 << 22);

    let (status, forwarded) = call(
        &app,
        Method::POST,
        &format!("/api/v1/channels/{target}/messages"),
        Some(ALICE_TOKEN),
        Some(json!({"content": "fyi", "message_reference": {
            "type": 1, "message_id": source_id, "channel_id": CHANNEL.to_string()}})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{forwarded}");
    assert_eq!(forwarded["content"], "fyi");
    assert_eq!(forwarded["type"], 0);
    assert_eq!(forwarded["message_reference"]["type"], 1);
    assert_eq!(forwarded["message_reference"]["channel_id"], CHANNEL.to_string());
    assert_eq!(forwarded["message_reference"]["message_id"], source_id);
    let snapshot = &forwarded["message_snapshots"][0]["message"];
    assert_eq!(snapshot["content"], "look");
    let url = snapshot["attachments"][0]["url"].as_str().unwrap();
    assert!(url.contains(&format!("/attachments/{target}/")), "{url}");
    let (status, _, body) = fetch(&app, url.strip_prefix(CDN_PREFIX).unwrap(), None).await;
    assert_eq!((status, body), (StatusCode::OK, b"meow-meow".to_vec()));

    // The snapshot survives in history and a plain message has no snapshot field.
    let (_, history) = call(
        &app,
        Method::GET,
        &format!("/api/v1/channels/{target}/messages"),
        Some(ALICE_TOKEN),
        None,
    )
    .await;
    assert_eq!(history[0]["message_snapshots"][0]["message"]["content"], "look");
    let (_, source) = call(&app, Method::GET, &messages_uri(), Some(ALICE_TOKEN), None).await;
    assert!(source[0].get("message_snapshots").is_none());
}

#[tokio::test]
async fn forwarding_validates_the_reference_and_permissions() {
    let harness = Harness::new();
    let app = app(&harness);
    let uri = messages_uri();
    let (_, source) = call(
        &app,
        Method::POST,
        &uri,
        Some(ALICE_TOKEN),
        Some(json!({"content": "x"})),
    )
    .await;
    let id = source["id"].as_str().unwrap();
    let (status, _) = call(
        &app,
        Method::POST,
        &uri,
        Some(ALICE_TOKEN),
        Some(json!({"message_reference": {"type": 1, "message_id": id}})),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "a forward needs its source channel");
    let (status, _) = call(
        &app,
        Method::POST,
        &uri,
        Some(ALICE_TOKEN),
        Some(json!({"message_reference": {"type": 7, "message_id": id, "channel_id": CHANNEL.to_string()}})),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    harness.access.set(Ok(dm_access(DeliveryMode::P2p)));
    let (status, body) = call(
        &app,
        Method::POST,
        &uri,
        Some(ALICE_TOKEN),
        Some(json!({"message_reference": {"type": 1, "message_id": id, "channel_id": CHANNEL.to_string()}})),
    )
    .await;
    assert_eq!((status, body["code"].as_u64()), (StatusCode::CONFLICT, Some(50160)));
}

#[tokio::test]
async fn internal_get_message_returns_the_stored_message() {
    let harness = Harness::new();
    let message = harness.seed(BOB, "report me").await;
    let app = app(&harness);
    let uri = format!("/internal/v1/channels/{CHANNEL}/messages/{}", message.id);
    let get = |uri: &str, token: Option<&str>| {
        let mut request = Request::builder().method(Method::GET).uri(uri.to_owned());
        if let Some(token) = token {
            request = request.header("x-internal-token", token);
        }
        request.body(Body::empty()).unwrap()
    };

    let (status, _) = send(&app, get(&uri, None)).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    let (status, body) = send(&app, get(&uri, Some(INTERNAL))).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["content"], "report me");
    assert_eq!(body["author"]["id"], BOB.to_string());

    let missing = format!("/internal/v1/channels/{CHANNEL}/messages/{}", message.id.value() + 1);
    let (status, _) = send(&app, get(&missing, Some(INTERNAL))).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

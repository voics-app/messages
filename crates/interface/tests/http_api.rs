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
use discordy_messages_interface::{AppState, AttachmentUrls, InternalToken, ReadinessCheck, router};
use http_body_util::BodyExt;
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
    assert!(text.contains("messages_http_requests_total{status=\"2xx\"} 1"));
    assert!(text.contains("messages_http_requests_total{status=\"4xx\"} 1"));
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

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use axum::{
    Router,
    body::Body,
    http::{Method, Request, StatusCode},
};
use discordy_messages_application::{
    fakes::{ALICE, BOB, CHANNEL, Harness, dm_access},
    ports::{AuthError, Authenticator, Clock, Principal},
    scheduled::{
        ScheduledMessages, ScheduledPorts,
        fakes::{MemorySchedules, SequenceAttempts},
    },
    use_cases::CreateMessage,
};
use discordy_messages_domain::DeliveryMode;
use discordy_messages_interface::{AppState, AttachmentUrls, InternalToken, metrics::Metrics, router};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tower::ServiceExt;

#[path = "scheduled_http/recovery.rs"]
mod recovery;
#[path = "scheduled_http/states.rs"]
mod states;

const REQUEST: &str = "d71cf4c0-2a63-49b5-85de-3c3cbe407820";

struct Auth;
#[async_trait]
impl Authenticator for Auth {
    async fn authenticate(&self, header: &str) -> Result<Principal, AuthError> {
        match header {
            "Bearer alice" => Ok(Principal {
                user_id: ALICE,
                is_bot: false,
            }),
            "Bearer bob" => Ok(Principal {
                user_id: BOB,
                is_bot: false,
            }),
            "Bot app" => Ok(Principal {
                user_id: ALICE,
                is_bot: true,
            }),
            _ => Err(AuthError::Invalid),
        }
    }
}

struct Fixture {
    h: Harness,
    app: Router,
    memory: Arc<MemorySchedules>,
}
impl Fixture {
    fn new(enabled: bool) -> Self {
        let h = Harness::new();
        let memory = Arc::new(MemorySchedules::default());
        let service = Arc::new(ScheduledMessages::new(ScheduledPorts {
            journal: memory.clone(),
            index: memory.clone(),
            delivery: Arc::new(CreateMessage::new(&h.ports)),
            attempts: Arc::new(SequenceAttempts::default()),
            clock: h.clock.clone(),
        }));
        let metrics = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let state = AppState::new(
            &h.ports,
            Arc::new(Auth),
            InternalToken::new("test-internal"),
            AttachmentUrls::new(
                "http://cdn.test",
                "fixture-signing-key-at-least-thirty-two-characters",
                Duration::from_secs(60),
                h.clock.clone(),
            ),
            h.media.clone(),
            vec![],
            Metrics::new(metrics.handle()),
        );
        Self {
            app: router(if enabled { state.with_scheduled(service) } else { state }),
            h,
            memory,
        }
    }
    fn create(&self) -> Value {
        let due =
            OffsetDateTime::from_unix_timestamp_nanos(i128::from(self.h.clock.now_unix_ms() + 120_000) * 1_000_000)
                .unwrap()
                .format(&Rfc3339)
                .unwrap();
        json!({"request_id": REQUEST, "scheduled_at": due, "message": {"content": "later"}})
    }
}

fn base() -> String {
    format!("/api/v1/channels/{CHANNEL}/messages/scheduled")
}
fn item() -> String {
    format!("{}/{REQUEST}", base())
}

async fn call(
    app: &Router,
    method: Method,
    path: &str,
    auth: Option<&str>,
    body: Option<Value>,
) -> (StatusCode, Value) {
    let mut request = Request::builder().method(method).uri(path);
    if let Some(auth) = auth {
        request = request.header("authorization", auth);
    }
    let body = body.map_or_else(Body::empty, |body| {
        request = std::mem::take(&mut request).header("content-type", "application/json");
        Body::from(body.to_string())
    });
    let response = app.clone().oneshot(request.body(body).unwrap()).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap_or(Value::Null))
}

#[tokio::test]
async fn creates_replays_edits_and_cancels_the_same_author_resource() {
    let f = Fixture::new(true);
    let original = f.create();
    let (status, first) = call(
        &f.app,
        Method::POST,
        &base(),
        Some("Bearer alice"),
        Some(original.clone()),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{first}");
    assert_eq!(first["revision"], 1);
    assert_eq!(first["status"], "pending");
    assert_eq!(first["message"]["replied_user"], true);
    assert!(first["message_id"].is_null());
    let (status, edited) = call(
        &f.app,
        Method::PATCH,
        &item(),
        Some("Bearer alice"),
        Some(json!({"expected_revision":1,"message":{"content":"edited","silent":true}})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{edited}");
    assert_eq!(edited["revision"], 2);
    let (status, replay) = call(&f.app, Method::POST, &base(), Some("Bearer alice"), Some(original)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(replay, edited);
    assert_eq!(
        call(
            &f.app,
            Method::PATCH,
            &item(),
            Some("Bearer alice"),
            Some(json!({"expected_revision":1,"message":{"content":"stale"}}))
        )
        .await
        .0,
        StatusCode::CONFLICT
    );
    let (status, cancelled) = call(
        &f.app,
        Method::DELETE,
        &format!("{}?expected_revision=2", item()),
        Some("Bearer alice"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(cancelled["revision"], 3);
    assert!(cancelled["message"].is_null());
    assert_eq!(
        call(
            &f.app,
            Method::DELETE,
            &format!("{}?expected_revision=2", item()),
            Some("Bearer alice"),
            None
        )
        .await
        .1,
        cancelled
    );
}

#[tokio::test]
async fn auth_privacy_disable_and_private_modes_fail_closed() {
    let f = Fixture::new(true);
    assert_eq!(
        call(&f.app, Method::POST, &base(), None, Some(f.create())).await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        call(&f.app, Method::POST, &base(), Some("Bot app"), Some(f.create()))
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    call(&f.app, Method::POST, &base(), Some("Bearer alice"), Some(f.create())).await;
    assert_eq!(
        call(&f.app, Method::GET, &item(), Some("Bearer bob"), None).await.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(&f.app, Method::GET, &base(), Some("Bearer bob"), None).await.1["items"],
        json!([])
    );
    assert_eq!(
        call(
            &f.app,
            Method::GET,
            &item().replace(&CHANNEL.to_string(), "999"),
            Some("Bearer alice"),
            None
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    let disabled = Fixture::new(false);
    assert_eq!(
        call(
            &disabled.app,
            Method::POST,
            &base(),
            Some("Bearer alice"),
            Some(disabled.create())
        )
        .await
        .0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    for mode in [DeliveryMode::Ephemeral, DeliveryMode::P2p] {
        let f = Fixture::new(true);
        f.h.access.set(Ok(dm_access(mode)));
        let (status, body) = call(&f.app, Method::POST, &base(), Some("Bearer alice"), Some(f.create())).await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(body["code"], 50160);
    }
}

#[tokio::test]
async fn own_management_and_original_replay_survive_membership_loss() {
    let f = Fixture::new(true);
    let original = f.create();
    call(
        &f.app,
        Method::POST,
        &base(),
        Some("Bearer alice"),
        Some(original.clone()),
    )
    .await;
    f.h.access.set_permissions(0);
    assert_eq!(
        call(&f.app, Method::GET, &item(), Some("Bearer alice"), None).await.0,
        StatusCode::OK
    );
    assert_eq!(
        call(&f.app, Method::GET, &base(), Some("Bearer alice"), None).await.1["items"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        call(&f.app, Method::POST, &base(), Some("Bearer alice"), Some(original))
            .await
            .0,
        StatusCode::OK
    );
    assert_eq!(
        call(
            &f.app,
            Method::DELETE,
            &format!("{}?expected_revision=1", item()),
            Some("Bearer alice"),
            None
        )
        .await
        .0,
        StatusCode::OK
    );
}

#[tokio::test]
async fn invalid_and_unsupported_fields_are_rejected_before_any_acceptance() {
    let f = Fixture::new(true);
    let invalid = [
        json!({"request_id":REQUEST.to_uppercase(),"scheduled_at":f.create()["scheduled_at"],"message":{"content":"x"}}),
        json!({"request_id":REQUEST,"scheduled_at":f.create()["scheduled_at"],"message":{"content":" "}}),
        json!({"request_id":REQUEST,"scheduled_at":f.create()["scheduled_at"],"message":{"content":"x","attachments":[]}}),
        json!({"request_id":REQUEST,"scheduled_at":f.create()["scheduled_at"],"message":{"content":"x"},"author_id":"11"}),
        json!({"request_id":REQUEST,"scheduled_at":"2020-01-01T00:00:00Z","message":{"content":"x"}}),
    ];
    for body in invalid {
        assert_eq!(
            call(&f.app, Method::POST, &base(), Some("Bearer alice"), Some(body))
                .await
                .0,
            StatusCode::BAD_REQUEST
        );
    }
    call(&f.app, Method::POST, &base(), Some("Bearer alice"), Some(f.create())).await;
    for body in [
        json!({"expected_revision":1}),
        json!({"expected_revision":2147483648u64,"message":{"content":"x"}}),
        json!({"expected_revision":1,"message":null,"scheduled_at":f.create()["scheduled_at"]}),
    ] {
        assert_eq!(
            call(&f.app, Method::PATCH, &item(), Some("Bearer alice"), Some(body))
                .await
                .0,
            StatusCode::BAD_REQUEST
        );
    }
}

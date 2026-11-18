//! A refresh must not inherit authorization observed before its invocation.
use axum::{Json, Router, extract::State, http::StatusCode, routing::get};
use discordy_messages_application::ports::{AccessError, ChannelAccessProvider};
use discordy_messages_domain::Snowflake;
use discordy_messages_infrastructure::api::{HttpChannelAccess, InternalApi};
use serde_json::{Value, json};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;
use tokio::sync::Notify;

#[derive(Default)]
struct LookupGate {
    calls: AtomicUsize,
    first_started: Notify,
    release_first: Notify,
}

async fn lookup(State(gate): State<Arc<LookupGate>>) -> (StatusCode, Json<Value>) {
    if gate.calls.fetch_add(1, Ordering::SeqCst) == 0 {
        gate.first_started.notify_one();
        gate.release_first.notified().await;
        (
            StatusCode::OK,
            Json(json!({ "permissions": "3072", "guild_id": "500", "channel_type": 0,
            "delivery_mode": "standard", "recipient_ids": [] })),
        )
    } else {
        (StatusCode::FORBIDDEN, Json(Value::Null))
    }
}

#[tokio::test]
async fn uncached_refresh_does_not_join_an_older_in_flight_permission_lookup() {
    let gate = Arc::new(LookupGate::default());
    let app = Router::new()
        .route("/internal/v1/channels/{channel}/access/{user}", get(lookup))
        .with_state(gate.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let provider = Arc::new(HttpChannelAccess::new(InternalApi::new(
        reqwest::Client::new(),
        &format!("http://{address}"),
        "local-fixture-only".into(),
    )));
    let old_provider = provider.clone();
    let old = tokio::spawn(async move { old_provider.access(Snowflake::new(1), Snowflake::new(10)).await });
    gate.first_started.notified().await;
    let fresh = tokio::time::timeout(
        Duration::from_secs(1),
        provider.refresh(Snowflake::new(1), Snowflake::new(10)),
    )
    .await;
    gate.release_first.notify_one();
    old.await.unwrap().unwrap();
    server.abort();
    assert_eq!(
        fresh.expect("refresh must issue its own uncached HTTP request"),
        Err(AccessError::NotMember)
    );
    assert_eq!(gate.calls.load(Ordering::SeqCst), 2);
}

//! Real HTTP boundary for contracts#44: current policy and fail-closed decoding.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    routing::get,
};
use discordy_messages_application::ports::{NotificationRecipients, RecipientQuery};
use discordy_messages_domain::{NotificationLevel, Snowflake};
use discordy_messages_infrastructure::api::{HttpNotificationRecipients, InternalApi};
use serde_json::{Value, json};

#[derive(Clone)]
struct Fixture {
    response: Arc<Mutex<(StatusCode, Value)>>,
    calls: Arc<Mutex<Vec<HashMap<String, String>>>>,
}

async fn handler(
    State(state): State<Fixture>,
    headers: HeaderMap,
    Path(channel): Path<String>,
    Query(query): Query<HashMap<String, String>>,
) -> (StatusCode, Json<Value>) {
    assert_eq!(channel, "200");
    assert_eq!(headers.get("x-internal-token").unwrap(), "notification-test-token");
    state.calls.lock().unwrap().push(query);
    let (status, body) = state.response.lock().unwrap().clone();
    (status, Json(body))
}

struct Server {
    state: Fixture,
    directory: HttpNotificationRecipients,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn server() -> Server {
    // Byte-for-byte values of contracts#44 notification-recipients-valid fixture.
    let body = json!({"channel_id":"200", "guild_id":"100", "delivery_mode":"standard", "evaluated_at":"2026-11-18T10:00:00Z", "next_after":"20", "recipients":[{"user_id":"20", "role_ids":["300"], "online":false, "level":1, "muted":false, "suppress_everyone":true, "suppress_roles":false}]});
    let state = Fixture {
        response: Arc::new(Mutex::new((StatusCode::OK, body))),
        calls: Arc::default(),
    };
    let router = Router::new()
        .route("/internal/v1/channels/{channel}/notification-recipients", get(handler))
        .with_state(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let api = InternalApi::new(
        reqwest::Client::new(),
        &format!("http://{address}"),
        "notification-test-token".into(),
    );
    Server {
        state,
        directory: HttpNotificationRecipients(api),
        task,
    }
}

fn query() -> RecipientQuery {
    RecipientQuery {
        channel_id: Snowflake::new(200),
        author_id: Snowflake::new(10),
        after: Some(Snowflake::new(19)),
        limit: 1,
    }
}

#[tokio::test]
async fn permitted_tokenless_recipients_are_fetched_without_caching_policy() {
    let server = server().await;
    let page = server.directory.page(query()).await.unwrap();
    assert_eq!(page.next_after, Some(Snowflake::new(20)));
    let recipient = &page.recipients[0];
    assert_eq!(recipient.user_id, Snowflake::new(20));
    assert_eq!(recipient.role_ids, vec![Snowflake::new(300)]);
    assert_eq!(recipient.level, NotificationLevel::OnlyMentions);
    assert!(recipient.suppress_everyone);
    server.state.response.lock().unwrap().1["recipients"][0]["muted"] = json!(true);
    assert!(server.directory.page(query()).await.unwrap().recipients[0].muted);
    let calls = server.state.calls.lock().unwrap();
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[0]["author_id"], "10");
    assert_eq!(calls[0]["after"], "19");
    assert_eq!(calls[0]["limit"], "1");
}

#[tokio::test]
async fn empty_filtered_pages_and_private_terminal_pages_remain_distinct() {
    let server = server().await;
    server.state.response.lock().unwrap().1["recipients"] = json!([]);
    let page = server.directory.page(query()).await.unwrap();
    assert!(page.recipients.is_empty());
    assert_eq!(page.next_after, Some(Snowflake::new(20)));
    {
        let mut response = server.state.response.lock().unwrap();
        response.1["delivery_mode"] = json!("p2p");
        response.1["next_after"] = Value::Null;
    }
    let page = server.directory.page(query()).await.unwrap();
    assert!(page.recipients.is_empty() && page.next_after.is_none());
}

#[tokio::test]
async fn malformed_policy_never_produces_an_unfiltered_recipient() {
    let server = server().await;
    let original = server.state.response.lock().unwrap().1.clone();
    let patches = [
        ("/channel_id", json!("201")),
        ("/recipients/0/user_id", json!("10")),
        ("/recipients/0/user_id", json!("19")),
        ("/recipients/0/user_id", json!("21")),
        ("/recipients/0/role_ids/0", json!("invalid")),
        ("/recipients/0/level", json!(3)),
        ("/next_after", json!("19")),
        ("/next_after", json!("18446744073709551615")),
        ("/delivery_mode", json!("unknown")),
        ("/delivery_mode", json!("ephemeral")),
        ("/evaluated_at", json!("invalid")),
        (
            "/recipients",
            json!([original["recipients"][0], original["recipients"][0]]),
        ),
    ];
    for (path, value) in patches {
        let mut body = original.clone();
        *body.pointer_mut(path).unwrap() = value;
        server.state.response.lock().unwrap().1 = body;
        assert!(server.directory.page(query()).await.is_err(), "{path}");
    }
    for missing in ["guild_id", "next_after", "evaluated_at"] {
        let mut body = original.clone();
        body.as_object_mut().unwrap().remove(missing);
        server.state.response.lock().unwrap().1 = body;
        assert!(server.directory.page(query()).await.is_err(), "missing {missing}");
    }
    let mut with_device_data = original;
    with_device_data["recipients"][0]["tokens"] = json!([]);
    server.state.response.lock().unwrap().1 = with_device_data;
    assert!(server.directory.page(query()).await.is_err());
}

#[tokio::test]
async fn unavailable_policy_is_observable_and_retried_on_the_next_request() {
    let server = server().await;
    server.state.response.lock().unwrap().0 = StatusCode::INTERNAL_SERVER_ERROR;
    assert!(server.directory.page(query()).await.is_err());
    server.state.response.lock().unwrap().0 = StatusCode::OK;
    assert_eq!(server.directory.page(query()).await.unwrap().recipients.len(), 1);
    assert_eq!(server.state.calls.lock().unwrap().len(), 2);
    let unavailable = HttpNotificationRecipients(InternalApi::new(
        reqwest::Client::new(),
        "http://127.0.0.1:1",
        "notification-test-token".into(),
    ));
    assert!(unavailable.page(query()).await.is_err());
}

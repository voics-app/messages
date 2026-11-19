//! The notifier end to end without Redpanda: a MESSAGE_CREATE envelope goes through the real api
//! client and APNs sender, against fake api and APNs hosts, and the requests that arrive are the
//! contract (who is pushed, which host, what the push says, which token is removed).

use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Router;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{delete, get, post};
use contracts::Envelope;
use notifier::application::{DispatchConfig, Dispatcher, EventHandler, HandlerConfig};
use notifier::infrastructure::api_client::ApiClient;
use notifier::infrastructure::apns::{ApnsConfig, ApnsSender};
use p256::pkcs8::{EncodePrivateKey, LineEnding};
use serde_json::{Value, json};

#[derive(Clone, Default)]
struct Record(Arc<Mutex<Vec<String>>>);

impl Record {
    fn add(&self, line: String) {
        self.0.lock().unwrap().push(line);
    }

    fn lines(&self) -> Vec<String> {
        let mut lines = self.0.lock().unwrap().clone();
        lines.sort();
        lines
    }
}

async fn serve(app: Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    url
}

fn target(user: &str, level: u8, tokens: Value) -> Value {
    json!({"user_id": user, "role_ids": [], "online": false, "level": level, "muted": false,
           "suppress_everyone": false, "suppress_roles": false, "tokens": tokens})
}

async fn fake_api(log: Record, delivery_mode: &'static str) -> String {
    let phone = json!([{"platform": "apns", "token": "aa11", "environment": "sandbox", "previews": true}]);
    let store = json!([{"platform": "apns", "token": "bb22", "environment": "production", "previews": true}]);
    let dead = json!([{"platform": "apns", "token": "dead", "environment": "sandbox", "previews": true}]);
    let mut body = json!({
        "guild_id": "1", "guild_name": "Test Guild", "channel_name": "general", "delivery_mode": delivery_mode,
        "targets": [target("1", 0, phone), target("2", 0, store), target("3", 2, dead.clone()), target("4", 0, dead)]
    });
    // PushKit tokens: user 1 has a sandbox one, user 2 a production one, user 4 a dead one.
    body["targets"][0]["voip_tokens"] = json!([{"token": "cc33", "environment": "sandbox"}]);
    body["targets"][1]["voip_tokens"] = json!([{"token": "dd44", "environment": "production"}]);
    body["targets"][3]["voip_tokens"] = json!([{"token": "dead", "environment": "sandbox"}]);
    async fn targets(State((_, body)): State<(Record, Value)>, Path(_): Path<String>) -> axum::Json<Value> {
        axum::Json(body)
    }
    async fn forget(State((log, _)): State<(Record, Value)>, axum::Json(body): axum::Json<Value>) -> StatusCode {
        log.add(format!("forget {}", body["token"].as_str().unwrap()));
        StatusCode::NO_CONTENT
    }
    async fn event_targets() -> axum::Json<Value> {
        axum::Json(
            json!({"users":[{"user_id":"1","tokens":[{"platform":"apns","token":"aa11","environment":"sandbox","previews":false,"only_mentions":false}]}],"next_after":null}),
        )
    }
    serve(
        Router::new()
            .route("/internal/v1/channels/{id}/push-targets", get(targets))
            .route("/internal/v1/push-tokens", delete(forget))
            .route(
                "/internal/v1/guilds/{guild}/scheduled-events/{event}/push-targets",
                get(event_targets),
            )
            .with_state((log, body)),
    )
    .await
}

#[tokio::test]
async fn event_start_reaches_the_recipient_with_generic_text() {
    let started = time::OffsetDateTime::now_utc() - time::Duration::seconds(1);
    let millis = i64::try_from(started.unix_timestamp_nanos() / 1_000_000).unwrap();
    let start = started.format(&time::format_description::well_known::Rfc3339).unwrap();
    let mut event = envelope("");
    event.kind = "GUILD_SCHEDULED_EVENT_START".into();
    event.target_user_ids = Some(vec!["1".into()]);
    event.data = json!({"guild_id":"1", "guild_scheduled_event_id":"9", "scheduled_start_time":start, "revision":0, "dedupe_key":format!("1:9:{millis}:0:1")});
    let sent = run_event("standard", event, 1).await;
    assert_eq!(
        sent,
        vec!["sandbox aa11 topic=nl.example.app title=Event starting body=An event you are interested in is starting."]
    );
}

async fn fake_apns(host: &'static str, log: Record) -> String {
    async fn device(
        State((host, log)): State<(&'static str, Record)>,
        Path(token): Path<String>,
        headers: HeaderMap,
        axum::Json(body): axum::Json<Value>,
    ) -> (StatusCode, String) {
        if headers["apns-push-type"] == "voip" {
            log.add(format!(
                "{host} {token} voip topic={} expiration={} type={} name={} text={}",
                headers["apns-topic"].to_str().unwrap(),
                headers["apns-expiration"].to_str().unwrap(),
                body["type"].as_str().unwrap(),
                body["caller_name"].as_str().unwrap_or("-"),
                body.get("content").is_some()
            ));
            if token == "dead" {
                return (StatusCode::GONE, json!({"reason": "Unregistered"}).to_string());
            }
            return (StatusCode::OK, String::new());
        }
        log.add(format!(
            "{host} {token} topic={} title={} body={}",
            headers["apns-topic"].to_str().unwrap(),
            body["aps"]["alert"]["title"].as_str().unwrap(),
            body["aps"]["alert"]["body"].as_str().unwrap_or("-"),
        ));
        if token == "dead" {
            return (StatusCode::GONE, json!({"reason": "Unregistered"}).to_string());
        }
        (StatusCode::OK, String::new())
    }
    serve(
        Router::new()
            .route("/3/device/{token}", post(device))
            .with_state((host, log)),
    )
    .await
}

fn envelope(content: &str) -> Envelope {
    serde_json::from_value(json!({
        "id": "evt-1", "type": "MESSAGE_CREATE", "v": 1,
        "occurred_at": time::OffsetDateTime::now_utc()
            .format(&time::format_description::well_known::Rfc3339).unwrap(),
        "guild_id": "1", "channel_id": "10", "target_user_ids": null,
        "data": {
            "id": "100", "channel_id": "10", "guild_id": "1",
            "author": {"id": "7", "username": "alice", "global_name": "Alice"},
            "content": content, "timestamp": "2026-11-05T10:00:00Z", "edited_timestamp": null,
            "tts": false, "mention_everyone": false, "mentions": [], "mention_roles": [],
            "attachments": [], "embeds": [], "pinned": false, "type": 0, "flags": 0, "components": []
        }
    }))
    .unwrap()
}

fn call_envelope() -> Envelope {
    serde_json::from_value(json!({
        "id": "evt-call", "type": "MESSAGE_CREATE", "v": 1,
        "occurred_at": time::OffsetDateTime::now_utc()
            .format(&time::format_description::well_known::Rfc3339).unwrap(),
        "guild_id": null, "channel_id": "10", "target_user_ids": null,
        "data": {
            "id": "101", "channel_id": "10", "guild_id": null,
            "author": {"id": "7", "username": "alice", "global_name": "Alice"},
            "content": "top secret", "timestamp": "2026-11-05T10:00:00Z", "edited_timestamp": null,
            "tts": false, "mention_everyone": false, "mentions": [], "mention_roles": [],
            "attachments": [], "embeds": [], "pinned": false, "type": 3, "flags": 0, "components": [],
            "call": {"participants": ["7"]}
        }
    }))
    .unwrap()
}

async fn run(delivery_mode: &'static str, content: &str) -> Vec<String> {
    run_event(delivery_mode, envelope(content), 3).await
}

async fn run_event(delivery_mode: &'static str, event: Envelope, lines_wanted: usize) -> Vec<String> {
    let log = Record::default();
    let api_url = fake_api(log.clone(), delivery_mode).await;
    let production = fake_apns("production", log.clone()).await;
    let sandbox = fake_apns("sandbox", log.clone()).await;
    let key = p256::SecretKey::random(&mut rand_core::OsRng)
        .to_pkcs8_pem(LineEnding::LF)
        .unwrap();
    let mut apns = ApnsConfig::new(
        key.to_string(),
        "KEYID12345".into(),
        "TEAMID1234".into(),
        "nl.example.app".into(),
    );
    apns.production_url = production;
    apns.sandbox_url = sandbox;
    let api = Arc::new(ApiClient::new(&api_url, "secret".into()).unwrap());
    let dispatcher = Dispatcher::spawn(
        DispatchConfig {
            queue_capacity: 100,
            max_in_flight: 10,
            max_attempts: 2,
            base_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(5),
        },
        Arc::new(ApnsSender::new(apns).unwrap()),
        api.clone(),
    );
    let handler = EventHandler::new(
        api,
        dispatcher,
        HandlerConfig {
            max_event_age: Duration::from_secs(60),
            dedupe_capacity: 100,
        },
        Arc::new(|| {
            i64::try_from(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis(),
            )
            .unwrap()
        }),
    );
    handler.handle(&event).await;
    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if log.lines().len() >= lines_wanted {
            break;
        }
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    log.lines()
}

#[tokio::test]
async fn a_standard_message_reaches_the_right_hosts_and_a_dead_token_is_removed() {
    let lines = run("standard", "see you at 8").await;
    assert_eq!(
        lines,
        vec![
            "forget dead",
            "production bb22 topic=nl.example.app title=Alice (#general, Test Guild) body=see you at 8",
            "sandbox aa11 topic=nl.example.app title=Alice (#general, Test Guild) body=see you at 8",
            "sandbox dead topic=nl.example.app title=Alice (#general, Test Guild) body=see you at 8",
        ],
        "user 3 has level nothing and is not pushed; user 4's dead token is sent once and removed"
    );
}

#[tokio::test]
async fn an_encrypted_channel_sends_only_the_generic_text_through_the_whole_pipeline() {
    let lines = run("ephemeral", "the password is hunter2").await;
    assert!(!lines.is_empty());
    for line in lines.iter().filter(|line| !line.starts_with("forget")) {
        assert!(line.ends_with("title=New message body=-"), "{line}");
        assert!(!line.contains("hunter2") && !line.contains("Alice"), "{line}");
    }
}

#[tokio::test]
async fn a_dm_call_rings_the_voip_devices_on_the_voip_topic_and_a_dead_voip_token_is_removed() {
    let lines = run_event("standard", call_envelope(), 4).await;
    assert_eq!(
        lines,
        vec![
            "forget dead",
            "production dd44 voip topic=nl.example.app.voip expiration=0 type=call name=Alice text=false",
            "sandbox cc33 voip topic=nl.example.app.voip expiration=0 type=call name=Alice text=false",
            "sandbox dead voip topic=nl.example.app.voip expiration=0 type=call name=Alice text=false",
        ],
        "only PushKit tokens ring; the alert tokens get nothing for a call"
    );
}

#[tokio::test]
async fn an_encrypted_dm_call_rings_without_the_callers_name_or_any_text() {
    let lines = run_event("ephemeral", call_envelope(), 4).await;
    assert!(!lines.is_empty());
    for line in lines.iter().filter(|line| !line.starts_with("forget")) {
        assert!(line.contains("name=- text=false"), "{line}");
        assert!(!line.contains("Alice") && !line.contains("secret"), "{line}");
    }
}

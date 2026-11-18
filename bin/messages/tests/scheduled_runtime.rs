//! Actual production Scheduler lifecycle + TCP HTTP + Scylla. Auth/access/event ports are fixture adapters.
#[allow(dead_code)]
#[path = "../src/config.rs"]
mod config;
#[allow(dead_code)]
#[path = "../src/probes.rs"]
mod probes;
#[allow(dead_code)]
#[path = "../src/scheduled.rs"]
mod scheduled;
mod scheduled_runtime_support;
#[path = "../../../crates/infrastructure/tests/support/mod.rs"]
mod support;

use std::sync::{Arc, atomic::Ordering};
use std::time::{Duration, Instant};

use discordy_messages_application::{
    fakes::{CHANNEL, Harness, START_MS},
    ports::{Clock, MessageRepository},
};
use discordy_messages_infrastructure::{scylla::ScyllaMessageRepository, system::SystemClock};
use discordy_messages_interface::{AppState, AttachmentUrls, InternalToken, metrics::Metrics, router};
use scheduled_runtime_support::{Auth, request, wait_for};
use serde_json::{Value, json};

#[tokio::test]
async fn real_scheduler_lifecycle_delivers_through_http_with_two_workers_and_no_connected_author() {
    let database = support::start_scylla("scheduled_runtime").await;
    let mut config = scheduled_runtime_support::config();
    let mut h = Harness::new();
    h.clock.advance_ms(SystemClock.now_unix_ms().saturating_sub(START_MS));
    let stored = Arc::new(ScyllaMessageRepository::new(database.session.clone()).await.unwrap());
    h.ports.messages = stored.clone();
    h.ports.history = Arc::new(discordy_messages_application::HistoryReader::new(
        stored.clone(),
        h.ports.reactions.clone(),
    ));
    assert!(
        scheduled::Scheduler::start(&config, database.session.clone(), &h.ports)
            .await
            .unwrap()
            .is_none()
    );
    let absent = database
        .session
        .query_unpaged(
            "SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name='scheduled_runtime_scheduled'",
            (),
        )
        .await
        .unwrap()
        .into_rows_result()
        .unwrap();
    assert_eq!(absent.rows_num(), 0, "disabled startup must not create schedule schema");

    config.scheduled_messages_enabled = true;
    let first = scheduled::Scheduler::start(&config, database.session.clone(), &h.ports)
        .await
        .unwrap()
        .unwrap();
    let second = scheduled::Scheduler::start(&config, database.session.clone(), &h.ports)
        .await
        .unwrap()
        .unwrap();
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let state = AppState::new(
        &h.ports,
        Arc::new(Auth),
        InternalToken::new("fixture-internal"),
        AttachmentUrls::new(
            "http://cdn.test",
            "fixture-signing-key-at-least-thirty-two-characters",
            Duration::from_secs(60),
            h.clock.clone(),
        ),
        h.media.clone(),
        vec![Arc::new(probes::ScheduledProbe {
            repository: first.repository.clone(),
            running: first.running.clone(),
        })],
        Metrics::new(recorder.handle()),
    )
    .with_scheduled(first.service.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, router(state)).await.unwrap() });
    let server = scheduled_runtime_support::Server(server);
    let path = format!("{base}/api/v1/channels/{CHANNEL}/messages/scheduled");
    let author = reqwest::Client::new();
    let observer = reqwest::Client::new();
    assert_eq!(author.get(format!("{base}/readyz")).send().await.unwrap().status(), 200);

    let original = request(h.clock.now_unix_ms(), 1, "delivered while the author is offline");
    let created = author
        .post(&path)
        .bearer_auth("alice")
        .json(&original)
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), 201);
    let pending: Value = created.json().await.unwrap();
    let detail = format!("{path}/{}", pending["id"].as_str().unwrap());
    let replay = observer
        .post(&path)
        .bearer_auth("alice")
        .json(&original)
        .send()
        .await
        .unwrap();
    assert_eq!(replay.status(), 200);
    assert_eq!(replay.json::<Value>().await.unwrap(), pending);
    assert_eq!(
        observer.get(&detail).bearer_auth("bob").send().await.unwrap().status(),
        404
    );
    assert_eq!(
        observer
            .get(&path)
            .bearer_auth("bob")
            .send()
            .await
            .unwrap()
            .json::<Value>()
            .await
            .unwrap()["items"],
        json!([])
    );
    let cancelled = author
        .post(&path)
        .bearer_auth("alice")
        .json(&request(h.clock.now_unix_ms(), 2, "must never be delivered"))
        .send()
        .await
        .unwrap()
        .json::<Value>()
        .await
        .unwrap();
    let cancel = author
        .delete(format!(
            "{path}/{}?expected_revision=1",
            cancelled["id"].as_str().unwrap()
        ))
        .bearer_auth("alice")
        .send()
        .await
        .unwrap();
    assert_eq!(cancel.status(), 200);
    assert_eq!(cancel.json::<Value>().await.unwrap()["status"], "cancelled");

    // The worker has no dependency on a live author client/socket. Advance only the fixture clock.
    drop(author);
    assert!(stored.last_message_id(CHANNEL).await.unwrap().is_none());
    h.clock.advance_ms(59_999);
    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert!(
        stored.last_message_id(CHANNEL).await.unwrap().is_none(),
        "the real timer must not dispatch early"
    );
    h.clock.advance_ms(1);
    let started = Instant::now();
    let sent = wait_for(&observer, &detail, "sent").await;
    println!(
        "real scheduler TCP/Scylla delivery after due: {:?}; two workers, one sent intent",
        started.elapsed()
    );
    assert!(sent["message"].is_null());
    let message_id = sent["message_id"].as_str().unwrap().parse().unwrap();
    let message = stored.find(CHANNEL, message_id).await.unwrap().unwrap();
    assert_eq!(message.content, "delivered while the author is offline");
    assert_eq!(h.events.events.lock().unwrap().len(), 1);
    let history = observer
        .get(format!("{base}/api/v1/channels/{CHANNEL}/messages"))
        .bearer_auth("bob")
        .send()
        .await
        .unwrap();
    assert_eq!(history.status(), 200);
    let history: Value = history.json().await.unwrap();
    assert_eq!(history.as_array().unwrap().len(), 1);
    assert_eq!(history[0]["content"], message.content);
    let running = first.running.clone();
    first.shutdown().await;
    second.shutdown().await;
    assert!(!running.load(Ordering::SeqCst));
    assert_eq!(
        observer.get(format!("{base}/readyz")).send().await.unwrap().status(),
        503
    );
    drop(server);
}

//! Actual TCP routes, main/companion Scylla repositories and production expiry lifecycle.
//! Authentication, API access/user directory and final event transport are fixture adapters.
mod poll_runtime_support;
#[allow(dead_code)]
#[path = "../src/polls.rs"]
mod polls;
#[allow(dead_code)]
#[path = "../src/probes.rs"]
mod probes;
#[path = "../../../crates/infrastructure/tests/support/mod.rs"]
mod support;

use discordy_messages_application::{
    HistoryReader,
    fakes::{BOB, CHANNEL, Harness, MEMBER, START_MS},
    ports::{AccessError, Clock, MessageRepository, PollDueEntry, PollKey, PollRepository},
};
use discordy_messages_domain::{Permissions, Snowflake};
use discordy_messages_infrastructure::{
    scylla::{ScyllaMessageRepository, ScyllaPollRepository},
    system::SystemClock,
};
use poll_runtime_support::{Server, Sink, body, request, wire_events};
use reqwest::Method;
use serde_json::{Value, json};
use std::sync::{Arc, atomic::Ordering};
use std::time::{Duration, Instant};

#[tokio::test]
async fn tcp_polls_persist_votes_and_two_workers_finalize_after_restart_despite_lost_publication() {
    let database = support::start_scylla("poll_runtime").await;
    let mut h = Harness::new();
    h.clock.advance_ms(SystemClock.now_unix_ms().saturating_sub(START_MS));
    h.access.set_permissions(MEMBER | Permissions::SEND_POLLS);
    let messages = Arc::new(ScyllaMessageRepository::new(database.session.clone()).await.unwrap());
    let repository = Arc::new(
        ScyllaPollRepository::new(database.session.clone(), h.clock.clone(), None)
            .await
            .unwrap(),
    );
    h.ports.messages = messages.clone();
    h.ports.history = Arc::new(HistoryReader::new(messages.clone(), h.ports.reactions.clone()));
    h.ports.polls = Some(repository.clone());
    h.ports.poll_creation_enabled = true;
    let sink = Arc::new(Sink::default());
    wire_events(&mut h, sink.clone());
    repository.check_ready().await.unwrap();
    let server = Server::new(&h, vec![]).await;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    let create = format!("{}/api/v1/channels/{CHANNEL}/messages", server.base);
    // The last shard makes the real one-second rotation observable.
    for _ in 0..63 {
        h.ports.ids.next_id().unwrap();
    }
    let created = request(&client, Method::POST, &create, "alice", Some(&body()), 200).await;
    let id = created["id"].as_str().unwrap();
    let key = PollKey {
        channel_id: CHANNEL,
        message_id: Snowflake::new(id.parse().unwrap()),
    };
    assert_eq!(PollDueEntry { key, expires_at_ms: 0 }.shard(), 63);
    let message_url = format!("{create}/{id}");
    let poll_url = format!("{}/api/v1/channels/{CHANNEL}/polls/{id}", server.base);
    let history = request(&client, Method::GET, &create, "bob", None, 200).await;
    assert_eq!(history[0]["poll"], created["poll"]);
    let edited = request(
        &client,
        Method::PATCH,
        &message_url,
        "alice",
        Some(&json!({"content":"New comment"})),
        200,
    )
    .await;
    assert_eq!(edited["poll"], created["poll"]);
    // Even a repository content update cannot mutate the immutable poll column.
    let mut stored = messages.find(CHANNEL, key.message_id).await.unwrap().unwrap();
    let definition = stored.poll.clone();
    stored.poll.as_mut().unwrap().question = "must not replace".into();
    messages.update(&stored).await.unwrap();
    assert_eq!(
        messages.find(CHANNEL, key.message_id).await.unwrap().unwrap().poll,
        definition
    );
    let alice_vote = format!("{poll_url}/answers/1/@me");
    let bob_vote = format!("{poll_url}/answers/2/@me");
    let revision = json!({"expected_vote_version":"0"});
    let (alice, bob) = tokio::join!(
        request(&client, Method::PUT, &alice_vote, "alice", Some(&revision), 200),
        request(&client, Method::PUT, &bob_vote, "bob", Some(&revision), 200)
    );
    assert_eq!(alice["results"]["vote_version"], "1");
    assert_eq!(bob["results"]["vote_version"], "1");
    let replay = request(&client, Method::PUT, &bob_vote, "bob", Some(&revision), 200).await;
    assert_eq!(replay["results"]["version"], "2");
    assert_eq!(replay["results"]["answer_counts"][0]["count"], 1);
    assert_eq!(replay["results"]["answer_counts"][1]["count"], 1);
    *h.access.fresh.lock().unwrap() = Some(Err(AccessError::NotMember));
    request(&client, Method::GET, &poll_url, "bob", None, 403).await;
    request(&client, Method::PUT, &bob_vote, "bob", Some(&revision), 403).await;
    *h.access.fresh.lock().unwrap() = None;
    h.clock.advance_ms(3_600_000);
    let overdue = request(&client, Method::GET, &poll_url, "bob", None, 200).await;
    assert_eq!(overdue["results"]["is_finalized"], false);
    assert_eq!(
        repository.snapshot(key, BOB).await.unwrap().unwrap().aggregate.version,
        2,
        "overdue GET must perform no repair writes"
    );
    drop(server);
    // New repository/HTTP/worker instances simulate restart, with creation disabled.
    let reopened = Arc::new(
        ScyllaPollRepository::new(database.session.clone(), h.clock.clone(), None)
            .await
            .unwrap(),
    );
    h.ports.polls = Some(reopened.clone());
    h.ports.poll_creation_enabled = false;
    sink.fail_final.store(true, Ordering::SeqCst);
    let first = polls::PollExpiry::start(&h.ports).unwrap();
    let second_repository = Arc::new(
        ScyllaPollRepository::new(database.session.clone(), h.clock.clone(), None)
            .await
            .unwrap(),
    );
    let mut second_ports = h.ports.clone();
    second_ports.polls = Some(second_repository);
    let second = polls::PollExpiry::start(&second_ports).unwrap();
    let running = first.running.clone();
    let server = Server::new(
        &h,
        vec![Arc::new(probes::PollProbe {
            repository: reopened.clone(),
            running: running.clone(),
        })],
    )
    .await;
    assert_eq!(
        client
            .get(format!("{}/readyz", server.base))
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    let create = format!("{}/api/v1/channels/{CHANNEL}/messages", server.base);
    request(&client, Method::POST, &create, "alice", Some(&body()), 503).await;
    let poll_url = format!("{}/api/v1/channels/{CHANNEL}/polls/{id}", server.base);
    let started = Instant::now();
    let final_state: Value = tokio::time::timeout(Duration::from_secs(40), async {
        loop {
            let state = request(&client, Method::GET, &poll_url, "bob", None, 200).await;
            if state["results"]["is_finalized"] == true {
                break state;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("production expiry worker did not finish its bounded idle rotation");
    assert_eq!(final_state["results"]["version"], "3");
    assert_eq!(final_state["results"]["vote_version"], "1");
    assert_eq!(final_state["results"]["answer_counts"][1]["me_voted"], true);
    first.shutdown().await;
    second.shutdown().await;
    assert!(!running.load(Ordering::SeqCst));
    assert_eq!(
        sink.final_attempts.load(Ordering::SeqCst),
        1,
        "exactly one closing CAS publishes an attempt"
    );
    assert!(
        sink.events
            .lock()
            .unwrap()
            .iter()
            .all(|event| event["data"]["poll"]["results"]["is_finalized"] != true)
    );
    assert!(reopened.due_page(63, None, 100).await.unwrap().is_empty());
    assert_eq!(
        client
            .get(format!("{}/readyz", server.base))
            .send()
            .await
            .unwrap()
            .status(),
        503
    );
    eprintln!(
        "real poll expiry: two workers, last shard63, creation disabled, lost final publication repaired by read; elapsed={:?}",
        started.elapsed()
    );
    request(
        &client,
        Method::PUT,
        &format!("{poll_url}/answers/1/@me"),
        "bob",
        Some(&json!({"expected_vote_version":"1"})),
        409,
    )
    .await;
    let message_url = format!("{create}/{id}");
    request(&client, Method::DELETE, &message_url, "alice", None, 204).await;
    request(&client, Method::GET, &poll_url, "bob", None, 404).await;
    assert!(messages.find(CHANNEL, key.message_id).await.unwrap().is_none());
    let rows = database
        .session
        .query_unpaged(
            "SELECT count(*) FROM poll_runtime_polls.poll_state WHERE channel_id=? AND message_id=?",
            (CHANNEL.value() as i64, key.message_id.value() as i64),
        )
        .await
        .unwrap()
        .into_rows_result()
        .unwrap()
        .single_row::<(i64,)>()
        .unwrap()
        .0;
    assert_eq!(rows, 1, "deletion retains only the content-free fence");
    database
        .session
        .query_unpaged("DROP TABLE poll_runtime_polls.poll_due", ())
        .await
        .unwrap();
    assert!(
        reopened.check_ready().await.is_err(),
        "missing companion table fails readiness"
    );
    drop(server);
}

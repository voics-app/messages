//! Redpanda to the purge sink: BAN_MESSAGE_PURGE on discordy.guild.v1 reaches the sink, other
//! guild events are ignored, and a restarted consumer resumes from its stored offset
//! (Docker required).

mod support;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use discordy_messages_application::PortError;
use discordy_messages_application::ports::{EventSink, SearchIndex};
use discordy_messages_domain::Snowflake;
use discordy_messages_infrastructure::ban_purge::{self, PurgeSink};
use discordy_messages_infrastructure::kafka::KafkaEventSink;
use discordy_messages_infrastructure::search::{PgSearchIndex, connect};
use rskafka::client::ClientBuilder;
use serde_json::json;

const TOPIC: &str = "discordy.guild.v1";

type Call = (Vec<Snowflake>, Snowflake, u64);

/// Records purges. Once frozen it never returns, standing in for a stopped service: it neither
/// advances its offset nor competes with a restarted consumer.
#[derive(Default)]
struct Recorder {
    calls: Mutex<Vec<Call>>,
    frozen: AtomicBool,
}

#[async_trait]
impl PurgeSink for Recorder {
    async fn purge(&self, channel_ids: &[Snowflake], author: Snowflake, since_ms: u64) -> Result<(), PortError> {
        if self.frozen.load(Ordering::SeqCst) {
            std::future::pending::<()>().await;
        }
        self.calls
            .lock()
            .unwrap()
            .push((channel_ids.to_vec(), author, since_ms));
        Ok(())
    }
}

fn envelope(kind: &str, user: &str, since_ms: u64) -> Vec<u8> {
    json!({
        "id": "e", "type": kind, "v": 1, "occurred_at": "2026-01-01T00:00:00Z",
        "guild_id": "3", "channel_id": null, "target_user_ids": null,
        "data": {
            "guild_id": "3", "user_id": user, "channel_ids": ["5", "6"],
            "since_ms": since_ms, "delete_message_seconds": 3600
        }
    })
    .to_string()
    .into_bytes()
}

async fn eventually(recorder: &Recorder, calls: usize) {
    for _ in 0..100 {
        if recorder.calls.lock().unwrap().len() >= calls {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!(
        "expected {calls} purge calls, saw {}",
        recorder.calls.lock().unwrap().len()
    );
}

#[tokio::test]
async fn purge_events_reach_the_sink_and_survive_a_restart() {
    let redpanda = support::start_redpanda().await;
    let pg = support::start_postgres().await;
    ClientBuilder::new(vec![redpanda.broker.clone()])
        .build()
        .await
        .unwrap()
        .controller_client()
        .unwrap()
        .create_topic(TOPIC, 1, 1, 5_000)
        .await
        .unwrap();
    let index = Arc::new(PgSearchIndex::new(connect(&pg.url).unwrap()));
    index.migrate().await.unwrap();
    let sink = KafkaEventSink::connect(vec![redpanda.broker.clone()]).await.unwrap();

    // Published before the consumer exists and with no stored offset: ignored (only new bans count).
    sink.send(TOPIC, "3", envelope("BAN_MESSAGE_PURGE", "6", 1))
        .await
        .unwrap();

    let first = Arc::new(Recorder::default());
    ban_purge::spawn(vec![redpanda.broker.clone()], first.clone(), index.clone())
        .await
        .unwrap();
    sink.send(TOPIC, "3", envelope("GUILD_BAN_ADD", "7", 2)).await.unwrap();
    sink.send(TOPIC, "3", envelope("BAN_MESSAGE_PURGE", "7", 1234))
        .await
        .unwrap();
    eventually(&first, 1).await;
    assert_eq!(
        first.calls.lock().unwrap().clone(),
        vec![(vec![Snowflake::new(5), Snowflake::new(6)], Snowflake::new(7), 1234)]
    );

    // The offset is stored after the batch.
    for _ in 0..100 {
        if index.checkpoint(TOPIC, 0).await.unwrap() == Some(3) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(index.checkpoint(TOPIC, 0).await.unwrap(), Some(3));

    // The first consumer stops; a second one (a restart) starts at the stored offset and sees only
    // what came after it.
    first.frozen.store(true, Ordering::SeqCst);
    sink.send(TOPIC, "3", envelope("BAN_MESSAGE_PURGE", "8", 99))
        .await
        .unwrap();
    let second = Arc::new(Recorder::default());
    ban_purge::spawn(vec![redpanda.broker.clone()], second.clone(), index.clone())
        .await
        .unwrap();
    eventually(&second, 1).await;
    let calls = second.calls.lock().unwrap().clone();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].1, Snowflake::new(8));
}

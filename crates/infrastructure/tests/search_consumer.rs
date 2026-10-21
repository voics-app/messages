//! Redpanda to PostgreSQL: events on discordy.message.v1 reach the index, edits
//! and deletes follow, and a restarted consumer resumes from its stored offset
//! (Docker required).

mod support;

use std::sync::Arc;
use std::time::Duration;

use discordy_messages_application::ports::{EventSink, SearchIndex, SearchRequest};
use discordy_messages_domain::{SearchFilters, Snowflake};
use discordy_messages_infrastructure::kafka::KafkaEventSink;
use discordy_messages_infrastructure::search::{PgSearchIndex, connect, consumer};
use rskafka::client::ClientBuilder;
use serde_json::json;

const TOPIC: &str = "discordy.message.v1";
const CHANNEL: Snowflake = Snowflake::new(5);

fn envelope(kind: &str, data: serde_json::Value) -> Vec<u8> {
    json!({
        "id": "e", "type": kind, "v": 1, "occurred_at": "2026-01-01T00:00:00Z",
        "guild_id": "3", "channel_id": "5", "target_user_ids": null, "data": data
    })
    .to_string()
    .into_bytes()
}

fn message(id: &str, content: &str, flags: u32, edited: Option<&str>) -> serde_json::Value {
    json!({
        "id": id, "channel_id": "5", "author": {"id": "7"}, "content": content,
        "timestamp": "2026-01-01T00:00:00Z", "edited_timestamp": edited, "tts": false,
        "mention_everyone": false, "mentions": [], "mention_roles": [], "attachments": [],
        "embeds": [], "pinned": false, "type": 0, "flags": flags, "components": []
    })
}

async fn found(index: &PgSearchIndex, text: &str) -> u64 {
    let filters = SearchFilters {
        content: Some(text.to_owned()),
        ..SearchFilters::default()
    };
    index
        .search(&SearchRequest {
            channels: vec![CHANNEL],
            filters,
            pinned: None,
        })
        .await
        .unwrap()
        .total
}

async fn eventually(index: &PgSearchIndex, text: &str, expected: u64) {
    for _ in 0..100 {
        if found(index, text).await == expected {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(found(index, text).await, expected, "search for {text}");
}

#[tokio::test]
async fn the_consumer_indexes_updates_and_deletes_from_the_topic() {
    let redpanda = support::start_redpanda().await;
    let pg = support::start_postgres().await;
    ClientBuilder::new(vec![redpanda.broker.clone()])
        .build()
        .await
        .unwrap()
        .controller_client()
        .unwrap()
        .create_topic(TOPIC, 2, 1, 5_000)
        .await
        .unwrap();
    let index = Arc::new(PgSearchIndex::new(connect(&pg.url).unwrap()));
    index.migrate().await.unwrap();
    let sink = KafkaEventSink::connect(vec![redpanda.broker.clone()]).await.unwrap();

    // Published before the consumer exists: read from the earliest offset.
    sink.send(
        TOPIC,
        "5",
        envelope("MESSAGE_CREATE", message("100", "hello from the past", 0, None)),
    )
    .await
    .unwrap();
    consumer::spawn(vec![redpanda.broker.clone()], index.clone())
        .await
        .unwrap();
    eventually(&index, "past", 1).await;

    sink.send(
        TOPIC,
        "5",
        envelope("MESSAGE_CREATE", message("101", "second words", 0, None)),
    )
    .await
    .unwrap();
    sink.send(
        TOPIC,
        "5",
        envelope("MESSAGE_CREATE", message("102", "secret relay", 64, None)),
    )
    .await
    .unwrap();
    sink.send(
        TOPIC,
        "5",
        envelope(
            "MESSAGE_UPDATE",
            message("101", "edited words", 0, Some("2026-01-01T00:00:05Z")),
        ),
    )
    .await
    .unwrap();
    sink.send(TOPIC, "5", envelope("MESSAGE_REACTION_ADD", json!({"user_id": "7"})))
        .await
        .unwrap();
    eventually(&index, "edited", 1).await;
    eventually(&index, "second", 0).await;
    assert_eq!(found(&index, "secret").await, 0, "ephemeral relays are never indexed");

    sink.send(
        TOPIC,
        "5",
        envelope(
            "MESSAGE_DELETE",
            json!({"id": "100", "channel_id": "5", "guild_id": "3"}),
        ),
    )
    .await
    .unwrap();
    eventually(&index, "past", 0).await;

    // The offsets were stored with the rows.
    let mut stored = 0;
    for partition in 0..2 {
        stored += index.checkpoint(TOPIC, partition).await.unwrap().unwrap_or(0);
    }
    assert_eq!(stored, 6, "six records were consumed in total");
}

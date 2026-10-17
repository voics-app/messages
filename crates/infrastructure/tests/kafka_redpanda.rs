//! The event producer against a real Redpanda: keyed records land on the
//! partition the Java client's partitioner would choose, in order.

mod support;

use discordy_messages_application::ports::EventSink;
use discordy_messages_infrastructure::kafka::{KafkaEventSink, partition_for};
use rskafka::client::ClientBuilder;
use rskafka::client::partition::UnknownTopicHandling;

const TOPIC: &str = "discordy.message.v1";
const PARTITIONS: i32 = 3;

#[tokio::test]
async fn keyed_events_land_on_their_partition_in_order() {
    let redpanda = support::start_redpanda().await;
    let admin = ClientBuilder::new(vec![redpanda.broker.clone()]).build().await.unwrap();
    admin
        .controller_client()
        .unwrap()
        .create_topic(TOPIC, PARTITIONS, 1, 5_000)
        .await
        .unwrap();

    let sink = KafkaEventSink::connect(vec![redpanda.broker.clone()]).await.unwrap();
    sink.ping().await.unwrap();
    for sequence in 0..3 {
        sink.send(TOPIC, "channel-42", format!("event-{sequence}").into_bytes())
            .await
            .unwrap();
    }

    let partition = partition_for(b"channel-42", PARTITIONS);
    let reader = admin
        .partition_client(TOPIC, partition, UnknownTopicHandling::Retry)
        .await
        .unwrap();
    let (records, _) = reader.fetch_records(0, 1..1_000_000, 5_000).await.unwrap();
    let values: Vec<String> = records
        .iter()
        .map(|r| String::from_utf8(r.record.value.clone().unwrap()).unwrap())
        .collect();
    assert_eq!(values, ["event-0", "event-1", "event-2"]);
    assert_eq!(records[0].record.key.as_deref(), Some(b"channel-42".as_slice()));

    let missing = sink.send("discordy.no-such-topic.v1", "k", vec![]).await;
    assert!(missing.is_err(), "sending to a missing topic must fail");
}

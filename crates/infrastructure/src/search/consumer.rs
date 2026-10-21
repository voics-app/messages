//! Reads `discordy.message.v1` and keeps the search index current.
//!
//! One task per partition. Each batch is applied together with its offset in
//! one transaction, so a crash replays at most the unfinished batch; replays
//! are harmless because index operations are idempotent. The consumer starts
//! from the stored offset, or from the earliest record on a fresh index.

use std::sync::Arc;
use std::time::Duration;

use contracts::{Envelope, MESSAGE_TOPIC};
use discordy_messages_application::PortError;
use discordy_messages_application::ports::{Checkpoint, SearchIndex};
use rskafka::client::error::{Error as KafkaError, ProtocolError};
use rskafka::client::partition::{OffsetAt, PartitionClient, UnknownTopicHandling};
use rskafka::client::{Client, ClientBuilder};

use super::events::operations;

const FETCH_BYTES: std::ops::Range<i32> = 1..4_000_000;
const MAX_WAIT_MS: i32 = 1_000;
const RETRY_DELAY: Duration = Duration::from_secs(3);

fn port_error(error: impl std::fmt::Display) -> PortError {
    PortError::new(format!("search consumer: {error}"))
}

/// Starts one consumer task per partition and returns immediately.
pub async fn spawn(brokers: Vec<String>, index: Arc<dyn SearchIndex>) -> Result<(), PortError> {
    let client = Arc::new(
        ClientBuilder::new(brokers)
            .client_id("discordy-messages-search")
            .build()
            .await
            .map_err(port_error)?,
    );
    let partitions = loop {
        let topics = client.list_topics().await.map_err(port_error)?;
        if let Some(topic) = topics.iter().find(|t| t.name == MESSAGE_TOPIC) {
            break topic.partitions.iter().copied().collect::<Vec<i32>>();
        }
        tracing::warn!(topic = MESSAGE_TOPIC, "search consumer: topic does not exist yet");
        tokio::time::sleep(RETRY_DELAY).await;
    };
    for partition in partitions {
        let client = Arc::clone(&client);
        let index = Arc::clone(&index);
        tokio::spawn(async move { run_partition(client, index, partition).await });
    }
    Ok(())
}

async fn run_partition(client: Arc<Client>, index: Arc<dyn SearchIndex>, partition: i32) {
    loop {
        if let Err(error) = consume(&client, index.as_ref(), partition).await {
            tracing::error!(partition, %error, "search consumer failed, retrying");
            tokio::time::sleep(RETRY_DELAY).await;
        }
    }
}

async fn consume(client: &Client, index: &dyn SearchIndex, partition: i32) -> Result<(), PortError> {
    let source: PartitionClient = client
        .partition_client(MESSAGE_TOPIC, partition, UnknownTopicHandling::Retry)
        .await
        .map_err(port_error)?;
    let mut next = match index.checkpoint(MESSAGE_TOPIC, partition).await? {
        Some(offset) => offset,
        None => source.get_offset(OffsetAt::Earliest).await.map_err(port_error)?,
    };
    tracing::info!(partition, offset = next, "search consumer started");
    loop {
        let (records, high_watermark) = match source.fetch_records(next, FETCH_BYTES, MAX_WAIT_MS).await {
            Ok(fetched) => fetched,
            Err(KafkaError::ServerError {
                protocol_error: ProtocolError::OffsetOutOfRange,
                ..
            }) => {
                // Retention removed what we had not read yet: resume at the oldest record.
                next = source.get_offset(OffsetAt::Earliest).await.map_err(port_error)?;
                tracing::warn!(
                    partition,
                    offset = next,
                    "search consumer offset out of range, resetting"
                );
                continue;
            }
            Err(error) => return Err(port_error(error)),
        };
        let Some(last) = records.last().map(|r| r.offset) else {
            continue;
        };
        let mut ops = Vec::new();
        for record in &records {
            let Some(value) = record.record.value.as_deref() else {
                continue;
            };
            match serde_json::from_slice::<Envelope>(value) {
                Ok(envelope) => ops.extend(operations(&envelope)),
                Err(error) => tracing::warn!(partition, offset = record.offset, %error, "search: unreadable record"),
            }
        }
        let checkpoint = Checkpoint {
            topic: MESSAGE_TOPIC.to_owned(),
            partition,
            next_offset: last + 1,
        };
        index.apply(&ops, Some(&checkpoint)).await?;
        next = last + 1;
        let lag = high_watermark - next;
        if lag > 1_000 {
            tracing::info!(partition, lag, "search consumer catching up");
        }
    }
}

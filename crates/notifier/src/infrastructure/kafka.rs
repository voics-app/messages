//! Reads `discordy.message.v1` from Redpanda, one task per partition, starting at the end of the
//! topic.
//!
//! There is no stored offset on purpose: a push for a message from before a restart is stale and
//! would only annoy, and the handler drops events older than its maximum age anyway. There is no
//! consumer group either (the pure-Rust client has none), so exactly one replica must run.

use std::sync::Arc;
use std::time::Duration;

use contracts::{Envelope, MESSAGE_TOPIC};
use rskafka::client::error::{Error as KafkaError, ProtocolError};
use rskafka::client::partition::{OffsetAt, PartitionClient, UnknownTopicHandling};
use rskafka::client::{Client, ClientBuilder};

use crate::application::{EventHandler, PortError};

const FETCH_BYTES: std::ops::Range<i32> = 1..4_000_000;
const MAX_WAIT_MS: i32 = 1_000;
const RETRY_DELAY: Duration = Duration::from_secs(3);

fn port_error(error: impl std::fmt::Display) -> PortError {
    PortError::new(format!("redpanda: {error}"))
}

pub struct MessageTopic {
    client: Arc<Client>,
}

impl MessageTopic {
    pub async fn connect(brokers: Vec<String>) -> Result<Self, PortError> {
        let client = ClientBuilder::new(brokers)
            .client_id("discordy-notifier")
            .build()
            .await
            .map_err(port_error)?;
        Ok(Self {
            client: Arc::new(client),
        })
    }

    /// Readiness probe: fetches cluster metadata.
    pub async fn ping(&self) -> Result<(), PortError> {
        self.client.list_topics().await.map(|_| ()).map_err(port_error)
    }

    /// Waits for the topic to exist, then starts one consumer task per partition.
    pub async fn spawn(&self, handler: Arc<EventHandler>) -> Result<(), PortError> {
        let partitions = loop {
            let topics = self.client.list_topics().await.map_err(port_error)?;
            if let Some(topic) = topics.iter().find(|t| t.name == MESSAGE_TOPIC) {
                break topic.partitions.iter().copied().collect::<Vec<i32>>();
            }
            tracing::warn!(topic = MESSAGE_TOPIC, "topic does not exist yet");
            tokio::time::sleep(RETRY_DELAY).await;
        };
        for partition in partitions {
            let source = self
                .client
                .partition_client(MESSAGE_TOPIC, partition, UnknownTopicHandling::Retry)
                .await
                .map_err(port_error)?;
            let next = source.get_offset(OffsetAt::Latest).await.map_err(port_error)?;
            tracing::info!(partition, offset = next, "consumer started");
            let handler = Arc::clone(&handler);
            tokio::spawn(async move { run_partition(source, handler, partition, next).await });
        }
        Ok(())
    }
}

async fn run_partition(source: PartitionClient, handler: Arc<EventHandler>, partition: i32, mut next: i64) {
    loop {
        if let Err(error) = consume(&source, &handler, partition, &mut next).await {
            tracing::error!(partition, %error, "consumer failed, retrying");
            metrics::counter!("notifier_consumer_errors_total").increment(1);
            tokio::time::sleep(RETRY_DELAY).await;
        }
    }
}

async fn consume(
    source: &PartitionClient,
    handler: &EventHandler,
    partition: i32,
    next: &mut i64,
) -> Result<(), PortError> {
    let records = match source.fetch_records(*next, FETCH_BYTES, MAX_WAIT_MS).await {
        Ok((records, _)) => records,
        Err(KafkaError::ServerError {
            protocol_error: ProtocolError::OffsetOutOfRange,
            ..
        }) => {
            *next = source.get_offset(OffsetAt::Latest).await.map_err(port_error)?;
            tracing::warn!(partition, offset = *next, "offset out of range, resetting");
            return Ok(());
        }
        Err(error) => return Err(port_error(error)),
    };
    let Some(last) = records.last().map(|r| r.offset) else {
        return Ok(());
    };
    for record in &records {
        let Some(envelope) = record
            .record
            .value
            .as_deref()
            .and_then(|value| serde_json::from_slice::<Envelope>(value).ok())
        else {
            continue;
        };
        handler.handle(&envelope).await;
    }
    *next = last + 1;
    Ok(())
}

//! Redpanda (Kafka API) producer built on the pure-Rust `rskafka` client.
//!
//! Records are routed to a partition with Kafka's default partitioner
//! (murmur2 of the key), so every event of a channel lands on one partition
//! and stays ordered, and other Kafka clients agree on the placement.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use async_trait::async_trait;
use discordy_messages_application::PortError;
use discordy_messages_application::ports::EventSink;
use rskafka::client::partition::{Compression, PartitionClient, UnknownTopicHandling};
use rskafka::client::{Client, ClientBuilder};
use rskafka::record::Record;
use tokio::sync::RwLock;

fn port_error(error: impl std::fmt::Display) -> PortError {
    PortError::new(format!("kafka: {error}"))
}

pub struct KafkaEventSink {
    client: Client,
    partition_counts: RwLock<HashMap<String, i32>>,
    partitions: RwLock<HashMap<(String, i32), Arc<PartitionClient>>>,
}

impl KafkaEventSink {
    pub async fn connect(brokers: Vec<String>) -> Result<Self, PortError> {
        let client = ClientBuilder::new(brokers)
            .client_id("discordy-messages")
            .build()
            .await
            .map_err(port_error)?;
        Ok(Self {
            client,
            partition_counts: RwLock::new(HashMap::new()),
            partitions: RwLock::new(HashMap::new()),
        })
    }

    /// Readiness probe: fetches cluster metadata.
    pub async fn ping(&self) -> Result<(), PortError> {
        self.client.list_topics().await.map(|_| ()).map_err(port_error)
    }

    async fn partition_count(&self, topic: &str) -> Result<i32, PortError> {
        if let Some(count) = self.partition_counts.read().await.get(topic) {
            return Ok(*count);
        }
        let topics = self.client.list_topics().await.map_err(port_error)?;
        let count = topics
            .iter()
            .find(|t| t.name == topic)
            .map(|t| t.partitions.len() as i32)
            .filter(|n| *n > 0)
            .ok_or_else(|| PortError::new(format!("kafka: topic {topic} does not exist")))?;
        self.partition_counts.write().await.insert(topic.to_owned(), count);
        Ok(count)
    }

    async fn partition_client(&self, topic: &str, partition: i32) -> Result<Arc<PartitionClient>, PortError> {
        let key = (topic.to_owned(), partition);
        if let Some(client) = self.partitions.read().await.get(&key) {
            return Ok(Arc::clone(client));
        }
        let client = Arc::new(
            self.client
                .partition_client(topic, partition, UnknownTopicHandling::Retry)
                .await
                .map_err(port_error)?,
        );
        self.partitions.write().await.insert(key, Arc::clone(&client));
        Ok(client)
    }
}

#[async_trait]
impl EventSink for KafkaEventSink {
    async fn send(&self, topic: &str, key: &str, payload: Vec<u8>) -> Result<(), PortError> {
        let partitions = self.partition_count(topic).await?;
        let partition = partition_for(key.as_bytes(), partitions);
        let record = Record {
            key: Some(key.as_bytes().to_vec()),
            value: Some(payload),
            headers: BTreeMap::new(),
            timestamp: chrono::Utc::now(),
        };
        self.partition_client(topic, partition)
            .await?
            .produce(vec![record], Compression::NoCompression)
            .await
            .map(|_| ())
            .map_err(port_error)
    }
}

/// Kafka's default partitioner: `toPositive(murmur2(key)) % partitions`.
pub fn partition_for(key: &[u8], partitions: i32) -> i32 {
    ((murmur2(key) & 0x7fff_ffff) % partitions as u32) as i32
}

/// Murmur2 as implemented by the Java Kafka client (seed 0x9747b28c).
fn murmur2(data: &[u8]) -> u32 {
    const SEED: u32 = 0x9747_b28c;
    const M: u32 = 0x5bd1_e995;
    const R: u32 = 24;
    let mut h = SEED ^ data.len() as u32;
    let (chunks, tail) = data.as_chunks::<4>();
    for chunk in chunks {
        let mut k = u32::from_le_bytes(*chunk);
        k = k.wrapping_mul(M);
        k ^= k >> R;
        k = k.wrapping_mul(M);
        h = h.wrapping_mul(M);
        h ^= k;
    }
    if tail.len() >= 3 {
        h ^= u32::from(tail[2]) << 16;
    }
    if tail.len() >= 2 {
        h ^= u32::from(tail[1]) << 8;
    }
    if !tail.is_empty() {
        h ^= u32::from(tail[0]);
        h = h.wrapping_mul(M);
    }
    h ^= h >> 13;
    h = h.wrapping_mul(M);
    h ^= h >> 15;
    h
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn murmur2_matches_the_java_client() {
        // Reference values from org.apache.kafka.common.utils.Utils.murmur2.
        assert_eq!(murmur2(b"21") as i32, -973_932_308);
        assert_eq!(murmur2(b"foobar") as i32, -790_332_482);
        assert_eq!(murmur2(b"a-little-bit-long-string") as i32, -985_981_536);
        assert_eq!(murmur2(b"") as i32, 275_646_681);
    }

    #[test]
    fn partitions_are_stable_and_in_range() {
        for key in ["1", "175928847299117063", "channel"] {
            let p = partition_for(key.as_bytes(), 12);
            assert!((0..12).contains(&p));
            assert_eq!(p, partition_for(key.as_bytes(), 12));
        }
    }
}

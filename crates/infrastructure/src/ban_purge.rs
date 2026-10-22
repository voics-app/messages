//! Reads BAN_MESSAGE_PURGE from `discordy.guild.v1` and deletes the banned user's recent messages.
//!
//! One task per partition. After each batch the offset is stored through the search index's
//! checkpoint table (the only durable store the service has besides ScyllaDB) so a restart does
//! not replay old bans. When that store is unavailable the consumer still works but resumes from
//! the end of the topic after a restart. Purges are idempotent, so a replayed batch is harmless.
//! A failed purge is retried until it succeeds.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use contracts::{BanMessagePurgeData, Envelope, GUILD_TOPIC, event_type};
use discordy_messages_application::PortError;
use discordy_messages_application::ports::{Checkpoint, SearchIndex};
use discordy_messages_application::use_cases::PurgeAuthorMessages;
use discordy_messages_domain::Snowflake;
use rskafka::client::ClientBuilder;
use rskafka::client::error::{Error as KafkaError, ProtocolError};
use rskafka::client::partition::{OffsetAt, PartitionClient, UnknownTopicHandling};

const FETCH_BYTES: std::ops::Range<i32> = 1..4_000_000;
const MAX_WAIT_MS: i32 = 1_000;
const RETRY_DELAY: Duration = Duration::from_secs(3);

fn port_error(error: impl std::fmt::Display) -> PortError {
    PortError::new(format!("ban purge consumer: {error}"))
}

/// Executes one purge request; implemented by the use case and by test doubles.
#[async_trait]
pub trait PurgeSink: Send + Sync {
    async fn purge(&self, channel_ids: &[Snowflake], author: Snowflake, since_ms: u64) -> Result<(), PortError>;
}

#[async_trait]
impl PurgeSink for PurgeAuthorMessages {
    async fn purge(&self, channel_ids: &[Snowflake], author: Snowflake, since_ms: u64) -> Result<(), PortError> {
        let report = self
            .execute(channel_ids, author, since_ms)
            .await
            .map_err(|error| PortError::new(error.to_string()))?;
        tracing::info!(
            %author,
            channels = report.channels,
            messages = report.messages,
            "ban purge finished"
        );
        Ok(())
    }
}

/// The purge a record asks for, or `None` when it is some other guild event or unreadable.
pub fn parse(envelope: &Envelope) -> Option<(Vec<Snowflake>, Snowflake, u64)> {
    if envelope.kind != event_type::BAN_MESSAGE_PURGE {
        return None;
    }
    let data: BanMessagePurgeData = match serde_json::from_value(envelope.data.clone()) {
        Ok(data) => data,
        Err(error) => {
            tracing::warn!(%error, "ban purge: unreadable event");
            return None;
        }
    };
    let author = data.user_id.parse().ok()?;
    let channels = data.channel_ids.iter().filter_map(|id| id.parse().ok()).collect();
    Some((channels, author, data.since_ms))
}

/// Resolves each partition's starting offset, then starts one consumer task per partition.
pub async fn spawn(
    brokers: Vec<String>,
    purger: Arc<dyn PurgeSink>,
    offsets: Arc<dyn SearchIndex>,
) -> Result<(), PortError> {
    let client = Arc::new(
        ClientBuilder::new(brokers)
            .client_id("discordy-messages-ban-purge")
            .build()
            .await
            .map_err(port_error)?,
    );
    let partitions = loop {
        let topics = client.list_topics().await.map_err(port_error)?;
        if let Some(topic) = topics.iter().find(|t| t.name == GUILD_TOPIC) {
            break topic.partitions.iter().copied().collect::<Vec<i32>>();
        }
        tracing::warn!(topic = GUILD_TOPIC, "ban purge consumer: topic does not exist yet");
        tokio::time::sleep(RETRY_DELAY).await;
    };
    for partition in partitions {
        let source = client
            .partition_client(GUILD_TOPIC, partition, UnknownTopicHandling::Retry)
            .await
            .map_err(port_error)?;
        let next = match offsets.checkpoint(GUILD_TOPIC, partition).await {
            Ok(Some(offset)) => offset,
            // Nothing stored yet (or no store): only bans from now on matter.
            Ok(None) | Err(_) => source.get_offset(OffsetAt::Latest).await.map_err(port_error)?,
        };
        tracing::info!(partition, offset = next, "ban purge consumer started");
        let purger = Arc::clone(&purger);
        let offsets = Arc::clone(&offsets);
        tokio::spawn(async move { run_partition(source, purger, offsets, partition, next).await });
    }
    Ok(())
}

async fn run_partition(
    source: PartitionClient,
    purger: Arc<dyn PurgeSink>,
    offsets: Arc<dyn SearchIndex>,
    partition: i32,
    mut next: i64,
) {
    loop {
        match consume(&source, purger.as_ref(), offsets.as_ref(), partition, &mut next).await {
            Ok(()) => {}
            Err(error) => {
                tracing::error!(partition, %error, "ban purge consumer failed, retrying");
                tokio::time::sleep(RETRY_DELAY).await;
            }
        }
    }
}

async fn consume(
    source: &PartitionClient,
    purger: &dyn PurgeSink,
    offsets: &dyn SearchIndex,
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
            tracing::warn!(partition, offset = *next, "ban purge offset out of range, resetting");
            return Ok(());
        }
        Err(error) => return Err(port_error(error)),
    };
    let Some(last) = records.last().map(|r| r.offset) else {
        return Ok(());
    };
    for record in &records {
        let Some(value) = record.record.value.as_deref() else {
            continue;
        };
        let Ok(envelope) = serde_json::from_slice::<Envelope>(value) else {
            continue;
        };
        if let Some((channels, author, since_ms)) = parse(&envelope) {
            purger.purge(&channels, author, since_ms).await?;
        }
    }
    *next = last + 1;
    let checkpoint = Checkpoint {
        topic: GUILD_TOPIC.to_owned(),
        partition,
        next_offset: *next,
    };
    if let Err(error) = offsets.apply(&[], Some(&checkpoint)).await {
        tracing::debug!(partition, %error, "ban purge offset not stored");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn envelope(kind: &str, data: serde_json::Value) -> Envelope {
        serde_json::from_value(json!({
            "id": "e", "type": kind, "v": 1, "occurred_at": "2026-01-01T00:00:00Z",
            "guild_id": "3", "channel_id": null, "target_user_ids": null, "data": data
        }))
        .unwrap()
    }

    #[test]
    fn parses_only_purge_events() {
        let data = json!({
            "guild_id": "3", "user_id": "7", "channel_ids": ["5", "6"],
            "since_ms": 1000, "delete_message_seconds": 3600
        });
        let (channels, author, since) = parse(&envelope("BAN_MESSAGE_PURGE", data.clone())).unwrap();
        assert_eq!(channels, vec![Snowflake::new(5), Snowflake::new(6)]);
        assert_eq!(author, Snowflake::new(7));
        assert_eq!(since, 1000);
        assert!(parse(&envelope("GUILD_BAN_ADD", data)).is_none());
        assert!(parse(&envelope("BAN_MESSAGE_PURGE", json!({"nope": 1}))).is_none());
    }
}

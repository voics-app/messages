use std::sync::Arc;

use discordy_messages_domain::{Bucket, ChannelId, Message, MessageId, ScanDirection, Snowflake, UserId};

use super::MessagingPorts;
use super::support::{announce, event, purge_dependents};
use crate::error::AppResult;
use crate::ports::{
    AccessError, AttachmentStorage, BucketQuery, ChannelAccessProvider, Clock, EventPublisher, MessageEventKind,
    MessageRepository, PinRepository, ReactionRepository,
};

/// Messages read from one partition per round trip.
const SCAN_BATCH: usize = 500;
/// Ids per MESSAGE_DELETE_BULK event (Discord's limit).
const EVENT_CHUNK: usize = 100;
/// A purge never reaches further back than Discord's longest window (seven days) plus a day of
/// slack, whatever the event says.
const MAX_LOOKBACK_MS: u64 = 8 * 24 * 60 * 60 * 1000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PurgeReport {
    pub channels: u64,
    pub messages: u64,
}

/// Deletes everything one author wrote in some channels since an instant: used when a moderator
/// bans a member with "delete message history". Each channel gets MESSAGE_DELETE_BULK events;
/// the search index follows from them. Safe to run twice: a replay finds nothing left.
pub struct PurgeAuthorMessages {
    polls: Option<Arc<dyn crate::ports::PollRepository>>,
    access: Arc<dyn ChannelAccessProvider>,
    messages: Arc<dyn MessageRepository>,
    reactions: Arc<dyn ReactionRepository>,
    pins: Arc<dyn PinRepository>,
    storage: Arc<dyn AttachmentStorage>,
    events: Arc<dyn EventPublisher>,
    clock: Arc<dyn Clock>,
}

impl PurgeAuthorMessages {
    pub fn new(ports: &MessagingPorts) -> Self {
        Self {
            polls: ports.polls.clone(),
            access: ports.access.clone(),
            messages: ports.messages.clone(),
            reactions: ports.reactions.clone(),
            pins: ports.pins.clone(),
            storage: ports.storage.clone(),
            events: ports.events.clone(),
            clock: ports.clock.clone(),
        }
    }

    /// Channels that no longer exist are skipped; a store failure is returned so the caller retries.
    pub async fn execute(&self, channel_ids: &[ChannelId], author: UserId, since_ms: u64) -> AppResult<PurgeReport> {
        let mut report = PurgeReport::default();
        for &channel_id in channel_ids {
            let access = match self.access.channel(channel_id).await {
                Ok(access) => access,
                Err(AccessError::UnknownChannel | AccessError::NotMember) => continue,
                Err(error) => return Err(error.into()),
            };
            let found = self.authored_since(channel_id, author, since_ms).await?;
            if found.is_empty() {
                continue;
            }
            report.channels += 1;
            let ids: Vec<MessageId> = found.iter().map(|m| m.id).collect();
            for message in &found {
                super::support::fence_poll(self.polls.as_deref(), message).await?;
            }
            self.messages.delete(channel_id, &ids).await?;
            for message in &found {
                purge_dependents(
                    self.reactions.as_ref(),
                    self.pins.as_ref(),
                    self.storage.as_ref(),
                    message,
                )
                .await?;
            }
            report.messages += ids.len() as u64;
            for chunk in ids.chunks(EVENT_CHUNK) {
                announce(
                    self.events.as_ref(),
                    event(
                        &access,
                        channel_id,
                        MessageEventKind::DeletedBulk {
                            message_ids: chunk.to_vec(),
                        },
                    ),
                )
                .await;
            }
        }
        Ok(report)
    }

    /// The author's messages created at or after `since_ms`, walking the ten-day buckets from the
    /// bucket of `since_ms` up to the newest one.
    async fn authored_since(&self, channel_id: ChannelId, author: UserId, since_ms: u64) -> AppResult<Vec<Message>> {
        let Some(newest) = self.messages.last_message_id(channel_id).await? else {
            return Ok(Vec::new());
        };
        let floor = self.clock.now_unix_ms().saturating_sub(MAX_LOOKBACK_MS);
        let first = Snowflake::first_at_unix_millis(since_ms.max(floor));
        if first > newest {
            return Ok(Vec::new());
        }
        let mut found = Vec::new();
        for bucket in Bucket::of(first).ascending_to(Bucket::of(newest)) {
            // Strictly above the id just below the first possible one.
            let mut bound = (Bucket::of(first) == bucket).then(|| Snowflake::new(first.value().saturating_sub(1)));
            loop {
                let rows = self
                    .messages
                    .scan_bucket(BucketQuery {
                        channel_id,
                        bucket,
                        direction: ScanDirection::Newer,
                        bound,
                        limit: SCAN_BATCH,
                    })
                    .await?;
                let Some(last) = rows.last().map(|m| m.id) else {
                    break;
                };
                found.extend(rows.iter().filter(|m| m.author_id == author).cloned());
                if rows.len() < SCAN_BATCH {
                    break;
                }
                bound = Some(last);
            }
        }
        Ok(found)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fakes::*;

    const DAY_MS: u64 = 86_400_000;

    #[tokio::test]
    async fn deletes_only_the_authors_recent_messages_and_announces_bulk() {
        let h = Harness::new();
        let old = h.seed(ALICE, "old").await;
        h.clock.advance_ms(2 * DAY_MS);
        let since = h.clock.now_unix_ms() - DAY_MS;
        let recent = h.seed(ALICE, "recent").await;
        let other = h.seed(BOB, "bob").await;
        let report = PurgeAuthorMessages::new(&h.ports)
            .execute(&[CHANNEL], ALICE, since)
            .await
            .unwrap();
        assert_eq!(
            report,
            PurgeReport {
                channels: 1,
                messages: 1
            }
        );
        assert!(h.messages.find(CHANNEL, recent.id).await.unwrap().is_none());
        assert!(h.messages.find(CHANNEL, old.id).await.unwrap().is_some());
        assert!(h.messages.find(CHANNEL, other.id).await.unwrap().is_some());
        assert_eq!(
            h.events.kinds(),
            vec![MessageEventKind::DeletedBulk {
                message_ids: vec![recent.id]
            }]
        );
    }

    #[tokio::test]
    async fn crosses_ten_day_buckets_and_replays_harmlessly() {
        let h = Harness::new();
        let first = h.seed(ALICE, "a").await;
        h.clock.advance_ms(6 * DAY_MS);
        let second = h.seed(ALICE, "b").await;
        let since = first.id.unix_millis();
        let purge = PurgeAuthorMessages::new(&h.ports);
        let report = purge.execute(&[CHANNEL], ALICE, since).await.unwrap();
        assert_eq!(report.messages, 2);
        assert!(h.messages.find(CHANNEL, second.id).await.unwrap().is_none());
        let again = purge.execute(&[CHANNEL], ALICE, since).await.unwrap();
        assert_eq!(again, PurgeReport::default());
    }

    #[tokio::test]
    async fn empty_channel_is_a_no_op() {
        let h = Harness::new();
        let report = PurgeAuthorMessages::new(&h.ports)
            .execute(&[CHANNEL], ALICE, 0)
            .await
            .unwrap();
        assert_eq!(report, PurgeReport::default());
        assert!(h.events.kinds().is_empty());
    }
}

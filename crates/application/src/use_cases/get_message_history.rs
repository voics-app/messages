use std::collections::BTreeMap;
use std::sync::Arc;

use discordy_messages_domain::paging::plan;
use discordy_messages_domain::reaction::summarize;
use discordy_messages_domain::{Bucket, ChannelId, HistoryCursor, Message, PageLimit, ReactionRow, Scan, UserId};

use super::MessagingPorts;
use crate::error::AppResult;
use crate::history_reader::HistoryReader;
use crate::ports::{BucketQuery, ChannelAccessProvider};
use crate::views::MessageView;

pub struct GetMessageHistory {
    access: Arc<dyn ChannelAccessProvider>,
    history: Arc<HistoryReader>,
}

impl GetMessageHistory {
    pub fn new(ports: &MessagingPorts) -> Self {
        Self {
            access: ports.access.clone(),
            history: ports.history.clone(),
        }
    }

    /// A page of history, newest first, walking buckets until the page is
    /// full or the channel's creation bucket is reached.
    pub async fn execute(
        &self,
        channel_id: ChannelId,
        viewer: UserId,
        cursor: HistoryCursor,
        limit: PageLimit,
    ) -> AppResult<Vec<MessageView>> {
        self.access
            .access(channel_id, viewer)
            .await?
            .ensure_can_read_history()?;
        let newest = self.history.last_message_id(channel_id).await?;
        let mut page = Vec::new();
        for scan in plan(cursor, limit, channel_id, newest) {
            page.extend(self.run_scan(channel_id, scan).await?);
        }
        page.sort_by_key(|m| std::cmp::Reverse(m.id));
        page.dedup_by_key(|m| m.id);
        self.decorate(channel_id, page, viewer).await
    }

    async fn run_scan(&self, channel_id: ChannelId, scan: Scan) -> AppResult<Vec<Message>> {
        let mut found: Vec<Message> = Vec::with_capacity(scan.limit);
        for bucket in scan.buckets {
            let remaining = scan.limit - found.len();
            if remaining == 0 {
                break;
            }
            let query = BucketQuery {
                channel_id,
                bucket,
                direction: scan.direction,
                bound: scan.bound,
                limit: remaining,
            };
            found.extend(self.history.scan_bucket(query).await?.iter().cloned());
        }
        Ok(found)
    }

    /// Attaches reactions with one (coalesced) range query per bucket.
    async fn decorate(&self, channel_id: ChannelId, page: Vec<Message>, viewer: UserId) -> AppResult<Vec<MessageView>> {
        let mut by_bucket: BTreeMap<Bucket, (Message, Message)> = BTreeMap::new();
        for message in &page {
            by_bucket
                .entry(Bucket::of(message.id))
                .and_modify(|(oldest, newest)| {
                    if message.id < oldest.id {
                        *oldest = message.clone();
                    }
                    if message.id > newest.id {
                        *newest = message.clone();
                    }
                })
                .or_insert_with(|| (message.clone(), message.clone()));
        }
        let mut rows: Vec<ReactionRow> = Vec::new();
        for (bucket, (oldest, newest)) in by_bucket {
            let found = self
                .history
                .reactions_in_range(channel_id, bucket, oldest.id, newest.id)
                .await?;
            rows.extend(found.iter().cloned());
        }
        Ok(page
            .into_iter()
            .map(|message| MessageView {
                reactions: summarize(rows.iter().filter(|r| r.message_id == message.id), viewer),
                message,
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::AppError;
    use crate::fakes::*;
    use crate::ports::ReactionRepository;
    use discordy_messages_domain::bucket::BUCKET_SIZE_MS;
    use discordy_messages_domain::{DomainError, Emoji, Permissions};
    use std::sync::atomic::Ordering;

    fn limit(n: u32) -> PageLimit {
        PageLimit::parse(Some(n)).unwrap()
    }

    fn contents(views: &[MessageView]) -> Vec<String> {
        views.iter().map(|v| v.message.content.clone()).collect()
    }

    /// Seeds one message per listed bucket offset (in ten-day steps).
    async fn seed_across_buckets(h: &Harness, count: u64) -> Vec<Message> {
        let mut seeded = Vec::new();
        for i in 0..count {
            seeded.push(h.seed(ALICE, &format!("m{i}")).await);
            h.clock.advance_ms(BUCKET_SIZE_MS);
        }
        seeded
    }

    #[tokio::test]
    async fn latest_page_walks_buckets_backwards_until_full() {
        let h = Harness::new();
        seed_across_buckets(&h, 5).await;
        let page = GetMessageHistory::new(&h.ports)
            .execute(CHANNEL, BOB, HistoryCursor::Latest, limit(3))
            .await
            .unwrap();
        assert_eq!(contents(&page), vec!["m4", "m3", "m2"]);
        assert_eq!(h.messages.scans.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn walks_down_to_the_creation_bucket_when_history_is_short() {
        let h = Harness::new();
        seed_across_buckets(&h, 2).await;
        let page = GetMessageHistory::new(&h.ports)
            .execute(CHANNEL, BOB, HistoryCursor::Latest, limit(50))
            .await
            .unwrap();
        assert_eq!(contents(&page), vec!["m1", "m0"]);
        let newest_bucket = Bucket::of(page[0].message.id).value();
        let creation_bucket = Bucket::of(CHANNEL).value();
        let expected_scans = (newest_bucket - creation_bucket + 1) as usize;
        assert_eq!(h.messages.scans.load(Ordering::SeqCst), expected_scans);
    }

    #[tokio::test]
    async fn before_after_and_around_page_correctly() {
        let h = Harness::new();
        let seeded = seed_across_buckets(&h, 5).await;
        let use_case = GetMessageHistory::new(&h.ports);
        let before = use_case
            .execute(CHANNEL, BOB, HistoryCursor::Before(seeded[3].id), limit(2))
            .await
            .unwrap();
        assert_eq!(contents(&before), vec!["m2", "m1"]);
        let after = use_case
            .execute(CHANNEL, BOB, HistoryCursor::After(seeded[1].id), limit(2))
            .await
            .unwrap();
        assert_eq!(contents(&after), vec!["m3", "m2"]);
        let around = use_case
            .execute(CHANNEL, BOB, HistoryCursor::Around(seeded[2].id), limit(3))
            .await
            .unwrap();
        assert_eq!(contents(&around), vec!["m3", "m2", "m1"]);
    }

    #[tokio::test]
    async fn empty_channels_cost_no_partition_reads() {
        let h = Harness::new();
        let page = GetMessageHistory::new(&h.ports)
            .execute(CHANNEL, BOB, HistoryCursor::Latest, limit(50))
            .await
            .unwrap();
        assert!(page.is_empty());
        assert_eq!(h.messages.scans.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn reactions_are_summarised_for_the_viewer() {
        let h = Harness::new();
        let message = h.seed(ALICE, "vote").await;
        let emoji = Emoji::parse("👍").unwrap();
        h.reactions.add(CHANNEL, message.id, &emoji, BOB).await.unwrap();
        h.reactions.add(CHANNEL, message.id, &emoji, ALICE).await.unwrap();
        let page = GetMessageHistory::new(&h.ports)
            .execute(CHANNEL, BOB, HistoryCursor::Latest, limit(10))
            .await
            .unwrap();
        assert_eq!(page[0].reactions[0].count, 2);
        assert!(page[0].reactions[0].me);
    }

    #[tokio::test]
    async fn requires_read_message_history() {
        let h = Harness::new();
        h.access.set_permissions(Permissions::VIEW_CHANNEL);
        let result = GetMessageHistory::new(&h.ports)
            .execute(CHANNEL, BOB, HistoryCursor::Latest, limit(10))
            .await;
        assert_eq!(result, Err(AppError::Domain(DomainError::MissingAccess)));
    }

    #[tokio::test]
    async fn one_hundred_concurrent_identical_reads_read_each_partition_once() {
        let h = Harness::new();
        let announcement = h.seed(ALICE, "announcement @everyone").await;
        let partitions = (Bucket::of(announcement.id).value() - Bucket::of(CHANNEL).value() + 1) as usize;
        let use_case = Arc::new(GetMessageHistory::new(&h.ports));
        let readers: Vec<_> = (0..100u64)
            .map(|i| {
                let use_case = Arc::clone(&use_case);
                tokio::spawn(async move {
                    use_case
                        .execute(CHANNEL, Snowflake::new(1_000 + i), HistoryCursor::Latest, limit(50))
                        .await
                })
            })
            .collect();
        for reader in readers {
            assert_eq!(reader.await.unwrap().unwrap().len(), 1);
        }
        assert_eq!(h.messages.scans.load(Ordering::SeqCst), partitions);
    }
}

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use discordy_messages_domain::{Bucket, DeliveryMode, IndexedMessage, ScanDirection};

use super::MessagingPorts;
use crate::error::AppResult;
use crate::ports::{
    AccessError, BackfillState, BucketQuery, ChannelAccessProvider, IndexOp, MessageRepository, SearchIndex,
};

/// Messages read from one partition per round trip.
const BATCH: usize = 500;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ReindexReport {
    pub channels: u64,
    pub messages: u64,
    /// Channels that could not be read and will be retried by the next run.
    pub failed_channels: u64,
}

/// Rebuilds the search index from ScyllaDB, the source of truth. Idempotent
/// and safe to run while the live consumer keeps the index current: upserts
/// never replace an entry with an older edit.
pub struct ReindexSearch {
    messages: Arc<dyn MessageRepository>,
    access: Arc<dyn ChannelAccessProvider>,
    index: Arc<dyn SearchIndex>,
    running: AtomicBool,
}

impl ReindexSearch {
    pub fn new(ports: &MessagingPorts) -> Self {
        Self {
            messages: ports.messages.clone(),
            access: ports.access.clone(),
            index: ports.search.clone(),
            running: AtomicBool::new(false),
        }
    }

    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    /// Runs a full pass. Returns `None` when another pass is already running.
    pub async fn execute(&self) -> AppResult<Option<ReindexReport>> {
        if self.running.swap(true, Ordering::SeqCst) {
            return Ok(None);
        }
        let result = self.run().await;
        self.running.store(false, Ordering::SeqCst);
        if result.is_err() {
            // Leave the state retryable rather than stuck on "running".
            let _ = self.index.set_backfill_state(BackfillState::Idle).await;
        }
        result.map(Some)
    }

    async fn run(&self) -> AppResult<ReindexReport> {
        self.index.set_backfill_state(BackfillState::Running).await?;
        let mut report = ReindexReport::default();
        for (channel_id, newest) in self.messages.channels_with_messages().await? {
            let guild_id = match self.access.channel(channel_id).await {
                Ok(access) if access.delivery_mode == DeliveryMode::Standard => access.guild_id,
                Ok(_) | Err(AccessError::UnknownChannel | AccessError::NotMember) => continue,
                Err(AccessError::Unavailable(error)) => {
                    tracing::warn!(%channel_id, %error, "reindex: channel lookup failed");
                    report.failed_channels += 1;
                    continue;
                }
            };
            report.channels += 1;
            for bucket in Bucket::of(newest).descending_to(Bucket::of(channel_id)) {
                let mut bound = None;
                loop {
                    let rows = self
                        .messages
                        .scan_bucket(BucketQuery {
                            channel_id,
                            bucket,
                            direction: ScanDirection::Older,
                            bound,
                            limit: BATCH,
                        })
                        .await?;
                    let Some(oldest) = rows.last().map(|m| m.id) else {
                        break;
                    };
                    let ops: Vec<IndexOp> = rows
                        .iter()
                        .filter_map(|m| IndexedMessage::from_message(m, guild_id))
                        .map(IndexOp::Upsert)
                        .collect();
                    report.messages += ops.len() as u64;
                    self.index.apply(&ops, None).await?;
                    if rows.len() < BATCH {
                        break;
                    }
                    bound = Some(oldest);
                }
            }
        }
        let state = if report.failed_channels == 0 {
            BackfillState::Complete
        } else {
            BackfillState::Idle
        };
        self.index.set_backfill_state(state).await?;
        tracing::info!(
            channels = report.channels,
            messages = report.messages,
            failed = report.failed_channels,
            "search reindex finished"
        );
        Ok(report)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fakes::*;
    use crate::ports::SearchRequest;
    use discordy_messages_domain::SearchFilters;

    fn everything() -> SearchRequest {
        SearchRequest {
            channels: vec![CHANNEL],
            filters: SearchFilters::default(),
            pinned: None,
        }
    }

    #[tokio::test]
    async fn indexes_every_stored_message_and_completes() {
        let harness = Harness::new();
        for text in ["one", "two", "three"] {
            harness.clock.advance_ms(7);
            harness.seed(ALICE, text).await;
        }
        let job = ReindexSearch::new(&harness.ports);
        let report = job.execute().await.unwrap().unwrap();
        assert_eq!(report.messages, 3);
        assert_eq!(report.channels, 1);
        assert_eq!(harness.search.len(), 3);
        assert_eq!(harness.search.backfill_state().await.unwrap(), BackfillState::Complete);
        // A second pass changes nothing.
        job.execute().await.unwrap().unwrap();
        assert_eq!(harness.search.len(), 3);
        let page = harness.search.search(&everything()).await.unwrap();
        assert_eq!(page.total, 3);
    }

    #[tokio::test]
    async fn non_standard_channels_are_skipped() {
        let harness = Harness::new();
        harness.seed(ALICE, "secret").await;
        harness.access.set(Ok(dm_access(DeliveryMode::Ephemeral)));
        let report = ReindexSearch::new(&harness.ports).execute().await.unwrap().unwrap();
        assert_eq!(report.messages, 0);
        assert_eq!(harness.search.len(), 0);
    }

    #[tokio::test]
    async fn an_unreachable_channel_keeps_the_job_retryable() {
        let harness = Harness::new();
        harness.seed(ALICE, "hello").await;
        harness
            .access
            .set(Err(AccessError::Unavailable(crate::PortError::new("api down"))));
        let report = ReindexSearch::new(&harness.ports).execute().await.unwrap().unwrap();
        assert_eq!(report.failed_channels, 1);
        assert_eq!(harness.search.backfill_state().await.unwrap(), BackfillState::Idle);
    }

    #[tokio::test]
    async fn only_one_pass_runs_at_a_time() {
        let harness = Harness::new();
        let job = ReindexSearch::new(&harness.ports);
        job.running.store(true, Ordering::SeqCst);
        assert!(job.execute().await.unwrap().is_none());
    }
}

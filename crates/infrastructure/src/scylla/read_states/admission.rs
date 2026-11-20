//! Serialize local work for a hot partition; Scylla LWT still fences every replica.

use std::sync::Arc;
use std::time::Duration;

use discordy_messages_domain::{ChannelId, UserId};
use moka::future::Cache;
use tokio::sync::{Mutex, OwnedMutexGuard};

const MAX_PARTITIONS: u64 = 4096;
const IDLE_RETENTION: Duration = Duration::from_secs(60);

pub(super) struct PartitionAdmission(Cache<(UserId, ChannelId), Arc<Mutex<()>>>);

impl PartitionAdmission {
    pub fn new() -> Self {
        Self(
            Cache::builder()
                .max_capacity(MAX_PARTITIONS)
                .time_to_idle(IDLE_RETENTION)
                .build(),
        )
    }

    pub async fn acquire(&self, user: UserId, channel: ChannelId) -> OwnedMutexGuard<()> {
        self.0
            .get_with((user, channel), async { Arc::new(Mutex::new(())) })
            .await
            .lock_owned()
            .await
    }
}

// Eviction can admit another local contender while an older guard lives. That
// only loses this optimization: revisions and the ledger's IF NOT EXISTS remain
// the correctness boundary. No lock or state is shared between repository instances.

use async_trait::async_trait;
use discordy_messages_domain::{ChannelId, GuildId, IndexedMessage, MessageId, SearchFilters};

use crate::error::PortError;

/// One change to the search index. Applying the same operation twice leaves
/// the index in the same state (at-least-once delivery is safe).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IndexOp {
    /// Inserts the message, or replaces it when the stored copy is not newer
    /// (compared by `edited_at_ms`).
    Upsert(IndexedMessage),
    Delete {
        channel_id: ChannelId,
        message_ids: Vec<MessageId>,
    },
}

/// The consumer position stored in the same transaction as the operations it
/// produced, so a crash never loses or double-applies a batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Checkpoint {
    pub topic: String,
    pub partition: i32,
    /// The offset to read next.
    pub next_offset: i64,
}

/// Restricts results by the channels' pins, which live in ScyllaDB, not in
/// the index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PinnedFilter {
    Only(Vec<MessageId>),
    Except(Vec<MessageId>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchRequest {
    /// Channels the caller may read; the index never looks beyond them.
    pub channels: Vec<ChannelId>,
    pub filters: SearchFilters,
    pub pinned: Option<PinnedFilter>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SearchHit {
    pub channel_id: ChannelId,
    pub message_id: MessageId,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SearchPage {
    pub total: u64,
    pub hits: Vec<SearchHit>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackfillState {
    /// No backfill has finished or is running.
    Idle,
    Running,
    /// Every stored message has been indexed at least once.
    Complete,
}

#[async_trait]
pub trait SearchIndex: Send + Sync {
    async fn apply(&self, ops: &[IndexOp], checkpoint: Option<&Checkpoint>) -> Result<(), PortError>;

    async fn checkpoint(&self, topic: &str, partition: i32) -> Result<Option<i64>, PortError>;

    /// Channels of the guild that hold indexed messages, limited to `only`
    /// when it is not empty.
    async fn guild_channels(&self, guild_id: GuildId, only: &[ChannelId]) -> Result<Vec<ChannelId>, PortError>;

    async fn search(&self, request: &SearchRequest) -> Result<SearchPage, PortError>;

    async fn backfill_state(&self) -> Result<BackfillState, PortError>;

    async fn set_backfill_state(&self, state: BackfillState) -> Result<(), PortError>;
}

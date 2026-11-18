use async_trait::async_trait;
use discordy_messages_domain::poll::{PollAggregate, PollDefinition, PollError, PollTransition, PollVote};
use discordy_messages_domain::{ChannelId, MessageId, UserId};

use crate::PortError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PollKey {
    pub channel_id: ChannelId,
    pub message_id: MessageId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PollSnapshotState {
    pub aggregate: PollAggregate,
    pub voter: Option<PollVote>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PollDueEntry {
    pub key: PollKey,
    pub expires_at_ms: i64,
}

impl PollDueEntry {
    pub fn shard(self) -> u8 {
        ((self.key.message_id.value() ^ self.key.channel_id.value()) % 64) as u8
    }
}

#[derive(Debug, Clone, Copy)]
pub struct VoteCommand {
    pub key: PollKey,
    pub user_id: UserId,
    pub answer_id: u8,
    pub selected: bool,
    pub expected_vote_version: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PollStoreError {
    #[error(transparent)]
    Rule(#[from] PollError),
    #[error(transparent)]
    Unavailable(#[from] PortError),
}

pub type PollStoreResult<T> = Result<T, PollStoreError>;

/// A metadata row and one viewer row are read in the same serial partition
/// snapshot. Vote writes atomically compare metadata and update both rows.
#[async_trait]
pub trait PollRepository: Send + Sync {
    /// Reserve metadata and its durable expiry index before inserting a message.
    /// Reusing an existing message ID is an error, never a poll reset.
    async fn initialize(&self, key: PollKey, definition: &PollDefinition) -> PollStoreResult<()>;
    async fn snapshot(&self, key: PollKey, viewer: UserId) -> PollStoreResult<Option<PollSnapshotState>>;
    async fn vote(&self, command: VoteCommand) -> PollStoreResult<PollTransition>;
    /// Returns the final snapshot and whether this call won the closing CAS.
    async fn close(&self, key: PollKey, early: bool) -> PollStoreResult<Option<(PollAggregate, bool)>>;
    /// Install a deleted fence before removing participant rows/index/message.
    async fn delete(&self, key: PollKey) -> PollStoreResult<()>;
    /// One bounded shard page; cursors must reset after reaching its end.
    async fn due_page(&self, shard: u8, after: Option<PollDueEntry>, limit: u16) -> PollStoreResult<Vec<PollDueEntry>>;
    async fn remove_due(&self, entry: PollDueEntry) -> PollStoreResult<()>;
}

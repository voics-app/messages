use crate::{AppResult, PortError};
use async_trait::async_trait;
use discordy_messages_domain::scheduled::{
    DispatchOutcome, ScheduleChange, ScheduleId, ScheduledDraft, ScheduledMessage,
};
use discordy_messages_domain::{ChannelAccess, ChannelId, Message, UserId};

pub const SCHEDULE_SHARDS: u8 = 64;
pub const SCHEDULE_PAGE_SIZE: u16 = 100;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ScheduleKey {
    pub author_id: UserId,
    pub id: ScheduleId,
}

impl From<&ScheduledMessage> for ScheduleKey {
    fn from(job: &ScheduledMessage) -> Self {
        Self {
            author_id: job.author_id,
            id: job.id,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct DueSchedule {
    pub wake_at_ms: u64,
    pub key: ScheduleKey,
    pub revision: u32,
}

impl DueSchedule {
    pub fn shard(self) -> u8 {
        let bits = self.key.id.bits();
        ((bits as u64 ^ (bits >> 64) as u64 ^ self.key.author_id.value()) % u64::from(SCHEDULE_SHARDS)) as u8
    }

    pub fn pending(job: &ScheduledMessage) -> Self {
        Self {
            wake_at_ms: job.scheduled_at_ms,
            key: job.into(),
            revision: job.revision,
        }
    }
}

pub struct ScheduleReservation {
    pub job: ScheduledMessage,
    pub created: bool,
}
pub struct SchedulePage {
    pub items: Vec<ScheduledMessage>,
    pub next_after: Option<ScheduleId>,
}

pub struct ScheduleListQuery {
    pub author_id: UserId,
    pub channel_id: ChannelId,
    pub after: Option<ScheduleId>,
    pub limit: u16,
    pub now_ms: u64,
}

/// All mutations serialize the job, author/channel quota and active discovery in one author partition.
#[async_trait]
pub trait ScheduleJournal: Send + Sync {
    async fn find(&self, key: ScheduleKey) -> Result<Option<ScheduledMessage>, PortError>;
    async fn reserve(&self, job: ScheduledMessage) -> AppResult<ScheduleReservation>;
    async fn change(&self, key: ScheduleKey, change: ScheduleChange, now: u64) -> AppResult<ScheduledMessage>;
    /// Bounded by the hard author quota, including internal preparing reservations.
    async fn active(&self, author: UserId) -> Result<Vec<ScheduledMessage>, PortError>;
    /// Reads at most limit journal rows; a filtered empty page can still have a cursor.
    async fn list(&self, query: ScheduleListQuery) -> Result<SchedulePage, PortError>;
}

/// The due index only discovers candidates. It never authorizes a dispatch or owns a draft.
#[async_trait]
pub trait ScheduleIndex: Send + Sync {
    async fn stage(&self, due: DueSchedule) -> Result<(), PortError>;
    async fn remove(&self, due: DueSchedule) -> Result<(), PortError>;
    async fn page(&self, shard: u8, after: Option<DueSchedule>, now: u64) -> Result<Vec<DueSchedule>, PortError>;
    async fn cursor(&self, shard: u8) -> Result<Option<DueSchedule>, PortError>;
    async fn save_cursor(&self, shard: u8, after: Option<DueSchedule>) -> Result<(), PortError>;
}

pub trait ScheduleAttempts: Send + Sync {
    fn next_attempt(&self) -> Result<ScheduleId, PortError>;
}

#[derive(Clone)]
pub struct PreparedScheduledMessage {
    pub message: Message,
    pub access: ChannelAccess,
}

#[derive(Clone)]
pub struct ScheduledAuthorDraft {
    pub author_id: UserId,
    pub channel_id: ChannelId,
    pub draft: ScheduledDraft,
}

/// The insertion method performs the fresh post-claim authorization and at most one storage call.
#[async_trait]
pub trait ScheduledDelivery: Send + Sync {
    async fn validate(&self, draft: &ScheduledAuthorDraft) -> AppResult<()>;
    async fn prepare(&self, draft: &ScheduledAuthorDraft) -> AppResult<PreparedScheduledMessage>;
    async fn insert(&self, message: &mut PreparedScheduledMessage) -> DispatchOutcome;
    async fn publish(&self, message: &PreparedScheduledMessage);
}

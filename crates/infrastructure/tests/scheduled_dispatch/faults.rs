use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

use async_trait::async_trait;
use discordy_messages_application::{
    AppResult, PortError,
    ports::{BucketQuery, MessageRepository, scheduled::*},
};
use discordy_messages_domain::scheduled::{ScheduleChange, ScheduledMessage};
use discordy_messages_domain::{ChannelId, Message, MessageId, UserId};

pub struct ObservedMessages {
    pub inner: Arc<dyn MessageRepository>,
    pub calls: AtomicUsize,
    pub lose_result: AtomicBool,
}

#[async_trait]
impl MessageRepository for ObservedMessages {
    async fn insert(&self, message: &Message) -> Result<(), PortError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.inner.insert(message).await?;
        if self.lose_result.swap(false, Ordering::SeqCst) {
            return Err(PortError::new("fixture lost successful message-write result"));
        }
        Ok(())
    }
    async fn update(&self, message: &Message) -> Result<(), PortError> {
        self.inner.update(message).await
    }
    async fn find(&self, channel: ChannelId, message: MessageId) -> Result<Option<Message>, PortError> {
        self.inner.find(channel, message).await
    }
    async fn delete(&self, channel: ChannelId, messages: &[MessageId]) -> Result<(), PortError> {
        self.inner.delete(channel, messages).await
    }
    async fn set_pinned(&self, channel: ChannelId, message: MessageId, pinned: bool) -> Result<(), PortError> {
        self.inner.set_pinned(channel, message, pinned).await
    }
    async fn scan_bucket(&self, query: BucketQuery) -> Result<Vec<Message>, PortError> {
        self.inner.scan_bucket(query).await
    }
    async fn last_message_id(&self, channel: ChannelId) -> Result<Option<MessageId>, PortError> {
        self.inner.last_message_id(channel).await
    }
    async fn channels_with_messages(&self) -> Result<Vec<(ChannelId, MessageId)>, PortError> {
        self.inner.channels_with_messages().await
    }
}

pub struct PausedJournal {
    pub inner: Arc<dyn ScheduleJournal>,
    pub before_claim: bool,
    pub armed: AtomicBool,
    pub entered: tokio::sync::Notify,
    pub release: tokio::sync::Notify,
    pub lose_claim_result: AtomicBool,
}

impl PausedJournal {
    pub fn new(inner: Arc<dyn ScheduleJournal>, before_claim: bool) -> Self {
        Self {
            inner,
            before_claim,
            armed: AtomicBool::new(true),
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
            lose_claim_result: AtomicBool::new(false),
        }
    }
}

#[async_trait]
impl ScheduleJournal for PausedJournal {
    async fn find(&self, key: ScheduleKey) -> Result<Option<ScheduledMessage>, PortError> {
        self.inner.find(key).await
    }
    async fn reserve(&self, job: ScheduledMessage) -> AppResult<ScheduleReservation> {
        self.inner.reserve(job).await
    }
    async fn change(&self, key: ScheduleKey, change: ScheduleChange, now: u64) -> AppResult<ScheduledMessage> {
        let claim = matches!(&change, ScheduleChange::Claim { .. });
        let guarded = if self.before_claim {
            claim
        } else {
            matches!(&change, ScheduleChange::Activate)
        };
        if guarded && self.armed.swap(false, Ordering::SeqCst) {
            self.entered.notify_one();
            self.release.notified().await;
        }
        let job = self.inner.change(key, change, now).await?;
        if claim && self.lose_claim_result.swap(false, Ordering::SeqCst) {
            return Err(PortError::new("fixture lost successful claim result").into());
        }
        Ok(job)
    }
    async fn active(&self, author: UserId) -> Result<Vec<ScheduledMessage>, PortError> {
        self.inner.active(author).await
    }
    async fn list(&self, query: ScheduleListQuery) -> Result<SchedulePage, PortError> {
        self.inner.list(query).await
    }
}

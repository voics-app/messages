use async_trait::async_trait;
use discordy_messages_domain::{ChannelId, MessageId, ReadState, UserId};

use crate::error::PortError;

#[async_trait]
pub trait ReadStateRepository: Send + Sync {
    async fn find(&self, user_id: UserId, channel_id: ChannelId) -> Result<Option<ReadState>, PortError>;

    /// Atomically records one eligible message, once. Replays and messages at or
    /// below the acknowledged high-water mark must not add a count.
    async fn record_mention(
        &self,
        user_id: UserId,
        channel_id: ChannelId,
        message_id: MessageId,
    ) -> Result<ReadState, PortError>;

    /// Advances the read marker and clears only mentions at or before it.
    /// Repeated/stale ACKs preserve newer counts and cannot move it backwards.
    async fn acknowledge(
        &self,
        user_id: UserId,
        channel_id: ChannelId,
        message_id: MessageId,
    ) -> Result<ReadState, PortError>;

    /// Manual Mark Unread: atomically replaces the visible marker/count. It
    /// must not reopen previously acknowledged or recorded message deliveries.
    async fn set_position(
        &self,
        user_id: UserId,
        channel_id: ChannelId,
        message_id: MessageId,
        mention_count: u32,
    ) -> Result<ReadState, PortError>;

    async fn for_user(&self, user_id: UserId) -> Result<Vec<ReadState>, PortError>;
}

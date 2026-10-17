use async_trait::async_trait;
use discordy_messages_domain::{Bucket, ChannelId, Emoji, MessageId, ReactionRow, UserId};

use crate::error::PortError;

#[async_trait]
pub trait ReactionRepository: Send + Sync {
    async fn add(
        &self,
        channel_id: ChannelId,
        message_id: MessageId,
        emoji: &Emoji,
        user_id: UserId,
    ) -> Result<(), PortError>;

    async fn remove(
        &self,
        channel_id: ChannelId,
        message_id: MessageId,
        emoji: &Emoji,
        user_id: UserId,
    ) -> Result<(), PortError>;

    async fn remove_all(&self, channel_id: ChannelId, message_id: MessageId) -> Result<(), PortError>;

    async fn for_message(&self, channel_id: ChannelId, message_id: MessageId) -> Result<Vec<ReactionRow>, PortError>;

    /// Reactions on messages with ids in `oldest..=newest`, all inside one
    /// bucket. Used to decorate a page of history with one query per bucket.
    async fn for_range(
        &self,
        channel_id: ChannelId,
        bucket: Bucket,
        oldest: MessageId,
        newest: MessageId,
    ) -> Result<Vec<ReactionRow>, PortError>;
}

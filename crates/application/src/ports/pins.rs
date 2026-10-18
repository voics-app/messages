use async_trait::async_trait;
use discordy_messages_domain::{ChannelId, MessageId};

use crate::error::PortError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Pin {
    pub message_id: MessageId,
    pub pinned_at_ms: u64,
}

#[async_trait]
pub trait PinRepository: Send + Sync {
    async fn pin(&self, channel_id: ChannelId, pin: Pin) -> Result<(), PortError>;

    async fn unpin(&self, channel_id: ChannelId, message_id: MessageId) -> Result<(), PortError>;

    /// Every pin of the channel, most recently pinned first.
    async fn list(&self, channel_id: ChannelId) -> Result<Vec<Pin>, PortError>;
}

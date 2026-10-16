use async_trait::async_trait;
use discordy_messages_domain::{ChannelId, ReadState, UserId};

use crate::error::PortError;

#[async_trait]
pub trait ReadStateRepository: Send + Sync {
    async fn find(&self, user_id: UserId, channel_id: ChannelId) -> Result<Option<ReadState>, PortError>;

    async fn save(&self, state: &ReadState) -> Result<(), PortError>;

    async fn for_user(&self, user_id: UserId) -> Result<Vec<ReadState>, PortError>;
}

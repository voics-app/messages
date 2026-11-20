use async_trait::async_trait;
pub use discordy_messages_domain::NotificationRecipient;
use discordy_messages_domain::{ChannelId, DeliveryMode, GuildId, UserId};

use crate::PortError;

#[derive(Clone, Debug)]
pub struct NotificationRecipientPage {
    pub guild_id: Option<GuildId>,
    pub delivery_mode: DeliveryMode,
    pub recipients: Vec<NotificationRecipient>,
    /// Last examined candidate, including candidates excluded by permission.
    pub next_after: Option<UserId>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecipientQuery {
    pub channel_id: ChannelId,
    pub author_id: UserId,
    pub after: Option<UserId>,
    pub limit: u16,
}

#[async_trait]
pub trait NotificationRecipients: Send + Sync {
    /// No cached policy or fallback to an unfiltered audience on failure.
    async fn page(&self, query: RecipientQuery) -> Result<NotificationRecipientPage, PortError>;
}

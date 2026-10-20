use async_trait::async_trait;
use discordy_messages_domain::{ChannelAccess, ChannelId, DomainError, UserId};

use crate::error::{AppError, PortError};

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AccessError {
    #[error("user is not a member of the channel")]
    NotMember,
    #[error("unknown channel")]
    UnknownChannel,
    #[error(transparent)]
    Unavailable(#[from] PortError),
}

impl From<AccessError> for AppError {
    fn from(error: AccessError) -> Self {
        match error {
            AccessError::NotMember => AppError::Domain(DomainError::MissingAccess),
            AccessError::UnknownChannel => AppError::Domain(DomainError::UnknownChannel),
            AccessError::Unavailable(port) => AppError::Unavailable(port),
        }
    }
}

/// Resolves a user's permissions and the channel's delivery mode (the api
/// service's internal access endpoint).
#[async_trait]
pub trait ChannelAccessProvider: Send + Sync {
    async fn access(&self, channel_id: ChannelId, user_id: UserId) -> Result<ChannelAccess, AccessError>;

    /// The channel's guild and delivery mode, without any user: for posts
    /// made by webhooks, which are not members. The permissions are empty.
    async fn channel(&self, channel_id: ChannelId) -> Result<ChannelAccess, AccessError>;

    /// Like [`access`](Self::access) but never answered from a cache. Used to double-check a
    /// delivery mode conflict: a user may have switched the chat back to Standard moments ago.
    async fn refresh(&self, channel_id: ChannelId, user_id: UserId) -> Result<ChannelAccess, AccessError> {
        self.access(channel_id, user_id).await
    }
}

use async_trait::async_trait;
use discordy_messages_domain::UserId;

use crate::error::PortError;

/// The caller behind a request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Principal {
    pub user_id: UserId,
    pub is_bot: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AuthError {
    #[error("invalid or expired credentials")]
    Invalid,
    #[error(transparent)]
    Unavailable(#[from] PortError),
}

/// Verifies an `Authorization` header value (`Bearer <jwt>` or `Bot <token>`).
#[async_trait]
pub trait Authenticator: Send + Sync {
    async fn authenticate(&self, authorization: &str) -> Result<Principal, AuthError>;
}

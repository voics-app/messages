//! Errors surfaced by use cases.

use discordy_messages_domain::DomainError;

/// An infrastructure failure (database, broker, storage or peer service).
/// Cloneable so coalesced subscribers can all receive it.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct PortError(pub String);

impl PortError {
    pub fn new(message: impl std::fmt::Display) -> Self {
        Self(message.to_string())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AppError {
    #[error(transparent)]
    Domain(#[from] DomainError),
    #[error("dependency unavailable: {0}")]
    Unavailable(#[from] PortError),
}

pub type AppResult<T> = Result<T, AppError>;

//! Typed domain errors. The interface layer maps each one to the Discord-style
//! error envelope `{code, message, errors?}`.

use crate::delivery_mode::DeliveryMode;

/// One invalid field of a request body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FieldError {
    pub field: String,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DomainError {
    #[error("invalid form body")]
    InvalidForm(Vec<FieldError>),
    #[error("missing access")]
    MissingAccess,
    #[error("missing permissions")]
    MissingPermissions,
    #[error("channel uses {0} delivery")]
    DeliveryModeConflict(DeliveryMode),
    #[error("unknown channel")]
    UnknownChannel,
    #[error("unknown message")]
    UnknownMessage,
    #[error("maximum number of pins reached")]
    MaxPinsReached,
}

impl DomainError {
    /// Shorthand for a single-field validation error.
    pub fn invalid(field: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::InvalidForm(vec![FieldError {
            field: field.into(),
            reason: reason.into(),
        }])
    }
}

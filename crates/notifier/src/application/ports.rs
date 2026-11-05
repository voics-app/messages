//! What the notifier needs from the outside world.

use std::time::Duration;

use async_trait::async_trait;

use crate::domain::{ChannelContext, OutboundPush, Recipient};

#[derive(Debug, Clone, thiserror::Error)]
#[error("{0}")]
pub struct PortError(pub String);

impl PortError {
    pub fn new(message: impl std::fmt::Display) -> Self {
        Self(message.to_string())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Platform {
    Apns,
    Fcm,
}

impl Platform {
    pub const fn label(self) -> &'static str {
        match self {
            Platform::Apns => "apns",
            Platform::Fcm => "fcm",
        }
    }
}

/// Which APNs host a token was issued for: development builds get sandbox tokens.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Environment {
    Production,
    Sandbox,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeviceToken {
    pub platform: Platform,
    pub token: String,
    pub environment: Environment,
    /// Whether the device wants a text preview in standard chats.
    pub previews: bool,
}

/// A member with push tokens, as the api service reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Target {
    pub recipient: Recipient,
    pub tokens: Vec<DeviceToken>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelTargets {
    pub context: ChannelContext,
    pub targets: Vec<Target>,
}

#[async_trait]
pub trait TargetDirectory: Send + Sync {
    /// The members who may be pushed for a message `author_id` wrote in `channel_id`.
    async fn targets(&self, channel_id: &str, author_id: &str) -> Result<ChannelTargets, PortError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SendError {
    /// The provider says this token will never work again: remove it.
    InvalidToken(String),
    /// A transient failure; try again, after `after` when the provider said how long.
    Retry { reason: String, after: Option<Duration> },
    /// Not worth retrying (bad request, bad credentials).
    Failed(String),
}

#[async_trait]
pub trait PushSender: Send + Sync {
    async fn send(&self, token: &DeviceToken, push: &OutboundPush) -> Result<(), SendError>;
}

#[async_trait]
pub trait TokenRegistry: Send + Sync {
    /// Removes a token the provider rejected.
    async fn forget(&self, token: &DeviceToken) -> Result<(), PortError>;
}

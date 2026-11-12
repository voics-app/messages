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
    /// The device wants pushes for mentions and direct messages only, whatever the server settings say.
    pub only_mentions: bool,
}

/// A member with push tokens, as the api service reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Target {
    pub recipient: Recipient,
    pub tokens: Vec<DeviceToken>,
    /// PushKit tokens: they only ring the phone for a call and need no notification permission.
    pub voip_tokens: Vec<DeviceToken>,
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

/// One page of devices of the whole audience, in user id order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DevicePage {
    pub devices: Vec<DeviceToken>,
    /// Pass it back to get the next page; `None` on the last one.
    pub next_after: Option<String>,
}

/// Where a notification sent by hand finds its devices: one user's, or everyone's in pages.
#[async_trait]
pub trait DeviceDirectory: Send + Sync {
    /// The live notification tokens of one user (none for an unknown user).
    async fn user_devices(&self, user_id: &str) -> Result<Vec<DeviceToken>, PortError>;
    async fn device_page(&self, after: Option<&str>) -> Result<DevicePage, PortError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SendError {
    /// The provider says this token will never work again: remove it.
    InvalidToken(String),
    /// A transient failure; try again, after `after` when the provider said how long.
    Retry { reason: String, after: Option<Duration> },
    /// Not worth retrying (bad request, bad credentials).
    Failed(String),
    /// No sender is configured for the token's provider: nothing was tried.
    Unsupported(String),
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

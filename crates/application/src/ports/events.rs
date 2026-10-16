use async_trait::async_trait;
use discordy_messages_domain::{Audience, ChannelId, Emoji, GuildId, Message, MessageId, UserId};

use crate::error::PortError;

/// A state change to announce on `discordy.message.v1`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageEvent {
    pub channel_id: ChannelId,
    /// The guild the channel belongs to, echoed in the dispatch payload.
    pub guild_id: Option<GuildId>,
    /// Who the gateway delivers the event to.
    pub audience: Audience,
    pub kind: MessageEventKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MessageEventKind {
    Created { message: Message, nonce: Option<String> },
    Updated { message: Message },
    Deleted { message_id: MessageId },
    DeletedBulk { message_ids: Vec<MessageId> },
    ReactionAdded { message_id: MessageId, user_id: UserId, emoji: Emoji },
    ReactionRemoved { message_id: MessageId, user_id: UserId, emoji: Emoji },
    ReactionsCleared { message_id: MessageId },
    PinsUpdated { last_pin_timestamp_ms: Option<u64> },
    Acknowledged { message_id: MessageId },
}

/// Publishes domain events in the P6 envelope.
#[async_trait]
pub trait EventPublisher: Send + Sync {
    async fn publish(&self, event: MessageEvent) -> Result<(), PortError>;
}

/// Raw access to the event bus: one keyed record on one topic.
#[async_trait]
pub trait EventSink: Send + Sync {
    async fn send(&self, topic: &str, key: &str, payload: Vec<u8>) -> Result<(), PortError>;
}

//! Message forwarding: a frozen copy of a message sent into another channel.

use crate::attachment::Attachment;
use crate::mentions::Mentions;
use crate::message::Message;
use crate::{ChannelId, GuildId, MessageId};

/// A frozen copy of the forwarded message. It never changes when the source is
/// edited or deleted. Its attachments are copies stored under the channel of
/// the message that carries the snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageSnapshot {
    pub content: String,
    pub attachments: Vec<Attachment>,
    pub embeds: Vec<String>,
    pub mentions: Mentions,
    pub flags: u32,
    /// Creation time of the original message.
    pub created_at_ms: u64,
    pub edited_at_ms: Option<u64>,
}

/// Where a forwarded message came from, plus the snapshot taken at forward time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForwardOrigin {
    pub channel_id: ChannelId,
    pub message_id: MessageId,
    pub guild_id: Option<GuildId>,
    pub snapshot: MessageSnapshot,
}

impl MessageSnapshot {
    /// Snapshots `source`. `attachments` are the already copied files.
    pub fn of(source: &Message, attachments: Vec<Attachment>) -> Self {
        Self {
            content: source.content.clone(),
            attachments,
            embeds: source.embeds.clone(),
            mentions: Mentions {
                users: source.mentions.users.clone(),
                roles: source.mentions.roles.clone(),
                // The forward must not notify everyone again.
                everyone: false,
                here: false,
            },
            flags: source.flags,
            created_at_ms: source.timestamp_ms(),
            edited_at_ms: source.edited_at_ms,
        }
    }
}

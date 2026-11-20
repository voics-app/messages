//! A user's access to one channel, as resolved by the api service
//! (`GET /internal/v1/channels/{id}/access/{user}`), plus the message rules
//! that depend on it.

use crate::delivery_mode::DeliveryMode;
use crate::error::DomainError;
use crate::permissions::Permissions;
use crate::{GuildId, UserId};

/// Discord channel types for threads (10, 11, 12) use
/// SEND_MESSAGES_IN_THREADS instead of SEND_MESSAGES.
const THREAD_CHANNEL_TYPES: [u8; 3] = [10, 11, 12];

/// Maximum number of pinned messages per channel (Discord parity).
pub const MAX_PINS: usize = 50;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelAccess {
    pub permissions: Permissions,
    pub guild_id: Option<GuildId>,
    pub channel_type: u8,
    pub thread_locked: bool,
    pub delivery_mode: DeliveryMode,
    pub recipient_ids: Vec<UserId>,
}

/// Who receives an event about this channel (pinned interface P6 routing).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Audience {
    /// Fan out through the guild process, filtered by VIEW_CHANNEL.
    Guild(GuildId),
    /// Deliver to every session of each listed user (DMs, group DMs, acks).
    Users(Vec<UserId>),
}

impl ChannelAccess {
    fn is_guild_channel(&self) -> bool {
        self.guild_id.is_some()
    }

    pub fn is_thread(&self) -> bool {
        THREAD_CHANNEL_TYPES.contains(&self.channel_type)
    }

    pub fn audience(&self) -> Audience {
        match self.guild_id {
            Some(guild_id) => Audience::Guild(guild_id),
            None => Audience::Users(self.recipient_ids.clone()),
        }
    }

    pub fn ensure_can_view(&self) -> Result<(), DomainError> {
        if self.permissions.has(Permissions::VIEW_CHANNEL) {
            Ok(())
        } else {
            Err(DomainError::MissingAccess)
        }
    }

    pub fn ensure_can_read_history(&self) -> Result<(), DomainError> {
        self.ensure_can_view()?;
        if self.permissions.has(Permissions::READ_MESSAGE_HISTORY) {
            Ok(())
        } else {
            Err(DomainError::MissingAccess)
        }
    }

    /// A locked thread accepts no new messages until explicitly unlocked.
    pub fn ensure_thread_unlocked(&self) -> Result<(), DomainError> {
        if self.is_thread() && self.thread_locked {
            Err(DomainError::MissingPermissions)
        } else {
            Ok(())
        }
    }

    /// Sending requires the right permissions and a Standard channel: other
    /// delivery modes never touch the server's message store.
    pub fn ensure_can_send(&self, with_attachments: bool) -> Result<(), DomainError> {
        self.ensure_can_view()?;
        self.ensure_thread_unlocked()?;
        let send = if self.is_thread() {
            Permissions::SEND_MESSAGES_IN_THREADS
        } else {
            Permissions::SEND_MESSAGES
        };
        if !self.permissions.has(send) {
            return Err(DomainError::MissingPermissions);
        }
        if with_attachments && !self.permissions.has(Permissions::ATTACH_FILES) {
            return Err(DomainError::MissingPermissions);
        }
        self.ensure_standard_delivery()
    }

    /// Upload slots for attachments. Standard channels store the file as is; Ephemeral channels
    /// store client-side encrypted ciphertext (opaque to this service, referenced only from inside
    /// the end-to-end encrypted message). P2P channels never touch the server.
    pub fn ensure_can_upload(&self) -> Result<(), DomainError> {
        match self.delivery_mode {
            DeliveryMode::Standard => self.ensure_can_send(true),
            DeliveryMode::Ephemeral => {
                // Same permission checks as sending; only the mode gate differs.
                let mut as_standard = self.clone();
                as_standard.delivery_mode = DeliveryMode::Standard;
                as_standard.ensure_can_send(true)
            }
            DeliveryMode::P2p => Err(DomainError::DeliveryModeConflict(DeliveryMode::P2p)),
        }
    }

    /// Only Standard channels are stored by this service.
    pub fn ensure_standard_delivery(&self) -> Result<(), DomainError> {
        match self.delivery_mode {
            DeliveryMode::Standard => Ok(()),
            other => Err(DomainError::DeliveryModeConflict(other)),
        }
    }

    /// `@everyone` / `@here` only notify in guild channels and need
    /// MENTION_EVERYONE.
    pub fn may_mention_everyone(&self) -> bool {
        self.is_guild_channel() && self.permissions.has(Permissions::MENTION_EVERYONE)
    }

    /// Moderation (deleting others' messages, bulk delete, clearing
    /// reactions) exists only in guild channels.
    pub fn ensure_can_manage_messages(&self) -> Result<(), DomainError> {
        self.ensure_can_view()?;
        if self.is_guild_channel() && self.permissions.has(Permissions::MANAGE_MESSAGES) {
            Ok(())
        } else {
            Err(DomainError::MissingPermissions)
        }
    }

    pub fn ensure_can_delete(&self, actor: UserId, author: UserId) -> Result<(), DomainError> {
        self.ensure_can_view()?;
        if actor == author {
            Ok(())
        } else {
            self.ensure_can_manage_messages()
        }
    }

    /// The author or a moderator may hide a message's link previews.
    pub fn ensure_can_suppress_embeds(&self, actor: UserId, author: UserId) -> Result<(), DomainError> {
        self.ensure_can_delete(actor, author)
    }

    /// Anyone in a DM may pin; guild channels need MANAGE_MESSAGES or
    /// PIN_MESSAGES.
    pub fn ensure_can_pin(&self) -> Result<(), DomainError> {
        self.ensure_can_view()?;
        let allowed = !self.is_guild_channel()
            || self.permissions.has(Permissions::MANAGE_MESSAGES)
            || self.permissions.has(Permissions::PIN_MESSAGES);
        if allowed {
            Ok(())
        } else {
            Err(DomainError::MissingPermissions)
        }
    }

    /// Joining an existing reaction only needs history access; adding a new
    /// emoji also needs ADD_REACTIONS (Discord's rule).
    pub fn ensure_can_react(&self, emoji_already_present: bool) -> Result<(), DomainError> {
        self.ensure_can_read_history()?;
        if emoji_already_present || self.permissions.has(Permissions::ADD_REACTIONS) {
            Ok(())
        } else {
            Err(DomainError::MissingPermissions)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Snowflake;

    const VIEW: u64 = Permissions::VIEW_CHANNEL;
    const SEND: u64 = Permissions::SEND_MESSAGES;
    const HISTORY: u64 = Permissions::READ_MESSAGE_HISTORY;

    fn guild(bits: u64) -> ChannelAccess {
        ChannelAccess {
            permissions: Permissions::new(bits),
            guild_id: Some(Snowflake::new(1)),
            channel_type: 0,
            thread_locked: false,
            delivery_mode: DeliveryMode::Standard,
            recipient_ids: vec![],
        }
    }

    fn dm(bits: u64, mode: DeliveryMode) -> ChannelAccess {
        ChannelAccess {
            permissions: Permissions::new(bits),
            guild_id: None,
            channel_type: 1,
            thread_locked: false,
            delivery_mode: mode,
            recipient_ids: vec![Snowflake::new(10), Snowflake::new(11)],
        }
    }

    #[test]
    fn locked_threads_reject_sends_even_for_administrators() {
        for kind in [10, 11, 12] {
            let mut thread = guild(Permissions::ADMINISTRATOR);
            thread.channel_type = kind;
            thread.thread_locked = true;
            assert_eq!(thread.ensure_can_send(false), Err(DomainError::MissingPermissions));
            assert_eq!(thread.ensure_thread_unlocked(), Err(DomainError::MissingPermissions));
            thread.thread_locked = false;
            assert!(thread.ensure_can_send(false).is_ok());
        }
    }

    #[test]
    fn sending_needs_view_and_send() {
        assert_eq!(guild(SEND).ensure_can_send(false), Err(DomainError::MissingAccess));
        assert_eq!(guild(VIEW).ensure_can_send(false), Err(DomainError::MissingPermissions));
        assert!(guild(VIEW | SEND).ensure_can_send(false).is_ok());
    }

    #[test]
    fn attachments_need_attach_files() {
        assert_eq!(
            guild(VIEW | SEND).ensure_can_send(true),
            Err(DomainError::MissingPermissions)
        );
        assert!(
            guild(VIEW | SEND | Permissions::ATTACH_FILES)
                .ensure_can_send(true)
                .is_ok()
        );
    }

    #[test]
    fn threads_use_send_messages_in_threads() {
        let mut thread = guild(VIEW | SEND);
        thread.channel_type = 11;
        assert_eq!(thread.ensure_can_send(false), Err(DomainError::MissingPermissions));
        thread.permissions = Permissions::new(VIEW | Permissions::SEND_MESSAGES_IN_THREADS);
        assert!(thread.ensure_can_send(false).is_ok());
    }

    #[test]
    fn non_standard_delivery_modes_conflict() {
        for mode in [DeliveryMode::Ephemeral, DeliveryMode::P2p] {
            assert_eq!(
                dm(VIEW | SEND, mode).ensure_can_send(false),
                Err(DomainError::DeliveryModeConflict(mode))
            );
        }
        assert!(dm(VIEW | SEND, DeliveryMode::Standard).ensure_can_send(false).is_ok());
    }

    #[test]
    fn uploads_are_allowed_for_standard_and_ephemeral_but_not_p2p() {
        assert!(
            dm(VIEW | SEND | Permissions::ATTACH_FILES, DeliveryMode::Standard)
                .ensure_can_upload()
                .is_ok()
        );
        assert!(
            dm(VIEW | SEND | Permissions::ATTACH_FILES, DeliveryMode::Ephemeral)
                .ensure_can_upload()
                .is_ok()
        );
        assert_eq!(
            dm(VIEW | SEND | Permissions::ATTACH_FILES, DeliveryMode::P2p).ensure_can_upload(),
            Err(DomainError::DeliveryModeConflict(DeliveryMode::P2p))
        );
        assert_eq!(
            dm(VIEW | SEND, DeliveryMode::Ephemeral).ensure_can_upload(),
            Err(DomainError::MissingPermissions)
        );
    }

    #[test]
    fn internal_posts_need_only_a_standard_channel() {
        assert!(guild(0).ensure_standard_delivery().is_ok());
        assert_eq!(
            dm(0, DeliveryMode::Ephemeral).ensure_standard_delivery(),
            Err(DomainError::DeliveryModeConflict(DeliveryMode::Ephemeral))
        );
    }

    #[test]
    fn history_needs_read_message_history() {
        assert_eq!(guild(VIEW).ensure_can_read_history(), Err(DomainError::MissingAccess));
        assert!(guild(VIEW | HISTORY).ensure_can_read_history().is_ok());
    }

    #[test]
    fn authors_delete_their_own_messages_moderators_delete_any() {
        let author = Snowflake::new(5);
        let other = Snowflake::new(6);
        assert!(guild(VIEW).ensure_can_delete(author, author).is_ok());
        assert_eq!(
            guild(VIEW).ensure_can_delete(other, author),
            Err(DomainError::MissingPermissions)
        );
        assert!(
            guild(VIEW | Permissions::MANAGE_MESSAGES)
                .ensure_can_delete(other, author)
                .is_ok()
        );
    }

    #[test]
    fn dms_have_no_moderators() {
        let access = dm(VIEW | Permissions::MANAGE_MESSAGES, DeliveryMode::Standard);
        assert_eq!(
            access.ensure_can_manage_messages(),
            Err(DomainError::MissingPermissions)
        );
    }

    #[test]
    fn anyone_pins_in_dms_guilds_need_manage_or_pin() {
        assert!(dm(VIEW, DeliveryMode::Standard).ensure_can_pin().is_ok());
        assert_eq!(guild(VIEW).ensure_can_pin(), Err(DomainError::MissingPermissions));
        assert!(guild(VIEW | Permissions::PIN_MESSAGES).ensure_can_pin().is_ok());
        assert!(guild(VIEW | Permissions::MANAGE_MESSAGES).ensure_can_pin().is_ok());
    }

    #[test]
    fn new_reactions_need_add_reactions_existing_ones_do_not() {
        let access = guild(VIEW | HISTORY);
        assert_eq!(access.ensure_can_react(false), Err(DomainError::MissingPermissions));
        assert!(access.ensure_can_react(true).is_ok());
    }

    #[test]
    fn everyone_mentions_only_in_guilds_with_permission() {
        assert!(!guild(VIEW).may_mention_everyone());
        assert!(guild(VIEW | Permissions::MENTION_EVERYONE).may_mention_everyone());
        assert!(!dm(Permissions::ADMINISTRATOR, DeliveryMode::Standard).may_mention_everyone());
    }

    #[test]
    fn audience_is_guild_or_recipients() {
        assert_eq!(guild(VIEW).audience(), Audience::Guild(Snowflake::new(1)));
        assert_eq!(
            dm(VIEW, DeliveryMode::Standard).audience(),
            Audience::Users(vec![Snowflake::new(10), Snowflake::new(11)])
        );
    }
}

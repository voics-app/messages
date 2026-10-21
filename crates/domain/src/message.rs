//! The Message aggregate.

use crate::attachment::{Attachment, MAX_ATTACHMENTS_PER_MESSAGE};
use crate::error::{DomainError, FieldError};
use crate::mentions::Mentions;
use crate::message_origin::{InteractionLink, WebhookAuthor};
use crate::{ChannelId, MessageId, UserId};

/// Maximum content length in characters (Discord parity).
pub const MAX_CONTENT_CHARS: usize = 2000;
/// Maximum embeds per message (Discord parity).
pub const MAX_EMBEDS: usize = 10;

/// Discord message types used by this service.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageKind {
    Default,
    /// Group DM: a member was added (the target is in `mentions.users`).
    RecipientAdd,
    /// Group DM: a member was removed or left.
    RecipientRemove,
    /// Group DM: renamed (content is the new name).
    ChannelNameChange,
    /// Group DM: icon changed.
    ChannelIconChange,
    Reply,
}

impl MessageKind {
    pub const fn code(self) -> u8 {
        match self {
            Self::Default => 0,
            Self::RecipientAdd => 1,
            Self::RecipientRemove => 2,
            Self::ChannelNameChange => 4,
            Self::ChannelIconChange => 5,
            Self::Reply => 19,
        }
    }

    pub const fn from_code(code: u8) -> Self {
        match code {
            1 => Self::RecipientAdd,
            2 => Self::RecipientRemove,
            4 => Self::ChannelNameChange,
            5 => Self::ChannelIconChange,
            19 => Self::Reply,
            _ => Self::Default,
        }
    }

    /// Server-generated messages that carry no user text.
    pub const fn is_system(self) -> bool {
        matches!(
            self,
            Self::RecipientAdd | Self::RecipientRemove | Self::ChannelNameChange | Self::ChannelIconChange
        )
    }
}

/// What an author submits when sending a message. Embeds and components are
/// opaque JSON objects (validated as JSON by the interface layer).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MessageDraft {
    pub content: String,
    pub attachments: Vec<Attachment>,
    pub embeds: Vec<String>,
    pub components: Vec<String>,
    pub reply_to: Option<MessageId>,
    pub flags: u32,
    pub webhook: Option<WebhookAuthor>,
    pub interaction: Option<InteractionLink>,
    /// Set by internal posts of group DM system messages.
    pub system_kind: Option<MessageKind>,
    /// The user a recipient add/remove message is about.
    pub system_target: Option<UserId>,
}

/// A partial update; `None` leaves the field unchanged.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MessageEdit {
    pub content: Option<String>,
    pub embeds: Option<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    pub id: MessageId,
    pub channel_id: ChannelId,
    pub author_id: UserId,
    pub kind: MessageKind,
    pub content: String,
    pub attachments: Vec<Attachment>,
    pub embeds: Vec<String>,
    pub components: Vec<String>,
    pub mentions: Mentions,
    pub reply_to: Option<MessageId>,
    pub webhook: Option<WebhookAuthor>,
    pub interaction: Option<InteractionLink>,
    pub flags: u32,
    pub edited_at_ms: Option<u64>,
    pub pinned: bool,
}

impl Message {
    pub fn compose(
        id: MessageId,
        channel_id: ChannelId,
        author_id: UserId,
        draft: MessageDraft,
        may_mention_everyone: bool,
    ) -> Result<Self, DomainError> {
        let kind = match draft.system_kind {
            Some(kind) if kind.is_system() => kind,
            _ if draft.reply_to.is_some() => MessageKind::Reply,
            _ => MessageKind::Default,
        };
        let mut mentions = if kind.is_system() {
            Mentions::default()
        } else {
            Mentions::parse(&draft.content, may_mention_everyone)
        };
        if let (true, Some(target)) = (kind.is_system(), draft.system_target) {
            mentions.users.insert(target);
        }
        let message = Self {
            id,
            channel_id,
            author_id,
            kind,
            mentions,
            content: draft.content,
            attachments: draft.attachments,
            embeds: draft.embeds,
            components: draft.components,
            reply_to: draft.reply_to,
            flags: draft.flags,
            webhook: draft.webhook,
            interaction: draft.interaction,
            edited_at_ms: None,
            pinned: false,
        };
        message.validate()?;
        Ok(message)
    }

    /// Only the author may edit a message.
    pub fn apply_edit(
        &mut self,
        editor: UserId,
        edit: MessageEdit,
        may_mention_everyone: bool,
        now_unix_ms: u64,
    ) -> Result<(), DomainError> {
        if editor != self.author_id {
            return Err(DomainError::MissingPermissions);
        }
        let mut edited = self.clone();
        if let Some(content) = edit.content {
            edited.mentions = Mentions::parse(&content, may_mention_everyone);
            edited.content = content;
        }
        if let Some(embeds) = edit.embeds {
            edited.embeds = embeds;
        }
        edited.validate()?;
        edited.edited_at_ms = Some(now_unix_ms);
        *self = edited;
        Ok(())
    }

    /// Creation time, derived from the Snowflake.
    pub fn timestamp_ms(&self) -> u64 {
        self.id.unix_millis()
    }

    fn validate(&self) -> Result<(), DomainError> {
        let mut errors = Vec::new();
        let mut reject = |field: &str, reason: &str| {
            errors.push(FieldError {
                field: field.to_owned(),
                reason: reason.to_owned(),
            });
        };
        if self.content.chars().count() > MAX_CONTENT_CHARS {
            reject("content", "must be 2000 or fewer characters");
        }
        if self.attachments.len() > MAX_ATTACHMENTS_PER_MESSAGE {
            reject("attachments", "must be 10 or fewer");
        }
        if self.embeds.len() > MAX_EMBEDS {
            reject("embeds", "must be 10 or fewer");
        }
        let is_empty = !self.kind.is_system()
            && self.content.trim().is_empty()
            && self.attachments.is_empty()
            && self.embeds.is_empty();
        if is_empty {
            reject("content", "cannot send an empty message");
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(DomainError::InvalidForm(errors))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Snowflake;
    use crate::attachment::AttachmentFilename;

    const AUTHOR: Snowflake = Snowflake::new(5);

    fn draft(content: &str) -> MessageDraft {
        MessageDraft {
            content: content.to_owned(),
            ..MessageDraft::default()
        }
    }

    fn compose(d: MessageDraft) -> Result<Message, DomainError> {
        Message::compose(Snowflake::new(100), Snowflake::new(1), AUTHOR, d, false)
    }

    fn attachment() -> Attachment {
        Attachment {
            id: Snowflake::new(9),
            filename: AttachmentFilename::parse("a.png").unwrap(),
            content_type: Some("image/png".into()),
            size: 10,
            width: None,
            height: None,
        }
    }

    #[test]
    fn composes_a_system_message_without_content() {
        let message = compose(MessageDraft {
            system_kind: Some(MessageKind::RecipientAdd),
            system_target: Some(Snowflake::new(9)),
            ..MessageDraft::default()
        })
        .unwrap();
        assert_eq!(message.kind.code(), 1);
        assert!(message.mentions.users.contains(&Snowflake::new(9)));
        assert!(compose(MessageDraft::default()).is_err());
    }

    #[test]
    fn composes_a_default_message_with_mentions() {
        let message = compose(draft("hi <@7>")).unwrap();
        assert_eq!(message.kind, MessageKind::Default);
        assert!(message.mentions.users.contains(&Snowflake::new(7)));
        assert_eq!(message.edited_at_ms, None);
    }

    #[test]
    fn replies_have_the_reply_kind() {
        let mut d = draft("yes");
        d.reply_to = Some(Snowflake::new(50));
        assert_eq!(compose(d).unwrap().kind, MessageKind::Reply);
    }

    #[test]
    fn rejects_empty_messages() {
        assert!(matches!(compose(draft("   ")), Err(DomainError::InvalidForm(_))));
    }

    #[test]
    fn attachments_alone_are_not_empty() {
        let mut d = draft("");
        d.attachments = vec![attachment()];
        assert!(compose(d).is_ok());
    }

    #[test]
    fn enforces_content_and_collection_limits() {
        assert!(compose(draft(&"é".repeat(2000))).is_ok());
        assert!(compose(draft(&"é".repeat(2001))).is_err());
        let mut d = draft("x");
        d.attachments = vec![attachment(); 11];
        assert!(compose(d).is_err());
        let mut d = draft("x");
        d.embeds = vec!["{}".into(); 11];
        assert!(compose(d).is_err());
    }

    #[test]
    fn authors_edit_content_and_mentions_are_reparsed() {
        let mut message = compose(draft("hi <@7>")).unwrap();
        let edit = MessageEdit {
            content: Some("bye <@8>".into()),
            embeds: None,
        };
        message.apply_edit(AUTHOR, edit, false, 1234).unwrap();
        assert_eq!(message.content, "bye <@8>");
        assert!(message.mentions.users.contains(&Snowflake::new(8)));
        assert!(!message.mentions.users.contains(&Snowflake::new(7)));
        assert_eq!(message.edited_at_ms, Some(1234));
    }

    #[test]
    fn only_the_author_may_edit() {
        let mut message = compose(draft("hi")).unwrap();
        let edit = MessageEdit {
            content: Some("hacked".into()),
            embeds: None,
        };
        assert_eq!(
            message.apply_edit(Snowflake::new(6), edit, false, 1),
            Err(DomainError::MissingPermissions)
        );
        assert_eq!(message.content, "hi");
    }

    #[test]
    fn invalid_edits_leave_the_message_untouched() {
        let mut message = compose(draft("hi")).unwrap();
        let edit = MessageEdit {
            content: Some(String::new()),
            embeds: None,
        };
        assert!(message.apply_edit(AUTHOR, edit, false, 1).is_err());
        assert_eq!(message.content, "hi");
        assert_eq!(message.edited_at_ms, None);
    }

    #[test]
    fn webhook_and_interaction_origins_are_kept() {
        let mut d = draft("from a webhook");
        d.webhook = Some(WebhookAuthor {
            webhook_id: Snowflake::new(40),
            username: Some("Relay".into()),
            avatar_url: None,
        });
        d.interaction = Some(InteractionLink {
            interaction_id: Snowflake::new(41),
            application_id: Snowflake::new(42),
        });
        d.flags = 4;
        let message = compose(d).unwrap();
        assert_eq!(message.webhook.as_ref().unwrap().webhook_id, Snowflake::new(40));
        assert_eq!(message.webhook.as_ref().unwrap().username.as_deref(), Some("Relay"));
        assert_eq!(message.interaction.unwrap().application_id, Snowflake::new(42));
        assert_eq!(message.flags, 4);
    }

    #[test]
    fn plain_messages_have_no_origin_metadata() {
        let message = compose(draft("hi")).unwrap();
        assert_eq!(message.webhook, None);
        assert_eq!(message.interaction, None);
        assert_eq!(message.flags, 0);
    }

    #[test]
    fn kind_codes_match_discord() {
        assert_eq!(MessageKind::Default.code(), 0);
        assert_eq!(MessageKind::Reply.code(), 19);
        assert_eq!(MessageKind::from_code(19), MessageKind::Reply);
    }
}

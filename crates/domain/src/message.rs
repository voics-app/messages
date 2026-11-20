//! The Message aggregate.

use crate::attachment::{
    ATTACHMENT_FLAG_SPOILER, Attachment, AttachmentMetadataEdit, MAX_ATTACHMENTS_PER_MESSAGE, validate_description,
};
use crate::error::{DomainError, FieldError};
use crate::forward::ForwardOrigin;
use crate::mentions::Mentions;
use crate::message_origin::{FLAG_SUPPRESS_EMBEDS, InteractionLink, WebhookAuthor};
use crate::{ChannelId, MessageId, UserId};

/// Maximum content length in characters (Discord parity).
pub const MAX_CONTENT_CHARS: usize = 2000;
/// Internal durable forum reservation row; never exposed as a message.
pub const FORUM_MARKER_FLAG: u32 = 1 << 30;
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
    /// DM or group DM: a call started (see [`Call`]).
    Call,
    /// A message was pinned (`reply_to` is the pinned message).
    ChannelPinnedMessage,
    /// A member joined the guild (rendered with a greeting picked from the id).
    GuildMemberJoin,
    /// A thread was created (`thread_id` is the thread, content its name).
    ThreadCreated,
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
            Self::Call => 3,
            Self::ChannelPinnedMessage => 6,
            Self::GuildMemberJoin => 7,
            Self::ThreadCreated => 18,
            Self::Reply => 19,
        }
    }

    pub const fn from_code(code: u8) -> Self {
        match code {
            1 => Self::RecipientAdd,
            2 => Self::RecipientRemove,
            3 => Self::Call,
            6 => Self::ChannelPinnedMessage,
            7 => Self::GuildMemberJoin,
            18 => Self::ThreadCreated,
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
            Self::RecipientAdd
                | Self::RecipientRemove
                | Self::ChannelNameChange
                | Self::ChannelIconChange
                | Self::Call
                | Self::ChannelPinnedMessage
                | Self::GuildMemberJoin
                | Self::ThreadCreated
        )
    }
}

/// The payload of a call system message (type 3).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Call {
    pub participants: Vec<UserId>,
    /// Unset while the call is running.
    pub ended_at_ms: Option<u64>,
}

impl Call {
    /// The call a type 3 message starts with: the caller is always a participant.
    fn starting(call: Option<Self>, caller: UserId) -> Self {
        let mut call = call.unwrap_or_default();
        if call.participants.is_empty() {
            call.participants.push(caller);
        }
        call
    }
}

/// What an author submits when sending a message. Embeds and components are
/// opaque JSON objects (validated as JSON by the interface layer).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MessageDraft {
    pub content: String,
    pub tts: bool,
    pub attachments: Vec<Attachment>,
    pub embeds: Vec<String>,
    pub components: Vec<String>,
    pub reply_to: Option<MessageId>,
    /// The author of the replied-to message when the reply pings them
    /// (`allowed_mentions.replied_user` not false); added to `mentions.users`.
    pub reply_ping: Option<UserId>,
    pub flags: u32,
    pub webhook: Option<WebhookAuthor>,
    pub interaction: Option<InteractionLink>,
    /// Set by internal posts of group DM system messages.
    pub system_kind: Option<MessageKind>,
    /// The user a recipient add/remove message is about.
    pub system_target: Option<UserId>,
    /// The call a type 3 message reports.
    pub call: Option<Call>,
    /// The thread a type 18 message announces.
    pub thread_id: Option<ChannelId>,
    /// Set when forwarding another message.
    pub forward: Option<ForwardOrigin>,
}

/// A partial update; `None` leaves the field unchanged.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MessageEdit {
    pub content: Option<String>,
    pub embeds: Option<Vec<String>>,
    pub attachments: Option<Vec<AttachmentMetadataEdit>>,
    /// Only the SUPPRESS_EMBEDS bit is honoured; other bits are ignored.
    pub flags: Option<u32>,
}

impl MessageEdit {
    /// Whether the edit rewrites the message body (as opposed to only flags).
    pub fn changes_body(&self) -> bool {
        self.content.is_some()
            || self.embeds.is_some()
            || self.attachments.as_ref().is_some_and(|edits| !edits.is_empty())
    }

    /// The requested SUPPRESS_EMBEDS state, if the edit carries flags.
    pub fn suppress_embeds(&self) -> Option<bool> {
        self.flags.map(|flags| flags & FLAG_SUPPRESS_EMBEDS != 0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    pub id: MessageId,
    pub channel_id: ChannelId,
    pub author_id: UserId,
    pub kind: MessageKind,
    pub content: String,
    pub tts: bool,
    pub attachments: Vec<Attachment>,
    pub embeds: Vec<String>,
    pub components: Vec<String>,
    pub mentions: Mentions,
    pub reply_to: Option<MessageId>,
    pub webhook: Option<WebhookAuthor>,
    pub interaction: Option<InteractionLink>,
    /// Present on forwarded messages (message_reference type FORWARD).
    pub forward: Option<ForwardOrigin>,
    /// Present on call messages (type 3).
    pub call: Option<Call>,
    /// Present on thread created messages (type 18).
    pub thread_id: Option<ChannelId>,
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
        if let (MessageKind::Reply, Some(author)) = (kind, draft.reply_ping) {
            mentions.users.insert(author);
        }
        let message = Self {
            id,
            channel_id,
            author_id,
            kind,
            mentions,
            content: draft.content,
            tts: draft.tts && !kind.is_system(),
            attachments: draft.attachments,
            embeds: draft.embeds,
            components: draft.components,
            reply_to: draft.reply_to,
            flags: draft.flags,
            webhook: draft.webhook,
            interaction: draft.interaction,
            forward: draft.forward,
            call: (kind == MessageKind::Call).then(|| Call::starting(draft.call, author_id)),
            thread_id: draft.thread_id.filter(|_| kind == MessageKind::ThreadCreated),
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
        if let Some(attachments) = edit.attachments {
            if attachments.len() > MAX_ATTACHMENTS_PER_MESSAGE {
                return Err(DomainError::invalid("attachments", "must be 10 or fewer"));
            }
            let mut seen = std::collections::HashSet::new();
            for update in attachments {
                if !seen.insert(update.id) {
                    return Err(DomainError::invalid("attachments", "duplicate attachment id"));
                }
                let attachment = edited
                    .attachments
                    .iter_mut()
                    .find(|attachment| attachment.id == update.id)
                    .ok_or_else(|| DomainError::invalid("attachments", "unknown attachment id"))?;
                if let Some(description) = update.description {
                    attachment.description = validate_description(description.as_deref())?;
                }
                if let Some(flags) = update.flags {
                    attachment.flags =
                        (attachment.flags & !ATTACHMENT_FLAG_SPOILER) | (flags & ATTACHMENT_FLAG_SPOILER);
                }
            }
        }
        edited.validate()?;
        edited.edited_at_ms = Some(now_unix_ms);
        *self = edited;
        Ok(())
    }

    /// Records how a call went: when it ended and who took part. Only call
    /// messages carry a call.
    pub fn update_call(
        &mut self,
        ended_at_ms: Option<u64>,
        participants: Option<Vec<UserId>>,
    ) -> Result<(), DomainError> {
        let call = self
            .call
            .as_mut()
            .ok_or_else(|| DomainError::invalid("message_id", "not a call message"))?;
        if let Some(participants) = participants {
            call.participants = participants;
        }
        if ended_at_ms.is_some() {
            call.ended_at_ms = ended_at_ms;
        }
        Ok(())
    }

    pub fn embeds_suppressed(&self) -> bool {
        self.flags & FLAG_SUPPRESS_EMBEDS != 0
    }

    /// Sets or clears SUPPRESS_EMBEDS. Not an edit: `edited_at` is untouched.
    pub fn set_embeds_suppressed(&mut self, suppressed: bool) {
        if suppressed {
            self.flags |= FLAG_SUPPRESS_EMBEDS;
        } else {
            self.flags &= !FLAG_SUPPRESS_EMBEDS;
        }
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
        if self.kind == MessageKind::ThreadCreated && self.thread_id.is_none() {
            reject("thread_id", "required for thread created messages");
        }
        let is_empty = !self.kind.is_system()
            && self.content.trim().is_empty()
            && self.attachments.is_empty()
            && self.embeds.is_empty()
            && self.forward.is_none();
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
            description: None,
            flags: 0,
        }
    }

    #[test]
    fn attachment_edits_are_atomic_and_preserve_unmentioned_metadata() {
        let mut original = compose(MessageDraft {
            content: "original".into(),
            attachments: vec![Attachment {
                flags: 16,
                ..attachment()
            }],
            ..MessageDraft::default()
        })
        .unwrap();
        let update = AttachmentMetadataEdit {
            id: Snowflake::new(9),
            description: Some(Some("new alt".into())),
            flags: Some(8),
        };
        for invalid in [
            vec![update.clone(), update.clone()],
            vec![
                update.clone(),
                AttachmentMetadataEdit {
                    id: Snowflake::new(10),
                    ..update.clone()
                },
            ],
            vec![AttachmentMetadataEdit {
                description: Some(Some("x".repeat(1025))),
                ..update.clone()
            }],
            vec![update.clone(); 11],
        ] {
            let mut message = original.clone();
            assert!(
                message
                    .apply_edit(
                        AUTHOR,
                        MessageEdit {
                            content: Some("new body".into()),
                            attachments: Some(invalid),
                            ..MessageEdit::default()
                        },
                        false,
                        100
                    )
                    .is_err()
            );
            assert_eq!(
                message, original,
                "invalid attachment edit must not partially change a message"
            );
        }
        original
            .apply_edit(
                AUTHOR,
                MessageEdit {
                    attachments: Some(vec![update]),
                    ..MessageEdit::default()
                },
                false,
                100,
            )
            .unwrap();
        assert_eq!(
            original.attachments[0].flags, 24,
            "non-spoiler stored bits are preserved"
        );
        original
            .apply_edit(
                AUTHOR,
                MessageEdit {
                    attachments: Some(vec![AttachmentMetadataEdit {
                        id: Snowflake::new(9),
                        description: None,
                        flags: Some(0),
                    }]),
                    ..MessageEdit::default()
                },
                false,
                200,
            )
            .unwrap();
        assert_eq!(original.attachments[0].description.as_deref(), Some("new alt"));
        assert_eq!(original.attachments[0].flags, 16);
        assert_eq!(original.content, "original");
    }

    #[test]
    fn composes_a_system_message_without_content() {
        let message = compose(MessageDraft {
            system_kind: Some(MessageKind::RecipientAdd),
            tts: true,
            system_target: Some(Snowflake::new(9)),
            ..MessageDraft::default()
        })
        .unwrap();
        assert_eq!(message.kind.code(), 1);
        assert!(!message.tts, "system messages never trigger text to speech");
        assert!(message.mentions.users.contains(&Snowflake::new(9)));
        assert!(compose(MessageDraft::default()).is_err());
    }

    #[test]
    fn composes_the_new_system_kinds() {
        for code in [3, 6, 7, 18] {
            assert!(MessageKind::from_code(code).is_system());
            assert_eq!(MessageKind::from_code(code).code(), code);
        }
        let pin = compose(MessageDraft {
            system_kind: Some(MessageKind::ChannelPinnedMessage),
            reply_to: Some(Snowflake::new(50)),
            ..MessageDraft::default()
        })
        .unwrap();
        assert_eq!(pin.reply_to, Some(Snowflake::new(50)));
        assert!(pin.mentions.users.is_empty());
        assert!(
            compose(MessageDraft {
                system_kind: Some(MessageKind::GuildMemberJoin),
                ..MessageDraft::default()
            })
            .is_ok()
        );
    }

    #[test]
    fn a_call_message_always_lists_its_caller() {
        let call = compose(MessageDraft {
            system_kind: Some(MessageKind::Call),
            ..MessageDraft::default()
        })
        .unwrap()
        .call
        .unwrap();
        assert_eq!(call.participants, vec![AUTHOR]);
        assert_eq!(call.ended_at_ms, None);
    }

    #[test]
    fn updating_a_call_sets_the_end_and_participants() {
        let mut call = compose(MessageDraft {
            system_kind: Some(MessageKind::Call),
            ..MessageDraft::default()
        })
        .unwrap();
        call.update_call(Some(9_000), Some(vec![AUTHOR, Snowflake::new(6)]))
            .unwrap();
        assert_eq!(call.call.as_ref().unwrap().ended_at_ms, Some(9_000));
        assert_eq!(call.call.unwrap().participants.len(), 2);
        let mut text = compose(draft("hi")).unwrap();
        assert!(text.update_call(Some(1), None).is_err());
    }

    #[test]
    fn call_and_thread_payloads_belong_to_their_kind_only() {
        let plain = compose(MessageDraft {
            content: "hi".into(),
            call: Some(Call::default()),
            thread_id: Some(Snowflake::new(8)),
            ..MessageDraft::default()
        })
        .unwrap();
        assert_eq!((plain.call, plain.thread_id), (None, None));
        let thread = MessageDraft {
            system_kind: Some(MessageKind::ThreadCreated),
            content: "plans".into(),
            ..MessageDraft::default()
        };
        assert!(compose(thread.clone()).is_err());
        let thread = compose(MessageDraft {
            thread_id: Some(Snowflake::new(8)),
            ..thread
        })
        .unwrap();
        assert_eq!(thread.thread_id, Some(Snowflake::new(8)));
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
    fn a_forward_without_a_comment_is_not_empty() {
        let mut d = draft("");
        d.forward = Some(ForwardOrigin {
            channel_id: Snowflake::new(2),
            message_id: Snowflake::new(3),
            guild_id: None,
            snapshot: crate::MessageSnapshot {
                content: "orig".into(),
                attachments: vec![],
                embeds: vec![],
                mentions: Mentions::default(),
                flags: 0,
                created_at_ms: 1,
                edited_at_ms: None,
            },
        });
        let message = compose(d).unwrap();
        assert_eq!(message.kind, MessageKind::Default);
        assert!(message.forward.is_some());
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
            attachments: None,
            flags: None,
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
            attachments: None,
            flags: None,
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
            attachments: None,
            flags: None,
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
    fn suppressing_embeds_toggles_only_its_bit() {
        let mut d = draft("hi");
        d.flags = 1 << 6;
        let mut message = compose(d).unwrap();
        message.set_embeds_suppressed(true);
        assert!(message.embeds_suppressed());
        assert_eq!(message.flags, (1 << 6) | 4);
        message.set_embeds_suppressed(false);
        assert_eq!(message.flags, 1 << 6);
        assert_eq!(message.edited_at_ms, None);
    }

    #[test]
    fn flag_only_edits_do_not_change_the_body() {
        let edit = MessageEdit {
            flags: Some(4),
            ..MessageEdit::default()
        };
        assert!(!edit.changes_body());
        assert_eq!(edit.suppress_embeds(), Some(true));
        assert_eq!(MessageEdit::default().suppress_embeds(), None);
    }

    #[test]
    fn kind_codes_match_discord() {
        assert_eq!(MessageKind::Default.code(), 0);
        assert_eq!(MessageKind::Reply.code(), 19);
        assert_eq!(MessageKind::from_code(19), MessageKind::Reply);
    }
}

use std::collections::HashSet;
use std::sync::Arc;

use discordy_messages_domain::attachment::MAX_ATTACHMENT_BYTES;
use discordy_messages_domain::attachment::{upload_flags, validate_description};
use discordy_messages_domain::{
    Attachment, Audience, Call, ChannelAccess, ChannelId, DomainError, FLAG_EPHEMERAL, ForwardOrigin, InteractionLink,
    Message, MessageDraft, MessageId, MessageKind, MessageSnapshot, UploadKey, UserId, WebhookAuthor,
};

use super::MessagingPorts;
use super::UnfurlLinks;
use super::record_mentions::RecordMentions;
use super::support::{announce, checked_access, event};
use crate::error::AppResult;
use crate::ports::{
    ActivityRepository, AttachmentStorage, ChannelAccessProvider, EventPublisher, IdGenerator, MessageEvent,
    MessageEventKind, MessageRepository,
};
use crate::views::MessageView;

/// A file uploaded through a presigned URL, referenced by its upload key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttachmentReference {
    pub uploaded_filename: String,
    /// Alt text.
    pub description: Option<String>,
    /// Requested `ATTACHMENT_FLAG_*` bits (only the spoiler bit is kept).
    pub flags: u32,
}

/// The message to forward (message_reference type FORWARD).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ForwardSource {
    pub channel_id: ChannelId,
    pub message_id: MessageId,
}

/// Trusted, journalled delivery of one announcement into a follower channel.
pub struct CrosspostDelivery {
    pub source_channel: ChannelId,
    pub target_channel: ChannelId,
    pub message_id: MessageId,
    pub webhook_id: UserId,
    pub webhook_name: String,
    pub content: String,
    pub embeds: Vec<String>,
    pub attachments: Vec<Attachment>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CreateMessageCommand {
    pub channel_id: ChannelId,
    pub author_id: UserId,
    pub content: String,
    pub tts: bool,
    pub nonce: Option<String>,
    pub attachments: Vec<AttachmentReference>,
    pub embeds: Vec<String>,
    pub components: Vec<String>,
    pub reply_to: Option<MessageId>,
    /// `allowed_mentions.replied_user == false`: the reply does not mention
    /// (or notify) the replied-to author.
    pub suppress_reply_ping: bool,
    /// Forwards another message; `content` is then the optional added comment.
    pub forward: Option<ForwardSource>,
    /// Discord message flags (internal posts only; `FLAG_EPHEMERAL` relays).
    pub flags: u32,
    /// Set for webhook posts; `author_id` is then the webhook id.
    pub webhook: Option<WebhookAuthor>,
    /// Set for interaction responses.
    pub interaction: Option<InteractionLink>,
    /// Recipients of an ephemeral message. Only these users receive it.
    pub target_user_ids: Vec<UserId>,
    /// Group DM system message kind (internal posts only).
    pub system_kind: Option<MessageKind>,
    /// The user a recipient add/remove system message is about.
    pub system_target: Option<UserId>,
    /// The call a type 3 system message reports.
    pub call: Option<Call>,
    /// The thread a type 18 system message announces.
    pub thread_id: Option<ChannelId>,
}

pub struct CreateMessage {
    access: Arc<dyn ChannelAccessProvider>,
    messages: Arc<dyn MessageRepository>,
    mentions: RecordMentions,
    activity: Arc<dyn ActivityRepository>,
    storage: Arc<dyn AttachmentStorage>,
    events: Arc<dyn EventPublisher>,
    ids: Arc<dyn IdGenerator>,
    unfurl: Arc<UnfurlLinks>,
}

fn ensure_tts_permission(access: &ChannelAccess, tts: bool) -> Result<(), DomainError> {
    if tts
        && !access
            .permissions
            .has(discordy_messages_domain::Permissions::SEND_TTS_MESSAGES)
    {
        Err(DomainError::MissingPermissions)
    } else {
        Ok(())
    }
}

#[cfg(test)]
#[path = "create_message_mentions.rs"]
mod mention_tests;

#[path = "create_message_scheduled.rs"]
mod scheduled;

impl CreateMessage {
    pub async fn deliver_crosspost(&self, command: CrosspostDelivery) -> AppResult<MessageView> {
        let access = self.access.channel(command.target_channel).await?;
        if access.guild_id.is_none() || access.channel_type != 0 {
            return Err(DomainError::invalid("channel_id", "must be a guild text channel").into());
        }
        access.ensure_standard_delivery()?;
        for attachment in &command.attachments {
            if !self
                .storage
                .copy(
                    &attachment.key(command.source_channel),
                    &attachment.key(command.target_channel),
                )
                .await?
            {
                return Err(DomainError::invalid("attachments", "source attachment is unavailable").into());
            }
        }
        let draft = MessageDraft {
            content: command.content,
            embeds: command.embeds,
            attachments: command.attachments,
            flags: discordy_messages_domain::FLAG_IS_CROSSPOST,
            webhook: Some(WebhookAuthor {
                webhook_id: command.webhook_id,
                username: Some(command.webhook_name),
                avatar_url: None,
            }),
            ..MessageDraft::default()
        };
        let mut message = Message::compose(
            command.message_id,
            command.target_channel,
            command.webhook_id,
            draft,
            false,
        )?;
        // A publisher cannot notify arbitrary users in the destination server.
        message.mentions = Default::default();
        self.messages.insert(&message).await?;
        self.publish_stored(&message, &access, None).await;
        Ok(MessageView::without_reactions(message))
    }
    pub fn new(ports: &MessagingPorts) -> Self {
        Self {
            access: ports.access.clone(),
            messages: ports.messages.clone(),
            mentions: RecordMentions::new(ports),
            activity: ports.activity.clone(),
            storage: ports.storage.clone(),
            events: ports.events.clone(),
            ids: ports.ids.clone(),
            unfurl: Arc::new(UnfurlLinks::new(ports)),
        }
    }

    pub async fn execute(&self, command: CreateMessageCommand) -> AppResult<MessageView> {
        if let Some(source) = command.forward {
            return self.forward(source, command).await;
        }
        let with_attachments = !command.attachments.is_empty();
        let access = checked_access(&*self.access, command.channel_id, command.author_id, |a| {
            a.ensure_can_send(with_attachments)?;
            ensure_tts_permission(a, command.tts)
        })
        .await?;
        self.store(&access, command, None).await
    }

    /// Posts on behalf of a bot, webhook or interaction. api has already
    /// authorised the call, so the user permission check is skipped; the
    /// delivery mode is still enforced. A webhook is not a guild member, so
    /// its channel is resolved without a user.
    pub async fn execute_internal(&self, command: CreateMessageCommand) -> AppResult<MessageView> {
        let access = match command.webhook {
            Some(_) => self.access.channel(command.channel_id).await?,
            None => self.access.access(command.channel_id, command.author_id).await?,
        };
        access.ensure_standard_delivery()?;
        if command.flags & FLAG_EPHEMERAL != 0 {
            return self.relay_ephemeral(&access, command).await;
        }
        self.store(&access, command, None).await
    }

    /// Forwards a message: the caller needs to read the source and to send in
    /// the target. The snapshot owns copies of the source's files, so deleting
    /// the source never breaks the forward. Both ends must be standard-mode;
    /// ephemeral and P2P chats are re-encrypted by the client.
    async fn forward(&self, source: ForwardSource, command: CreateMessageCommand) -> AppResult<MessageView> {
        let source_access = checked_access(&*self.access, source.channel_id, command.author_id, |a| {
            a.ensure_can_read_history()?;
            a.ensure_standard_delivery()
        })
        .await?;
        let original = self
            .messages
            .find(source.channel_id, source.message_id)
            .await?
            .filter(|m| !m.kind.is_system() && m.flags & FLAG_EPHEMERAL == 0)
            .ok_or_else(|| DomainError::invalid("message_reference", "unknown message"))?;
        // Forwarding a forward re-forwards the original content.
        let (channel_id, message_id, guild_id, snapshot) = match &original.forward {
            Some(origin) => (
                origin.channel_id,
                origin.message_id,
                origin.guild_id,
                origin.snapshot.clone(),
            ),
            None => (
                original.channel_id,
                original.id,
                source_access.guild_id,
                MessageSnapshot::of(&original, original.attachments.clone()),
            ),
        };
        let with_attachments = !snapshot.attachments.is_empty() || !command.attachments.is_empty();
        let access = checked_access(&*self.access, command.channel_id, command.author_id, |a| {
            a.ensure_can_send(with_attachments)?;
            ensure_tts_permission(a, command.tts)
        })
        .await?;
        // Files live under the channel of the message that owns them.
        let attachments = self
            .copy_attachments(original.channel_id, command.channel_id, &snapshot.attachments)
            .await?;
        let origin = ForwardOrigin {
            channel_id,
            message_id,
            guild_id,
            snapshot: MessageSnapshot {
                attachments,
                ..snapshot
            },
        };
        self.store(&access, command, Some(origin)).await
    }

    async fn copy_attachments(
        &self,
        from: ChannelId,
        to: ChannelId,
        attachments: &[Attachment],
    ) -> AppResult<Vec<Attachment>> {
        let mut copies = Vec::with_capacity(attachments.len());
        for attachment in attachments {
            let copy = Attachment {
                id: self.ids.next_id()?,
                ..attachment.clone()
            };
            if self.storage.copy(&attachment.key(from), &copy.key(to)).await? {
                copies.push(copy);
            } else {
                tracing::warn!(attachment = %attachment.id, "forwarded attachment is gone from storage");
            }
        }
        Ok(copies)
    }

    async fn store(
        &self,
        access: &ChannelAccess,
        command: CreateMessageCommand,
        forward: Option<ForwardOrigin>,
    ) -> AppResult<MessageView> {
        let nonce = command.nonce.clone();
        let message = self.compose_message(access, &command, forward).await?;
        self.messages.insert(&message).await?;
        self.publish_stored(&message, access, nonce).await;
        Ok(MessageView::without_reactions(message))
    }

    async fn publish_stored(&self, message: &Message, access: &ChannelAccess, nonce: Option<String>) {
        self.record_side_effects(message, access.guild_id).await;
        announce(
            self.events.as_ref(),
            event(
                access,
                message.channel_id,
                MessageEventKind::Created {
                    message: message.clone(),
                    nonce,
                },
            ),
        )
        .await;
        self.unfurl.spawn(message);
    }

    /// Ephemeral replies reach only their targets and are never stored, so
    /// they leave no trace in ScyllaDB, read states or activity counters.
    async fn relay_ephemeral(&self, access: &ChannelAccess, command: CreateMessageCommand) -> AppResult<MessageView> {
        if command.target_user_ids.is_empty() {
            return Err(DomainError::invalid("target_user_ids", "required for ephemeral messages").into());
        }
        let message = self.compose_message(access, &command, None).await?;
        let event = MessageEvent {
            channel_id: command.channel_id,
            guild_id: access.guild_id,
            audience: Audience::Users(command.target_user_ids),
            kind: MessageEventKind::Created {
                message: message.clone(),
                nonce: command.nonce,
            },
        };
        announce(self.events.as_ref(), event).await;
        Ok(MessageView::without_reactions(message))
    }

    async fn compose_message(
        &self,
        access: &ChannelAccess,
        command: &CreateMessageCommand,
        forward: Option<ForwardOrigin>,
    ) -> AppResult<Message> {
        let mut reply_ping = None;
        if let Some(reply_to) = command.reply_to {
            let target = self.reply_target(command.channel_id, reply_to).await?;
            if !command.suppress_reply_ping && target.webhook.is_none() {
                reply_ping = Some(target.author_id);
            }
        }
        let draft = MessageDraft {
            content: command.content.clone(),
            tts: command.tts,
            attachments: self
                .resolve_attachments(command.channel_id, &command.attachments)
                .await?,
            embeds: command.embeds.clone(),
            components: command.components.clone(),
            reply_to: command.reply_to,
            reply_ping,
            flags: command.flags,
            webhook: command.webhook.clone(),
            interaction: command.interaction,
            system_kind: command.system_kind,
            system_target: command.system_target,
            call: command.call.clone(),
            thread_id: command.thread_id,
            forward,
        };
        Ok(Message::compose(
            self.ids.next_id()?,
            command.channel_id,
            command.author_id,
            draft,
            access.may_mention_everyone(),
        )?)
    }

    async fn reply_target(&self, channel_id: ChannelId, reply_to: MessageId) -> AppResult<Message> {
        match self.messages.find(channel_id, reply_to).await? {
            Some(message) => Ok(message),
            None => Err(DomainError::invalid("message_reference", "unknown message").into()),
        }
    }

    async fn resolve_attachments(
        &self,
        channel_id: ChannelId,
        references: &[AttachmentReference],
    ) -> AppResult<Vec<Attachment>> {
        let mut seen = HashSet::new();
        let mut attachments = Vec::with_capacity(references.len());
        for reference in references {
            let key = UploadKey::parse_for_channel(&reference.uploaded_filename, channel_id)?;
            if !seen.insert(key.attachment_id) {
                return Err(DomainError::invalid("attachments", "duplicate attachment").into());
            }
            let stored = self
                .storage
                .stat(&key)
                .await?
                .ok_or_else(|| DomainError::invalid("attachments", "file was not uploaded"))?;
            if stored.size > MAX_ATTACHMENT_BYTES {
                return Err(DomainError::invalid("attachments", "file is larger than 25 MiB").into());
            }
            attachments.push(Attachment {
                id: key.attachment_id,
                content_type: stored.content_type,
                size: stored.size,
                width: None,
                height: None,
                description: validate_description(reference.description.as_deref())?,
                flags: upload_flags(&key.filename, reference.flags),
                filename: key.filename,
            });
        }
        Ok(attachments)
    }

    /// Admin counters and mention badges. Best effort: the message itself is
    /// already stored.
    async fn record_side_effects(&self, message: &Message, guild: Option<discordy_messages_domain::GuildId>) {
        if let Err(error) = self.activity.record_standard_message(message.id).await {
            tracing::warn!(%error, "failed to record message activity");
        }
        if let Err(error) = self.mentions.execute(message, guild).await {
            tracing::warn!(message_id = %message.id, channel_id = %message.channel_id, %error, "failed to record mention accounting");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::AppError;
    use crate::fakes::*;
    use crate::ports::{AccessError, ReadStateRepository};
    use discordy_messages_domain::{
        Audience, DeliveryMode, FLAG_SUPPRESS_NOTIFICATIONS, Permissions, ReadState, Snowflake,
    };

    fn command(content: &str) -> CreateMessageCommand {
        CreateMessageCommand {
            channel_id: CHANNEL,
            author_id: ALICE,
            content: content.to_owned(),
            nonce: Some("n-1".into()),
            ..CreateMessageCommand::default()
        }
    }

    fn ephemeral_reply(targets: Vec<UserId>) -> CreateMessageCommand {
        CreateMessageCommand {
            flags: FLAG_EPHEMERAL,
            target_user_ids: targets,
            ..command("only you")
        }
    }

    #[tokio::test]
    async fn stores_and_publishes_a_message() {
        let h = Harness::new();
        let view = CreateMessage::new(&h.ports).execute(command("hello")).await.unwrap();
        let stored = h.messages.find(CHANNEL, view.message.id).await.unwrap().unwrap();
        assert_eq!(stored.content, "hello");
        let events = h.events.events.lock().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].audience, Audience::Guild(GUILD));
        assert_eq!(
            events[0].kind,
            MessageEventKind::Created {
                message: stored,
                nonce: Some("n-1".into())
            }
        );
    }

    #[tokio::test]
    async fn ids_come_from_the_messages_worker_range() {
        let h = Harness::new();
        let view = CreateMessage::new(&h.ports).execute(command("hi")).await.unwrap();
        assert_eq!((view.message.id.value() >> 12) & 0x3FF, 64);
    }

    #[tokio::test]
    async fn unavailable_ids_store_and_publish_nothing() {
        struct Expired;
        impl IdGenerator for Expired {
            fn next_id(&self) -> Result<Snowflake, crate::PortError> {
                Err(crate::PortError::new("ID reservation expired"))
            }
            fn is_ready(&self) -> bool {
                false
            }
        }
        let mut h = Harness::new();
        h.ports.ids = Arc::new(Expired);
        let result = CreateMessage::new(&h.ports).execute(command("hello")).await;
        assert!(matches!(result, Err(AppError::Unavailable(_))));
        assert_eq!(h.messages.last_message_id(CHANNEL).await.unwrap(), None);
        assert!(h.events.kinds().is_empty());
        let result = CreateMessage::new(&h.ports)
            .execute_internal(ephemeral_reply(vec![BOB]))
            .await;
        assert!(matches!(result, Err(AppError::Unavailable(_))));
        assert!(h.events.kinds().is_empty());
    }

    #[tokio::test]
    async fn non_standard_channels_conflict_and_store_nothing() {
        let h = Harness::new();
        h.access.set(Ok(dm_access(DeliveryMode::Ephemeral)));
        let result = CreateMessage::new(&h.ports).execute(command("secret")).await;
        assert_eq!(
            result,
            Err(AppError::Domain(DomainError::DeliveryModeConflict(
                DeliveryMode::Ephemeral
            )))
        );
        assert_eq!(h.messages.last_message_id(CHANNEL).await.unwrap(), None);
        assert!(h.events.kinds().is_empty());
    }

    #[tokio::test]
    async fn a_stale_cached_mode_is_rechecked_before_rejecting() {
        // The user switched the chat back to Standard; the cached access still says Ephemeral.
        let h = Harness::new();
        h.access.set(Ok(dm_access(DeliveryMode::Ephemeral)));
        *h.access.fresh.lock().unwrap() = Some(Ok(dm_access(DeliveryMode::Standard)));
        let view = CreateMessage::new(&h.ports)
            .execute(command("back to standard"))
            .await
            .unwrap();
        assert!(h.messages.find(CHANNEL, view.message.id).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn internal_posts_skip_the_user_permission_check() {
        let h = Harness::new();
        h.access.set_permissions(Permissions::VIEW_CHANNEL);
        let view = CreateMessage::new(&h.ports)
            .execute_internal(command("from a bot"))
            .await
            .unwrap();
        assert_eq!(view.message.author_id, ALICE);
        assert!(h.messages.find(CHANNEL, view.message.id).await.unwrap().is_some());
        assert_eq!(h.events.kinds().len(), 1);
    }

    #[tokio::test]
    async fn internal_posts_still_refuse_non_standard_delivery() {
        let h = Harness::new();
        h.access.set(Ok(dm_access(DeliveryMode::P2p)));
        let result = CreateMessage::new(&h.ports).execute_internal(command("hi")).await;
        assert_eq!(
            result,
            Err(AppError::Domain(DomainError::DeliveryModeConflict(DeliveryMode::P2p)))
        );
        assert_eq!(h.messages.last_message_id(CHANNEL).await.unwrap(), None);
        assert!(h.events.kinds().is_empty());
    }

    #[tokio::test]
    async fn webhook_posts_are_authored_by_the_webhook_without_a_member_check() {
        let h = Harness::new();
        h.access.set(Err(AccessError::NotMember));
        *h.access.channel.lock().unwrap() = Ok(guild_access(MEMBER));
        let hook = Snowflake::new(900);
        let mut cmd = command("relayed");
        cmd.author_id = hook;
        cmd.webhook = Some(WebhookAuthor {
            webhook_id: hook,
            username: Some("Relay".into()),
            avatar_url: None,
        });
        let view = CreateMessage::new(&h.ports).execute_internal(cmd).await.unwrap();
        assert_eq!(view.message.author_id, hook);
        assert_eq!(view.message.webhook.unwrap().username.as_deref(), Some("Relay"));
        assert!(h.messages.find(CHANNEL, view.message.id).await.unwrap().is_some());
        assert_eq!(h.events.events.lock().unwrap()[0].audience, Audience::Guild(GUILD));
    }

    #[tokio::test]
    async fn ephemeral_replies_reach_only_their_targets_and_are_never_stored() {
        let h = Harness::new();
        let view = CreateMessage::new(&h.ports)
            .execute_internal(ephemeral_reply(vec![BOB]))
            .await
            .unwrap();
        assert_eq!(view.message.flags, FLAG_EPHEMERAL);
        assert_eq!(h.messages.last_message_id(CHANNEL).await.unwrap(), None);
        assert!(h.messages.find(CHANNEL, view.message.id).await.unwrap().is_none());
        assert!(h.read_states.find(BOB, CHANNEL).await.unwrap().is_none());
        let events = h.events.events.lock().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].audience, Audience::Users(vec![BOB]));
        assert_eq!(events[0].guild_id, Some(GUILD));
    }

    #[tokio::test]
    async fn ephemeral_replies_need_a_target() {
        let h = Harness::new();
        let result = CreateMessage::new(&h.ports)
            .execute_internal(ephemeral_reply(vec![]))
            .await;
        assert!(matches!(result, Err(AppError::Domain(DomainError::InvalidForm(_)))));
        assert!(h.events.kinds().is_empty());
    }

    #[tokio::test]
    async fn ephemeral_replies_in_non_standard_channels_conflict() {
        let h = Harness::new();
        h.access.set(Ok(dm_access(DeliveryMode::P2p)));
        let result = CreateMessage::new(&h.ports)
            .execute_internal(ephemeral_reply(vec![BOB]))
            .await;
        assert_eq!(
            result,
            Err(AppError::Domain(DomainError::DeliveryModeConflict(DeliveryMode::P2p)))
        );
    }

    #[tokio::test]
    async fn interaction_links_are_kept_on_stored_messages() {
        let h = Harness::new();
        let link = InteractionLink {
            interaction_id: Snowflake::new(31),
            application_id: Snowflake::new(32),
        };
        let mut cmd = command("answer");
        cmd.interaction = Some(link);
        let view = CreateMessage::new(&h.ports).execute_internal(cmd).await.unwrap();
        let stored = h.messages.find(CHANNEL, view.message.id).await.unwrap().unwrap();
        assert_eq!(stored.interaction, Some(link));
    }

    #[tokio::test]
    async fn requires_send_permission() {
        let h = Harness::new();
        h.access.set_permissions(Permissions::VIEW_CHANNEL);
        let result = CreateMessage::new(&h.ports).execute(command("hi")).await;
        assert_eq!(result, Err(AppError::Domain(DomainError::MissingPermissions)));
    }

    #[tokio::test]
    async fn non_members_get_missing_access() {
        let h = Harness::new();
        h.access.set(Err(crate::ports::AccessError::NotMember));
        let result = CreateMessage::new(&h.ports).execute(command("hi")).await;
        assert_eq!(result, Err(AppError::Domain(DomainError::MissingAccess)));
    }

    #[tokio::test]
    async fn replies_must_reference_a_message_in_the_channel() {
        let h = Harness::new();
        let mut cmd = command("reply");
        cmd.reply_to = Some(Snowflake::new(999));
        assert!(matches!(
            CreateMessage::new(&h.ports).execute(cmd).await,
            Err(AppError::Domain(DomainError::InvalidForm(_)))
        ));
        let target = h.seed(BOB, "original").await;
        let mut cmd = command("reply");
        cmd.reply_to = Some(target.id);
        let view = CreateMessage::new(&h.ports).execute(cmd).await.unwrap();
        assert_eq!(view.message.reply_to, Some(target.id));
    }

    #[tokio::test]
    async fn attachments_must_have_been_uploaded() {
        let h = Harness::new();
        let mut cmd = command("");
        cmd.attachments = vec![AttachmentReference {
            uploaded_filename: format!("attachments/{CHANNEL}/77/cat.png"),
            description: None,
            flags: 0,
        }];
        assert!(CreateMessage::new(&h.ports).execute(cmd.clone()).await.is_err());

        h.storage.put(&format!("attachments/{CHANNEL}/77/cat.png"), 1234);
        let view = CreateMessage::new(&h.ports).execute(cmd).await.unwrap();
        let attachment = &view.message.attachments[0];
        assert_eq!(attachment.id, Snowflake::new(77));
        assert_eq!(attachment.size, 1234);
        assert_eq!(attachment.filename.as_str(), "cat.png");
    }

    #[tokio::test]
    async fn attachments_keep_alt_text_and_spoiler_flags() {
        let h = Harness::new();
        for name in ["a.png", "SPOILER_b.png"] {
            h.storage.put(&format!("attachments/{CHANNEL}/77/{name}"), 10);
        }
        let reference = |name: &str, description: Option<&str>, flags| AttachmentReference {
            uploaded_filename: format!("attachments/{CHANNEL}/77/{name}"),
            description: description.map(str::to_owned),
            flags,
        };
        let mut cmd = command("");
        cmd.attachments = vec![reference("a.png", Some(" a red cat "), 8)];
        let view = CreateMessage::new(&h.ports).execute(cmd).await.unwrap();
        let attachment = &view.message.attachments[0];
        assert_eq!(attachment.description.as_deref(), Some("a red cat"));
        assert!(attachment.is_spoiler());

        let mut cmd = command("");
        cmd.attachments = vec![reference("SPOILER_b.png", None, 0)];
        let view = CreateMessage::new(&h.ports).execute(cmd).await.unwrap();
        assert!(view.message.attachments[0].is_spoiler());

        let mut cmd = command("");
        cmd.attachments = vec![reference("a.png", Some(&"x".repeat(1025)), 0)];
        assert!(CreateMessage::new(&h.ports).execute(cmd).await.is_err());
    }

    #[tokio::test]
    async fn creating_a_message_with_a_link_unfurls_it_in_the_background() {
        let h = Harness::new();
        h.unfurler.set("https://a.example/", r#"{"type":"link","title":"A"}"#);
        let view = CreateMessage::new(&h.ports)
            .execute(command("see https://a.example/"))
            .await
            .unwrap();
        assert!(view.message.embeds.is_empty(), "the reply does not wait for the fetch");
        for _ in 0..200 {
            if h.events.kinds().len() == 2 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        let kinds = h.events.kinds();
        assert!(matches!(&kinds[1], MessageEventKind::Updated { message } if message.embeds.len() == 1));
    }

    #[tokio::test]
    async fn ephemeral_relays_are_never_unfurled() {
        let h = Harness::new();
        h.unfurler.set("https://a.example/", r#"{"type":"link"}"#);
        let mut cmd = ephemeral_reply(vec![ALICE]);
        cmd.content = "https://a.example/".into();
        CreateMessage::new(&h.ports).execute_internal(cmd).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert_eq!(h.unfurler.calls(), 0);
    }

    #[tokio::test]
    async fn replying_pings_the_replied_author() {
        let h = Harness::new();
        let target = h.seed(BOB, "original").await;
        let mut cmd = command("reply");
        cmd.reply_to = Some(target.id);
        let view = CreateMessage::new(&h.ports).execute(cmd).await.unwrap();
        assert!(view.message.mentions.users.contains(&BOB));
        assert_eq!(
            h.read_states.find(BOB, CHANNEL).await.unwrap().unwrap().mention_count,
            1
        );
    }

    #[tokio::test]
    async fn reply_with_ping_off_mentions_nobody() {
        let h = Harness::new();
        let target = h.seed(BOB, "original").await;
        let mut cmd = command("reply");
        cmd.reply_to = Some(target.id);
        cmd.suppress_reply_ping = true;
        let view = CreateMessage::new(&h.ports).execute(cmd).await.unwrap();
        assert!(view.message.mentions.users.is_empty());
        assert!(h.read_states.find(BOB, CHANNEL).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn silent_messages_keep_mentions_but_record_no_unread_mention() {
        let h = Harness::new();
        let mut cmd = command(&format!("hey <@{BOB}>"));
        cmd.flags = FLAG_SUPPRESS_NOTIFICATIONS;
        let view = CreateMessage::new(&h.ports).execute(cmd).await.unwrap();
        assert_eq!(view.message.flags, FLAG_SUPPRESS_NOTIFICATIONS);
        assert!(view.message.mentions.users.contains(&BOB));
        assert!(h.read_states.find(BOB, CHANNEL).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn mentions_increment_read_state_counters() {
        let h = Harness::new();
        CreateMessage::new(&h.ports)
            .execute(command(&format!("hey <@{BOB}> and me <@{ALICE}>")))
            .await
            .unwrap();
        let bob = h.read_states.find(BOB, CHANNEL).await.unwrap().unwrap();
        assert_eq!(bob.mention_count, 1);
        assert_eq!(bob.version, 1);
        assert!(h.events.events.lock().unwrap().iter().any(|event| {
            event.audience == Audience::Users(vec![BOB])
                && matches!(&event.kind, MessageEventKind::ReadStateUpdated { state } if state == &bob)
        }));
        assert!(h.read_states.find(ALICE, CHANNEL).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn regression_ack_keeps_mentions_newer_than_its_marker() {
        let h = Harness::new();
        let create = CreateMessage::new(&h.ports);
        let first = create.execute(command(&format!("first <@{BOB}>"))).await.unwrap();
        create.execute(command(&format!("second <@{BOB}>"))).await.unwrap();
        let ack = super::super::AcknowledgeMessage::new(&h.ports);
        let state = ack.execute(CHANNEL, first.message.id, BOB).await.unwrap();
        assert_eq!(state.last_read_id, Some(first.message.id));
        assert_eq!(state.mention_count, 1, "an ACK cannot clear a newer mention");
        let stale = ack.execute(CHANNEL, first.message.id, BOB).await.unwrap();
        assert_eq!(stale.mention_count, 1, "a repeated ACK must preserve newer mentions");
    }

    #[tokio::test]
    async fn regression_muted_direct_mentions_do_not_increment_a_badge() {
        let h = Harness::new();
        h.notification_recipients.recipients.lock().unwrap()[1].muted = true;
        CreateMessage::new(&h.ports)
            .execute(command(&format!("hello <@{BOB}>")))
            .await
            .unwrap();
        assert!(h.read_states.find(BOB, CHANNEL).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn regression_replaying_accounting_does_not_count_the_same_message_twice() {
        let h = Harness::new();
        let create = CreateMessage::new(&h.ports);
        let message = create
            .execute(command(&format!("hello <@{BOB}>")))
            .await
            .unwrap()
            .message;
        create.record_side_effects(&message, Some(GUILD)).await;
        assert_eq!(
            h.read_states.find(BOB, CHANNEL).await.unwrap().unwrap().mention_count,
            1
        );

        let ack = super::super::AcknowledgeMessage::new(&h.ports);
        ack.execute(CHANNEL, message.id, BOB).await.unwrap();
        ack.execute_manual(CHANNEL, Snowflake::new(message.id.value() - 1), BOB, 2)
            .await
            .unwrap();
        create.record_side_effects(&message, Some(GUILD)).await;
        assert_eq!(
            h.read_states.find(BOB, CHANNEL).await.unwrap().unwrap().mention_count,
            2,
            "manual unread does not reopen an already-accounted delivery"
        );
    }

    #[tokio::test]
    async fn regression_concurrent_message_creates_do_not_lose_a_mention() {
        struct SynchronizedReads {
            inner: Arc<InMemoryReadStates>,
            barrier: tokio::sync::Barrier,
        }
        #[async_trait::async_trait]
        impl ReadStateRepository for SynchronizedReads {
            async fn find(&self, user: UserId, channel: ChannelId) -> Result<Option<ReadState>, crate::PortError> {
                let before = self.inner.find(user, channel).await?;
                self.barrier.wait().await;
                Ok(before)
            }
            async fn record_mention(
                &self,
                user: UserId,
                channel: ChannelId,
                message: MessageId,
            ) -> Result<ReadState, crate::PortError> {
                self.inner.record_mention(user, channel, message).await
            }
            async fn acknowledge(
                &self,
                user: UserId,
                channel: ChannelId,
                message: MessageId,
            ) -> Result<ReadState, crate::PortError> {
                self.inner.acknowledge(user, channel, message).await
            }
            async fn set_position(
                &self,
                user: UserId,
                channel: ChannelId,
                message: MessageId,
                mentions: u32,
            ) -> Result<ReadState, crate::PortError> {
                self.inner.set_position(user, channel, message, mentions).await
            }
            async fn for_user(&self, user: UserId) -> Result<Vec<ReadState>, crate::PortError> {
                self.inner.for_user(user).await
            }
        }
        let mut h = Harness::new();
        h.ports.read_states = Arc::new(SynchronizedReads {
            inner: Arc::clone(&h.read_states),
            barrier: tokio::sync::Barrier::new(2),
        });
        let create = CreateMessage::new(&h.ports);
        let (first, second) = tokio::join!(
            create.execute(command(&format!("first <@{BOB}>"))),
            create.execute(command(&format!("second <@{BOB}>"))),
        );
        first.unwrap();
        second.unwrap();
        assert_eq!(
            h.read_states.find(BOB, CHANNEL).await.unwrap().unwrap().mention_count,
            2
        );
    }

    #[tokio::test]
    async fn here_is_flagged_on_the_event_so_delivery_can_filter_by_presence() {
        let h = Harness::new();
        h.access.set_permissions(MEMBER | Permissions::MENTION_EVERYONE);
        let use_case = CreateMessage::new(&h.ports);
        let here = use_case.execute(command("@here standup")).await.unwrap().message;
        assert!(h.read_states.find(BOB, CHANNEL).await.unwrap().is_none());
        let everyone = use_case.execute(command("@everyone standup")).await.unwrap().message;
        assert!(here.mentions.everyone && here.mentions.here);
        assert!(everyone.mentions.everyone && !everyone.mentions.here);
        let stored = h.messages.find(CHANNEL, here.id).await.unwrap().unwrap();
        assert!(stored.mentions.here);
        assert_eq!(
            h.read_states.find(BOB, CHANNEL).await.unwrap().unwrap().mention_count,
            1
        );
    }

    const TARGET: Snowflake = Snowflake::new(2 << 22);

    fn forward_command(source: &Message, comment: &str) -> CreateMessageCommand {
        CreateMessageCommand {
            channel_id: TARGET,
            content: comment.to_owned(),
            forward: Some(ForwardSource {
                channel_id: source.channel_id,
                message_id: source.id,
            }),
            ..command("")
        }
    }

    #[tokio::test]
    async fn forwarding_stores_a_snapshot_in_the_target_without_notifying() {
        let h = Harness::new();
        let source = h.seed(BOB, "look <@11>").await;
        let view = CreateMessage::new(&h.ports)
            .execute(forward_command(&source, "fyi"))
            .await
            .unwrap();
        let stored = h.messages.find(TARGET, view.message.id).await.unwrap().unwrap();
        assert_eq!(stored.content, "fyi");
        assert_eq!(stored.author_id, ALICE);
        let origin = stored.forward.expect("forward origin");
        assert_eq!((origin.channel_id, origin.message_id), (CHANNEL, source.id));
        assert_eq!(origin.guild_id, Some(GUILD));
        assert_eq!(origin.snapshot.content, "look <@11>");
        assert!(
            h.read_states.find(BOB, TARGET).await.unwrap().is_none(),
            "snapshot mentions never ping"
        );
    }

    #[tokio::test]
    async fn a_forward_may_have_no_comment() {
        let h = Harness::new();
        let source = h.seed(BOB, "hello").await;
        let view = CreateMessage::new(&h.ports)
            .execute(forward_command(&source, ""))
            .await
            .unwrap();
        assert_eq!(view.message.content, "");
        assert!(view.message.forward.is_some());
    }

    #[tokio::test]
    async fn forwarding_copies_attachments_so_the_source_can_go() {
        let h = Harness::new();
        let mut source = h.seed(BOB, "pic").await;
        source.attachments = vec![Attachment {
            id: Snowflake::new(77),
            filename: discordy_messages_domain::AttachmentFilename::parse("cat.png").unwrap(),
            content_type: Some("image/png".into()),
            size: 5,
            width: None,
            height: None,
            description: None,
            flags: 0,
        }];
        h.messages.insert(&source).await.unwrap();
        h.storage
            .put_body(&format!("attachments/{CHANNEL}/77/cat.png"), b"meow!");
        let view = CreateMessage::new(&h.ports)
            .execute(forward_command(&source, ""))
            .await
            .unwrap();
        let copy = &view.message.forward.as_ref().unwrap().snapshot.attachments[0];
        assert_ne!(copy.id, Snowflake::new(77), "the copy has its own id");
        let key = format!("attachments/{TARGET}/{}/cat.png", copy.id);
        assert!(h.storage.objects.lock().unwrap().contains_key(&key));
        assert!(
            h.storage
                .objects
                .lock()
                .unwrap()
                .contains_key(&format!("attachments/{CHANNEL}/77/cat.png"))
        );
    }

    #[tokio::test]
    async fn forwarding_needs_attach_files_when_the_source_has_attachments() {
        let h = Harness::new();
        let mut source = h.seed(BOB, "pic").await;
        source.attachments = vec![Attachment {
            id: Snowflake::new(77),
            filename: discordy_messages_domain::AttachmentFilename::parse("cat.png").unwrap(),
            content_type: None,
            size: 5,
            width: None,
            height: None,
            description: None,
            flags: 0,
        }];
        h.messages.insert(&source).await.unwrap();
        h.access.set_permissions(
            Permissions::VIEW_CHANNEL | Permissions::READ_MESSAGE_HISTORY | Permissions::SEND_MESSAGES,
        );
        let result = CreateMessage::new(&h.ports).execute(forward_command(&source, "")).await;
        assert_eq!(result, Err(AppError::Domain(DomainError::MissingPermissions)));
    }

    #[tokio::test]
    async fn forwarding_needs_read_access_to_the_source() {
        let h = Harness::new();
        let source = h.seed(BOB, "secret").await;
        h.access.deny_channel(CHANNEL);
        let result = CreateMessage::new(&h.ports).execute(forward_command(&source, "")).await;
        assert_eq!(result, Err(AppError::Domain(DomainError::MissingAccess)));
        assert_eq!(h.messages.last_message_id(TARGET).await.unwrap(), None);
    }

    #[tokio::test]
    async fn forwarding_needs_send_permission_in_the_target() {
        let h = Harness::new();
        let source = h.seed(BOB, "x").await;
        h.access.deny_channel(TARGET);
        let result = CreateMessage::new(&h.ports).execute(forward_command(&source, "")).await;
        assert_eq!(result, Err(AppError::Domain(DomainError::MissingAccess)));
    }

    #[tokio::test]
    async fn forwarding_from_or_to_non_standard_channels_conflicts() {
        let h = Harness::new();
        let source = h.seed(BOB, "x").await;
        h.access.set(Ok(dm_access(DeliveryMode::Ephemeral)));
        let result = CreateMessage::new(&h.ports).execute(forward_command(&source, "")).await;
        assert_eq!(
            result,
            Err(AppError::Domain(DomainError::DeliveryModeConflict(
                DeliveryMode::Ephemeral
            )))
        );
    }

    #[tokio::test]
    async fn unknown_and_system_messages_cannot_be_forwarded() {
        let h = Harness::new();
        let mut ghost = h.seed(BOB, "x").await;
        ghost.id = Snowflake::new(999);
        let result = CreateMessage::new(&h.ports).execute(forward_command(&ghost, "")).await;
        assert!(matches!(result, Err(AppError::Domain(DomainError::InvalidForm(_)))));
    }

    #[tokio::test]
    async fn forwarding_a_forward_keeps_the_original_reference() {
        let h = Harness::new();
        let source = h.seed(BOB, "orig").await;
        let first = CreateMessage::new(&h.ports)
            .execute(forward_command(&source, ""))
            .await
            .unwrap();
        let mut again = forward_command(&first.message, "");
        again.channel_id = CHANNEL;
        // The first forward lives in TARGET, which the fake answers like any channel.
        let second = CreateMessage::new(&h.ports).execute(again).await.unwrap();
        let origin = second.message.forward.unwrap();
        assert_eq!((origin.channel_id, origin.message_id), (CHANNEL, source.id));
        assert_eq!(origin.snapshot.content, "orig");
    }
}

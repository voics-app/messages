use std::collections::HashSet;
use std::sync::Arc;

use discordy_messages_domain::attachment::MAX_ATTACHMENT_BYTES;
use discordy_messages_domain::{
    Attachment, Audience, ChannelAccess, ChannelId, DomainError, FLAG_EPHEMERAL, InteractionLink, Message,
    MessageDraft, MessageId, ReadState, UploadKey, UserId, WebhookAuthor,
};

use super::MessagingPorts;
use super::support::{announce, checked_access, event};
use crate::error::AppResult;
use crate::ports::{
    ActivityRepository, AttachmentStorage, ChannelAccessProvider, EventPublisher, IdGenerator, MessageEvent,
    MessageEventKind, MessageRepository, ReadStateRepository,
};
use crate::views::MessageView;

/// A file uploaded through a presigned URL, referenced by its upload key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttachmentReference {
    pub uploaded_filename: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CreateMessageCommand {
    pub channel_id: ChannelId,
    pub author_id: UserId,
    pub content: String,
    pub nonce: Option<String>,
    pub attachments: Vec<AttachmentReference>,
    pub embeds: Vec<String>,
    pub components: Vec<String>,
    pub reply_to: Option<MessageId>,
    /// Discord message flags (internal posts only; `FLAG_EPHEMERAL` relays).
    pub flags: u32,
    /// Set for webhook posts; `author_id` is then the webhook id.
    pub webhook: Option<WebhookAuthor>,
    /// Set for interaction responses.
    pub interaction: Option<InteractionLink>,
    /// Recipients of an ephemeral message. Only these users receive it.
    pub target_user_ids: Vec<UserId>,
}

pub struct CreateMessage {
    access: Arc<dyn ChannelAccessProvider>,
    messages: Arc<dyn MessageRepository>,
    read_states: Arc<dyn ReadStateRepository>,
    activity: Arc<dyn ActivityRepository>,
    storage: Arc<dyn AttachmentStorage>,
    events: Arc<dyn EventPublisher>,
    ids: Arc<dyn IdGenerator>,
}

impl CreateMessage {
    pub fn new(ports: &MessagingPorts) -> Self {
        Self {
            access: ports.access.clone(),
            messages: ports.messages.clone(),
            read_states: ports.read_states.clone(),
            activity: ports.activity.clone(),
            storage: ports.storage.clone(),
            events: ports.events.clone(),
            ids: ports.ids.clone(),
        }
    }

    pub async fn execute(&self, command: CreateMessageCommand) -> AppResult<MessageView> {
        let with_attachments = !command.attachments.is_empty();
        let access = checked_access(&*self.access, command.channel_id, command.author_id, |a| {
            a.ensure_can_send(with_attachments)
        })
        .await?;
        self.store(&access, command).await
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
        self.store(&access, command).await
    }

    async fn store(&self, access: &ChannelAccess, command: CreateMessageCommand) -> AppResult<MessageView> {
        let channel_id = command.channel_id;
        let nonce = command.nonce.clone();
        let message = self.compose_message(access, &command).await?;
        self.messages.insert(&message).await?;
        self.record_side_effects(&message).await;
        announce(
            self.events.as_ref(),
            event(
                access,
                channel_id,
                MessageEventKind::Created {
                    message: message.clone(),
                    nonce,
                },
            ),
        )
        .await;
        Ok(MessageView::without_reactions(message))
    }

    /// Ephemeral replies reach only their targets and are never stored, so
    /// they leave no trace in ScyllaDB, read states or activity counters.
    async fn relay_ephemeral(&self, access: &ChannelAccess, command: CreateMessageCommand) -> AppResult<MessageView> {
        if command.target_user_ids.is_empty() {
            return Err(DomainError::invalid("target_user_ids", "required for ephemeral messages").into());
        }
        let message = self.compose_message(access, &command).await?;
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

    async fn compose_message(&self, access: &ChannelAccess, command: &CreateMessageCommand) -> AppResult<Message> {
        if let Some(reply_to) = command.reply_to {
            self.ensure_reply_target_exists(command.channel_id, reply_to).await?;
        }
        let draft = MessageDraft {
            content: command.content.clone(),
            attachments: self
                .resolve_attachments(command.channel_id, &command.attachments)
                .await?,
            embeds: command.embeds.clone(),
            components: command.components.clone(),
            reply_to: command.reply_to,
            flags: command.flags,
            webhook: command.webhook.clone(),
            interaction: command.interaction,
        };
        Ok(Message::compose(
            self.ids.next_id(),
            command.channel_id,
            command.author_id,
            draft,
            access.may_mention_everyone(),
        )?)
    }

    async fn ensure_reply_target_exists(&self, channel_id: ChannelId, reply_to: MessageId) -> AppResult<()> {
        match self.messages.find(channel_id, reply_to).await? {
            Some(_) => Ok(()),
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
                filename: key.filename,
                content_type: stored.content_type,
                size: stored.size,
                width: None,
                height: None,
            });
        }
        Ok(attachments)
    }

    /// Admin counters and mention badges. Best effort: the message itself is
    /// already stored.
    async fn record_side_effects(&self, message: &Message) {
        if let Err(error) = self.activity.record_standard_message(message.id).await {
            tracing::warn!(%error, "failed to record message activity");
        }
        for user_id in message.mentions.users.iter().filter(|u| **u != message.author_id) {
            if let Err(error) = self.bump_mention(*user_id, message).await {
                tracing::warn!(%user_id, %error, "failed to record mention");
            }
        }
    }

    async fn bump_mention(&self, user_id: UserId, message: &Message) -> AppResult<()> {
        let mut state = self
            .read_states
            .find(user_id, message.channel_id)
            .await?
            .unwrap_or_else(|| ReadState::unread(user_id, message.channel_id));
        state.record_mention(message.id);
        self.read_states.save(&state).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::AppError;
    use crate::fakes::*;
    use crate::ports::AccessError;
    use discordy_messages_domain::{Audience, DeliveryMode, Permissions, Snowflake};

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
    async fn mentions_increment_read_state_counters() {
        let h = Harness::new();
        CreateMessage::new(&h.ports)
            .execute(command(&format!("hey <@{BOB}> and me <@{ALICE}>")))
            .await
            .unwrap();
        let bob = h.read_states.find(BOB, CHANNEL).await.unwrap().unwrap();
        assert_eq!(bob.mention_count, 1);
        assert!(h.read_states.find(ALICE, CHANNEL).await.unwrap().is_none());
    }
}

use std::collections::HashSet;
use std::sync::Arc;

use discordy_messages_domain::attachment::MAX_ATTACHMENT_BYTES;
use discordy_messages_domain::{
    Attachment, ChannelAccess, ChannelId, DomainError, Message, MessageDraft, MessageId, ReadState, UploadKey, UserId,
};

use super::MessagingPorts;
use super::support::{announce, event};
use crate::error::AppResult;
use crate::ports::{
    ActivityRepository, AttachmentStorage, ChannelAccessProvider, EventPublisher, IdGenerator, MessageEventKind,
    MessageRepository, ReadStateRepository,
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
        let access = self.access.access(command.channel_id, command.author_id).await?;
        access.ensure_can_send(!command.attachments.is_empty())?;
        self.store(access, command).await
    }

    /// Posts on behalf of a bot, webhook or interaction. api has already
    /// authorised the call, so the user permission check is skipped; the
    /// delivery mode is still enforced.
    pub async fn execute_internal(&self, command: CreateMessageCommand) -> AppResult<MessageView> {
        let access = self.access.access(command.channel_id, command.author_id).await?;
        access.ensure_standard_delivery()?;
        self.store(access, command).await
    }

    async fn store(&self, access: ChannelAccess, command: CreateMessageCommand) -> AppResult<MessageView> {
        let channel_id = command.channel_id;
        if let Some(reply_to) = command.reply_to {
            self.ensure_reply_target_exists(channel_id, reply_to).await?;
        }
        let draft = MessageDraft {
            content: command.content,
            attachments: self.resolve_attachments(channel_id, &command.attachments).await?,
            embeds: command.embeds,
            components: command.components,
            reply_to: command.reply_to,
        };
        let message = Message::compose(
            self.ids.next_id(),
            channel_id,
            command.author_id,
            draft,
            access.may_mention_everyone(),
        )?;
        self.messages.insert(&message).await?;
        self.record_side_effects(&message).await;
        announce(
            self.events.as_ref(),
            event(
                &access,
                channel_id,
                MessageEventKind::Created {
                    message: message.clone(),
                    nonce: command.nonce,
                },
            ),
        )
        .await;
        Ok(MessageView::without_reactions(message))
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
    use discordy_messages_domain::{Audience, DeliveryMode, Permissions};

    fn command(content: &str) -> CreateMessageCommand {
        CreateMessageCommand {
            channel_id: CHANNEL,
            author_id: ALICE,
            content: content.to_owned(),
            nonce: Some("n-1".into()),
            ..CreateMessageCommand::default()
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
    async fn dm_events_target_the_recipients() {
        let h = Harness::new();
        h.access.set(Ok(dm_access(DeliveryMode::Standard)));
        CreateMessage::new(&h.ports).execute(command("yo")).await.unwrap();
        let events = h.events.events.lock().unwrap();
        assert_eq!(events[0].audience, Audience::Users(vec![ALICE, BOB]));
        assert_eq!(events[0].guild_id, None);
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

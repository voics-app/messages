//! Scheduling reuses ordinary composition and publication, with fresh authorization around its claim.
use std::time::Duration;

use async_trait::async_trait;
use discordy_messages_domain::scheduled::{DispatchOutcome, ScheduledFailure};
use discordy_messages_domain::{ChannelAccess, DomainError, FLAG_SUPPRESS_NOTIFICATIONS, Message};

use super::{CreateMessage, CreateMessageCommand, ensure_tts_permission};
use crate::ports::scheduled::{PreparedScheduledMessage, ScheduledAuthorDraft, ScheduledDelivery};
use crate::{AppResult, scheduled::failure_reason};

const POST_CLAIM_TIMEOUT: Duration = Duration::from_secs(5);
const INSERT_TIMEOUT: Duration = Duration::from_secs(10);

#[async_trait]
impl ScheduledDelivery for CreateMessage {
    async fn validate(&self, request: &ScheduledAuthorDraft) -> AppResult<()> {
        self.scheduled_access(request).await?;
        Ok(())
    }

    async fn prepare(&self, request: &ScheduledAuthorDraft) -> AppResult<PreparedScheduledMessage> {
        let access = self.scheduled_access(request).await?;
        let command = CreateMessageCommand {
            channel_id: request.channel_id,
            author_id: request.author_id,
            content: request.draft.content.clone(),
            tts: request.draft.tts,
            reply_to: request.draft.reply_to,
            suppress_reply_ping: !request.draft.replied_user,
            flags: if request.draft.silent {
                FLAG_SUPPRESS_NOTIFICATIONS
            } else {
                0
            },
            ..CreateMessageCommand::default()
        };
        let message = self.compose_message(&access, &command, None).await?;
        Ok(PreparedScheduledMessage { message, access })
    }

    async fn insert(&self, prepared: &mut PreparedScheduledMessage) -> DispatchOutcome {
        // This starts after the claim succeeds. An earlier permission response cannot authorize insertion.
        match tokio::time::timeout(POST_CLAIM_TIMEOUT, self.scheduled_final_access(&prepared.message)).await {
            Ok(Ok(access)) => prepared.access = access,
            Ok(Err(error)) => return DispatchOutcome::Failed(failure_reason(&error)),
            Err(_) => return DispatchOutcome::Failed(ScheduledFailure::DependencyUnavailable),
        }
        // One storage invocation only: a timeout or lost acknowledgement can mean the message was stored.
        match tokio::time::timeout(INSERT_TIMEOUT, self.messages.insert(&prepared.message)).await {
            Ok(Ok(())) => DispatchOutcome::Sent,
            Ok(Err(_)) | Err(_) => DispatchOutcome::Unknown,
        }
    }

    async fn publish(&self, prepared: &PreparedScheduledMessage) {
        self.publish_stored(&prepared.message, &prepared.access, None).await;
    }
}

impl CreateMessage {
    async fn scheduled_access(&self, request: &ScheduledAuthorDraft) -> AppResult<ChannelAccess> {
        request.draft.validate()?;
        let access = self.access.refresh(request.channel_id, request.author_id).await?;
        access.ensure_can_send(false)?;
        ensure_tts_permission(&access, request.draft.tts)?;
        if let Some(reply) = request.draft.reply_to {
            self.reply_target(request.channel_id, reply).await?;
        }
        Ok(access)
    }

    async fn scheduled_final_access(&self, message: &Message) -> AppResult<ChannelAccess> {
        let access = self.access.refresh(message.channel_id, message.author_id).await?;
        access.ensure_can_send(false)?;
        ensure_tts_permission(&access, message.tts)?;
        if message.mentions.everyone && !access.may_mention_everyone() {
            return Err(DomainError::MissingPermissions.into());
        }
        if let Some(reply) = message.reply_to {
            self.reply_target(message.channel_id, reply).await?;
        }
        Ok(access)
    }
}

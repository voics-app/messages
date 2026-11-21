use std::sync::Arc;

use discordy_messages_domain::{ChannelId, MessageId, UserId};

use super::MessagingPorts;
use super::support::{announce, event, find_message, purge_dependents};
use crate::error::AppResult;
use crate::ports::{
    AttachmentStorage, ChannelAccessProvider, EventPublisher, MessageEventKind, MessageRepository, PinRepository,
    ReactionRepository,
};

pub struct DeleteMessage {
    polls: Option<Arc<dyn crate::ports::PollRepository>>,
    access: Arc<dyn ChannelAccessProvider>,
    messages: Arc<dyn MessageRepository>,
    reactions: Arc<dyn ReactionRepository>,
    pins: Arc<dyn PinRepository>,
    storage: Arc<dyn AttachmentStorage>,
    events: Arc<dyn EventPublisher>,
}

impl DeleteMessage {
    pub fn new(ports: &MessagingPorts) -> Self {
        Self {
            polls: ports.polls.clone(),
            access: ports.access.clone(),
            messages: ports.messages.clone(),
            reactions: ports.reactions.clone(),
            pins: ports.pins.clone(),
            storage: ports.storage.clone(),
            events: ports.events.clone(),
        }
    }

    pub async fn execute(&self, channel_id: ChannelId, message_id: MessageId, actor: UserId) -> AppResult<()> {
        let access = self.access.access(channel_id, actor).await?;
        access.ensure_can_view()?;
        let message = find_message(self.messages.as_ref(), channel_id, message_id).await?;
        access.ensure_can_delete(actor, message.author_id)?;
        super::support::fence_poll(self.polls.as_deref(), &message).await?;
        self.messages.delete(channel_id, &[message_id]).await?;
        purge_dependents(
            self.reactions.as_ref(),
            self.pins.as_ref(),
            self.storage.as_ref(),
            &message,
        )
        .await?;
        announce(
            self.events.as_ref(),
            event(&access, channel_id, MessageEventKind::Deleted { message_id }),
        )
        .await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::AppError;
    use crate::fakes::*;
    use discordy_messages_domain::{DomainError, Emoji, Permissions};

    #[tokio::test]
    async fn authors_delete_their_messages_with_reactions() {
        let h = Harness::new();
        let message = h.seed(ALICE, "oops").await;
        let emoji = Emoji::parse("👍").unwrap();
        h.reactions.add(CHANNEL, message.id, &emoji, BOB).await.unwrap();
        DeleteMessage::new(&h.ports)
            .execute(CHANNEL, message.id, ALICE)
            .await
            .unwrap();
        assert!(h.messages.find(CHANNEL, message.id).await.unwrap().is_none());
        assert!(h.reactions.for_message(CHANNEL, message.id).await.unwrap().is_empty());
        assert_eq!(
            h.events.kinds(),
            vec![MessageEventKind::Deleted { message_id: message.id }]
        );
    }

    #[tokio::test]
    async fn others_need_manage_messages() {
        let h = Harness::new();
        let message = h.seed(ALICE, "mine").await;
        let result = DeleteMessage::new(&h.ports).execute(CHANNEL, message.id, BOB).await;
        assert_eq!(result, Err(AppError::Domain(DomainError::MissingPermissions)));

        h.access.set_permissions(MEMBER | Permissions::MANAGE_MESSAGES);
        DeleteMessage::new(&h.ports)
            .execute(CHANNEL, message.id, BOB)
            .await
            .unwrap();
        assert!(h.messages.find(CHANNEL, message.id).await.unwrap().is_none());
    }
}

use std::sync::Arc;

use discordy_messages_domain::{ChannelId, MessageId, UserId};

use super::MessagingPorts;
use super::support::{announce, event, find_message};
use crate::error::AppResult;
use crate::ports::{ChannelAccessProvider, EventPublisher, MessageEventKind, MessageRepository, ReactionRepository};

pub struct RemoveAllReactions {
    access: Arc<dyn ChannelAccessProvider>,
    messages: Arc<dyn MessageRepository>,
    reactions: Arc<dyn ReactionRepository>,
    events: Arc<dyn EventPublisher>,
}

impl RemoveAllReactions {
    pub fn new(ports: &MessagingPorts) -> Self {
        Self {
            access: ports.access.clone(),
            messages: ports.messages.clone(),
            reactions: ports.reactions.clone(),
            events: ports.events.clone(),
        }
    }

    pub async fn execute(&self, channel_id: ChannelId, message_id: MessageId, actor: UserId) -> AppResult<()> {
        let access = self.access.access(channel_id, actor).await?;
        access.ensure_can_manage_messages()?;
        find_message(self.messages.as_ref(), channel_id, message_id).await?;
        self.reactions.remove_all(channel_id, message_id).await?;
        announce(
            self.events.as_ref(),
            event(&access, channel_id, MessageEventKind::ReactionsCleared { message_id }),
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
    async fn moderators_clear_every_reaction() {
        let h = Harness::new();
        let message = h.seed(ALICE, "vote").await;
        let emoji = Emoji::parse("👍").unwrap();
        h.reactions.add(CHANNEL, message.id, &emoji, ALICE).await.unwrap();
        let use_case = RemoveAllReactions::new(&h.ports);
        assert_eq!(
            use_case.execute(CHANNEL, message.id, BOB).await,
            Err(AppError::Domain(DomainError::MissingPermissions))
        );
        h.access.set_permissions(MEMBER | Permissions::MANAGE_MESSAGES);
        use_case.execute(CHANNEL, message.id, BOB).await.unwrap();
        assert!(h.reactions.for_message(CHANNEL, message.id).await.unwrap().is_empty());
        assert_eq!(
            h.events.kinds(),
            vec![MessageEventKind::ReactionsCleared { message_id: message.id }]
        );
    }
}

use std::sync::Arc;

use discordy_messages_domain::{ChannelId, Emoji, MessageId, UserId};

use super::MessagingPorts;
use super::support::{announce, event, find_message};
use crate::error::AppResult;
use crate::ports::{ChannelAccessProvider, EventPublisher, MessageEventKind, MessageRepository, ReactionRepository};

pub struct RemoveOwnReaction {
    access: Arc<dyn ChannelAccessProvider>,
    messages: Arc<dyn MessageRepository>,
    reactions: Arc<dyn ReactionRepository>,
    events: Arc<dyn EventPublisher>,
}

impl RemoveOwnReaction {
    pub fn new(ports: &MessagingPorts) -> Self {
        Self {
            access: ports.access.clone(),
            messages: ports.messages.clone(),
            reactions: ports.reactions.clone(),
            events: ports.events.clone(),
        }
    }

    /// Removes `user`'s reaction; removing one that doesn't exist is a no-op.
    pub async fn execute(&self, channel_id: ChannelId, message_id: MessageId, emoji: Emoji, user: UserId) -> AppResult<()> {
        let access = self.access.access(channel_id, user).await?;
        access.ensure_can_read_history()?;
        find_message(self.messages.as_ref(), channel_id, message_id).await?;
        let rows = self.reactions.for_message(channel_id, message_id).await?;
        if !rows.iter().any(|r| r.emoji == emoji && r.user_id == user) {
            return Ok(());
        }
        self.reactions.remove(channel_id, message_id, &emoji, user).await?;
        announce(
            self.events.as_ref(),
            event(
                &access,
                channel_id,
                MessageEventKind::ReactionRemoved {
                    message_id,
                    user_id: user,
                    emoji,
                },
            ),
        )
        .await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fakes::*;

    #[tokio::test]
    async fn removes_only_the_callers_reaction() {
        let h = Harness::new();
        let message = h.seed(ALICE, "vote").await;
        let emoji = Emoji::parse("👍").unwrap();
        h.reactions.add(CHANNEL, message.id, &emoji, ALICE).await.unwrap();
        h.reactions.add(CHANNEL, message.id, &emoji, BOB).await.unwrap();
        RemoveOwnReaction::new(&h.ports)
            .execute(CHANNEL, message.id, emoji.clone(), BOB)
            .await
            .unwrap();
        let rows = h.reactions.for_message(CHANNEL, message.id).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].user_id, ALICE);
        assert!(matches!(h.events.kinds()[0], MessageEventKind::ReactionRemoved { .. }));
    }

    #[tokio::test]
    async fn removing_a_missing_reaction_publishes_nothing() {
        let h = Harness::new();
        let message = h.seed(ALICE, "vote").await;
        RemoveOwnReaction::new(&h.ports)
            .execute(CHANNEL, message.id, Emoji::parse("👍").unwrap(), BOB)
            .await
            .unwrap();
        assert!(h.events.kinds().is_empty());
    }
}

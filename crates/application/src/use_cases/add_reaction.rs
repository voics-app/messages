use std::collections::HashSet;
use std::sync::Arc;

use discordy_messages_domain::reaction::MAX_DISTINCT_EMOJI;
use discordy_messages_domain::{ChannelId, DomainError, Emoji, MessageId, UserId};

use super::MessagingPorts;
use super::support::{announce, event, find_message};
use crate::error::AppResult;
use crate::ports::{ChannelAccessProvider, EventPublisher, MessageEventKind, MessageRepository, ReactionRepository};

pub struct AddReaction {
    access: Arc<dyn ChannelAccessProvider>,
    messages: Arc<dyn MessageRepository>,
    reactions: Arc<dyn ReactionRepository>,
    events: Arc<dyn EventPublisher>,
}

impl AddReaction {
    pub fn new(ports: &MessagingPorts) -> Self {
        Self {
            access: ports.access.clone(),
            messages: ports.messages.clone(),
            reactions: ports.reactions.clone(),
            events: ports.events.clone(),
        }
    }

    /// Adds `user`'s reaction. Reacting twice with the same emoji is a no-op.
    pub async fn execute(
        &self,
        channel_id: ChannelId,
        message_id: MessageId,
        emoji: Emoji,
        user: UserId,
    ) -> AppResult<()> {
        let access = self.access.access(channel_id, user).await?;
        access.ensure_can_read_history()?;
        find_message(self.messages.as_ref(), channel_id, message_id).await?;
        let rows = self.reactions.for_message(channel_id, message_id).await?;
        if rows.iter().any(|r| r.emoji == emoji && r.user_id == user) {
            return Ok(());
        }
        let present = rows.iter().any(|r| r.emoji == emoji);
        access.ensure_can_react(present)?;
        let distinct = rows.iter().map(|r| &r.emoji).collect::<HashSet<_>>().len();
        if !present && distinct >= MAX_DISTINCT_EMOJI {
            return Err(DomainError::invalid("emoji", "maximum number of reactions reached (20)").into());
        }
        self.reactions.add(channel_id, message_id, &emoji, user).await?;
        announce(
            self.events.as_ref(),
            event(
                &access,
                channel_id,
                MessageEventKind::ReactionAdded {
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
    use crate::error::AppError;
    use crate::fakes::*;
    use discordy_messages_domain::Permissions;

    fn thumbs() -> Emoji {
        Emoji::parse("👍").unwrap()
    }

    #[tokio::test]
    async fn adds_a_reaction_once() {
        let h = Harness::new();
        let message = h.seed(ALICE, "vote").await;
        let use_case = AddReaction::new(&h.ports);
        use_case.execute(CHANNEL, message.id, thumbs(), BOB).await.unwrap();
        use_case.execute(CHANNEL, message.id, thumbs(), BOB).await.unwrap();
        assert_eq!(h.reactions.for_message(CHANNEL, message.id).await.unwrap().len(), 1);
        assert_eq!(h.events.kinds().len(), 1);
    }

    #[tokio::test]
    async fn existing_reactions_can_be_joined_without_add_reactions() {
        let h = Harness::new();
        let message = h.seed(ALICE, "vote").await;
        h.access
            .set_permissions(Permissions::VIEW_CHANNEL | Permissions::READ_MESSAGE_HISTORY);
        let use_case = AddReaction::new(&h.ports);
        assert_eq!(
            use_case.execute(CHANNEL, message.id, thumbs(), BOB).await,
            Err(AppError::Domain(DomainError::MissingPermissions))
        );
        h.reactions.add(CHANNEL, message.id, &thumbs(), ALICE).await.unwrap();
        use_case.execute(CHANNEL, message.id, thumbs(), BOB).await.unwrap();
    }

    #[tokio::test]
    async fn caps_distinct_emoji() {
        let h = Harness::new();
        let message = h.seed(ALICE, "vote").await;
        for i in 0..MAX_DISTINCT_EMOJI {
            let emoji = Emoji::parse(&format!("e{i}:{i}")).unwrap();
            h.reactions.add(CHANNEL, message.id, &emoji, ALICE).await.unwrap();
        }
        let result = AddReaction::new(&h.ports)
            .execute(CHANNEL, message.id, thumbs(), BOB)
            .await;
        assert!(matches!(result, Err(AppError::Domain(DomainError::InvalidForm(_)))));
    }

    #[tokio::test]
    async fn unknown_messages_cannot_be_reacted_to() {
        let h = Harness::new();
        let result = AddReaction::new(&h.ports)
            .execute(CHANNEL, Snowflake::new(5), thumbs(), BOB)
            .await;
        assert_eq!(result, Err(AppError::Domain(DomainError::UnknownMessage)));
    }
}

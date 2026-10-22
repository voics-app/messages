//! Listing who reacted and removing reactions of one emoji or one user.

use std::sync::Arc;

use discordy_messages_domain::{ChannelId, Emoji, MessageId, UserId};

use super::MessagingPorts;
use super::support::{announce, event, find_message};
use crate::error::AppResult;
use crate::ports::{ChannelAccessProvider, EventPublisher, MessageEventKind, MessageRepository, ReactionRepository};

/// Cap on how many reacting users one call returns.
pub const MAX_REACTION_USERS: usize = 100;

/// Users that reacted to a message with one emoji, in the order stored.
pub struct ListReactionUsers {
    access: Arc<dyn ChannelAccessProvider>,
    messages: Arc<dyn MessageRepository>,
    reactions: Arc<dyn ReactionRepository>,
}

impl ListReactionUsers {
    pub fn new(ports: &MessagingPorts) -> Self {
        Self {
            access: ports.access.clone(),
            messages: ports.messages.clone(),
            reactions: ports.reactions.clone(),
        }
    }

    pub async fn execute(
        &self,
        channel_id: ChannelId,
        message_id: MessageId,
        emoji: &Emoji,
        viewer: UserId,
    ) -> AppResult<Vec<UserId>> {
        self.access
            .access(channel_id, viewer)
            .await?
            .ensure_can_read_history()?;
        find_message(self.messages.as_ref(), channel_id, message_id).await?;
        let rows = self.reactions.for_message(channel_id, message_id).await?;
        Ok(rows
            .into_iter()
            .filter(|r| r.emoji == *emoji)
            .map(|r| r.user_id)
            .take(MAX_REACTION_USERS)
            .collect())
    }
}

/// Moderator removal of every reaction of one emoji, or of a single user's
/// reaction. Removing one's own reaction needs no moderator permission.
pub struct RemoveReactions {
    access: Arc<dyn ChannelAccessProvider>,
    messages: Arc<dyn MessageRepository>,
    reactions: Arc<dyn ReactionRepository>,
    events: Arc<dyn EventPublisher>,
}

impl RemoveReactions {
    pub fn new(ports: &MessagingPorts) -> Self {
        Self {
            access: ports.access.clone(),
            messages: ports.messages.clone(),
            reactions: ports.reactions.clone(),
            events: ports.events.clone(),
        }
    }

    /// Removes every reaction of `emoji`; one `ReactionRemoved` event per user.
    pub async fn remove_emoji(
        &self,
        channel_id: ChannelId,
        message_id: MessageId,
        emoji: Emoji,
        actor: UserId,
    ) -> AppResult<()> {
        let access = self.access.access(channel_id, actor).await?;
        access.ensure_can_manage_messages()?;
        find_message(self.messages.as_ref(), channel_id, message_id).await?;
        let rows = self.reactions.for_message(channel_id, message_id).await?;
        for row in rows.into_iter().filter(|r| r.emoji == emoji) {
            self.reactions
                .remove(channel_id, message_id, &row.emoji, row.user_id)
                .await?;
            announce(
                self.events.as_ref(),
                event(
                    &access,
                    channel_id,
                    MessageEventKind::ReactionRemoved {
                        message_id,
                        user_id: row.user_id,
                        emoji: row.emoji,
                    },
                ),
            )
            .await;
        }
        Ok(())
    }

    /// Removes `user`'s reaction with `emoji`; a missing reaction is a no-op.
    pub async fn remove_user(
        &self,
        channel_id: ChannelId,
        message_id: MessageId,
        emoji: Emoji,
        user: UserId,
        actor: UserId,
    ) -> AppResult<()> {
        let access = self.access.access(channel_id, actor).await?;
        if user == actor {
            access.ensure_can_read_history()?;
        } else {
            access.ensure_can_manage_messages()?;
        }
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
    use crate::error::AppError;
    use crate::fakes::*;
    use discordy_messages_domain::{DomainError, Permissions};

    fn thumbs() -> Emoji {
        Emoji::parse("👍").unwrap()
    }

    #[tokio::test]
    async fn lists_only_users_of_the_emoji() {
        let h = Harness::new();
        let message = h.seed(ALICE, "vote").await;
        let heart = Emoji::parse("❤️").unwrap();
        h.reactions.add(CHANNEL, message.id, &thumbs(), ALICE).await.unwrap();
        h.reactions.add(CHANNEL, message.id, &heart, BOB).await.unwrap();
        h.reactions.add(CHANNEL, message.id, &thumbs(), BOB).await.unwrap();
        let users = ListReactionUsers::new(&h.ports)
            .execute(CHANNEL, message.id, &thumbs(), BOB)
            .await
            .unwrap();
        assert_eq!(users, vec![ALICE, BOB]);
    }

    #[tokio::test]
    async fn moderators_remove_one_emoji() {
        let h = Harness::new();
        let message = h.seed(ALICE, "vote").await;
        let heart = Emoji::parse("❤️").unwrap();
        h.reactions.add(CHANNEL, message.id, &thumbs(), ALICE).await.unwrap();
        h.reactions.add(CHANNEL, message.id, &thumbs(), BOB).await.unwrap();
        h.reactions.add(CHANNEL, message.id, &heart, BOB).await.unwrap();
        let use_case = RemoveReactions::new(&h.ports);
        assert_eq!(
            use_case.remove_emoji(CHANNEL, message.id, thumbs(), BOB).await,
            Err(AppError::Domain(DomainError::MissingPermissions))
        );
        h.access.set_permissions(MEMBER | Permissions::MANAGE_MESSAGES);
        use_case.remove_emoji(CHANNEL, message.id, thumbs(), BOB).await.unwrap();
        let left = h.reactions.for_message(CHANNEL, message.id).await.unwrap();
        assert_eq!(left.len(), 1);
        assert_eq!(left[0].emoji, heart);
        assert_eq!(h.events.kinds().len(), 2);
    }

    #[tokio::test]
    async fn removing_another_users_reaction_needs_manage_messages() {
        let h = Harness::new();
        let message = h.seed(ALICE, "vote").await;
        h.reactions.add(CHANNEL, message.id, &thumbs(), ALICE).await.unwrap();
        let use_case = RemoveReactions::new(&h.ports);
        assert_eq!(
            use_case.remove_user(CHANNEL, message.id, thumbs(), ALICE, BOB).await,
            Err(AppError::Domain(DomainError::MissingPermissions))
        );
        use_case
            .remove_user(CHANNEL, message.id, thumbs(), ALICE, ALICE)
            .await
            .unwrap();
        assert!(h.reactions.for_message(CHANNEL, message.id).await.unwrap().is_empty());
        h.reactions.add(CHANNEL, message.id, &thumbs(), ALICE).await.unwrap();
        h.access.set_permissions(MEMBER | Permissions::MANAGE_MESSAGES);
        use_case
            .remove_user(CHANNEL, message.id, thumbs(), ALICE, BOB)
            .await
            .unwrap();
        assert!(h.reactions.for_message(CHANNEL, message.id).await.unwrap().is_empty());
    }
}

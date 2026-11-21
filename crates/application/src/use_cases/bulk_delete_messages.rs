use std::collections::HashSet;
use std::sync::Arc;

use discordy_messages_domain::{ChannelId, DomainError, Message, MessageId, UserId};
use futures::future::try_join_all;

use super::MessagingPorts;
use super::support::{announce, event, purge_dependents};
use crate::error::AppResult;
use crate::ports::{
    AttachmentStorage, ChannelAccessProvider, Clock, EventPublisher, MessageEventKind, MessageRepository,
    PinRepository, ReactionRepository,
};

const MIN_IDS: usize = 2;
const MAX_IDS: usize = 100;
/// Discord refuses to bulk-delete messages older than two weeks.
const MAX_AGE_MS: u64 = 14 * 24 * 60 * 60 * 1000;

pub struct BulkDeleteMessages {
    polls: Option<Arc<dyn crate::ports::PollRepository>>,
    access: Arc<dyn ChannelAccessProvider>,
    messages: Arc<dyn MessageRepository>,
    reactions: Arc<dyn ReactionRepository>,
    pins: Arc<dyn PinRepository>,
    storage: Arc<dyn AttachmentStorage>,
    events: Arc<dyn EventPublisher>,
    clock: Arc<dyn Clock>,
}

impl BulkDeleteMessages {
    pub fn new(ports: &MessagingPorts) -> Self {
        Self {
            polls: ports.polls.clone(),
            access: ports.access.clone(),
            messages: ports.messages.clone(),
            reactions: ports.reactions.clone(),
            pins: ports.pins.clone(),
            storage: ports.storage.clone(),
            events: ports.events.clone(),
            clock: ports.clock.clone(),
        }
    }

    /// Deletes 2–100 messages younger than two weeks. Ids that don't exist
    /// are ignored. Returns the ids actually deleted.
    pub async fn execute(
        &self,
        channel_id: ChannelId,
        ids: Vec<MessageId>,
        actor: UserId,
    ) -> AppResult<Vec<MessageId>> {
        let access = self.access.access(channel_id, actor).await?;
        access.ensure_can_manage_messages()?;
        self.validate(&ids)?;
        let found = self.existing(channel_id, &ids).await?;
        if found.is_empty() {
            return Ok(Vec::new());
        }
        let deleted: Vec<MessageId> = found.iter().map(|m| m.id).collect();
        for message in &found {
            super::support::fence_poll(self.polls.as_deref(), message).await?;
        }
        self.messages.delete(channel_id, &deleted).await?;
        for message in &found {
            purge_dependents(
                self.reactions.as_ref(),
                self.pins.as_ref(),
                self.storage.as_ref(),
                message,
            )
            .await?;
        }
        announce(
            self.events.as_ref(),
            event(
                &access,
                channel_id,
                MessageEventKind::DeletedBulk {
                    message_ids: deleted.clone(),
                },
            ),
        )
        .await;
        Ok(deleted)
    }

    fn validate(&self, ids: &[MessageId]) -> Result<(), DomainError> {
        if !(MIN_IDS..=MAX_IDS).contains(&ids.len()) {
            return Err(DomainError::invalid("messages", "must contain between 2 and 100 ids"));
        }
        if ids.iter().collect::<HashSet<_>>().len() != ids.len() {
            return Err(DomainError::invalid("messages", "must not contain duplicates"));
        }
        let oldest_allowed = self.clock.now_unix_ms().saturating_sub(MAX_AGE_MS);
        if ids.iter().any(|id| id.unix_millis() < oldest_allowed) {
            return Err(DomainError::invalid(
                "messages",
                "cannot delete messages older than 2 weeks",
            ));
        }
        Ok(())
    }

    async fn existing(&self, channel_id: ChannelId, ids: &[MessageId]) -> AppResult<Vec<Message>> {
        let lookups = ids.iter().map(|id| self.messages.find(channel_id, *id));
        Ok(try_join_all(lookups).await?.into_iter().flatten().collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::AppError;
    use crate::fakes::*;
    use discordy_messages_domain::{Permissions, Snowflake};

    const MODERATOR: u64 = MEMBER | Permissions::MANAGE_MESSAGES;

    #[tokio::test]
    async fn deletes_existing_messages_and_publishes_one_event() {
        let h = Harness::new();
        h.access.set_permissions(MODERATOR);
        let a = h.seed(ALICE, "a").await;
        let b = h.seed(BOB, "b").await;
        let ghost = Snowflake::first_at_unix_millis(START_MS);
        let deleted = BulkDeleteMessages::new(&h.ports)
            .execute(CHANNEL, vec![a.id, b.id, ghost], BOB)
            .await
            .unwrap();
        assert_eq!(deleted, vec![a.id, b.id]);
        assert!(h.messages.find(CHANNEL, a.id).await.unwrap().is_none());
        assert_eq!(
            h.events.kinds(),
            vec![MessageEventKind::DeletedBulk {
                message_ids: vec![a.id, b.id]
            }]
        );
    }

    #[tokio::test]
    async fn requires_manage_messages() {
        let h = Harness::new();
        let a = h.seed(ALICE, "a").await;
        let b = h.seed(ALICE, "b").await;
        let result = BulkDeleteMessages::new(&h.ports)
            .execute(CHANNEL, vec![a.id, b.id], ALICE)
            .await;
        assert_eq!(result, Err(AppError::Domain(DomainError::MissingPermissions)));
    }

    #[tokio::test]
    async fn validates_count_duplicates_and_age() {
        let h = Harness::new();
        h.access.set_permissions(MODERATOR);
        let a = h.seed(ALICE, "a").await;
        let use_case = BulkDeleteMessages::new(&h.ports);
        assert!(use_case.execute(CHANNEL, vec![a.id], BOB).await.is_err());
        assert!(use_case.execute(CHANNEL, vec![a.id, a.id], BOB).await.is_err());
        let b = h.seed(ALICE, "b").await;
        h.clock.advance_ms(MAX_AGE_MS + 1);
        assert!(use_case.execute(CHANNEL, vec![a.id, b.id], BOB).await.is_err());
    }
}

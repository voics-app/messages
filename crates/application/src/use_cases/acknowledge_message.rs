use std::sync::Arc;

use discordy_messages_domain::{Audience, ChannelId, DomainError, MessageId, ReadState, UserId};

use super::MessagingPorts;
use super::support::announce;
use crate::error::AppResult;
use crate::ports::{ChannelAccessProvider, EventPublisher, MessageEvent, MessageEventKind, ReadStateRepository};

pub struct AcknowledgeMessage {
    access: Arc<dyn ChannelAccessProvider>,
    read_states: Arc<dyn ReadStateRepository>,
    events: Arc<dyn EventPublisher>,
}

impl AcknowledgeMessage {
    pub fn new(ports: &MessagingPorts) -> Self {
        Self {
            access: ports.access.clone(),
            read_states: ports.read_states.clone(),
            events: ports.events.clone(),
        }
    }

    /// Marks the channel read up to `message_id` and tells the user's other
    /// sessions (MESSAGE_ACK goes only to that user).
    pub async fn execute(&self, channel_id: ChannelId, message_id: MessageId, user: UserId) -> AppResult<ReadState> {
        self.apply(channel_id, message_id, user, None).await
    }

    /// Manual ack (Mark Unread): sets the marker to exactly `message_id`, even
    /// backwards, keeping `mention_count` unread mentions.
    pub async fn execute_manual(
        &self,
        channel_id: ChannelId,
        message_id: MessageId,
        user: UserId,
        mention_count: u32,
    ) -> AppResult<ReadState> {
        self.apply(channel_id, message_id, user, Some(mention_count)).await
    }

    async fn apply(
        &self,
        channel_id: ChannelId,
        message_id: MessageId,
        user: UserId,
        manual: Option<u32>,
    ) -> AppResult<ReadState> {
        if message_id.value() > i64::MAX as u64 {
            return Err(DomainError::invalid("message_id", "outside the supported ID range").into());
        }
        let access = self.access.access(channel_id, user).await?;
        access.ensure_can_view()?;
        let state = match manual {
            Some(mentions) => {
                self.read_states
                    .set_position(user, channel_id, message_id, mentions)
                    .await?
            }
            None => self.read_states.acknowledge(user, channel_id, message_id).await?,
        };
        let ack = MessageEvent {
            channel_id,
            guild_id: access.guild_id,
            audience: Audience::Users(vec![user]),
            kind: MessageEventKind::Acknowledged {
                message_id: state.last_read_id.unwrap_or(message_id),
                mention_count: state.mention_count,
                version: state.version,
            },
        };
        announce(self.events.as_ref(), ack).await;
        Ok(state)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fakes::*;

    #[tokio::test]
    async fn acknowledging_clears_mentions_and_notifies_only_the_user() {
        let h = Harness::new();
        h.read_states
            .record_mention(BOB, CHANNEL, Snowflake::new(5))
            .await
            .unwrap();

        let state = AcknowledgeMessage::new(&h.ports)
            .execute(CHANNEL, Snowflake::new(9), BOB)
            .await
            .unwrap();
        assert_eq!(state.last_read_id, Some(Snowflake::new(9)));
        assert_eq!(state.mention_count, 0);
        let events = h.events.events.lock().unwrap();
        assert_eq!(events[0].audience, Audience::Users(vec![BOB]));
        assert_eq!(events[0].guild_id, Some(GUILD));
    }

    #[tokio::test]
    async fn manual_ack_moves_the_marker_backwards_and_keeps_mentions() {
        let h = Harness::new();
        let use_case = AcknowledgeMessage::new(&h.ports);
        use_case.execute(CHANNEL, Snowflake::new(20), BOB).await.unwrap();
        let state = use_case
            .execute_manual(CHANNEL, Snowflake::new(10), BOB, 1)
            .await
            .unwrap();
        assert_eq!(state.last_read_id, Some(Snowflake::new(10)));
        assert_eq!(state.mention_count, 1);
        let events = h.events.events.lock().unwrap();
        assert!(matches!(
            events.last().unwrap().kind,
            MessageEventKind::Acknowledged { mention_count: 1, .. }
        ));
    }

    #[tokio::test]
    async fn read_position_never_moves_backwards() {
        let h = Harness::new();
        let use_case = AcknowledgeMessage::new(&h.ports);
        use_case.execute(CHANNEL, Snowflake::new(20), BOB).await.unwrap();
        let state = use_case.execute(CHANNEL, Snowflake::new(10), BOB).await.unwrap();
        assert_eq!(state.last_read_id, Some(Snowflake::new(20)));
    }

    #[tokio::test]
    async fn an_out_of_range_marker_cannot_poison_stored_read_state() {
        let h = Harness::new();
        assert!(
            AcknowledgeMessage::new(&h.ports)
                .execute(CHANNEL, Snowflake::new(u64::MAX), BOB)
                .await
                .is_err()
        );
        assert!(h.read_states.find(BOB, CHANNEL).await.unwrap().is_none());
        assert!(h.events.kinds().is_empty());
    }
}

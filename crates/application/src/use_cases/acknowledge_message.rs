use std::sync::Arc;

use discordy_messages_domain::{Audience, ChannelId, MessageId, ReadState, UserId};

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
        let access = self.access.access(channel_id, user).await?;
        access.ensure_can_view()?;
        let mut state = self
            .read_states
            .find(user, channel_id)
            .await?
            .unwrap_or_else(|| ReadState::unread(user, channel_id));
        state.acknowledge(message_id);
        self.read_states.save(&state).await?;
        let ack = MessageEvent {
            channel_id,
            guild_id: access.guild_id,
            audience: Audience::Users(vec![user]),
            kind: MessageEventKind::Acknowledged {
                message_id: state.last_read_id.unwrap_or(message_id),
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
        let mut state = ReadState::unread(BOB, CHANNEL);
        state.record_mention(Snowflake::new(5));
        h.read_states.save(&state).await.unwrap();

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
    async fn read_position_never_moves_backwards() {
        let h = Harness::new();
        let use_case = AcknowledgeMessage::new(&h.ports);
        use_case.execute(CHANNEL, Snowflake::new(20), BOB).await.unwrap();
        let state = use_case.execute(CHANNEL, Snowflake::new(10), BOB).await.unwrap();
        assert_eq!(state.last_read_id, Some(Snowflake::new(20)));
    }
}

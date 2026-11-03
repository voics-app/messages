use std::sync::Arc;

use discordy_messages_domain::{ChannelId, MessageId, UserId};

use super::MessagingPorts;
use super::support::{announce, event, find_message};
use crate::error::AppResult;
use crate::ports::{ChannelAccessProvider, EventPublisher, MessageEventKind, MessageRepository};
use crate::views::MessageView;

pub struct UpdateCall {
    access: Arc<dyn ChannelAccessProvider>,
    messages: Arc<dyn MessageRepository>,
    events: Arc<dyn EventPublisher>,
}

impl UpdateCall {
    pub fn new(ports: &MessagingPorts) -> Self {
        Self {
            access: ports.access.clone(),
            messages: ports.messages.clone(),
            events: ports.events.clone(),
        }
    }

    /// Records the end (and the final participants) of a call on its system
    /// message (internal: the voice side knows when a call ends).
    pub async fn execute(
        &self,
        channel_id: ChannelId,
        message_id: MessageId,
        ended_at_ms: Option<u64>,
        participants: Option<Vec<UserId>>,
    ) -> AppResult<MessageView> {
        let access = self.access.channel(channel_id).await?;
        let mut message = find_message(self.messages.as_ref(), channel_id, message_id).await?;
        message.update_call(ended_at_ms, participants)?;
        self.messages.update(&message).await?;
        announce(
            self.events.as_ref(),
            event(
                &access,
                channel_id,
                MessageEventKind::Updated {
                    message: message.clone(),
                },
            ),
        )
        .await;
        Ok(MessageView::without_reactions(message))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::AppError;
    use crate::fakes::*;
    use crate::use_cases::{CreateMessage, CreateMessageCommand};
    use discordy_messages_domain::{DomainError, MessageKind};

    async fn start_call(h: &Harness) -> MessageId {
        let command = CreateMessageCommand {
            channel_id: CHANNEL,
            author_id: ALICE,
            system_kind: Some(MessageKind::Call),
            ..CreateMessageCommand::default()
        };
        CreateMessage::new(&h.ports)
            .execute_internal(command)
            .await
            .unwrap()
            .message
            .id
    }

    #[tokio::test]
    async fn ending_a_call_stores_and_announces_the_update() {
        let h = Harness::new();
        let id = start_call(&h).await;
        let view = UpdateCall::new(&h.ports)
            .execute(CHANNEL, id, Some(42_000), Some(vec![ALICE, BOB]))
            .await
            .unwrap();
        let call = view.message.call.unwrap();
        assert_eq!(call.ended_at_ms, Some(42_000));
        assert_eq!(call.participants, vec![ALICE, BOB]);
        let stored = h.messages.find(CHANNEL, id).await.unwrap().unwrap();
        assert_eq!(stored.call.unwrap().ended_at_ms, Some(42_000));
        assert!(matches!(
            h.events.kinds().last(),
            Some(MessageEventKind::Updated { .. })
        ));
    }

    #[tokio::test]
    async fn other_messages_have_no_call_to_update() {
        let h = Harness::new();
        let message = h.seed(ALICE, "hello").await;
        let result = UpdateCall::new(&h.ports)
            .execute(CHANNEL, message.id, Some(1), None)
            .await;
        assert!(matches!(result, Err(AppError::Domain(DomainError::InvalidForm(_)))));
    }

    #[tokio::test]
    async fn unknown_messages_are_not_found() {
        let h = Harness::new();
        let result = UpdateCall::new(&h.ports)
            .execute(CHANNEL, MessageId::new(1), Some(1), None)
            .await;
        assert_eq!(result, Err(AppError::Domain(DomainError::UnknownMessage)));
    }
}

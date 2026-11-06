use std::sync::Arc;

use discordy_messages_domain::{ChannelId, Message, MessageId};

use super::MessagingPorts;
use super::support::find_message;
use crate::error::AppResult;
use crate::ports::MessageRepository;

/// Internal: one stored message (`/internal/v1/channels/{id}/messages/{id}`), used by api to
/// snapshot the content of a reported message.
pub struct GetMessage {
    messages: Arc<dyn MessageRepository>,
}

impl GetMessage {
    pub fn new(ports: &MessagingPorts) -> Self {
        Self {
            messages: ports.messages.clone(),
        }
    }

    pub async fn execute(&self, channel_id: ChannelId, message_id: MessageId) -> AppResult<Message> {
        find_message(self.messages.as_ref(), channel_id, message_id).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::AppError;
    use crate::fakes::*;
    use discordy_messages_domain::DomainError;

    #[tokio::test]
    async fn returns_the_stored_message() {
        let h = Harness::new();
        let message = h.seed(ALICE, "hello").await;
        let found = GetMessage::new(&h.ports).execute(CHANNEL, message.id).await.unwrap();
        assert_eq!(found.content, "hello");
        assert_eq!(found.author_id, ALICE);
    }

    #[tokio::test]
    async fn an_unknown_message_is_not_found() {
        let h = Harness::new();
        let missing = h.seed(ALICE, "x").await.id;
        let result = GetMessage::new(&h.ports)
            .execute(CHANNEL, MessageId::new(missing.value() + 1))
            .await;
        assert_eq!(result, Err(AppError::Domain(DomainError::UnknownMessage)));
    }
}

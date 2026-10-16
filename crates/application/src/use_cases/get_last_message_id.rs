use std::sync::Arc;

use discordy_messages_domain::{ChannelId, MessageId};

use super::MessagingPorts;
use crate::error::AppResult;
use crate::history_reader::HistoryReader;

/// Internal: the newest message id of a channel (`/internal/v1/channels/{id}/last-message-id`).
pub struct GetLastMessageId {
    history: Arc<HistoryReader>,
}

impl GetLastMessageId {
    pub fn new(ports: &MessagingPorts) -> Self {
        Self {
            history: ports.history.clone(),
        }
    }

    pub async fn execute(&self, channel_id: ChannelId) -> AppResult<Option<MessageId>> {
        Ok(self.history.last_message_id(channel_id).await?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fakes::*;

    #[tokio::test]
    async fn returns_the_newest_message() {
        let h = Harness::new();
        let use_case = GetLastMessageId::new(&h.ports);
        assert_eq!(use_case.execute(CHANNEL).await.unwrap(), None);
        h.seed(ALICE, "a").await;
        let b = h.seed(ALICE, "b").await;
        assert_eq!(use_case.execute(CHANNEL).await.unwrap(), Some(b.id));
    }
}

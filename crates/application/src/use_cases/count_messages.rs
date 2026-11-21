use std::sync::Arc;

use discordy_messages_domain::ChannelId;

use super::MessagingPorts;
use crate::error::{AppResult, PortError};
use crate::ports::MessageRepository;

/// Exact counts used to enrich thread and forum channel listings.
pub struct CountMessages {
    messages: Arc<dyn MessageRepository>,
}

impl CountMessages {
    pub fn new(ports: &MessagingPorts) -> Self {
        Self {
            messages: ports.messages.clone(),
        }
    }

    pub async fn execute(&self, channel_ids: &[ChannelId]) -> AppResult<Vec<(ChannelId, u64)>> {
        let counts = channel_ids.iter().copied().map(|channel_id| async move {
            Ok::<_, PortError>((channel_id, self.messages.message_count(channel_id).await?))
        });
        Ok(futures::future::try_join_all(counts).await?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fakes::*;

    #[tokio::test]
    async fn reflects_stored_rows_and_deletes() {
        let h = Harness::new();
        let use_case = CountMessages::new(&h.ports);
        let first = h.seed(ALICE, "first").await;
        h.seed(ALICE, "second").await;
        assert_eq!(use_case.execute(&[CHANNEL]).await.unwrap(), vec![(CHANNEL, 2)]);
        h.messages.delete(CHANNEL, &[first.id]).await.unwrap();
        assert_eq!(use_case.execute(&[CHANNEL]).await.unwrap(), vec![(CHANNEL, 1)]);
    }
}

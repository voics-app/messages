use std::sync::Arc;

use super::MessagingPorts;
use crate::error::AppResult;
use crate::ports::{ActivityRepository, Clock};
use crate::views::MessageStats;

const DAY_MS: u64 = 24 * 60 * 60 * 1000;

/// Internal admin stats (pinned interface P4). Counts only, never content.
///
/// This service only stores Standard messages. Ephemeral ciphertext and P2P
/// signals flow through the gateway and never reach it, so their counters
/// are reported as zero here; the admin aggregator takes them from the
/// gateway's telemetry instead.
pub struct GetMessageStats {
    activity: Arc<dyn ActivityRepository>,
    clock: Arc<dyn Clock>,
}

impl GetMessageStats {
    pub fn new(ports: &MessagingPorts) -> Self {
        Self {
            activity: ports.activity.clone(),
            clock: ports.clock.clone(),
        }
    }

    pub async fn execute(&self) -> AppResult<MessageStats> {
        let since = self.clock.now_unix_ms().saturating_sub(DAY_MS);
        let standard = self.activity.count_standard_since(since).await?;
        Ok(MessageStats {
            messages_24h: standard,
            standard,
            ephemeral_relayed: 0,
            p2p_signaled: 0,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fakes::*;
    use crate::use_cases::{CreateMessage, CreateMessageCommand};

    #[tokio::test]
    async fn counts_standard_messages_of_the_last_24_hours() {
        let h = Harness::new();
        let create = CreateMessage::new(&h.ports);
        let command = || CreateMessageCommand {
            channel_id: CHANNEL,
            author_id: ALICE,
            content: "hi".into(),
            ..CreateMessageCommand::default()
        };
        create.execute(command()).await.unwrap();
        h.clock.advance_ms(DAY_MS + 1);
        create.execute(command()).await.unwrap();
        create.execute(command()).await.unwrap();

        let stats = GetMessageStats::new(&h.ports).execute().await.unwrap();
        assert_eq!(stats.messages_24h, 2);
        assert_eq!(stats.standard, 2);
        assert_eq!(stats.ephemeral_relayed, 0);
        assert_eq!(stats.p2p_signaled, 0);
    }
}

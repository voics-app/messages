use std::sync::Arc;

use discordy_messages_domain::{ChannelId, MessageId, UserId};

use super::MessagingPorts;
use super::support::{announce, event, find_message};
use crate::error::AppResult;
use crate::ports::{ChannelAccessProvider, EventPublisher, MessageEventKind, MessageRepository, PinRepository};

pub struct UnpinMessage {
    access: Arc<dyn ChannelAccessProvider>,
    messages: Arc<dyn MessageRepository>,
    pins: Arc<dyn PinRepository>,
    events: Arc<dyn EventPublisher>,
}

impl UnpinMessage {
    pub fn new(ports: &MessagingPorts) -> Self {
        Self {
            access: ports.access.clone(),
            messages: ports.messages.clone(),
            pins: ports.pins.clone(),
            events: ports.events.clone(),
        }
    }

    pub async fn execute(&self, channel_id: ChannelId, message_id: MessageId, actor: UserId) -> AppResult<()> {
        let access = self.access.access(channel_id, actor).await?;
        access.ensure_can_pin()?;
        let message = find_message(self.messages.as_ref(), channel_id, message_id).await?;
        if !message.pinned {
            return Ok(());
        }
        self.pins.unpin(channel_id, message_id).await?;
        self.messages.set_pinned(channel_id, message_id, false).await?;
        let last_pin_timestamp_ms = self.pins.list(channel_id).await?.first().map(|p| p.pinned_at_ms);
        announce(
            self.events.as_ref(),
            event(
                &access,
                channel_id,
                MessageEventKind::PinsUpdated { last_pin_timestamp_ms },
            ),
        )
        .await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fakes::*;
    use crate::use_cases::PinMessage;
    use discordy_messages_domain::DeliveryMode;

    #[tokio::test]
    async fn unpinning_reports_the_remaining_latest_pin() {
        let h = Harness::new();
        h.access.set(Ok(dm_access(DeliveryMode::Standard)));
        let first = h.seed(ALICE, "first").await;
        let second = h.seed(ALICE, "second").await;
        let pin = PinMessage::new(&h.ports);
        pin.execute(CHANNEL, first.id, ALICE).await.unwrap();
        h.clock.advance_ms(1_000);
        pin.execute(CHANNEL, second.id, ALICE).await.unwrap();

        UnpinMessage::new(&h.ports)
            .execute(CHANNEL, second.id, BOB)
            .await
            .unwrap();
        assert!(!h.messages.find(CHANNEL, second.id).await.unwrap().unwrap().pinned);
        assert_eq!(
            h.events.kinds().last().unwrap(),
            &MessageEventKind::PinsUpdated {
                last_pin_timestamp_ms: Some(START_MS)
            }
        );
    }
}

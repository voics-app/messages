use std::sync::Arc;

use discordy_messages_domain::{ChannelId, UserId};
use futures::future::try_join_all;

use super::MessagingPorts;
use super::support::view_for;
use crate::error::AppResult;
use crate::ports::{ChannelAccessProvider, MessageRepository, PinRepository, ReactionRepository};
use crate::views::MessageView;

pub struct ListPins {
    access: Arc<dyn ChannelAccessProvider>,
    messages: Arc<dyn MessageRepository>,
    reactions: Arc<dyn ReactionRepository>,
    pins: Arc<dyn PinRepository>,
}

impl ListPins {
    pub fn new(ports: &MessagingPorts) -> Self {
        Self {
            access: ports.access.clone(),
            messages: ports.messages.clone(),
            reactions: ports.reactions.clone(),
            pins: ports.pins.clone(),
        }
    }

    /// Pinned messages, most recently pinned first.
    pub async fn execute(&self, channel_id: ChannelId, viewer: UserId) -> AppResult<Vec<MessageView>> {
        self.access.access(channel_id, viewer).await?.ensure_can_read_history()?;
        let pins = self.pins.list(channel_id).await?;
        let lookups = pins.iter().map(|pin| self.messages.find(channel_id, pin.message_id));
        let messages = try_join_all(lookups).await?.into_iter().flatten();
        let views = messages.map(|message| view_for(self.reactions.as_ref(), message, viewer));
        try_join_all(views).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fakes::*;
    use crate::use_cases::PinMessage;
    use discordy_messages_domain::DeliveryMode;

    #[tokio::test]
    async fn lists_pins_newest_pin_first() {
        let h = Harness::new();
        h.access.set(Ok(dm_access(DeliveryMode::Standard)));
        let first = h.seed(ALICE, "first").await;
        let second = h.seed(ALICE, "second").await;
        let pin = PinMessage::new(&h.ports);
        pin.execute(CHANNEL, second.id, ALICE).await.unwrap();
        h.clock.advance_ms(1_000);
        pin.execute(CHANNEL, first.id, ALICE).await.unwrap();

        let pins = ListPins::new(&h.ports).execute(CHANNEL, BOB).await.unwrap();
        let contents: Vec<_> = pins.iter().map(|v| v.message.content.as_str()).collect();
        assert_eq!(contents, vec!["first", "second"]);
    }
}

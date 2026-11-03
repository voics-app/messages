use std::sync::Arc;

use discordy_messages_domain::access::MAX_PINS;
use discordy_messages_domain::{ChannelId, DomainError, MessageId, MessageKind, UserId};

use super::support::{announce, event, find_message};
use super::{CreateMessage, CreateMessageCommand, MessagingPorts};
use crate::error::AppResult;
use crate::ports::{
    ChannelAccessProvider, Clock, EventPublisher, MessageEventKind, MessageRepository, Pin, PinRepository,
};

pub struct PinMessage {
    access: Arc<dyn ChannelAccessProvider>,
    messages: Arc<dyn MessageRepository>,
    pins: Arc<dyn PinRepository>,
    events: Arc<dyn EventPublisher>,
    clock: Arc<dyn Clock>,
    create: CreateMessage,
}

impl PinMessage {
    pub fn new(ports: &MessagingPorts) -> Self {
        Self {
            access: ports.access.clone(),
            messages: ports.messages.clone(),
            pins: ports.pins.clone(),
            events: ports.events.clone(),
            clock: ports.clock.clone(),
            create: CreateMessage::new(ports),
        }
    }

    /// Pins a message (at most 50 per channel) and posts the "pinned a
    /// message" system message (type 6) referencing it. Pinning twice is a
    /// no-op.
    pub async fn execute(&self, channel_id: ChannelId, message_id: MessageId, actor: UserId) -> AppResult<()> {
        let access = self.access.access(channel_id, actor).await?;
        access.ensure_can_pin()?;
        let message = find_message(self.messages.as_ref(), channel_id, message_id).await?;
        if message.pinned {
            return Ok(());
        }
        if self.pins.list(channel_id).await?.len() >= MAX_PINS {
            return Err(DomainError::MaxPinsReached.into());
        }
        let pinned_at_ms = self.clock.now_unix_ms();
        self.pins
            .pin(
                channel_id,
                Pin {
                    message_id,
                    pinned_at_ms,
                },
            )
            .await?;
        self.messages.set_pinned(channel_id, message_id, true).await?;
        announce(
            self.events.as_ref(),
            event(
                &access,
                channel_id,
                MessageEventKind::PinsUpdated {
                    last_pin_timestamp_ms: Some(pinned_at_ms),
                },
            ),
        )
        .await;
        self.announce_pin(channel_id, message_id, actor).await;
        Ok(())
    }

    /// Best effort: the pin itself is already stored.
    async fn announce_pin(&self, channel_id: ChannelId, message_id: MessageId, actor: UserId) {
        let command = CreateMessageCommand {
            channel_id,
            author_id: actor,
            reply_to: Some(message_id),
            system_kind: Some(MessageKind::ChannelPinnedMessage),
            ..CreateMessageCommand::default()
        };
        if let Err(error) = self.create.execute_internal(command).await {
            tracing::warn!(%channel_id, %message_id, %error, "failed to post the pin system message");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::AppError;
    use crate::fakes::*;
    use discordy_messages_domain::Permissions;

    #[tokio::test]
    async fn pins_a_message_and_publishes_the_pin_time() {
        let h = Harness::new();
        h.access.set_permissions(MEMBER | Permissions::MANAGE_MESSAGES);
        let message = h.seed(ALICE, "rules").await;
        PinMessage::new(&h.ports)
            .execute(CHANNEL, message.id, ALICE)
            .await
            .unwrap();
        assert!(h.messages.find(CHANNEL, message.id).await.unwrap().unwrap().pinned);
        assert_eq!(h.pins.list(CHANNEL).await.unwrap().len(), 1);
        let kinds = h.events.kinds();
        assert_eq!(
            kinds[0],
            MessageEventKind::PinsUpdated {
                last_pin_timestamp_ms: Some(START_MS)
            }
        );
        assert_eq!(kinds.len(), 2);
    }

    #[tokio::test]
    async fn posts_a_pin_system_message_referencing_the_pinned_message() {
        let h = Harness::new();
        h.access.set_permissions(MEMBER | Permissions::MANAGE_MESSAGES);
        let message = h.seed(ALICE, "rules").await;
        PinMessage::new(&h.ports)
            .execute(CHANNEL, message.id, BOB)
            .await
            .unwrap();
        let last = h.messages.last_message_id(CHANNEL).await.unwrap().unwrap();
        let notice = h.messages.find(CHANNEL, last).await.unwrap().unwrap();
        assert_eq!(notice.kind, MessageKind::ChannelPinnedMessage);
        assert_eq!(notice.author_id, BOB);
        assert_eq!(notice.reply_to, Some(message.id));
        assert!(notice.mentions.users.is_empty());
    }

    #[tokio::test]
    async fn pinning_twice_posts_one_system_message() {
        let h = Harness::new();
        h.access.set_permissions(MEMBER | Permissions::MANAGE_MESSAGES);
        let message = h.seed(ALICE, "rules").await;
        let use_case = PinMessage::new(&h.ports);
        use_case.execute(CHANNEL, message.id, ALICE).await.unwrap();
        use_case.execute(CHANNEL, message.id, ALICE).await.unwrap();
        assert_eq!(h.events.kinds().len(), 2);
    }

    #[tokio::test]
    async fn guild_members_need_permission_to_pin() {
        let h = Harness::new();
        let message = h.seed(ALICE, "rules").await;
        let result = PinMessage::new(&h.ports).execute(CHANNEL, message.id, ALICE).await;
        assert_eq!(result, Err(AppError::Domain(DomainError::MissingPermissions)));
    }

    #[tokio::test]
    async fn at_most_fifty_pins() {
        let h = Harness::new();
        h.access.set_permissions(MEMBER | Permissions::MANAGE_MESSAGES);
        let use_case = PinMessage::new(&h.ports);
        for _ in 0..MAX_PINS {
            let message = h.seed(ALICE, "pin me").await;
            use_case.execute(CHANNEL, message.id, ALICE).await.unwrap();
        }
        let extra = h.seed(ALICE, "one more").await;
        assert_eq!(
            use_case.execute(CHANNEL, extra.id, ALICE).await,
            Err(AppError::Domain(DomainError::MaxPinsReached))
        );
    }
}

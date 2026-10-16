use std::sync::Arc;

use discordy_messages_domain::{ChannelId, MessageEdit, MessageId, UserId};

use super::MessagingPorts;
use super::support::{announce, event, find_message, view_for};
use crate::error::AppResult;
use crate::ports::{ChannelAccessProvider, Clock, EventPublisher, MessageEventKind, MessageRepository, ReactionRepository};
use crate::views::MessageView;

pub struct EditMessage {
    access: Arc<dyn ChannelAccessProvider>,
    messages: Arc<dyn MessageRepository>,
    reactions: Arc<dyn ReactionRepository>,
    events: Arc<dyn EventPublisher>,
    clock: Arc<dyn Clock>,
}

impl EditMessage {
    pub fn new(ports: &MessagingPorts) -> Self {
        Self {
            access: ports.access.clone(),
            messages: ports.messages.clone(),
            reactions: ports.reactions.clone(),
            events: ports.events.clone(),
            clock: ports.clock.clone(),
        }
    }

    pub async fn execute(
        &self,
        channel_id: ChannelId,
        message_id: MessageId,
        editor: UserId,
        edit: MessageEdit,
    ) -> AppResult<MessageView> {
        let access = self.access.access(channel_id, editor).await?;
        access.ensure_can_view()?;
        let mut message = find_message(self.messages.as_ref(), channel_id, message_id).await?;
        message.apply_edit(editor, edit, access.may_mention_everyone(), self.clock.now_unix_ms())?;
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
        view_for(self.reactions.as_ref(), message, editor).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::AppError;
    use crate::fakes::*;
    use discordy_messages_domain::DomainError;

    fn edit(content: &str) -> MessageEdit {
        MessageEdit {
            content: Some(content.to_owned()),
            embeds: None,
        }
    }

    #[tokio::test]
    async fn authors_edit_and_an_update_is_published() {
        let h = Harness::new();
        let original = h.seed(ALICE, "draft").await;
        h.clock.advance_ms(5_000);
        let view = EditMessage::new(&h.ports)
            .execute(CHANNEL, original.id, ALICE, edit("final"))
            .await
            .unwrap();
        assert_eq!(view.message.content, "final");
        assert_eq!(view.message.edited_at_ms, Some(START_MS + 5_000));
        let stored = h.messages.find(CHANNEL, original.id).await.unwrap().unwrap();
        assert_eq!(stored.content, "final");
        assert!(matches!(h.events.kinds()[0], MessageEventKind::Updated { .. }));
    }

    #[tokio::test]
    async fn others_cannot_edit() {
        let h = Harness::new();
        let original = h.seed(ALICE, "mine").await;
        let result = EditMessage::new(&h.ports)
            .execute(CHANNEL, original.id, BOB, edit("yours"))
            .await;
        assert_eq!(result, Err(AppError::Domain(DomainError::MissingPermissions)));
        assert!(h.events.kinds().is_empty());
    }

    #[tokio::test]
    async fn unknown_messages_are_reported() {
        let h = Harness::new();
        let result = EditMessage::new(&h.ports)
            .execute(CHANNEL, Snowflake::new(1), ALICE, edit("x"))
            .await;
        assert_eq!(result, Err(AppError::Domain(DomainError::UnknownMessage)));
    }
}

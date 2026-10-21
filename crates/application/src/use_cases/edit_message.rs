use std::sync::Arc;

use discordy_messages_domain::{ChannelId, MessageEdit, MessageId, UserId};

use super::unfurl_links::is_generated_embed;
use super::{MessagingPorts, UnfurlLinks};
use super::support::{announce, event, find_message, view_for};
use crate::error::AppResult;
use crate::ports::{
    ChannelAccessProvider, Clock, EventPublisher, MessageEventKind, MessageRepository, ReactionRepository,
};
use crate::views::MessageView;

pub struct EditMessage {
    access: Arc<dyn ChannelAccessProvider>,
    messages: Arc<dyn MessageRepository>,
    reactions: Arc<dyn ReactionRepository>,
    events: Arc<dyn EventPublisher>,
    clock: Arc<dyn Clock>,
    unfurl: Arc<UnfurlLinks>,
}

impl EditMessage {
    pub fn new(ports: &MessagingPorts) -> Self {
        Self {
            access: ports.access.clone(),
            messages: ports.messages.clone(),
            reactions: ports.reactions.clone(),
            events: ports.events.clone(),
            clock: ports.clock.clone(),
            unfurl: Arc::new(UnfurlLinks::new(ports)),
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
        let suppress = edit.suppress_embeds();
        if let Some(suppressed) = suppress {
            access.ensure_can_suppress_embeds(editor, message.author_id)?;
            message.set_embeds_suppressed(suppressed);
        }
        let reunfurl = edit.changes_body() || suppress == Some(false);
        if edit.changes_body() {
            let may_mention = access.may_mention_everyone();
            message.apply_edit(editor, edit, may_mention, self.clock.now_unix_ms())?;
        }
        if suppress == Some(true) {
            message.embeds.retain(|embed| !is_generated_embed(embed));
        }
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
        if reunfurl {
            self.unfurl.spawn(&message);
        }
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
            flags: None,
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

    fn flags_only(flags: u32) -> MessageEdit {
        MessageEdit {
            flags: Some(flags),
            ..MessageEdit::default()
        }
    }

    #[tokio::test]
    async fn suppressing_embeds_drops_previews_keeps_bot_embeds_and_is_not_an_edit() {
        let h = Harness::new();
        let mut original = h.seed(ALICE, "https://a.example/").await;
        original.embeds = vec![r#"{"type":"link","url":"https://a.example/"}"#.into(), r#"{"type":"rich"}"#.into()];
        h.messages.update(&original).await.unwrap();
        let view = EditMessage::new(&h.ports)
            .execute(CHANNEL, original.id, ALICE, flags_only(4))
            .await
            .unwrap();
        assert!(view.message.embeds_suppressed());
        assert_eq!(view.message.embeds, vec![r#"{"type":"rich"}"#.to_owned()]);
        assert_eq!(view.message.edited_at_ms, None);
        assert!(matches!(h.events.kinds()[0], MessageEventKind::Updated { .. }));
    }

    #[tokio::test]
    async fn moderators_may_suppress_others_embeds_but_members_may_not() {
        let h = Harness::new();
        let original = h.seed(ALICE, "https://a.example/").await;
        let result = EditMessage::new(&h.ports)
            .execute(CHANNEL, original.id, BOB, flags_only(4))
            .await;
        assert_eq!(result, Err(AppError::Domain(DomainError::MissingPermissions)));
        h.access
            .set_permissions(MEMBER | discordy_messages_domain::Permissions::MANAGE_MESSAGES);
        let view = EditMessage::new(&h.ports)
            .execute(CHANNEL, original.id, BOB, flags_only(4))
            .await
            .unwrap();
        assert!(view.message.embeds_suppressed());
    }

    #[tokio::test]
    async fn unsuppressing_is_allowed_and_a_body_edit_by_a_moderator_still_is_not() {
        let h = Harness::new();
        let mut original = h.seed(ALICE, "hi").await;
        original.set_embeds_suppressed(true);
        h.messages.update(&original).await.unwrap();
        h.access
            .set_permissions(MEMBER | discordy_messages_domain::Permissions::MANAGE_MESSAGES);
        let mixed = MessageEdit {
            content: Some("changed".into()),
            flags: Some(0),
            ..MessageEdit::default()
        };
        let result = EditMessage::new(&h.ports).execute(CHANNEL, original.id, BOB, mixed).await;
        assert_eq!(result, Err(AppError::Domain(DomainError::MissingPermissions)));
        let view = EditMessage::new(&h.ports)
            .execute(CHANNEL, original.id, ALICE, flags_only(0))
            .await
            .unwrap();
        assert!(!view.message.embeds_suppressed());
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

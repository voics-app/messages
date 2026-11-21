use super::MessagingPorts;
use super::support::{announce, event, find_message};
use crate::error::AppResult;
use crate::ports::{ChannelAccessProvider, EventPublisher, MessageEventKind, MessageRepository};
use crate::views::MessageView;
use discordy_messages_domain::{ChannelId, DomainError, MessageId};
use std::sync::Arc;

pub struct MarkCrossposted {
    access: Arc<dyn ChannelAccessProvider>,
    messages: Arc<dyn MessageRepository>,
    events: Arc<dyn EventPublisher>,
}

impl MarkCrossposted {
    pub fn new(ports: &MessagingPorts) -> Self {
        Self {
            access: ports.access.clone(),
            messages: ports.messages.clone(),
            events: ports.events.clone(),
        }
    }

    /// Only trusted internal callers may mark a stored announcement published.
    pub async fn execute(&self, channel: ChannelId, id: MessageId) -> AppResult<MessageView> {
        let access = self.access.channel(channel).await?;
        if access.guild_id.is_none() || access.channel_type != 5 {
            return Err(DomainError::invalid("channel_id", "must be an announcement channel").into());
        }
        access.ensure_standard_delivery()?;
        let before = find_message(self.messages.as_ref(), channel, id).await?;
        if before.kind.is_system() || before.flags & discordy_messages_domain::FLAG_IS_CROSSPOST != 0 {
            return Err(DomainError::invalid("message_id", "cannot publish this message").into());
        }
        if !self.messages.mark_crossposted(channel, id).await? {
            return Err(DomainError::UnknownMessage.into());
        }
        let message = find_message(self.messages.as_ref(), channel, id).await?;
        announce(
            self.events.as_ref(),
            event(
                &access,
                channel,
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
    use crate::fakes::{ALICE, CHANNEL, Harness, MEMBER, guild_access};
    use crate::ports::MessageRepository;
    use discordy_messages_domain::{DomainError, FLAG_CROSSPOSTED};

    #[tokio::test]
    async fn only_existing_announcements_can_be_marked() {
        let h = Harness::new();
        let original = h.seed(ALICE, "news").await;
        let mut access = guild_access(MEMBER);
        access.channel_type = 5;
        *h.access.channel.lock().unwrap() = Ok(access);
        let use_case = MarkCrossposted::new(&h.ports);
        let marked = use_case.execute(CHANNEL, original.id).await.unwrap();
        assert_ne!(marked.message.flags & FLAG_CROSSPOSTED, 0);
        assert_ne!(
            h.messages.find(CHANNEL, original.id).await.unwrap().unwrap().flags & FLAG_CROSSPOSTED,
            0
        );
        h.messages.delete(CHANNEL, &[original.id]).await.unwrap();
        assert!(matches!(
            use_case.execute(CHANNEL, original.id).await,
            Err(crate::AppError::Domain(DomainError::UnknownMessage))
        ));
    }
}

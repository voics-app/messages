//! Bounded current-policy lookup followed by per-message idempotent accounting.

use std::sync::Arc;

use discordy_messages_domain::{
    Audience, DeliveryMode, FLAG_EPHEMERAL, FLAG_SUPPRESS_NOTIFICATIONS, GuildId, Message, UserId,
};
use futures::{StreamExt, TryStreamExt};

use super::MessagingPorts;
use crate::PortError;
use crate::ports::{
    EventPublisher, MessageEvent, MessageEventKind, NotificationRecipientPage, NotificationRecipients,
    ReadStateRepository, RecipientQuery,
};

const PAGE_SIZE: u16 = 200;
const MAX_IN_FLIGHT: usize = 16;

pub(super) struct RecordMentions {
    recipients: Arc<dyn NotificationRecipients>,
    reads: Arc<dyn ReadStateRepository>,
    events: Arc<dyn EventPublisher>,
}

impl RecordMentions {
    pub fn new(ports: &MessagingPorts) -> Self {
        Self {
            recipients: ports.notification_recipients.clone(),
            reads: ports.read_states.clone(),
            events: ports.events.clone(),
        }
    }

    pub async fn execute(&self, message: &Message, guild: Option<GuildId>) -> Result<(), PortError> {
        if message.kind.is_system() || message.flags & (FLAG_SUPPRESS_NOTIFICATIONS | FLAG_EPHEMERAL) != 0 {
            return Ok(());
        }
        if guild.is_none() || message.mentions.everyone || !message.mentions.roles.is_empty() {
            return self.scan(message, guild).await;
        }
        // A direct-only guild post needs at most one bounded candidate lookup per
        // explicit/reply user. Never scan a whole guild for a plain message.
        futures::stream::iter(
            message
                .mentions
                .users
                .iter()
                .copied()
                .filter(|id| *id != message.author_id && id.value() <= i64::MAX as u64),
        )
        .map(|user| async move {
            let query = RecipientQuery {
                channel_id: message.channel_id,
                author_id: message.author_id,
                after: user.value().checked_sub(1).map(UserId::new),
                limit: 1,
            };
            let mut page = self.recipients.page(query).await?;
            // The first permitted candidate can belong to someone else.
            page.recipients.retain(|recipient| recipient.user_id == user);
            self.record_page(message, guild, page).await
        })
        .buffer_unordered(MAX_IN_FLIGHT)
        .try_collect::<Vec<_>>()
        .await?;
        Ok(())
    }

    async fn scan(&self, message: &Message, guild: Option<GuildId>) -> Result<(), PortError> {
        let mut after = None;
        loop {
            let page = self
                .recipients
                .page(RecipientQuery {
                    channel_id: message.channel_id,
                    author_id: message.author_id,
                    after,
                    limit: PAGE_SIZE,
                })
                .await?;
            let next = page.next_after;
            if next.is_some_and(|next| after.is_some_and(|after| next <= after)) {
                return Err(PortError::new("notification recipient cursor did not advance"));
            }
            self.record_page(message, guild, page).await?;
            let Some(next) = next else { return Ok(()) };
            after = Some(next);
        }
    }

    async fn record_page(
        &self,
        message: &Message,
        guild: Option<GuildId>,
        page: NotificationRecipientPage,
    ) -> Result<(), PortError> {
        if page.guild_id != guild {
            return Err(PortError::new("notification recipient channel scope changed"));
        }
        if page.delivery_mode != DeliveryMode::Standard {
            return Ok(());
        }
        let results: Vec<_> = futures::stream::iter(page.recipients)
            .filter(|recipient| std::future::ready(recipient.counts_mention(message, guild.is_none())))
            .map(|recipient| async move {
                let state = self
                    .reads
                    .record_mention(recipient.user_id, message.channel_id, message.id)
                    .await?;
                self.events
                    .publish(MessageEvent {
                        channel_id: message.channel_id,
                        guild_id: guild,
                        audience: Audience::Users(vec![recipient.user_id]),
                        kind: MessageEventKind::ReadStateUpdated { state },
                    })
                    .await
            })
            .buffer_unordered(MAX_IN_FLIGHT)
            .collect()
            .await;
        // Finish this bounded page even if one write fails. Return an observable
        // error; no unfiltered fallback and no claim of durable outbox delivery.
        for result in results {
            result?;
        }
        Ok(())
    }
}

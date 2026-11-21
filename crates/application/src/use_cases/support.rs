//! Small helpers shared by several use cases.

use discordy_messages_domain::reaction::summarize;
use discordy_messages_domain::{ChannelAccess, ChannelId, DomainError, Message, MessageId, UserId};

use crate::error::AppResult;
use crate::ports::{
    AttachmentStorage, EventPublisher, MessageEvent, MessageEventKind, MessageRepository, PinRepository,
    ReactionRepository,
};
use crate::views::MessageView;

pub(crate) async fn find_message(
    messages: &dyn MessageRepository,
    channel_id: ChannelId,
    message_id: MessageId,
) -> AppResult<Message> {
    messages
        .find(channel_id, message_id)
        .await?
        .ok_or_else(|| DomainError::UnknownMessage.into())
}

pub(crate) fn event(access: &ChannelAccess, channel_id: ChannelId, kind: MessageEventKind) -> MessageEvent {
    MessageEvent {
        channel_id,
        guild_id: access.guild_id,
        audience: access.audience(),
        kind,
    }
}

/// Publishes an event after the write has committed. The ScyllaDB write is
/// the source of truth: a publish failure is logged rather than failing a
/// request whose effect is already durable (clients re-sync from history).
pub(crate) async fn announce(events: &dyn EventPublisher, event: MessageEvent) {
    let channel_id = event.channel_id;
    if let Err(error) = events.publish(event).await {
        tracing::error!(%channel_id, %error, "failed to publish message event");
    }
}

pub(crate) async fn view_for(
    reactions: &dyn ReactionRepository,
    message: Message,
    viewer: UserId,
) -> AppResult<MessageView> {
    let rows = reactions.for_message(message.channel_id, message.id).await?;
    Ok(MessageView {
        reactions: summarize(&rows, viewer),
        message,
    })
}

/// Removes everything that hangs off a deleted message: reactions, its pin
/// and its uploaded files.
pub(crate) async fn purge_dependents(
    reactions: &dyn ReactionRepository,
    pins: &dyn PinRepository,
    storage: &dyn AttachmentStorage,
    message: &Message,
) -> AppResult<()> {
    reactions.remove_all(message.channel_id, message.id).await?;
    if message.pinned {
        pins.unpin(message.channel_id, message.id).await?;
    }
    let snapshot_files = message.forward.iter().flat_map(|f| f.snapshot.attachments.iter());
    for attachment in message.attachments.iter().chain(snapshot_files) {
        let key = attachment.key(message.channel_id);
        if let Err(error) = storage.delete(&key).await {
            tracing::warn!(key = %key.as_object_key(), %error, "failed to delete attachment object");
        }
    }
    Ok(())
}

/// Fencing precedes message deletion, so a stale vote cannot recreate rows.
pub(crate) async fn fence_poll(polls: Option<&dyn crate::ports::PollRepository>, message: &Message) -> AppResult<()> {
    if message.poll.is_some() {
        polls
            .ok_or_else(|| crate::PortError::new("poll storage is unavailable"))?
            .delete(crate::ports::PollKey {
                channel_id: message.channel_id,
                message_id: message.id,
            })
            .await?;
    }
    Ok(())
}

/// Resolves the caller's access and applies `check`. A delivery mode conflict is re-checked once
/// against fresh data, because the access answer may be cached for a few seconds after the user
/// switched the chat's delivery mode.
pub(crate) async fn checked_access(
    provider: &dyn crate::ports::ChannelAccessProvider,
    channel_id: ChannelId,
    user: UserId,
    check: impl Fn(&ChannelAccess) -> Result<(), DomainError>,
) -> AppResult<ChannelAccess> {
    let access = provider.access(channel_id, user).await?;
    match check(&access) {
        Ok(()) => Ok(access),
        Err(DomainError::DeliveryModeConflict(_)) => {
            let fresh = provider.refresh(channel_id, user).await?;
            check(&fresh)?;
            Ok(fresh)
        }
        Err(error) => Err(error.into()),
    }
}

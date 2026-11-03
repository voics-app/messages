//! Mapping between domain objects and the wire contracts.

use contracts::{
    AttachmentDto, AuthorOverrideDto, CallDto, EmojiDto, InteractionMetadataDto, MessageDto, MessageReferenceDto,
    MessageSnapshotDto, ReactionDto, ReadStateDto, SnapshotMessageDto, UserRef,
};
use discordy_messages_application::MessageView;
use discordy_messages_domain::{
    Attachment, Call, ChannelId, Emoji, GuildId, Message, MessageSnapshot, ReactionCount, ReadState,
};
use serde_json::Value;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use crate::attachment_urls::AttachmentUrls;
use crate::authors::{Authors, author_dto, resolve_views};
use crate::embed_proxy::with_proxied_media;
use crate::state::AppState;

/// RFC 3339 with millisecond precision, as Discord renders timestamps.
pub fn rfc3339(unix_ms: u64) -> String {
    let nanos = i128::from(unix_ms) * 1_000_000;
    OffsetDateTime::from_unix_timestamp_nanos(nanos)
        .unwrap_or(OffsetDateTime::UNIX_EPOCH)
        .format(&Rfc3339)
        .unwrap_or_default()
}

/// Parses an RFC 3339 timestamp into unix milliseconds.
pub fn parse_rfc3339(text: &str) -> Option<u64> {
    let parsed = OffsetDateTime::parse(text, &Rfc3339).ok()?;
    u64::try_from(parsed.unix_timestamp_nanos() / 1_000_000).ok()
}

pub fn emoji_dto(emoji: &Emoji) -> EmojiDto {
    match emoji {
        Emoji::Unicode(name) => EmojiDto {
            id: None,
            name: name.clone(),
        },
        Emoji::Custom { name, id } => EmojiDto {
            id: Some(id.to_string()),
            name: name.clone(),
        },
    }
}

fn reaction_dto(reaction: &ReactionCount) -> ReactionDto {
    ReactionDto {
        count: reaction.count,
        me: reaction.me,
        emoji: emoji_dto(&reaction.emoji),
    }
}

fn attachment_dto(channel_id: ChannelId, attachment: &Attachment, urls: &AttachmentUrls) -> AttachmentDto {
    let url = urls.url(channel_id, attachment);
    AttachmentDto {
        id: attachment.id.to_string(),
        filename: attachment.filename.as_str().to_owned(),
        size: attachment.size,
        proxy_url: url.clone(),
        url,
        content_type: attachment.content_type.clone(),
        width: attachment.width,
        height: attachment.height,
        description: attachment.description.clone(),
        flags: (attachment.flags != 0).then_some(attachment.flags),
    }
}

/// Embeds and components are stored as JSON text; corrupt entries are dropped.
fn json_values(raw: &[String]) -> Vec<Value> {
    raw.iter().filter_map(|text| serde_json::from_str(text).ok()).collect()
}

/// Embeds as sent on the wire: remote media goes through the media proxy.
fn embed_values(raw: &[String], urls: &AttachmentUrls) -> Vec<Value> {
    json_values(raw)
        .into_iter()
        .map(|embed| with_proxied_media(embed, urls))
        .collect()
}

fn reference_dto(message: &Message, guild_id: Option<GuildId>) -> Option<MessageReferenceDto> {
    if let Some(origin) = &message.forward {
        return Some(MessageReferenceDto {
            kind: Some(1),
            message_id: Some(origin.message_id.to_string()),
            channel_id: Some(origin.channel_id.to_string()),
            guild_id: origin.guild_id.map(|g| g.to_string()),
        });
    }
    if let Some(thread) = message.thread_id {
        return Some(MessageReferenceDto {
            kind: None,
            message_id: None,
            channel_id: Some(thread.to_string()),
            guild_id: guild_id.map(|g| g.to_string()),
        });
    }
    message.reply_to.map(|id| MessageReferenceDto {
        kind: None,
        message_id: Some(id.to_string()),
        channel_id: Some(message.channel_id.to_string()),
        guild_id: guild_id.map(|g| g.to_string()),
    })
}

fn call_dto(call: &Call) -> CallDto {
    CallDto {
        participants: call.participants.iter().map(ToString::to_string).collect(),
        ended_timestamp: call.ended_at_ms.map(rfc3339),
    }
}

/// Snapshot files live under the channel of the message that carries them.
fn snapshot_dto(channel_id: ChannelId, snapshot: &MessageSnapshot, urls: &AttachmentUrls) -> MessageSnapshotDto {
    MessageSnapshotDto {
        message: SnapshotMessageDto {
            content: snapshot.content.clone(),
            timestamp: rfc3339(snapshot.created_at_ms),
            edited_timestamp: snapshot.edited_at_ms.map(rfc3339),
            mentions: snapshot
                .mentions
                .users
                .iter()
                .map(|id| UserRef { id: id.to_string() })
                .collect(),
            mention_roles: snapshot.mentions.roles.iter().map(ToString::to_string).collect(),
            attachments: snapshot
                .attachments
                .iter()
                .map(|a| attachment_dto(channel_id, a, urls))
                .collect(),
            embeds: embed_values(&snapshot.embeds, urls),
            kind: 0,
            flags: snapshot.flags,
        },
    }
}

pub fn message_dto(
    message: &Message,
    reactions: &[ReactionCount],
    guild_id: Option<GuildId>,
    nonce: Option<String>,
    urls: &AttachmentUrls,
    authors: &Authors,
) -> MessageDto {
    MessageDto {
        id: message.id.to_string(),
        channel_id: message.channel_id.to_string(),
        guild_id: guild_id.map(|id| id.to_string()),
        author: author_dto(message, authors),
        content: message.content.clone(),
        timestamp: rfc3339(message.timestamp_ms()),
        edited_timestamp: message.edited_at_ms.map(rfc3339),
        tts: false,
        mention_everyone: message.mentions.everyone,
        mentions: message
            .mentions
            .users
            .iter()
            .map(|id| UserRef { id: id.to_string() })
            .collect(),
        mention_roles: message.mentions.roles.iter().map(ToString::to_string).collect(),
        attachments: message
            .attachments
            .iter()
            .map(|a| attachment_dto(message.channel_id, a, urls))
            .collect(),
        embeds: embed_values(&message.embeds, urls),
        reactions: reactions.iter().map(reaction_dto).collect(),
        nonce,
        pinned: message.pinned,
        kind: message.kind.code(),
        flags: message.flags,
        message_reference: reference_dto(message, guild_id),
        call: message.call.as_ref().map(call_dto),
        message_snapshots: message
            .forward
            .iter()
            .map(|origin| snapshot_dto(message.channel_id, &origin.snapshot, urls))
            .collect(),
        components: json_values(&message.components),
        webhook_id: message.webhook.as_ref().map(|w| w.webhook_id.to_string()),
        application_id: message.interaction.map(|i| i.application_id.to_string()),
        interaction_metadata: message.interaction.map(|i| InteractionMetadataDto {
            id: i.interaction_id.to_string(),
        }),
        author_override: message
            .webhook
            .as_ref()
            .filter(|w| w.username.is_some() || w.avatar_url.is_some())
            .map(|w| AuthorOverrideDto {
                username: w.username.clone(),
                avatar_url: w.avatar_url.clone(),
            }),
        hit: None,
    }
}

/// A message as returned by REST: reactions carry the viewer's `me` flag. The
/// guild id is not stored with the message, so REST responses omit it.
pub fn view_dto(view: &MessageView, urls: &AttachmentUrls, authors: &Authors) -> MessageDto {
    message_dto(&view.message, &view.reactions, None, None, urls, authors)
}

/// REST body for a list of messages: authors are resolved in one batch.
pub async fn dtos(state: &AppState, views: &[MessageView]) -> Vec<MessageDto> {
    let authors = resolve_views(state.users.as_ref(), views).await;
    views.iter().map(|view| view_dto(view, &state.urls, &authors)).collect()
}

/// REST body for one message.
pub async fn single(state: &AppState, view: &MessageView) -> MessageDto {
    dtos(state, std::slice::from_ref(view)).await.remove(0)
}

pub fn read_state_dto(state: &ReadState) -> ReadStateDto {
    ReadStateDto {
        id: state.channel_id.to_string(),
        last_message_id: state.last_read_id.map(|id| id.to_string()),
        mention_count: state.mention_count,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicU64;

    use super::*;
    use discordy_messages_application::fakes::FakeClock;
    use discordy_messages_domain::{MessageDraft, MessageKind, Snowflake};

    fn urls() -> AttachmentUrls {
        AttachmentUrls::new(
            "http://cdn",
            "unit-test-signing-key-0123456789abcdef",
            std::time::Duration::from_secs(60),
            Arc::new(FakeClock(AtomicU64::new(0))),
        )
    }

    #[test]
    fn timestamps_render_with_milliseconds() {
        assert_eq!(rfc3339(1_767_225_600_123), "2026-01-01T00:00:00.123Z");
    }

    #[test]
    fn replies_carry_a_message_reference_and_mentions_are_listed() {
        let draft = MessageDraft {
            content: "hi <@7>".into(),
            reply_to: Some(Snowflake::new(3)),
            ..MessageDraft::default()
        };
        let message = Message::compose(
            Snowflake::new(1 << 22),
            Snowflake::new(5),
            Snowflake::new(6),
            draft,
            false,
        )
        .unwrap();
        let dto = message_dto(
            &message,
            &[],
            Some(Snowflake::new(8)),
            Some("n".into()),
            &urls(),
            &Authors::new(),
        );
        assert_eq!(dto.kind, 19);
        assert_eq!(dto.message_reference.unwrap().message_id.as_deref(), Some("3"));
        assert_eq!(dto.mentions, vec![UserRef { id: "7".into() }]);
        assert_eq!(dto.guild_id.as_deref(), Some("8"));
        assert_eq!(dto.nonce.as_deref(), Some("n"));
    }

    fn system_message(kind: MessageKind, draft: MessageDraft) -> MessageDto {
        let message = Message::compose(
            Snowflake::new(1 << 22),
            Snowflake::new(5),
            Snowflake::new(6),
            MessageDraft {
                system_kind: Some(kind),
                ..draft
            },
            false,
        )
        .unwrap();
        message_dto(&message, &[], Some(Snowflake::new(8)), None, &urls(), &Authors::new())
    }

    #[test]
    fn pins_reference_the_pinned_message() {
        let dto = system_message(
            MessageKind::ChannelPinnedMessage,
            MessageDraft {
                reply_to: Some(Snowflake::new(3)),
                ..MessageDraft::default()
            },
        );
        assert_eq!(dto.kind, 6);
        let reference = dto.message_reference.unwrap();
        assert_eq!(reference.message_id.as_deref(), Some("3"));
        assert_eq!(reference.channel_id.as_deref(), Some("5"));
    }

    #[test]
    fn thread_messages_reference_the_thread_channel() {
        let dto = system_message(
            MessageKind::ThreadCreated,
            MessageDraft {
                content: "plans".into(),
                thread_id: Some(Snowflake::new(77)),
                ..MessageDraft::default()
            },
        );
        assert_eq!(dto.kind, 18);
        let reference = dto.message_reference.unwrap();
        assert_eq!(reference.channel_id.as_deref(), Some("77"));
        assert_eq!(reference.message_id, None);
        assert_eq!(dto.content, "plans");
    }

    #[test]
    fn calls_render_participants_and_the_end_time() {
        let running = system_message(MessageKind::Call, MessageDraft::default());
        assert_eq!(running.kind, 3);
        assert_eq!(
            running.call,
            Some(CallDto {
                participants: vec!["6".into()],
                ended_timestamp: None
            })
        );
        let ended = call_dto(&Call {
            participants: vec![Snowflake::new(6), Snowflake::new(7)],
            ended_at_ms: Some(1_767_225_600_123),
        });
        assert_eq!(ended.ended_timestamp.as_deref(), Some("2026-01-01T00:00:00.123Z"));
        assert_eq!(ended.participants, ["6", "7"]);
        assert_eq!(parse_rfc3339("2026-01-01T00:00:00.123Z"), Some(1_767_225_600_123));
        assert_eq!(parse_rfc3339("nope"), None);
    }

    #[test]
    fn custom_emoji_keep_their_id() {
        let emoji = Emoji::Custom {
            name: "blob".into(),
            id: Snowflake::new(123),
        };
        assert_eq!(emoji_dto(&emoji).id.as_deref(), Some("123"));
        assert_eq!(emoji_dto(&Emoji::Unicode("👍".into())).id, None);
    }
}

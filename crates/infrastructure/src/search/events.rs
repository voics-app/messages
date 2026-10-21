//! Turns `discordy.message.v1` envelopes into index operations.
//!
//! Only message create, update and delete events matter. Messages flagged
//! ephemeral are interaction relays that are never stored, so they are never
//! indexed; Ephemeral and P2P chats do not use this topic at all.

use contracts::{Envelope, MessageDeleteBulkData, MessageDeleteData, MessageDto, event_type};
use discordy_messages_application::ports::IndexOp;
use discordy_messages_domain::{FLAG_EPHEMERAL, HasSet, IndexedMessage, Snowflake};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

fn id(raw: &str) -> Option<Snowflake> {
    raw.parse().ok()
}

fn millis(rfc3339: &str) -> Option<u64> {
    let parsed = OffsetDateTime::parse(rfc3339, &Rfc3339).ok()?;
    u64::try_from(parsed.unix_timestamp_nanos() / 1_000_000).ok()
}

fn upsert(envelope: &Envelope, message: &MessageDto) -> Option<IndexOp> {
    if message.flags & FLAG_EPHEMERAL != 0 {
        return None;
    }
    let guild_id = message
        .guild_id
        .as_deref()
        .or(envelope.guild_id.as_deref())
        .and_then(id);
    Some(IndexOp::Upsert(IndexedMessage {
        channel_id: id(&message.channel_id)?,
        message_id: id(&message.id)?,
        guild_id,
        author_id: id(&message.author.id)?,
        content: message.content.clone(),
        mentions: message.mentions.iter().filter_map(|m| id(&m.id)).collect(),
        has: HasSet::classify(
            &message.content,
            !message.embeds.is_empty(),
            message
                .attachments
                .iter()
                .map(|a| (a.content_type.as_deref(), a.filename.as_str())),
        ),
        edited_at_ms: message.edited_timestamp.as_deref().and_then(millis).unwrap_or(0),
    }))
}

/// The operations an envelope implies; empty for events search ignores or
/// payloads that do not parse (they are logged by the caller).
pub fn operations(envelope: &Envelope) -> Vec<IndexOp> {
    match envelope.kind.as_str() {
        event_type::MESSAGE_CREATE | event_type::MESSAGE_UPDATE => {
            match serde_json::from_value::<MessageDto>(envelope.data.clone()) {
                Ok(message) => upsert(envelope, &message).into_iter().collect(),
                Err(error) => {
                    tracing::warn!(%error, kind = %envelope.kind, "search: unreadable message event");
                    Vec::new()
                }
            }
        }
        event_type::MESSAGE_DELETE => serde_json::from_value::<MessageDeleteData>(envelope.data.clone())
            .ok()
            .and_then(|d| {
                Some(IndexOp::Delete {
                    channel_id: id(&d.channel_id)?,
                    message_ids: vec![id(&d.id)?],
                })
            })
            .into_iter()
            .collect(),
        event_type::MESSAGE_DELETE_BULK => serde_json::from_value::<MessageDeleteBulkData>(envelope.data.clone())
            .ok()
            .and_then(|d| {
                Some(IndexOp::Delete {
                    channel_id: id(&d.channel_id)?,
                    message_ids: d.ids.iter().filter_map(|raw| id(raw)).collect(),
                })
            })
            .into_iter()
            .collect(),
        _ => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn envelope(kind: &str, guild: Option<&str>, data: serde_json::Value) -> Envelope {
        Envelope {
            id: "e".into(),
            kind: kind.into(),
            v: 1,
            occurred_at: "2026-01-01T00:00:00Z".into(),
            guild_id: guild.map(str::to_owned),
            channel_id: None,
            target_user_ids: None,
            data,
        }
    }

    fn message(flags: u32, edited: Option<&str>) -> serde_json::Value {
        json!({
            "id": "100", "channel_id": "5", "author": {"id": "7", "username": "a", "discriminator": "0"},
            "content": "see https://x.test", "timestamp": "2026-01-01T00:00:00Z",
            "edited_timestamp": edited, "tts": false, "mention_everyone": false,
            "mentions": [{"id": "9"}], "mention_roles": [],
            "attachments": [{"id": "1", "filename": "a.png", "size": 1, "url": "u", "proxy_url": "u", "content_type": "image/png"}],
            "embeds": [], "pinned": false, "type": 0, "flags": flags, "components": []
        })
    }

    #[test]
    fn create_becomes_an_upsert_with_guild_from_the_envelope() {
        let ops = operations(&envelope("MESSAGE_CREATE", Some("3"), message(0, None)));
        let [IndexOp::Upsert(entry)] = ops.as_slice() else {
            panic!("expected one upsert, got {ops:?}");
        };
        assert_eq!(entry.guild_id, Some(Snowflake::new(3)));
        assert_eq!(entry.author_id, Snowflake::new(7));
        assert_eq!(entry.mentions, vec![Snowflake::new(9)]);
        assert!(entry.has.contains(discordy_messages_domain::HasFilter::Link));
        assert!(entry.has.contains(discordy_messages_domain::HasFilter::Image));
        assert_eq!(entry.edited_at_ms, 0);
    }

    #[test]
    fn updates_carry_the_edit_time_as_version() {
        let ops = operations(&envelope(
            "MESSAGE_UPDATE",
            None,
            message(0, Some("2026-01-01T00:00:01.500Z")),
        ));
        let [IndexOp::Upsert(entry)] = ops.as_slice() else {
            panic!("expected one upsert");
        };
        assert_eq!(entry.edited_at_ms, 1_767_225_601_500);
        assert_eq!(entry.guild_id, None);
    }

    #[test]
    fn ephemeral_relays_are_skipped() {
        assert!(operations(&envelope("MESSAGE_CREATE", Some("3"), message(FLAG_EPHEMERAL, None))).is_empty());
    }

    #[test]
    fn deletes_map_to_delete_ops() {
        let single = operations(&envelope("MESSAGE_DELETE", None, json!({"id": "4", "channel_id": "5"})));
        assert_eq!(
            single,
            vec![IndexOp::Delete {
                channel_id: Snowflake::new(5),
                message_ids: vec![Snowflake::new(4)]
            }]
        );
        let bulk = operations(&envelope(
            "MESSAGE_DELETE_BULK",
            None,
            json!({"ids": ["1", "2"], "channel_id": "5"}),
        ));
        assert_eq!(
            bulk,
            vec![IndexOp::Delete {
                channel_id: Snowflake::new(5),
                message_ids: vec![Snowflake::new(1), Snowflake::new(2)]
            }]
        );
    }

    #[test]
    fn other_events_and_garbage_are_ignored() {
        assert!(operations(&envelope("MESSAGE_REACTION_ADD", None, json!({}))).is_empty());
        assert!(operations(&envelope("MESSAGE_CREATE", None, json!({"nope": 1}))).is_empty());
    }
}

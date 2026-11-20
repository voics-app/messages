//! Mapping between domain objects and ScyllaDB rows.
//!
//! CQL `bigint` is signed; Snowflakes are below 2^63 so the casts are
//! lossless. Empty CQL collections come back as NULL, hence the `Option`s.

use std::collections::BTreeSet;

use discordy_messages_domain::{
    Attachment, AttachmentFilename, Bucket, Call, ForwardOrigin, InteractionLink, Mentions, Message, MessageKind,
    MessageSnapshot, ReadState, Snowflake, WebhookAuthor,
};
use scylla::value::CqlTimestamp;
use scylla::{DeserializeRow, DeserializeValue, SerializeRow, SerializeValue};

pub(crate) fn id(value: Snowflake) -> i64 {
    value.value() as i64
}

pub(crate) fn snowflake(value: i64) -> Snowflake {
    Snowflake::new(value as u64)
}

#[derive(Debug, Clone, PartialEq, DeserializeValue, SerializeValue)]
pub(crate) struct AttachmentUdt {
    id: i64,
    filename: String,
    content_type: Option<String>,
    size: i64,
    width: Option<i32>,
    height: Option<i32>,
    description: Option<String>,
    flags: Option<i32>,
}

#[derive(Debug, Clone, PartialEq, DeserializeRow, SerializeRow)]
pub(crate) struct MessageRow {
    channel_id: i64,
    bucket: i32,
    message_id: i64,
    author_id: Option<i64>,
    #[scylla(rename = "type")]
    kind: Option<i32>,
    content: Option<String>,
    tts: Option<bool>,
    attachments: Option<Vec<AttachmentUdt>>,
    embeds: Option<String>,
    components: Option<String>,
    mentions: Option<Vec<i64>>,
    mention_roles: Option<Vec<i64>>,
    mention_everyone: Option<bool>,
    mention_here: Option<bool>,
    reply_to: Option<i64>,
    flags: Option<i32>,
    edited_at: Option<CqlTimestamp>,
    pinned: Option<bool>,
    webhook_id: Option<i64>,
    webhook_username: Option<String>,
    webhook_avatar_url: Option<String>,
    interaction_id: Option<i64>,
    application_id: Option<i64>,
    forward_channel_id: Option<i64>,
    forward_message_id: Option<i64>,
    forward_guild_id: Option<i64>,
    forward_snapshot: Option<String>,
    call_participants: Option<Vec<i64>>,
    call_ended_at: Option<CqlTimestamp>,
    thread_id: Option<i64>,
}

/// The column list matching [`MessageRow`], for SELECT and INSERT.
pub(crate) const MESSAGE_COLUMNS: &str = "channel_id, bucket, message_id, author_id, type, content, attachments, \
     embeds, components, mentions, mention_roles, mention_everyone, reply_to, flags, edited_at, pinned, \
     webhook_id, webhook_username, webhook_avatar_url, interaction_id, application_id, forward_channel_id, \
     forward_message_id, forward_guild_id, forward_snapshot, call_participants, call_ended_at, thread_id, mention_here, tts";

/// Number of columns in [`MESSAGE_COLUMNS`], the INSERT's placeholder count.
pub(crate) const MESSAGE_COLUMN_COUNT: usize = 30;

impl MessageRow {
    pub(crate) fn from_message(message: &Message) -> Self {
        Self {
            channel_id: id(message.channel_id),
            bucket: Bucket::of(message.id).value(),
            message_id: id(message.id),
            author_id: Some(id(message.author_id)),
            kind: Some(i32::from(message.kind.code())),
            content: Some(message.content.clone()),
            tts: Some(message.tts),
            attachments: Some(message.attachments.iter().map(attachment_udt).collect()),
            embeds: Some(json_array(&message.embeds)),
            components: Some(json_array(&message.components)),
            mentions: Some(message.mentions.users.iter().copied().map(id).collect()),
            mention_roles: Some(message.mentions.roles.iter().copied().map(id).collect()),
            mention_everyone: Some(message.mentions.everyone),
            mention_here: Some(message.mentions.here),
            reply_to: message.reply_to.map(id),
            flags: Some(message.flags as i32),
            edited_at: message.edited_at_ms.map(|ms| CqlTimestamp(ms as i64)),
            pinned: Some(message.pinned),
            webhook_id: message.webhook.as_ref().map(|w| id(w.webhook_id)),
            webhook_username: message.webhook.as_ref().and_then(|w| w.username.clone()),
            webhook_avatar_url: message.webhook.as_ref().and_then(|w| w.avatar_url.clone()),
            interaction_id: message.interaction.map(|i| id(i.interaction_id)),
            application_id: message.interaction.map(|i| id(i.application_id)),
            forward_channel_id: message.forward.as_ref().map(|f| id(f.channel_id)),
            forward_message_id: message.forward.as_ref().map(|f| id(f.message_id)),
            forward_guild_id: message.forward.as_ref().and_then(|f| f.guild_id.map(id)),
            forward_snapshot: message.forward.as_ref().map(|f| snapshot_json(&f.snapshot)),
            call_participants: message
                .call
                .as_ref()
                .map(|c| c.participants.iter().copied().map(id).collect()),
            call_ended_at: message
                .call
                .as_ref()
                .and_then(|c| c.ended_at_ms)
                .map(|ms| CqlTimestamp(ms as i64)),
            thread_id: message.thread_id.map(id),
        }
    }

    /// `None` for partial rows (an update that raced a delete leaves a row
    /// without an author); such rows are treated as deleted.
    pub(crate) fn forum_marker(
        self,
    ) -> Option<(
        discordy_messages_domain::UserId,
        discordy_messages_domain::MessageId,
        String,
    )> {
        if self.flags? as u32 != discordy_messages_domain::FORUM_MARKER_FLAG {
            return None;
        }
        Some((snowflake(self.author_id?), snowflake(self.reply_to?), self.content?))
    }

    pub(crate) fn into_message(self) -> Option<Message> {
        if self.flags.map(|flags| flags as u32) == Some(discordy_messages_domain::FORUM_MARKER_FLAG) {
            return None;
        }
        let author_id = self.author_id?;
        let kind = MessageKind::from_code(self.kind.unwrap_or(0) as u8);
        let call = (kind == MessageKind::Call).then(|| Call {
            participants: self
                .call_participants
                .unwrap_or_default()
                .into_iter()
                .map(snowflake)
                .collect(),
            ended_at_ms: self.call_ended_at.map(|t| t.0 as u64),
        });
        Some(Message {
            id: snowflake(self.message_id),
            channel_id: snowflake(self.channel_id),
            author_id: snowflake(author_id),
            kind,
            content: self.content.unwrap_or_default(),
            tts: self.tts.unwrap_or(false),
            attachments: self
                .attachments
                .unwrap_or_default()
                .into_iter()
                .filter_map(attachment_from_udt)
                .collect(),
            embeds: split_json_array(self.embeds.as_deref()),
            components: split_json_array(self.components.as_deref()),
            mentions: Mentions {
                users: to_ids(self.mentions),
                roles: to_ids(self.mention_roles),
                everyone: self.mention_everyone.unwrap_or(false),
                here: self.mention_here.unwrap_or(false),
            },
            reply_to: self.reply_to.map(snowflake),
            flags: self.flags.unwrap_or(0) as u32,
            edited_at_ms: self.edited_at.map(|t| t.0 as u64),
            pinned: self.pinned.unwrap_or(false),
            webhook: self.webhook_id.map(|raw| WebhookAuthor {
                webhook_id: snowflake(raw),
                username: self.webhook_username,
                avatar_url: self.webhook_avatar_url,
            }),
            interaction: match (self.interaction_id, self.application_id) {
                (Some(interaction), Some(application)) => Some(InteractionLink {
                    interaction_id: snowflake(interaction),
                    application_id: snowflake(application),
                }),
                _ => None,
            },
            forward: match (self.forward_channel_id, self.forward_message_id, self.forward_snapshot) {
                (Some(channel), Some(message), Some(snapshot)) => {
                    snapshot_from_json(&snapshot).map(|snapshot| ForwardOrigin {
                        channel_id: snowflake(channel),
                        message_id: snowflake(message),
                        guild_id: self.forward_guild_id.map(snowflake),
                        snapshot,
                    })
                }
                _ => None,
            },
            call,
            thread_id: self.thread_id.map(snowflake),
        })
    }
}

#[derive(Debug, Clone, DeserializeRow)]
pub(crate) struct ReadStateRow {
    pub user_id: i64,
    pub channel_id: i64,
    pub last_read_id: Option<i64>,
    pub mention_count: Option<i32>,
}

impl ReadStateRow {
    pub(crate) fn into_read_state(self) -> ReadState {
        ReadState {
            version: 0,
            user_id: snowflake(self.user_id),
            channel_id: snowflake(self.channel_id),
            last_read_id: self.last_read_id.map(snowflake),
            mention_count: self.mention_count.unwrap_or(0).max(0) as u32,
        }
    }
}

pub(crate) fn attachment_udt(attachment: &Attachment) -> AttachmentUdt {
    AttachmentUdt {
        id: id(attachment.id),
        filename: attachment.filename.as_str().to_owned(),
        content_type: attachment.content_type.clone(),
        size: attachment.size as i64,
        width: attachment.width.map(|w| w as i32),
        height: attachment.height.map(|h| h as i32),
        description: attachment.description.clone(),
        flags: Some(attachment.flags as i32),
    }
}

fn attachment_from_udt(udt: AttachmentUdt) -> Option<Attachment> {
    Some(Attachment {
        id: snowflake(udt.id),
        filename: AttachmentFilename::parse(&udt.filename).ok()?,
        content_type: udt.content_type,
        size: udt.size.max(0) as u64,
        width: udt.width.map(|w| w as u32),
        height: udt.height.map(|h| h as u32),
        description: udt.description,
        flags: udt.flags.unwrap_or(0).max(0) as u32,
    })
}

/// The snapshot is stored as one JSON document: it is only ever read and
/// written together with its message.
#[derive(serde::Serialize, serde::Deserialize)]
struct SnapshotJson {
    content: String,
    attachments: Vec<AttachmentJson>,
    embeds: Vec<String>,
    users: Vec<u64>,
    roles: Vec<u64>,
    flags: u32,
    created_at_ms: u64,
    edited_at_ms: Option<u64>,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct AttachmentJson {
    id: u64,
    filename: String,
    content_type: Option<String>,
    size: u64,
    width: Option<u32>,
    height: Option<u32>,
    description: Option<String>,
    flags: u32,
}

fn snapshot_json(snapshot: &MessageSnapshot) -> String {
    let document = SnapshotJson {
        content: snapshot.content.clone(),
        attachments: snapshot
            .attachments
            .iter()
            .map(|a| AttachmentJson {
                id: a.id.value(),
                filename: a.filename.as_str().to_owned(),
                content_type: a.content_type.clone(),
                size: a.size,
                width: a.width,
                height: a.height,
                description: a.description.clone(),
                flags: a.flags,
            })
            .collect(),
        embeds: snapshot.embeds.clone(),
        users: snapshot.mentions.users.iter().map(|u| u.value()).collect(),
        roles: snapshot.mentions.roles.iter().map(|r| r.value()).collect(),
        flags: snapshot.flags,
        created_at_ms: snapshot.created_at_ms,
        edited_at_ms: snapshot.edited_at_ms,
    };
    serde_json::to_string(&document).unwrap_or_default()
}

fn snapshot_from_json(raw: &str) -> Option<MessageSnapshot> {
    let document: SnapshotJson = serde_json::from_str(raw).ok()?;
    Some(MessageSnapshot {
        content: document.content,
        attachments: document
            .attachments
            .into_iter()
            .filter_map(|a| {
                Some(Attachment {
                    id: Snowflake::new(a.id),
                    filename: AttachmentFilename::parse(&a.filename).ok()?,
                    content_type: a.content_type,
                    size: a.size,
                    width: a.width,
                    height: a.height,
                    description: a.description,
                    flags: a.flags,
                })
            })
            .collect(),
        embeds: document.embeds,
        mentions: Mentions {
            users: document.users.into_iter().map(Snowflake::new).collect(),
            roles: document.roles.into_iter().map(Snowflake::new).collect(),
            everyone: false,
            here: false,
        },
        flags: document.flags,
        created_at_ms: document.created_at_ms,
        edited_at_ms: document.edited_at_ms,
    })
}

fn to_ids(values: Option<Vec<i64>>) -> BTreeSet<Snowflake> {
    values.unwrap_or_default().into_iter().map(snowflake).collect()
}

/// Embeds and components are stored as one JSON array in a text column.
fn json_array(items: &[String]) -> String {
    format!("[{}]", items.join(","))
}

fn split_json_array(raw: Option<&str>) -> Vec<String> {
    raw.and_then(|text| serde_json::from_str::<Vec<serde_json::Value>>(text).ok())
        .unwrap_or_default()
        .iter()
        .map(serde_json::Value::to_string)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use discordy_messages_domain::MessageDraft;

    #[test]
    fn messages_round_trip_through_rows() {
        let draft = MessageDraft {
            content: "hi <@7> <@&8>".into(),
            tts: true,
            attachments: vec![Attachment {
                id: Snowflake::new(3),
                filename: AttachmentFilename::parse("a.png").unwrap(),
                content_type: Some("image/png".into()),
                size: 10,
                width: Some(4),
                height: None,
                description: Some("alt".into()),
                flags: 8,
            }],
            embeds: vec![r#"{"title":"t"}"#.into()],
            components: vec![],
            reply_to: Some(Snowflake::new(99)),
            flags: 0,
            webhook: None,
            interaction: None,
            ..MessageDraft::default()
        };
        let mut message = Message::compose(
            Snowflake::new(1 << 40),
            Snowflake::new(5),
            Snowflake::new(6),
            draft,
            false,
        )
        .unwrap();
        message.edited_at_ms = Some(1_800_000_000_000);
        let row = MessageRow::from_message(&message);
        assert_eq!(row.bucket, Bucket::of(message.id).value());
        assert_eq!(row.into_message().unwrap(), message);
    }

    #[test]
    fn messages_written_before_tts_migration_default_to_false() {
        let message = Message::compose(
            Snowflake::new(1 << 40),
            Snowflake::new(5),
            Snowflake::new(6),
            MessageDraft {
                content: "old row".into(),
                ..MessageDraft::default()
            },
            false,
        )
        .unwrap();
        let mut row = MessageRow::from_message(&message);
        row.tts = None;
        assert!(!row.into_message().unwrap().tts);
    }

    #[test]
    fn webhook_and_interaction_origins_round_trip() {
        let draft = MessageDraft {
            content: "relayed".into(),
            webhook: Some(WebhookAuthor {
                webhook_id: Snowflake::new(40),
                username: Some("Relay".into()),
                avatar_url: Some("https://cdn.test/a.png".into()),
            }),
            interaction: Some(InteractionLink {
                interaction_id: Snowflake::new(41),
                application_id: Snowflake::new(42),
            }),
            flags: 4,
            ..MessageDraft::default()
        };
        let message = Message::compose(
            Snowflake::new(1 << 40),
            Snowflake::new(5),
            Snowflake::new(40),
            draft,
            false,
        )
        .unwrap();
        assert_eq!(MessageRow::from_message(&message).into_message().unwrap(), message);
    }

    #[test]
    fn forwarded_messages_round_trip_through_rows() {
        let snapshot = MessageSnapshot {
            content: "original <@7>".into(),
            attachments: vec![Attachment {
                id: Snowflake::new(31),
                filename: AttachmentFilename::parse("cat.png").unwrap(),
                content_type: Some("image/png".into()),
                size: 10,
                width: Some(4),
                height: Some(5),
                description: Some("alt".into()),
                flags: 8,
            }],
            embeds: vec![r#"{"title":"t"}"#.into()],
            mentions: Mentions {
                users: [Snowflake::new(7)].into(),
                roles: [Snowflake::new(8)].into(),
                everyone: false,
                here: false,
            },
            flags: 4,
            created_at_ms: 1_700_000_000_000,
            edited_at_ms: Some(1_700_000_001_000),
        };
        let draft = MessageDraft {
            forward: Some(ForwardOrigin {
                channel_id: Snowflake::new(2),
                message_id: Snowflake::new(3),
                guild_id: Some(Snowflake::new(4)),
                snapshot,
            }),
            ..MessageDraft::default()
        };
        let message = Message::compose(
            Snowflake::new(1 << 40),
            Snowflake::new(5),
            Snowflake::new(6),
            draft,
            false,
        )
        .unwrap();
        let row = MessageRow::from_message(&message);
        assert_eq!(row.forward_channel_id, Some(2));
        assert_eq!(row.into_message().unwrap(), message);
    }

    #[test]
    fn here_mentions_round_trip_through_rows() {
        let message = Message::compose(
            Snowflake::new(1 << 40),
            Snowflake::new(5),
            Snowflake::new(6),
            MessageDraft {
                content: "@here hi".into(),
                ..MessageDraft::default()
            },
            true,
        )
        .unwrap();
        assert!(message.mentions.here);
        assert_eq!(MessageRow::from_message(&message).into_message().unwrap(), message);
    }

    #[test]
    fn call_and_thread_messages_round_trip_through_rows() {
        let compose = |kind, draft: MessageDraft| {
            Message::compose(
                Snowflake::new(1 << 40),
                Snowflake::new(5),
                Snowflake::new(6),
                MessageDraft {
                    system_kind: Some(kind),
                    ..draft
                },
                false,
            )
            .unwrap()
        };
        let mut call = compose(MessageKind::Call, MessageDraft::default());
        call.update_call(
            Some(1_700_000_000_000),
            Some(vec![Snowflake::new(6), Snowflake::new(7)]),
        )
        .unwrap();
        assert_eq!(MessageRow::from_message(&call).into_message().unwrap(), call);
        let running = compose(MessageKind::Call, MessageDraft::default());
        assert_eq!(MessageRow::from_message(&running).into_message().unwrap(), running);
        let thread = compose(
            MessageKind::ThreadCreated,
            MessageDraft {
                content: "plans".into(),
                thread_id: Some(Snowflake::new(77)),
                ..MessageDraft::default()
            },
        );
        assert_eq!(MessageRow::from_message(&thread).into_message().unwrap(), thread);
    }

    #[test]
    fn the_column_list_matches_the_placeholder_count() {
        assert_eq!(MESSAGE_COLUMNS.split(',').count(), MESSAGE_COLUMN_COUNT);
    }

    #[test]
    fn rows_without_an_author_are_ghosts() {
        let message = Message::compose(
            Snowflake::new(1 << 40),
            Snowflake::new(5),
            Snowflake::new(6),
            MessageDraft {
                content: "x".into(),
                ..MessageDraft::default()
            },
            false,
        )
        .unwrap();
        let mut row = MessageRow::from_message(&message);
        row.author_id = None;
        assert!(row.into_message().is_none());
    }
}

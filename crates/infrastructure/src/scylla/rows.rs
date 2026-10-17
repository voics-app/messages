//! Mapping between domain objects and ScyllaDB rows.
//!
//! CQL `bigint` is signed; Snowflakes are below 2^63 so the casts are
//! lossless. Empty CQL collections come back as NULL, hence the `Option`s.

use std::collections::BTreeSet;

use discordy_messages_domain::{
    Attachment, AttachmentFilename, Bucket, Mentions, Message, MessageKind, ReadState, Snowflake,
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
    attachments: Option<Vec<AttachmentUdt>>,
    embeds: Option<String>,
    components: Option<String>,
    mentions: Option<Vec<i64>>,
    mention_roles: Option<Vec<i64>>,
    mention_everyone: Option<bool>,
    reply_to: Option<i64>,
    flags: Option<i32>,
    edited_at: Option<CqlTimestamp>,
    pinned: Option<bool>,
}

/// The column list matching [`MessageRow`], for SELECT and INSERT.
pub(crate) const MESSAGE_COLUMNS: &str = "channel_id, bucket, message_id, author_id, type, content, attachments, \
     embeds, components, mentions, mention_roles, mention_everyone, reply_to, flags, edited_at, pinned";

impl MessageRow {
    pub(crate) fn from_message(message: &Message) -> Self {
        Self {
            channel_id: id(message.channel_id),
            bucket: Bucket::of(message.id).value(),
            message_id: id(message.id),
            author_id: Some(id(message.author_id)),
            kind: Some(i32::from(message.kind.code())),
            content: Some(message.content.clone()),
            attachments: Some(message.attachments.iter().map(attachment_udt).collect()),
            embeds: Some(json_array(&message.embeds)),
            components: Some(json_array(&message.components)),
            mentions: Some(message.mentions.users.iter().copied().map(id).collect()),
            mention_roles: Some(message.mentions.roles.iter().copied().map(id).collect()),
            mention_everyone: Some(message.mentions.everyone),
            reply_to: message.reply_to.map(id),
            flags: Some(message.flags as i32),
            edited_at: message.edited_at_ms.map(|ms| CqlTimestamp(ms as i64)),
            pinned: Some(message.pinned),
        }
    }

    /// `None` for partial rows (an update that raced a delete leaves a row
    /// without an author); such rows are treated as deleted.
    pub(crate) fn into_message(self) -> Option<Message> {
        let author_id = self.author_id?;
        Some(Message {
            id: snowflake(self.message_id),
            channel_id: snowflake(self.channel_id),
            author_id: snowflake(author_id),
            kind: MessageKind::from_code(self.kind.unwrap_or(0) as u8),
            content: self.content.unwrap_or_default(),
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
            },
            reply_to: self.reply_to.map(snowflake),
            flags: self.flags.unwrap_or(0) as u32,
            edited_at_ms: self.edited_at.map(|t| t.0 as u64),
            pinned: self.pinned.unwrap_or(false),
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
            user_id: snowflake(self.user_id),
            channel_id: snowflake(self.channel_id),
            last_read_id: self.last_read_id.map(snowflake),
            mention_count: self.mention_count.unwrap_or(0).max(0) as u32,
        }
    }
}

fn attachment_udt(attachment: &Attachment) -> AttachmentUdt {
    AttachmentUdt {
        id: id(attachment.id),
        filename: attachment.filename.as_str().to_owned(),
        content_type: attachment.content_type.clone(),
        size: attachment.size as i64,
        width: attachment.width.map(|w| w as i32),
        height: attachment.height.map(|h| h as i32),
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
            attachments: vec![Attachment {
                id: Snowflake::new(3),
                filename: AttachmentFilename::parse("a.png").unwrap(),
                content_type: Some("image/png".into()),
                size: 10,
                width: Some(4),
                height: None,
            }],
            embeds: vec![r#"{"title":"t"}"#.into()],
            components: vec![],
            reply_to: Some(Snowflake::new(99)),
        };
        let mut message = Message::compose(Snowflake::new(1 << 40), Snowflake::new(5), Snowflake::new(6), draft, false).unwrap();
        message.edited_at_ms = Some(1_800_000_000_000);
        let row = MessageRow::from_message(&message);
        assert_eq!(row.bucket, Bucket::of(message.id).value());
        assert_eq!(row.into_message().unwrap(), message);
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

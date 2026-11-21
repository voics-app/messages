use std::sync::Arc;

use async_trait::async_trait;
use discordy_messages_application::PortError;
use discordy_messages_application::ports::{BucketQuery, MessageRepository};
use discordy_messages_domain::{Bucket, ChannelId, Message, MessageId, ScanDirection};
use scylla::client::session::Session;
use scylla::statement::prepared::PreparedStatement;
use scylla::statement::{Consistency, SerialConsistency};
use scylla::value::CqlTimestamp;
use scylla::value::{CqlValue, Row};

use super::rows::{MESSAGE_COLUMN_COUNT, MESSAGE_COLUMNS, MessageRow, attachment_udt, id, snowflake};
use super::{execute, fetch_all, prepare};

pub struct ScyllaMessageRepository {
    session: Arc<Session>,
    insert: PreparedStatement,
    advance_last: PreparedStatement,
    update_content: PreparedStatement,
    set_pinned: PreparedStatement,
    mark_crossposted: PreparedStatement,
    find: PreparedStatement,
    delete: PreparedStatement,
    scan_older: PreparedStatement,
    scan_older_unbounded: PreparedStatement,
    scan_newer: PreparedStatement,
    last: PreparedStatement,
    all_channels: PreparedStatement,
}

impl ScyllaMessageRepository {
    /// Scheduled delivery cannot transparently repeat a message insertion after an ambiguous result.
    pub async fn for_scheduled_delivery(session: Arc<Session>) -> Result<Self, PortError> {
        let mut repository = Self::new(session).await?;
        for statement in [&mut repository.insert, &mut repository.advance_last] {
            statement.set_is_idempotent(false);
            statement.set_retry_policy(Some(Arc::new(scylla::policies::retry::FallthroughRetryPolicy)));
        }
        Ok(repository)
    }

    pub async fn new(session: Arc<Session>) -> Result<Self, PortError> {
        let s = session.as_ref();
        let select = format!("SELECT {MESSAGE_COLUMNS} FROM messages WHERE channel_id = ? AND bucket = ?");
        Ok(Self {
            insert: prepare(
                s,
                &format!(
                    "INSERT INTO messages ({MESSAGE_COLUMNS}) VALUES ({})",
                    vec!["?"; MESSAGE_COLUMN_COUNT].join(", ")
                ),
            )
            .await?,
            advance_last: prepare(
                s,
                "UPDATE channel_last_message USING TIMESTAMP ? SET last_message_id = ? WHERE channel_id = ?",
            )
            .await?,
            update_content: prepare(
                s,
                "UPDATE messages SET content = ?, attachments = ?, embeds = ?, flags = ?, mentions = ?, mention_roles = ?, \
                 mention_everyone = ?, mention_here = ?, edited_at = ?, call_participants = ?, call_ended_at = ? WHERE channel_id = ? AND bucket = ? AND message_id = ?",
            )
            .await?,
            set_pinned: prepare(
                s,
                "UPDATE messages SET pinned = ? WHERE channel_id = ? AND bucket = ? AND message_id = ?",
            )
            .await?,
            mark_crossposted: {
                let mut statement = prepare(s, "UPDATE messages SET crossposted = true WHERE channel_id = ? AND bucket = ? AND message_id = ? IF EXISTS").await?;
                statement.set_consistency(Consistency::Quorum);
                statement.set_serial_consistency(Some(SerialConsistency::Serial));
                statement.set_is_idempotent(false);
                statement.set_retry_policy(Some(Arc::new(scylla::policies::retry::FallthroughRetryPolicy)));
                statement
            },
            find: prepare(s, &format!("{select} AND message_id = ?")).await?,
            delete: prepare(
                s,
                "DELETE FROM messages WHERE channel_id = ? AND bucket = ? AND message_id = ?",
            )
            .await?,
            scan_older: prepare(s, &format!("{select} AND message_id < ? LIMIT ?")).await?,
            scan_older_unbounded: prepare(s, &format!("{select} LIMIT ?")).await?,
            scan_newer: prepare(
                s,
                &format!("{select} AND message_id > ? ORDER BY message_id ASC LIMIT ?"),
            )
            .await?,
            last: prepare(
                s,
                "SELECT last_message_id FROM channel_last_message WHERE channel_id = ?",
            )
            .await?,
            all_channels: prepare(s, "SELECT channel_id, last_message_id FROM channel_last_message").await?,
            session,
        })
    }

    fn key(channel_id: ChannelId, message_id: MessageId) -> (i64, i32, i64) {
        (id(channel_id), Bucket::of(message_id).value(), id(message_id))
    }
}

#[async_trait]
impl MessageRepository for ScyllaMessageRepository {
    async fn mark_crossposted(&self, channel_id: ChannelId, message_id: MessageId) -> Result<bool, PortError> {
        let result = self
            .session
            .execute_unpaged(&self.mark_crossposted, Self::key(channel_id, message_id))
            .await
            .map_err(super::port_error)?;
        let rows = result.into_rows_result().map_err(super::port_error)?;
        let mut iter = rows.rows::<Row>().map_err(super::port_error)?;
        let row = iter
            .next()
            .ok_or_else(|| PortError::new("publication marker returned no status"))?
            .map_err(super::port_error)?;
        match row.columns.first() {
            Some(Some(CqlValue::Boolean(value))) => Ok(*value),
            _ => Err(PortError::new("publication marker omitted applied status")),
        }
    }
    async fn insert(&self, message: &Message) -> Result<(), PortError> {
        execute(&self.session, &self.insert, MessageRow::from_message(message)).await?;
        // The write timestamp orders by message id (creation millisecond, then
        // sequence), so concurrent inserts can never move the channel's last
        // message id backwards. ScyllaDB rejects timestamps that are not
        // microseconds close to now, hence the unix-microsecond encoding.
        let write_timestamp = (message.id.unix_millis() * 1000 + message.id.value() % 1000) as i64;
        execute(
            &self.session,
            &self.advance_last,
            (write_timestamp, id(message.id), id(message.channel_id)),
        )
        .await
    }

    async fn update(&self, message: &Message) -> Result<(), PortError> {
        let (channel, bucket, message_id) = Self::key(message.channel_id, message.id);
        execute(
            &self.session,
            &self.update_content,
            (
                &message.content,
                message.attachments.iter().map(attachment_udt).collect::<Vec<_>>(),
                format!("[{}]", message.embeds.join(",")),
                message.flags as i32,
                message.mentions.users.iter().copied().map(id).collect::<Vec<_>>(),
                message.mentions.roles.iter().copied().map(id).collect::<Vec<_>>(),
                message.mentions.everyone,
                message.mentions.here,
                message.edited_at_ms.map(|ms| CqlTimestamp(ms as i64)),
                message
                    .call
                    .as_ref()
                    .map(|c| c.participants.iter().copied().map(id).collect::<Vec<_>>()),
                message
                    .call
                    .as_ref()
                    .and_then(|c| c.ended_at_ms)
                    .map(|ms| CqlTimestamp(ms as i64)),
                channel,
                bucket,
                message_id,
            ),
        )
        .await
    }

    async fn find(&self, channel_id: ChannelId, message_id: MessageId) -> Result<Option<Message>, PortError> {
        let rows: Vec<MessageRow> = fetch_all(&self.session, &self.find, Self::key(channel_id, message_id)).await?;
        Ok(rows.into_iter().find_map(MessageRow::into_message))
    }

    async fn delete(&self, channel_id: ChannelId, message_ids: &[MessageId]) -> Result<(), PortError> {
        let deletes = message_ids
            .iter()
            .map(|message_id| execute(&self.session, &self.delete, Self::key(channel_id, *message_id)));
        futures::future::try_join_all(deletes).await.map(|_| ())
    }

    async fn set_pinned(&self, channel_id: ChannelId, message_id: MessageId, pinned: bool) -> Result<(), PortError> {
        let (channel, bucket, message) = Self::key(channel_id, message_id);
        execute(&self.session, &self.set_pinned, (pinned, channel, bucket, message)).await
    }

    async fn scan_bucket(&self, query: BucketQuery) -> Result<Vec<Message>, PortError> {
        let channel = id(query.channel_id);
        let bucket = query.bucket.value();
        let limit = i32::try_from(query.limit).unwrap_or(i32::MAX);
        let rows: Vec<MessageRow> = match (query.direction, query.bound) {
            (ScanDirection::Older, None) => {
                fetch_all(&self.session, &self.scan_older_unbounded, (channel, bucket, limit)).await?
            }
            (ScanDirection::Older, Some(bound)) => {
                fetch_all(&self.session, &self.scan_older, (channel, bucket, id(bound), limit)).await?
            }
            (ScanDirection::Newer, bound) => {
                let after = bound.map_or(0, id);
                fetch_all(&self.session, &self.scan_newer, (channel, bucket, after, limit)).await?
            }
        };
        Ok(rows.into_iter().filter_map(MessageRow::into_message).collect())
    }

    async fn last_message_id(&self, channel_id: ChannelId) -> Result<Option<MessageId>, PortError> {
        let rows: Vec<(Option<i64>,)> = fetch_all(&self.session, &self.last, (id(channel_id),)).await?;
        Ok(rows.into_iter().find_map(|(last,)| last).map(snowflake))
    }

    async fn channels_with_messages(&self) -> Result<Vec<(ChannelId, MessageId)>, PortError> {
        let rows: Vec<(i64, Option<i64>)> = fetch_all(&self.session, &self.all_channels, ()).await?;
        Ok(rows
            .into_iter()
            .filter_map(|(channel, last)| last.map(|last| (snowflake(channel), snowflake(last))))
            .collect())
    }
}

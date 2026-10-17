use std::sync::Arc;

use async_trait::async_trait;
use discordy_messages_application::PortError;
use discordy_messages_application::ports::ReactionRepository;
use discordy_messages_domain::{Bucket, ChannelId, Emoji, MessageId, ReactionRow, UserId};
use scylla::client::session::Session;
use scylla::statement::prepared::PreparedStatement;

use super::rows::{id, snowflake};
use super::{execute, fetch_all, prepare};

pub struct ScyllaReactionRepository {
    session: Arc<Session>,
    insert: PreparedStatement,
    delete_one: PreparedStatement,
    delete_all: PreparedStatement,
    for_message: PreparedStatement,
    for_range: PreparedStatement,
}

impl ScyllaReactionRepository {
    pub async fn new(session: Arc<Session>) -> Result<Self, PortError> {
        let s = session.as_ref();
        let select = "SELECT message_id, emoji, user_id FROM reactions WHERE channel_id = ? AND bucket = ?";
        Ok(Self {
            insert: prepare(
                s,
                "INSERT INTO reactions (channel_id, bucket, message_id, emoji, user_id) VALUES (?, ?, ?, ?, ?)",
            )
            .await?,
            delete_one: prepare(
                s,
                "DELETE FROM reactions WHERE channel_id = ? AND bucket = ? AND message_id = ? AND emoji = ? AND user_id = ?",
            )
            .await?,
            delete_all: prepare(
                s,
                "DELETE FROM reactions WHERE channel_id = ? AND bucket = ? AND message_id = ?",
            )
            .await?,
            for_message: prepare(s, &format!("{select} AND message_id = ?")).await?,
            for_range: prepare(s, &format!("{select} AND message_id >= ? AND message_id <= ?")).await?,
            session,
        })
    }
}

fn to_rows(raw: Vec<(i64, String, i64)>) -> Vec<ReactionRow> {
    raw.into_iter()
        .filter_map(|(message_id, emoji, user_id)| {
            Some(ReactionRow {
                message_id: snowflake(message_id),
                emoji: Emoji::parse(&emoji).ok()?,
                user_id: snowflake(user_id),
            })
        })
        .collect()
}

fn partition(channel_id: ChannelId, message_id: MessageId) -> (i64, i32, i64) {
    (id(channel_id), Bucket::of(message_id).value(), id(message_id))
}

#[async_trait]
impl ReactionRepository for ScyllaReactionRepository {
    async fn add(
        &self,
        channel_id: ChannelId,
        message_id: MessageId,
        emoji: &Emoji,
        user_id: UserId,
    ) -> Result<(), PortError> {
        let (c, b, m) = partition(channel_id, message_id);
        execute(&self.session, &self.insert, (c, b, m, emoji.key(), id(user_id))).await
    }

    async fn remove(
        &self,
        channel_id: ChannelId,
        message_id: MessageId,
        emoji: &Emoji,
        user_id: UserId,
    ) -> Result<(), PortError> {
        let (c, b, m) = partition(channel_id, message_id);
        execute(&self.session, &self.delete_one, (c, b, m, emoji.key(), id(user_id))).await
    }

    async fn remove_all(&self, channel_id: ChannelId, message_id: MessageId) -> Result<(), PortError> {
        execute(&self.session, &self.delete_all, partition(channel_id, message_id)).await
    }

    async fn for_message(&self, channel_id: ChannelId, message_id: MessageId) -> Result<Vec<ReactionRow>, PortError> {
        let raw = fetch_all(&self.session, &self.for_message, partition(channel_id, message_id)).await?;
        Ok(to_rows(raw))
    }

    async fn for_range(
        &self,
        channel_id: ChannelId,
        bucket: Bucket,
        oldest: MessageId,
        newest: MessageId,
    ) -> Result<Vec<ReactionRow>, PortError> {
        let raw = fetch_all(
            &self.session,
            &self.for_range,
            (id(channel_id), bucket.value(), id(oldest), id(newest)),
        )
        .await?;
        Ok(to_rows(raw))
    }
}

use std::sync::Arc;

use async_trait::async_trait;
use discordy_messages_application::PortError;
use discordy_messages_application::ports::ReadStateRepository;
use discordy_messages_domain::{ChannelId, ReadState, UserId};
use scylla::client::session::Session;
use scylla::statement::prepared::PreparedStatement;

use super::rows::{ReadStateRow, id};
use super::{execute, fetch_all, prepare};

pub struct ScyllaReadStateRepository {
    session: Arc<Session>,
    upsert: PreparedStatement,
    find: PreparedStatement,
    for_user: PreparedStatement,
}

impl ScyllaReadStateRepository {
    pub async fn new(session: Arc<Session>) -> Result<Self, PortError> {
        let s = session.as_ref();
        let select = "SELECT user_id, channel_id, last_read_id, mention_count FROM read_states WHERE user_id = ?";
        Ok(Self {
            upsert: prepare(
                s,
                "INSERT INTO read_states (user_id, channel_id, last_read_id, mention_count) VALUES (?, ?, ?, ?)",
            )
            .await?,
            find: prepare(s, &format!("{select} AND channel_id = ?")).await?,
            for_user: prepare(s, select).await?,
            session,
        })
    }
}

#[async_trait]
impl ReadStateRepository for ScyllaReadStateRepository {
    async fn find(&self, user_id: UserId, channel_id: ChannelId) -> Result<Option<ReadState>, PortError> {
        let rows: Vec<ReadStateRow> = fetch_all(&self.session, &self.find, (id(user_id), id(channel_id))).await?;
        Ok(rows.into_iter().next().map(ReadStateRow::into_read_state))
    }

    async fn save(&self, state: &ReadState) -> Result<(), PortError> {
        execute(
            &self.session,
            &self.upsert,
            (
                id(state.user_id),
                id(state.channel_id),
                state.last_read_id.map(id),
                i32::try_from(state.mention_count).unwrap_or(i32::MAX),
            ),
        )
        .await
    }

    async fn for_user(&self, user_id: UserId) -> Result<Vec<ReadState>, PortError> {
        let rows: Vec<ReadStateRow> = fetch_all(&self.session, &self.for_user, (id(user_id),)).await?;
        Ok(rows.into_iter().map(ReadStateRow::into_read_state).collect())
    }
}

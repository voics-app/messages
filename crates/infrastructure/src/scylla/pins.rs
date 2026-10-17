use std::sync::Arc;

use async_trait::async_trait;
use discordy_messages_application::PortError;
use discordy_messages_application::ports::{Pin, PinRepository};
use discordy_messages_domain::{ChannelId, MessageId};
use scylla::client::session::Session;
use scylla::statement::prepared::PreparedStatement;
use scylla::value::CqlTimestamp;

use super::rows::{id, snowflake};
use super::{execute, fetch_all, prepare};

pub struct ScyllaPinRepository {
    session: Arc<Session>,
    insert: PreparedStatement,
    delete: PreparedStatement,
    list: PreparedStatement,
}

impl ScyllaPinRepository {
    pub async fn new(session: Arc<Session>) -> Result<Self, PortError> {
        let s = session.as_ref();
        Ok(Self {
            insert: prepare(
                s,
                "INSERT INTO pins (channel_id, message_id, pinned_at) VALUES (?, ?, ?)",
            )
            .await?,
            delete: prepare(s, "DELETE FROM pins WHERE channel_id = ? AND message_id = ?").await?,
            list: prepare(s, "SELECT message_id, pinned_at FROM pins WHERE channel_id = ?").await?,
            session,
        })
    }
}

#[async_trait]
impl PinRepository for ScyllaPinRepository {
    async fn pin(&self, channel_id: ChannelId, pin: Pin) -> Result<(), PortError> {
        execute(
            &self.session,
            &self.insert,
            (
                id(channel_id),
                id(pin.message_id),
                CqlTimestamp(pin.pinned_at_ms as i64),
            ),
        )
        .await
    }

    async fn unpin(&self, channel_id: ChannelId, message_id: MessageId) -> Result<(), PortError> {
        execute(&self.session, &self.delete, (id(channel_id), id(message_id))).await
    }

    async fn list(&self, channel_id: ChannelId) -> Result<Vec<Pin>, PortError> {
        let rows: Vec<(i64, Option<CqlTimestamp>)> = fetch_all(&self.session, &self.list, (id(channel_id),)).await?;
        let mut pins: Vec<Pin> = rows
            .into_iter()
            .map(|(message_id, pinned_at)| Pin {
                message_id: snowflake(message_id),
                pinned_at_ms: pinned_at.map_or(0, |t| t.0 as u64),
            })
            .collect();
        pins.sort_by_key(|p| std::cmp::Reverse((p.pinned_at_ms, p.message_id)));
        Ok(pins)
    }
}

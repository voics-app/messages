use std::sync::Arc;

use async_trait::async_trait;
use discordy_messages_application::PortError;
use discordy_messages_application::ports::ActivityRepository;
use discordy_messages_domain::{MessageId, Snowflake};
use scylla::client::session::Session;
use scylla::statement::prepared::PreparedStatement;

use super::rows::id;
use super::{execute, fetch_all, prepare};

const HOUR_MS: u64 = 3_600_000;
/// Hour partitions to scan for a 24-hour window (24 plus the partial one).
const WINDOW_HOURS: u64 = 25;

/// Records message ids in hourly partitions (with a TTL) and counts them.
pub struct ScyllaActivityRepository {
    session: Arc<Session>,
    insert: PreparedStatement,
    count: PreparedStatement,
}

impl ScyllaActivityRepository {
    pub async fn new(session: Arc<Session>) -> Result<Self, PortError> {
        let s = session.as_ref();
        Ok(Self {
            insert: prepare(s, "INSERT INTO message_activity (hour, message_id) VALUES (?, ?)").await?,
            count: prepare(
                s,
                "SELECT COUNT(*) FROM message_activity WHERE hour = ? AND message_id >= ?",
            )
            .await?,
            session,
        })
    }

    async fn count_hour(&self, hour: u64, lower_bound: Snowflake) -> Result<u64, PortError> {
        let rows: Vec<(i64,)> = fetch_all(&self.session, &self.count, (hour as i64, id(lower_bound))).await?;
        Ok(rows.first().map_or(0, |(n,)| (*n).max(0) as u64))
    }
}

#[async_trait]
impl ActivityRepository for ScyllaActivityRepository {
    async fn record_standard_message(&self, message_id: MessageId) -> Result<(), PortError> {
        let hour = message_id.unix_millis() / HOUR_MS;
        execute(&self.session, &self.insert, (hour as i64, id(message_id))).await
    }

    async fn count_standard_since(&self, since_unix_ms: u64) -> Result<u64, PortError> {
        let lower_bound = Snowflake::first_at_unix_millis(since_unix_ms);
        let first_hour = since_unix_ms / HOUR_MS;
        let counts = (first_hour..first_hour + WINDOW_HOURS).map(|hour| self.count_hour(hour, lower_bound));
        Ok(futures::future::try_join_all(counts).await?.into_iter().sum())
    }
}

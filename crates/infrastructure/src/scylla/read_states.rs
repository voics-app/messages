//! Metadata, delivery ledger and current-generation contributions share one
//! user/channel partition. All authoritative mutations use conditional batches.

mod admission;
mod schema;

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use discordy_messages_application::PortError;
use discordy_messages_application::ports::ReadStateRepository;
use discordy_messages_domain::{ChannelId, MessageId, ReadState, UserId};
use futures::{StreamExt, TryStreamExt};
use scylla::DeserializeRow;
use scylla::client::session::Session;
use scylla::response::query_result::QueryResult;
use scylla::statement::batch::Batch;
use scylla::statement::prepared::PreparedStatement;
use scylla::statement::{Consistency, SerialConsistency};
use scylla::value::{CqlValue, Row};
use uuid::Uuid;

use super::rows::{ReadStateRow, id};
use super::{execute, fetch_all, port_error, prepare};

const MAX_ATTEMPTS: usize = 64;
const NIL: Uuid = Uuid::nil();

fn checked_id(value: MessageId) -> Result<(), PortError> {
    if value.value() > i64::MAX as u64 {
        return Err(PortError::new("read-state ID exceeds the signed database range"));
    }
    Ok(())
}

#[derive(Clone, DeserializeRow)]
struct Metadata {
    revision: Uuid,
    active_generation: Uuid,
    last_read_id: Option<i64>,
    acknowledged_through: Option<i64>,
    baseline: i64,
    mention_count: i64,
    read_state_version: Option<i64>,
}

type UpdateValues = (
    Uuid,
    Uuid,
    Option<i64>,
    Option<i64>,
    i64,
    i64,
    i64,
    i64,
    i64,
    Uuid,
    Uuid,
);

impl Metadata {
    fn from_legacy(legacy: ReadState) -> Self {
        Self {
            revision: Uuid::new_v4(),
            active_generation: Uuid::new_v4(),
            last_read_id: legacy.last_read_id.map(id),
            acknowledged_through: legacy.last_read_id.map(id),
            baseline: i64::from(legacy.mention_count),
            mention_count: i64::from(legacy.mention_count),
            read_state_version: Some(0),
        }
    }

    fn validate(&self) -> Result<(), PortError> {
        if !(0..=i64::from(u32::MAX)).contains(&self.mention_count)
            || !(0..=self.mention_count).contains(&self.baseline)
            || self.last_read_id.is_some_and(|last| last < 0)
            || self.acknowledged_through.is_some_and(|last| last < 0)
            || self.acknowledged_through < self.last_read_id
            || self.read_state_version.is_some_and(|version| version < 0)
        {
            return Err(PortError::new("stored read-state metadata violates its invariants"));
        }
        Ok(())
    }

    fn state(&self, user_id: UserId, channel_id: ChannelId) -> ReadState {
        ReadState {
            user_id,
            channel_id,
            last_read_id: self.last_read_id.map(|value| MessageId::new(value as u64)),
            mention_count: self.mention_count as u32,
            version: self.read_state_version.unwrap_or(0) as u64,
        }
    }

    fn values(&self, user: UserId, channel: ChannelId, previous: Uuid) -> UpdateValues {
        (
            self.revision,
            self.active_generation,
            self.last_read_id,
            self.acknowledged_through,
            self.baseline,
            self.mention_count,
            self.read_state_version.unwrap_or(0),
            id(user),
            id(channel),
            NIL,
            previous,
        )
    }

    fn advance(&mut self) -> Result<(), PortError> {
        self.read_state_version = Some(
            self.read_state_version
                .unwrap_or(0)
                .checked_add(1)
                .ok_or_else(|| PortError::new("read-state version exhausted"))?,
        );
        self.revision = Uuid::new_v4();
        Ok(())
    }
}

fn applied(result: QueryResult) -> Result<bool, PortError> {
    let rows = result.into_rows_result().map_err(port_error)?;
    let mut result = None;
    for row in rows.rows::<Row>().map_err(port_error)? {
        match row.map_err(port_error)?.columns.first() {
            Some(Some(CqlValue::Boolean(value))) => result = Some(result.unwrap_or(true) && *value),
            _ => return Err(PortError::new("conditional read-state write omitted applied status")),
        }
    }
    result.ok_or_else(|| PortError::new("conditional read-state write returned no status"))
}

async fn quorum_read(session: &Session, cql: &str) -> Result<PreparedStatement, PortError> {
    let mut statement = prepare(session, cql).await?;
    statement.set_consistency(Consistency::Quorum);
    Ok(statement)
}

async fn conditional_batch(session: &Session, statements: &[String]) -> Result<Batch, PortError> {
    let mut batch = Batch::default();
    batch.set_consistency(Consistency::Quorum);
    batch.set_serial_consistency(Some(SerialConsistency::Serial));
    for statement in statements {
        batch.append_statement(statement.as_str());
    }
    session.prepare_batch(&batch).await.map_err(port_error)
}

pub struct ScyllaReadStateRepository {
    session: Arc<Session>,
    admission: admission::PartitionAdmission,
    index: PreparedStatement,
    legacy_find: PreparedStatement,
    legacy_for_user: PreparedStatement,
    metadata: PreparedStatement,
    initialize: PreparedStatement,
    seen: PreparedStatement,
    remaining: PreparedStatement,
    record: Batch,
    acknowledge: Batch,
    manual: Batch,
}

impl ScyllaReadStateRepository {
    pub async fn new(session: Arc<Session>) -> Result<Self, PortError> {
        Self::open(session, None).await
    }

    /// Probe the legacy discovery table and the authoritative companion table
    /// at the same quorum used by the writer, without creating user state.
    pub async fn check_ready(&self) -> Result<(), PortError> {
        self.session
            .execute_unpaged(&self.legacy_find, (0_i64, 0_i64))
            .await
            .map_err(port_error)?;
        self.session
            .execute_unpaged(&self.metadata, (0_i64, 0_i64, NIL))
            .await
            .map_err(port_error)?;
        Ok(())
    }

    /// Creates a companion vnode schema, preserving the bound main keyspace.
    pub async fn open(session: Arc<Session>, keyspace: Option<&str>) -> Result<Self, PortError> {
        let table = schema::bootstrap(&session, keyspace).await?;
        let s = session.as_ref();
        let legacy = "SELECT user_id, channel_id, last_read_id, mention_count FROM read_states WHERE user_id = ?";
        let mut index = prepare(s, "INSERT INTO read_states (user_id, channel_id) VALUES (?, ?)").await?;
        index.set_consistency(Consistency::Quorum);
        let mut legacy_find = prepare(s, &format!("{legacy} AND channel_id = ?")).await?;
        legacy_find.set_consistency(Consistency::Quorum);
        let mut legacy_for_user = prepare(s, legacy).await?;
        legacy_for_user.set_consistency(Consistency::Quorum);
        let metadata = quorum_read(s, &format!("SELECT revision, active_generation, last_read_id, acknowledged_through, baseline, mention_count, read_state_version FROM {table} WHERE user_id = ? AND channel_id = ? AND kind = 0 AND generation = ? AND message_id = 0")).await?;
        let mut initialize = prepare(s, &format!("INSERT INTO {table} (user_id, channel_id, kind, generation, message_id, revision, active_generation, last_read_id, acknowledged_through, baseline, mention_count, read_state_version) VALUES (?, ?, 0, ?, 0, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS")).await?;
        initialize.set_consistency(Consistency::Quorum);
        initialize.set_serial_consistency(Some(SerialConsistency::Serial));
        let seen = quorum_read(s, &format!("SELECT message_id FROM {table} WHERE user_id = ? AND channel_id = ? AND kind = 1 AND generation = ? AND message_id = ?")).await?;
        let remaining = quorum_read(s, &format!("SELECT count(*) FROM {table} WHERE user_id = ? AND channel_id = ? AND kind = 2 AND generation = ? AND message_id > ?")).await?;
        let update = format!(
            "UPDATE {table} SET revision = ?, active_generation = ?, last_read_id = ?, acknowledged_through = ?, baseline = ?, mention_count = ?, read_state_version = ? WHERE user_id = ? AND channel_id = ? AND kind = 0 AND generation = ? AND message_id = 0 IF revision = ?"
        );
        let forget = format!(
            "DELETE FROM {table} WHERE user_id = ? AND channel_id = ? AND kind = 1 AND generation = ? AND message_id <= ?"
        );
        let record = conditional_batch(s, &[
            update.clone(),
            format!("INSERT INTO {table} (user_id, channel_id, kind, generation, message_id) VALUES (?, ?, 1, ?, ?) IF NOT EXISTS"),
            format!("INSERT INTO {table} (user_id, channel_id, kind, generation, message_id) VALUES (?, ?, 2, ?, ?)"),
        ]).await?;
        let acknowledge = conditional_batch(s, &[
            update.clone(), forget.clone(),
            format!("DELETE FROM {table} WHERE user_id = ? AND channel_id = ? AND kind = 2 AND generation = ? AND message_id <= ?"),
        ]).await?;
        let manual = conditional_batch(
            s,
            &[
                update,
                forget,
                format!("DELETE FROM {table} WHERE user_id = ? AND channel_id = ? AND kind = 2 AND generation = ?"),
            ],
        )
        .await?;
        Ok(Self {
            session,
            admission: admission::PartitionAdmission::new(),
            index,
            legacy_find,
            legacy_for_user,
            metadata,
            initialize,
            seen,
            remaining,
            record,
            acknowledge,
            manual,
        })
    }

    async fn metadata(&self, user: UserId, channel: ChannelId) -> Result<Option<Metadata>, PortError> {
        checked_id(user)?;
        checked_id(channel)?;
        let rows = self
            .session
            .execute_unpaged(&self.metadata, (id(user), id(channel), NIL))
            .await
            .map_err(port_error)?
            .into_rows_result()
            .map_err(port_error)?;
        let value = rows
            .rows::<Metadata>()
            .map_err(port_error)?
            .next()
            .transpose()
            .map_err(port_error)?;
        if let Some(value) = &value {
            value.validate()?;
        }
        Ok(value)
    }

    async fn legacy(&self, user: UserId, channel: ChannelId) -> Result<Option<ReadState>, PortError> {
        let rows: Vec<ReadStateRow> = fetch_all(&self.session, &self.legacy_find, (id(user), id(channel))).await?;
        Ok(rows.into_iter().next().map(ReadStateRow::into_read_state))
    }

    async fn ensure(&self, user: UserId, channel: ChannelId) -> Result<Metadata, PortError> {
        if let Some(value) = self.metadata(user, channel).await? {
            return Ok(value);
        }
        // Write discovery first. An interrupted initialization can leave an empty
        // index row, never an undiscoverable count. Keys preserve legacy columns.
        execute(&self.session, &self.index, (id(user), id(channel))).await?;
        let old = self
            .legacy(user, channel)
            .await?
            .unwrap_or_else(|| ReadState::unread(user, channel));
        let value = Metadata::from_legacy(old);
        value.validate()?;
        if applied(
            self.session
                .execute_unpaged(
                    &self.initialize,
                    (
                        id(user),
                        id(channel),
                        NIL,
                        value.revision,
                        value.active_generation,
                        value.last_read_id,
                        value.acknowledged_through,
                        value.baseline,
                        value.mention_count,
                        value.read_state_version.unwrap_or(0),
                    ),
                )
                .await
                .map_err(port_error)?,
        )? {
            return Ok(value);
        }
        self.metadata(user, channel)
            .await?
            .ok_or_else(|| PortError::new("read-state initialization was not visible after its transaction"))
    }

    async fn was_seen(&self, user: UserId, channel: ChannelId, message: MessageId) -> Result<bool, PortError> {
        Ok(self
            .session
            .execute_unpaged(&self.seen, (id(user), id(channel), NIL, id(message)))
            .await
            .map_err(port_error)?
            .into_rows_result()
            .map_err(port_error)?
            .rows_num()
            > 0)
    }

    async fn remaining(
        &self,
        user: UserId,
        channel: ChannelId,
        generation: Uuid,
        marker: MessageId,
    ) -> Result<i64, PortError> {
        let count = self
            .session
            .execute_unpaged(&self.remaining, (id(user), id(channel), generation, id(marker)))
            .await
            .map_err(port_error)?
            .into_rows_result()
            .map_err(port_error)?
            .single_row::<(i64,)>()
            .map_err(port_error)?
            .0;
        Ok(count.clamp(0, i64::from(u32::MAX)))
    }

    async fn change_position(
        &self,
        user: UserId,
        channel: ChannelId,
        marker: MessageId,
        manual: Option<u32>,
    ) -> Result<ReadState, PortError> {
        checked_id(marker)?;
        let _admission = self.admission.acquire(user, channel).await;
        for attempt in 0..MAX_ATTEMPTS {
            let old = self.ensure(user, channel).await?;
            if manual.is_none() && old.last_read_id.is_some_and(|last| id(marker) <= last) {
                return Ok(old.state(user, channel));
            }
            let mut next = old.clone();
            next.advance()?;
            next.last_read_id = Some(id(marker));
            let watermark = old.acknowledged_through.map_or(id(marker), |last| last.max(id(marker)));
            next.acknowledged_through = Some(watermark);
            let committed = if let Some(count) = manual {
                next.active_generation = Uuid::new_v4();
                next.baseline = i64::from(count);
                next.mention_count = i64::from(count);
                applied(
                    self.session
                        .batch(
                            &self.manual,
                            (
                                next.values(user, channel, old.revision),
                                (id(user), id(channel), NIL, watermark),
                                (id(user), id(channel), old.active_generation),
                            ),
                        )
                        .await
                        .map_err(port_error)?,
                )?
            } else {
                next.baseline = 0;
                next.mention_count = self.remaining(user, channel, old.active_generation, marker).await?;
                applied(
                    self.session
                        .batch(
                            &self.acknowledge,
                            (
                                next.values(user, channel, old.revision),
                                (id(user), id(channel), NIL, watermark),
                                (id(user), id(channel), old.active_generation, id(marker)),
                            ),
                        )
                        .await
                        .map_err(port_error)?,
                )?
            };
            if committed {
                return Ok(next.state(user, channel));
            }
            backoff(attempt).await;
        }
        contention()
    }
}

async fn backoff(attempt: usize) {
    metrics::counter!("read_state_cas_conflicts_total").increment(1);
    // Fixed short delays make replicas collide repeatedly on a hot partition.
    // Full jitter gives an older revision another chance without unbounded retries.
    let ceiling = 1_u128 << (attempt + 1).min(7);
    let millis = 1 + Uuid::new_v4().as_u128() % ceiling;
    tokio::time::sleep(Duration::from_millis(millis as u64)).await;
}

fn contention<T>() -> Result<T, PortError> {
    metrics::counter!("read_state_cas_exhausted_total").increment(1);
    Err(PortError::new("read-state contention exceeded the retry bound"))
}

#[async_trait]
impl ReadStateRepository for ScyllaReadStateRepository {
    async fn find(&self, user: UserId, channel: ChannelId) -> Result<Option<ReadState>, PortError> {
        match self.metadata(user, channel).await? {
            Some(value) => Ok(Some(value.state(user, channel))),
            None => self.legacy(user, channel).await,
        }
    }

    async fn record_mention(
        &self,
        user: UserId,
        channel: ChannelId,
        message: MessageId,
    ) -> Result<ReadState, PortError> {
        checked_id(message)?;
        let _admission = self.admission.acquire(user, channel).await;
        for attempt in 0..MAX_ATTEMPTS {
            let old = self.ensure(user, channel).await?;
            if old.acknowledged_through.is_some_and(|last| id(message) <= last)
                || self.was_seen(user, channel, message).await?
            {
                return Ok(old.state(user, channel));
            }
            let mut next = old.clone();
            next.advance()?;
            next.mention_count = (next.mention_count + 1).min(i64::from(u32::MAX));
            if applied(
                self.session
                    .batch(
                        &self.record,
                        (
                            next.values(user, channel, old.revision),
                            (id(user), id(channel), NIL, id(message)),
                            (id(user), id(channel), old.active_generation, id(message)),
                        ),
                    )
                    .await
                    .map_err(port_error)?,
            )? {
                return Ok(next.state(user, channel));
            }
            backoff(attempt).await;
        }
        contention()
    }

    async fn acknowledge(&self, user: UserId, channel: ChannelId, marker: MessageId) -> Result<ReadState, PortError> {
        self.change_position(user, channel, marker, None).await
    }

    async fn set_position(
        &self,
        user: UserId,
        channel: ChannelId,
        marker: MessageId,
        count: u32,
    ) -> Result<ReadState, PortError> {
        self.change_position(user, channel, marker, Some(count)).await
    }

    async fn for_user(&self, user: UserId) -> Result<Vec<ReadState>, PortError> {
        checked_id(user)?;
        let rows: Vec<ReadStateRow> = fetch_all(&self.session, &self.legacy_for_user, (id(user),)).await?;
        let mut result: Vec<ReadState> = futures::stream::iter(rows)
            .map(|row| async move {
                let legacy = row.into_read_state();
                Ok::<_, PortError>(
                    self.metadata(user, legacy.channel_id)
                        .await?
                        .map_or(legacy.clone(), |value| value.state(user, legacy.channel_id)),
                )
            })
            .buffer_unordered(16)
            .try_collect()
            .await?;
        result.sort_by_key(|state| state.channel_id);
        Ok(result)
    }
}

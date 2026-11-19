//! Same-partition conditional voting and an independently bounded due index.
mod admission;
mod readiness;
mod rows;
mod schema;
mod statements;

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use discordy_messages_application::PortError;
use discordy_messages_application::ports::{
    Clock, PollDueEntry, PollKey, PollRepository, PollSnapshotState, PollStoreError, PollStoreResult, VoteCommand,
};
use discordy_messages_domain::poll::{PollAggregate, PollDefinition, PollError, PollStatus, PollTransition};
use discordy_messages_domain::{Snowflake, UserId};
use scylla::client::session::Session;
use scylla::errors::{DbError, ExecutionError, RequestAttemptError};

use super::{execute, port_error};
use statements::{Statements, applied};

const MAX_ATTEMPTS: usize = 64;

pub struct ScyllaPollRepository {
    admission: admission::Admission,
    session: Arc<Session>,
    clock: Arc<dyn Clock>,
    statements: Statements,
}

fn key_values(key: PollKey) -> PollStoreResult<(i64, i64)> {
    if key.channel_id.value() == 0 || key.message_id.value() == 0 {
        return Err(PollError::Corrupt.into());
    }
    Ok((
        i64::try_from(key.channel_id.value()).map_err(|_| PollError::Corrupt)?,
        i64::try_from(key.message_id.value()).map_err(|_| PollError::Corrupt)?,
    ))
}

fn exhausted() -> PollStoreError {
    metrics::counter!("poll_cas_exhausted_total").increment(1);
    PortError::new("poll contention exceeded the bounded retry budget").into()
}

// Retrying a read cannot replay an uncertain conditional mutation. Keep both the
// number of retries and the total time bounded under a sustained outage.
async fn retry_serial_read<T, E, F, Fut>(mut read: F, retryable: impl Fn(&E) -> bool) -> Result<T, Option<E>>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    let operation = async {
        for attempt in 0..3 {
            match read().await {
                Ok(value) => return Ok(value),
                Err(error) if attempt < 2 && retryable(&error) => {
                    metrics::counter!("poll_serial_read_retries_total").increment(1);
                    tokio::time::sleep(Duration::from_millis(25 * (attempt + 1))).await;
                }
                Err(error) => return Err(Some(error)),
            }
        }
        unreachable!("the final attempt always returns")
    };
    tokio::time::timeout(Duration::from_secs(15), operation)
        .await
        .map_err(|_| None)?
}

fn serial_read_timed_out(error: &ExecutionError) -> bool {
    matches!(
        error,
        ExecutionError::LastAttemptError(RequestAttemptError::DbError(DbError::ReadTimeout { .. }, _))
    )
}

async fn conflict(attempt: usize, salt: u64) {
    metrics::counter!("poll_cas_conflicts_total").increment(1);
    tokio::time::sleep(Duration::from_millis(1 + (attempt as u64 + salt) % 8)).await;
}

impl ScyllaPollRepository {
    pub async fn new(session: Arc<Session>, clock: Arc<dyn Clock>, keyspace: Option<&str>) -> Result<Self, PortError> {
        let name = schema::bootstrap(&session, keyspace).await?;
        let statements = Statements::prepare(&session, &name).await?;
        Ok(Self {
            admission: admission::Admission::new(),
            session,
            clock,
            statements,
        })
    }

    fn now(&self) -> PollStoreResult<i64> {
        i64::try_from(self.clock.now_unix_ms()).map_err(|_| PollError::Exhausted.into())
    }

    async fn read(&self, key: PollKey, viewer: i64) -> PollStoreResult<Option<PollSnapshotState>> {
        let (channel, message) = key_values(key)?;
        let result = retry_serial_read(
            || {
                self.session
                    .execute_unpaged(&self.statements.read, (channel, message, viewer))
            },
            serial_read_timed_out,
        )
        .await
        .map_err(|error| match error {
            Some(error) => port_error(error),
            None => PortError::new("poll serial read exceeded its deadline"),
        })?
        .into_rows_result()
        .map_err(port_error)?;
        let rows = result
            .rows::<rows::StoredRow>()
            .map_err(port_error)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(port_error)?;
        rows::snapshot(rows)
    }

    async fn update(&self, key: PollKey, next: &PollAggregate, previous: i64) -> PollStoreResult<bool> {
        applied(
            self.session
                .execute_unpaged(&self.statements.update, rows::values(key, next, previous))
                .await
                .map_err(port_error)?,
        )
    }

    async fn cleanup(&self, key: PollKey, state: &PollAggregate) -> PollStoreResult<()> {
        self.remove_due(PollDueEntry {
            key,
            expires_at_ms: state.expires_at_ms,
        })
        .await
    }
}

#[async_trait]
impl PollRepository for ScyllaPollRepository {
    async fn initialize(&self, key: PollKey, definition: &PollDefinition) -> PollStoreResult<()> {
        let (channel, message) = key_values(key)?;
        let counts = vec![0_i32; definition.answers.len()];
        let result = self
            .session
            .execute_unpaged(
                &self.statements.initialize,
                (
                    channel,
                    message,
                    definition.expires_at_ms,
                    definition.allow_multiselect,
                    counts,
                ),
            )
            .await
            .map_err(port_error)?;
        if !applied(result)? {
            return Err(PortError::new("poll message ID was already reserved; refusing to reset votes").into());
        }
        let entry = PollDueEntry {
            key,
            expires_at_ms: definition.expires_at_ms,
        };
        execute(
            &self.session,
            &self.statements.index,
            (i32::from(entry.shard()), entry.expires_at_ms, channel, message),
        )
        .await?;
        // Only the initial reservation expires. Persist every metadata column
        // after durable due discovery, before accepting the actual message.
        // An expired or already-finalized reservation cannot be recreated.
        if !self.update(key, &PollAggregate::new(definition), 0).await? {
            return Err(PortError::new("poll reservation expired or closed before activation").into());
        }
        Ok(())
    }

    async fn snapshot(&self, key: PollKey, viewer: UserId) -> PollStoreResult<Option<PollSnapshotState>> {
        let viewer = i64::try_from(viewer.value()).map_err(|_| PollError::Corrupt)?;
        if viewer == 0 {
            return Err(PollError::Corrupt.into());
        }
        self.read(key, viewer).await
    }

    async fn vote(&self, command: VoteCommand) -> PollStoreResult<PollTransition> {
        let _admission = self.admission.acquire(command.key).await?;
        let (channel, message) = key_values(command.key)?;
        let viewer = i64::try_from(command.user_id.value()).map_err(|_| PollError::Corrupt)?;
        if viewer == 0 {
            return Err(PollError::Corrupt.into());
        }
        for attempt in 0..MAX_ATTEMPTS {
            let current = self.read(command.key, viewer).await?.ok_or(PollError::Deleted)?;
            let next = current.aggregate.vote(
                current.voter.as_ref(),
                command.answer_id,
                command.selected,
                command.expected_vote_version,
                self.now()?,
            )?;
            if !next.changed {
                return Ok(next);
            }
            let values = (
                rows::values(command.key, &next.aggregate, current.aggregate.version),
                (
                    channel,
                    message,
                    viewer,
                    next.voter.revision,
                    i32::from(next.voter.mask),
                ),
            );
            if applied(
                self.session
                    .batch(&self.statements.vote, values)
                    .await
                    .map_err(port_error)?,
            )? {
                return Ok(next);
            }
            conflict(attempt, command.user_id.value()).await;
        }
        Err(exhausted())
    }

    async fn close(&self, key: PollKey, early: bool) -> PollStoreResult<Option<(PollAggregate, bool)>> {
        let _admission = self.admission.acquire(key).await?;
        for attempt in 0..MAX_ATTEMPTS {
            let Some(current) = self.read(key, 0).await? else {
                return Ok(None);
            };
            let Some(closed) = current.aggregate.close(self.now()?, early)? else {
                if current.aggregate.status == PollStatus::Finalized {
                    self.cleanup(key, &current.aggregate).await?;
                }
                return Ok(Some((current.aggregate, false)));
            };
            if self.update(key, &closed, current.aggregate.version).await? {
                self.cleanup(key, &closed).await?;
                return Ok(Some((closed, true)));
            }
            conflict(attempt, key.message_id.value()).await;
        }
        Err(exhausted())
    }

    async fn delete(&self, key: PollKey) -> PollStoreResult<()> {
        let _admission = self.admission.acquire(key).await?;
        let (channel, message) = key_values(key)?;
        for attempt in 0..MAX_ATTEMPTS {
            let Some(current) = self.read(key, 0).await? else {
                return Ok(());
            };
            if let Some(deleted) = current.aggregate.delete()?
                && !self.update(key, &deleted, current.aggregate.version).await?
            {
                conflict(attempt, key.message_id.value()).await;
                continue;
            }
            execute(&self.session, &self.statements.delete_voters, (channel, message)).await?;
            self.cleanup(key, &current.aggregate).await?;
            return Ok(());
        }
        Err(exhausted())
    }

    async fn due_page(&self, shard: u8, after: Option<PollDueEntry>, limit: u16) -> PollStoreResult<Vec<PollDueEntry>> {
        if shard >= 64 || !(1..=100).contains(&limit) || after.is_some_and(|entry| entry.shard() != shard) {
            return Err(PollError::Corrupt.into());
        }
        let now = self.now()?;
        let result = match after {
            None => {
                self.session
                    .execute_unpaged(&self.statements.due_first, (i32::from(shard), now, i32::from(limit)))
                    .await
            }
            Some(cursor) => {
                let (channel, message) = key_values(cursor.key)?;
                self.session
                    .execute_unpaged(
                        &self.statements.due_next,
                        (
                            i32::from(shard),
                            cursor.expires_at_ms,
                            channel,
                            message,
                            now,
                            i64::MAX,
                            i64::MAX,
                            i32::from(limit),
                        ),
                    )
                    .await
            }
        }
        .map_err(port_error)?
        .into_rows_result()
        .map_err(port_error)?;
        result
            .rows::<(i64, i64, i64)>()
            .map_err(port_error)?
            .map(|row| {
                let (expires_at_ms, channel, message) = row.map_err(port_error)?;
                if channel <= 0 || message <= 0 {
                    return Err(PollError::Corrupt.into());
                }
                Ok(PollDueEntry {
                    expires_at_ms,
                    key: PollKey {
                        channel_id: Snowflake::new(channel as u64),
                        message_id: Snowflake::new(message as u64),
                    },
                })
            })
            .collect()
    }

    async fn remove_due(&self, entry: PollDueEntry) -> PollStoreResult<()> {
        let (channel, message) = key_values(entry.key)?;
        execute(
            &self.session,
            &self.statements.remove_due,
            (i32::from(entry.shard()), entry.expires_at_ms, channel, message),
        )
        .await?;
        Ok(())
    }
}

#[cfg(test)]
mod retry_tests {
    use super::{retry_serial_read, serial_read_timed_out};
    use scylla::errors::{DbError, ExecutionError, RequestAttemptError};
    use scylla::statement::Consistency;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn retries_only_transient_reads_and_stops_after_three_attempts() {
        let calls = AtomicUsize::new(0);
        let result = retry_serial_read(
            || async {
                if calls.fetch_add(1, Ordering::SeqCst) < 2 {
                    Err("timeout")
                } else {
                    Ok(42)
                }
            },
            |error| *error == "timeout",
        )
        .await;
        assert_eq!(result, Ok(42));
        assert_eq!(calls.load(Ordering::SeqCst), 3);

        let calls = AtomicUsize::new(0);
        let result: Result<(), _> = retry_serial_read(
            || async {
                calls.fetch_add(1, Ordering::SeqCst);
                Err("timeout")
            },
            |error| *error == "timeout",
        )
        .await;
        assert_eq!(result, Err(Some("timeout")));
        assert_eq!(calls.load(Ordering::SeqCst), 3);

        let calls = AtomicUsize::new(0);
        let result: Result<(), _> = retry_serial_read(
            || async {
                calls.fetch_add(1, Ordering::SeqCst);
                Err("other")
            },
            |error| *error == "timeout",
        )
        .await;
        assert_eq!(result, Err(Some("other")));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
    #[test]
    fn classifies_scylla_read_timeout_only() {
        let timeout = ExecutionError::LastAttemptError(RequestAttemptError::DbError(
            DbError::ReadTimeout {
                consistency: Consistency::Serial,
                received: 0,
                required: 1,
                data_present: false,
            },
            "contention".into(),
        ));
        assert!(serial_read_timed_out(&timeout));
        let other =
            ExecutionError::LastAttemptError(RequestAttemptError::DbError(DbError::Invalid, "invalid query".into()));
        assert!(!serial_read_timed_out(&other));
    }

    #[tokio::test(start_paused = true)]
    async fn serial_read_has_an_overall_deadline() {
        let result = retry_serial_read(|| std::future::pending::<Result<(), ()>>(), |_| true).await;
        assert_eq!(result, Err(None));
    }
}

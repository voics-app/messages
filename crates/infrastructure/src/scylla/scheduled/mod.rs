//! One author-partition CAS commits the job, bounded active discovery and both quotas.
mod index;
mod journal;
mod observability;
#[cfg(test)]
mod observability_test;
mod rows;
mod schema;
mod statements;

use std::sync::Arc;
use std::time::Duration;

use discordy_messages_application::PortError;
use discordy_messages_domain::scheduled::AUTHOR_SCHEDULE_LIMIT;
use discordy_messages_domain::{ChannelId, UserId};
use scylla::client::session::Session;
use uuid::Uuid;

use super::{fetch_all, port_error};
use statements::{Statements, applied};

const MAX_CAS_ATTEMPTS: usize = 64;
const ADMISSION_STRIPES: usize = 64;

struct Metadata {
    revision: Uuid,
    active: i32,
}

pub struct ScyllaScheduledMessages {
    session: Arc<Session>,
    statements: Statements,
    admission: [tokio::sync::Mutex<()>; ADMISSION_STRIPES],
}

impl ScyllaScheduledMessages {
    pub async fn open(session: Arc<Session>, keyspace: Option<&str>) -> Result<Self, PortError> {
        let keyspace = schema::bootstrap(&session, keyspace).await?;
        Ok(Self {
            statements: Statements::new(&session, &keyspace).await?,
            session,
            admission: std::array::from_fn(|_| tokio::sync::Mutex::new(())),
        })
    }

    pub async fn check_ready(&self) -> Result<(), PortError> {
        self.session
            .execute_unpaged(&self.statements.metadata, (0_i64,))
            .await
            .map_err(port_error)?;
        self.session
            .execute_unpaged(&self.statements.cursor, (0_i8,))
            .await
            .map_err(port_error)?;
        self.session
            .execute_unpaged(
                &self.statements.page,
                (0_i8, -1_i64, 0_i64, "", 0_i32, 0_i64, i64::MAX, "z", i32::MAX),
            )
            .await
            .map_err(port_error)?;
        Ok(())
    }

    fn admission(&self, author: UserId) -> &tokio::sync::Mutex<()> {
        &self.admission[(author.value() % ADMISSION_STRIPES as u64) as usize]
    }

    async fn metadata(&self, author: UserId) -> Result<Option<Metadata>, PortError> {
        let rows: Vec<(Uuid, i32)> = fetch_all(
            &self.session,
            &self.statements.metadata,
            (rows::checked(author.value())?,),
        )
        .await?;
        rows.into_iter()
            .next()
            .map(|(revision, active)| {
                if !(0..=AUTHOR_SCHEDULE_LIMIT as i32).contains(&active) {
                    return Err(PortError::new("invalid scheduled author quota"));
                }
                Ok(Metadata { revision, active })
            })
            .transpose()
    }

    async fn ensure_metadata(&self, author: UserId) -> Result<Metadata, PortError> {
        if let Some(metadata) = self.metadata(author).await? {
            return Ok(metadata);
        }
        let revision = Uuid::new_v4();
        if applied(
            self.session
                .execute_unpaged(&self.statements.initialize, (rows::checked(author.value())?, revision))
                .await
                .map_err(port_error)?,
        )? {
            return Ok(Metadata { revision, active: 0 });
        }
        self.metadata(author)
            .await?
            .ok_or_else(|| PortError::new("scheduled metadata initialization not visible"))
    }

    async fn channel_count(&self, author: UserId, channel: ChannelId) -> Result<i32, PortError> {
        rows::checked(channel.value())?;
        let rows: Vec<(i32,)> = fetch_all(
            &self.session,
            &self.statements.channel_count,
            (rows::checked(author.value())?, channel.to_string()),
        )
        .await?;
        let count = rows.first().map_or(0, |row| row.0);
        if !(0..=discordy_messages_domain::scheduled::CHANNEL_SCHEDULE_LIMIT as i32).contains(&count) {
            return Err(PortError::new("invalid scheduled channel quota"));
        }
        Ok(count)
    }
}

async fn conflict_backoff(attempt: usize) {
    metrics::counter!("scheduled_cas_conflicts_total").increment(1);
    let ceiling = 1_u128 << (attempt + 1).min(7);
    tokio::time::sleep(Duration::from_millis((1 + Uuid::new_v4().as_u128() % ceiling) as u64)).await;
}

fn exhausted() -> PortError {
    metrics::counter!("scheduled_cas_exhausted_total").increment(1);
    PortError::new("scheduled contention exceeded the retry bound")
}

use async_trait::async_trait;
use discordy_messages_application::{PortError, ports::scheduled::*};
use discordy_messages_domain::Snowflake;

use super::super::{execute, fetch_all};
use super::{ScyllaScheduledMessages, rows::checked};

type IndexRow = (i64, i64, String, i32);
type IndexValues = (i8, i64, i64, String, i32);

fn shard(value: u8) -> Result<i8, PortError> {
    if value >= SCHEDULE_SHARDS {
        return Err(PortError::new("scheduled shard out of bounds"));
    }
    Ok(value as i8)
}

fn values(due: DueSchedule) -> Result<IndexValues, PortError> {
    Ok((
        shard(due.shard())?,
        checked(due.wake_at_ms)?,
        checked(due.key.author_id.value())?,
        due.key.id.to_string(),
        i32::try_from(due.revision).map_err(PortError::new)?,
    ))
}

fn decode(row: IndexRow, expected_shard: u8) -> Result<DueSchedule, PortError> {
    if row.0 < 0 || row.1 < 0 || row.3 < 0 {
        return Err(PortError::new("invalid scheduled index row"));
    }
    let due = DueSchedule {
        wake_at_ms: row.0 as u64,
        key: ScheduleKey {
            author_id: Snowflake::new(row.1 as u64),
            id: row.2.parse().map_err(PortError::new)?,
        },
        revision: row.3 as u32,
    };
    if due.shard() != expected_shard {
        return Err(PortError::new("scheduled index row belongs to a different shard"));
    }
    Ok(due)
}

#[async_trait]
impl ScheduleIndex for ScyllaScheduledMessages {
    async fn stage(&self, due: DueSchedule) -> Result<(), PortError> {
        execute(&self.session, &self.statements.stage, values(due)?).await
    }

    async fn remove(&self, due: DueSchedule) -> Result<(), PortError> {
        execute(&self.session, &self.statements.remove, values(due)?).await
    }

    async fn page(&self, requested: u8, after: Option<DueSchedule>, now: u64) -> Result<Vec<DueSchedule>, PortError> {
        let lower = match after {
            Some(due) if due.shard() == requested => {
                let (_, wake, author, id, revision) = values(due)?;
                (wake, author, id, revision)
            }
            Some(_) => return Err(PortError::new("scheduled cursor belongs to a different shard")),
            None => (-1, 0, String::new(), 0),
        };
        let found: Vec<IndexRow> = fetch_all(
            &self.session,
            &self.statements.page,
            (
                shard(requested)?,
                lower.0,
                lower.1,
                lower.2,
                lower.3,
                checked(now)?,
                i64::MAX,
                "z",
                i32::MAX,
            ),
        )
        .await?;
        found.into_iter().map(|row| decode(row, requested)).collect()
    }

    async fn cursor(&self, requested: u8) -> Result<Option<DueSchedule>, PortError> {
        let found: Vec<IndexRow> = fetch_all(&self.session, &self.statements.cursor, (shard(requested)?,)).await?;
        found.into_iter().next().map(|row| decode(row, requested)).transpose()
    }

    async fn save_cursor(&self, requested: u8, after: Option<DueSchedule>) -> Result<(), PortError> {
        match after {
            Some(due) if due.shard() == requested => {
                execute(&self.session, &self.statements.save_cursor, values(due)?).await
            }
            Some(_) => Err(PortError::new("scheduled cursor belongs to a different shard")),
            None => execute(&self.session, &self.statements.clear_cursor, (shard(requested)?,)).await,
        }
    }
}

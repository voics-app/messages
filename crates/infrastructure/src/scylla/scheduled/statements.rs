use std::sync::Arc;

use discordy_messages_application::PortError;
use scylla::client::session::Session;
use scylla::policies::retry::FallthroughRetryPolicy;
use scylla::response::query_result::QueryResult;
use scylla::statement::{Consistency, SerialConsistency, batch::Batch, prepared::PreparedStatement};
use scylla::value::{CqlValue, Row};

use super::super::{port_error, prepare};

pub(super) async fn statement(session: &Session, text: &str) -> Result<PreparedStatement, PortError> {
    let mut statement = prepare(session, text).await?;
    statement.set_consistency(Consistency::Quorum);
    statement.set_serial_consistency(Some(SerialConsistency::Serial));
    statement.set_is_idempotent(false);
    statement.set_retry_policy(Some(Arc::new(FallthroughRetryPolicy)));
    Ok(statement)
}

async fn batch(session: &Session, statements: &[String]) -> Result<Batch, PortError> {
    let mut batch = Batch::default();
    batch.set_consistency(Consistency::Quorum);
    batch.set_serial_consistency(Some(SerialConsistency::Serial));
    batch.set_is_idempotent(false);
    batch.set_retry_policy(Some(Arc::new(FallthroughRetryPolicy)));
    for text in statements {
        batch.append_statement(text.as_str());
    }
    session.prepare_batch(&batch).await.map_err(port_error)
}

pub(super) fn applied(result: QueryResult) -> Result<bool, PortError> {
    let rows = result.into_rows_result().map_err(port_error)?;
    let mut applied = None;
    for row in rows.rows::<Row>().map_err(port_error)? {
        match row.map_err(port_error)?.columns.first() {
            Some(Some(CqlValue::Boolean(value))) => applied = Some(applied.unwrap_or(true) && *value),
            _ => return Err(PortError::new("scheduled conditional write omitted applied status")),
        }
    }
    applied.ok_or_else(|| PortError::new("scheduled conditional write returned no status"))
}

pub(super) struct Statements {
    pub metadata: PreparedStatement,
    pub initialize: PreparedStatement,
    pub find: PreparedStatement,
    pub active: PreparedStatement,
    pub list: PreparedStatement,
    pub channel_count: PreparedStatement,
    pub keep_active: Batch,
    pub release_active: Batch,
    pub stage: PreparedStatement,
    pub remove: PreparedStatement,
    pub page: PreparedStatement,
    pub cursor: PreparedStatement,
    pub save_cursor: PreparedStatement,
    pub clear_cursor: PreparedStatement,
}

impl Statements {
    pub async fn new(session: &Session, keyspace: &str) -> Result<Self, PortError> {
        let table = format!("{keyspace}.scheduled_journal");
        let metadata = statement(
            session,
            &format!("SELECT revision, active_count FROM {table} WHERE author_id=? AND kind=0 AND entry='' "),
        )
        .await?;
        let initialize = statement(
            session,
            &format!(
                "INSERT INTO {table} (author_id,kind,entry,revision,active_count) VALUES (?,0,'',?,0) IF NOT EXISTS"
            ),
        )
        .await?;
        let find = statement(
            session,
            &format!("SELECT body FROM {table} WHERE author_id=? AND kind=1 AND entry=?"),
        )
        .await?;
        let active = statement(
            session,
            &format!("SELECT entry,body FROM {table} WHERE author_id=? AND kind=2 LIMIT 101"),
        )
        .await?;
        let list = statement(
            session,
            &format!("SELECT entry,body FROM {table} WHERE author_id=? AND kind=1 AND entry>? LIMIT ?"),
        )
        .await?;
        let channel_count = statement(
            session,
            &format!("SELECT active_count FROM {table} WHERE author_id=? AND kind=3 AND entry=?"),
        )
        .await?;
        let metadata_update = format!(
            "UPDATE {table} SET revision=?, active_count=? WHERE author_id=? AND kind=0 AND entry='' IF revision=?"
        );
        let job_update = format!("INSERT INTO {table} (author_id,kind,entry,body) VALUES (?,1,?,?)");
        let channel_update = format!("INSERT INTO {table} (author_id,kind,entry,active_count) VALUES (?,3,?,?)");
        let keep_active = batch(
            session,
            &[
                metadata_update.clone(),
                job_update.clone(),
                channel_update.clone(),
                format!("INSERT INTO {table} (author_id,kind,entry,body) VALUES (?,2,?,?)"),
            ],
        )
        .await?;
        let release_active = batch(
            session,
            &[
                metadata_update,
                job_update,
                channel_update,
                format!("DELETE FROM {table} WHERE author_id=? AND kind=2 AND entry=?"),
            ],
        )
        .await?;
        let due = format!("{keyspace}.scheduled_due");
        let cursors = format!("{keyspace}.scheduled_cursors");
        Ok(Self { metadata, initialize, find, active, list, channel_count, keep_active, release_active,
            stage: statement(session, &format!("INSERT INTO {due} (shard,wake_at_ms,author_id,schedule_id,revision) VALUES (?,?,?,?,?)")).await?,
            remove: statement(session, &format!("DELETE FROM {due} WHERE shard=? AND wake_at_ms=? AND author_id=? AND schedule_id=? AND revision=?")).await?,
            page: statement(session, &format!("SELECT wake_at_ms,author_id,schedule_id,revision FROM {due} WHERE shard=? AND (wake_at_ms,author_id,schedule_id,revision)>(?,?,?,?) AND (wake_at_ms,author_id,schedule_id,revision)<=(?,?,?,?) LIMIT 100")).await?,
            cursor: statement(session, &format!("SELECT wake_at_ms,author_id,schedule_id,revision FROM {cursors} WHERE shard=?")).await?,
            save_cursor: statement(session, &format!("INSERT INTO {cursors} (shard,wake_at_ms,author_id,schedule_id,revision) VALUES (?,?,?,?,?)")).await?,
            clear_cursor: statement(session, &format!("DELETE FROM {cursors} WHERE shard=?")).await?,
        })
    }
}

use discordy_messages_application::PortError;
use discordy_messages_application::ports::PollStoreResult;
use scylla::client::session::Session;
use scylla::response::query_result::QueryResult;
use scylla::statement::{Consistency, SerialConsistency};
use scylla::statement::{batch::Batch, prepared::PreparedStatement};
use scylla::value::{CqlValue, Row};

use super::super::{port_error, prepare};
use super::rows::COLUMNS;

pub(super) fn applied(result: QueryResult) -> PollStoreResult<bool> {
    let rows = result.into_rows_result().map_err(port_error)?;
    let mut result = None;
    for row in rows.rows::<Row>().map_err(port_error)? {
        match row.map_err(port_error)?.columns.first() {
            Some(Some(CqlValue::Boolean(value))) => result = Some(result.unwrap_or(true) && *value),
            _ => return Err(PortError::new("poll conditional write omitted applied status").into()),
        }
    }
    result.ok_or_else(|| PortError::new("poll conditional write returned no status").into())
}

pub(super) struct Statements {
    pub read: PreparedStatement,
    pub initialize: PreparedStatement,
    pub update: PreparedStatement,
    pub vote: Batch,
    pub delete_voters: PreparedStatement,
    pub index: PreparedStatement,
    pub due_first: PreparedStatement,
    pub due_next: PreparedStatement,
    pub remove_due: PreparedStatement,
}

async fn quorum(session: &Session, cql: &str) -> Result<PreparedStatement, PortError> {
    let mut query = prepare(session, cql).await?;
    query.set_consistency(Consistency::Quorum);
    Ok(query)
}

async fn conditional(session: &Session, cql: &str) -> Result<PreparedStatement, PortError> {
    let mut query = quorum(session, cql).await?;
    query.set_serial_consistency(Some(SerialConsistency::Serial));
    query.set_is_idempotent(false);
    Ok(query)
}

impl Statements {
    pub async fn prepare(session: &Session, name: &str) -> Result<Self, PortError> {
        let table = format!("{name}.poll_state");
        let index = format!("{name}.poll_due");
        let update = format!(
            "UPDATE {table} USING TTL 0 SET version=?,expires_at=?,allow_multiselect=?,status=?,counts=?,participants=? WHERE channel_id=? AND message_id=? AND row_id=0 IF version=?"
        );
        let mut vote = Batch::default();
        vote.set_consistency(Consistency::Quorum);
        vote.set_serial_consistency(Some(SerialConsistency::Serial));
        vote.set_is_idempotent(false);
        vote.append_statement(update.as_str());
        vote.append_statement(
            format!("INSERT INTO {table} (channel_id,message_id,row_id,vote_version,vote_mask) VALUES (?,?,?,?,?)")
                .as_str(),
        );
        let mut read = quorum(
            session,
            &format!("SELECT {COLUMNS} FROM {table} WHERE channel_id=? AND message_id=? AND row_id IN (0,?)"),
        )
        .await?;
        read.set_consistency(Consistency::Serial);
        Ok(Self {
            read,
            initialize: conditional(session, &format!("INSERT INTO {table} (channel_id,message_id,row_id,version,expires_at,allow_multiselect,status,counts,participants) VALUES (?,?,0,0,?,?,0,?,0) IF NOT EXISTS USING TTL 86400")).await?,
            update: conditional(session, &update).await?,
            vote: session.prepare_batch(&vote).await.map_err(port_error)?,
            delete_voters: quorum(session, &format!("DELETE FROM {table} WHERE channel_id=? AND message_id=? AND row_id>0")).await?,
            index: quorum(session, &format!("INSERT INTO {index} (shard,expires_at,channel_id,message_id) VALUES (?,?,?,?)")).await?,
            due_first: quorum(session, &format!("SELECT expires_at,channel_id,message_id FROM {index} WHERE shard=? AND expires_at<=? LIMIT ?")).await?,
            due_next: quorum(session, &format!("SELECT expires_at,channel_id,message_id FROM {index} WHERE shard=? AND (expires_at,channel_id,message_id)>(?,?,?) AND (expires_at,channel_id,message_id)<=(?,?,?) LIMIT ?")).await?,
            remove_due: quorum(session, &format!("DELETE FROM {index} WHERE shard=? AND expires_at=? AND channel_id=? AND message_id=?")).await?,
        })
    }
}

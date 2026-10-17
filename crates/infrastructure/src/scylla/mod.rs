//! ScyllaDB adapters. All statements are prepared once at startup.

mod activity;
mod messages;
mod migrations;
mod pins;
mod reactions;
mod read_states;
mod rows;

use std::sync::Arc;

use discordy_messages_application::PortError;
use futures::TryStreamExt;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::statement::prepared::PreparedStatement;

pub use activity::ScyllaActivityRepository;
pub use messages::ScyllaMessageRepository;
pub use migrations::migrate;
pub use pins::ScyllaPinRepository;
pub use reactions::ScyllaReactionRepository;
pub use read_states::ScyllaReadStateRepository;

pub(crate) fn port_error(error: impl std::fmt::Display) -> PortError {
    PortError::new(format!("scylla: {error}"))
}

/// Connects to the cluster, migrates the keyspace and returns a session bound
/// to it.
pub async fn connect(nodes: &[String], keyspace: &str) -> Result<Arc<Session>, PortError> {
    open(SessionBuilder::new().known_nodes(nodes), keyspace).await
}

/// Like [`connect`], for callers that need to customise the session (for
/// example an address translator when the node sits behind port mapping).
pub async fn open(builder: SessionBuilder, keyspace: &str) -> Result<Arc<Session>, PortError> {
    let session = builder.build().await.map_err(port_error)?;
    migrate(&session, keyspace).await?;
    Ok(Arc::new(session))
}

/// Readiness probe: a trivial query against the local node.
pub async fn ping(session: &Session) -> Result<(), PortError> {
    session
        .query_unpaged("SELECT now() FROM system.local", ())
        .await
        .map(|_| ())
        .map_err(port_error)
}

pub(crate) async fn prepare(session: &Session, cql: &str) -> Result<PreparedStatement, PortError> {
    session.prepare(cql).await.map_err(port_error)
}

/// Runs a prepared statement without reading rows.
pub(crate) async fn execute(
    session: &Session,
    statement: &PreparedStatement,
    values: impl scylla::serialize::row::SerializeRow,
) -> Result<(), PortError> {
    session
        .execute_unpaged(statement, values)
        .await
        .map(|_| ())
        .map_err(port_error)
}

/// Runs a prepared statement and collects every row, paging transparently.
pub(crate) async fn fetch_all<R>(
    session: &Session,
    statement: &PreparedStatement,
    values: impl scylla::serialize::row::SerializeRow,
) -> Result<Vec<R>, PortError>
where
    R: for<'frame, 'metadata> scylla::deserialize::row::DeserializeRow<'frame, 'metadata> + Send + 'static,
{
    session
        .execute_iter(statement.clone(), values)
        .await
        .map_err(port_error)?
        .rows_stream::<R>()
        .map_err(port_error)?
        .try_collect()
        .await
        .map_err(port_error)
}

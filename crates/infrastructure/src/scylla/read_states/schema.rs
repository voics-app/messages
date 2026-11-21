//! Companion vnode schema, retaining the original keyspace and its replication policy.

use std::collections::HashMap;

use discordy_messages_application::PortError;
use scylla::client::session::Session;

use super::super::port_error;

const MIGRATION: &str = include_str!("../../../../../migrations/0008_read_accounting.cql");
const VERSION_MIGRATION: &str = include_str!("../../../../../migrations/0009_read_state_version.cql");

fn identifier(value: &str) -> Result<String, PortError> {
    if value.is_empty()
        || value.len() > 48
        || value.starts_with(|c: char| c.is_ascii_digit())
        || !value.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
    {
        return Err(PortError::new(
            "invalid read-state keyspace; use SCYLLA_READ_STATE_KEYSPACE to select a distinct identifier of at most 48 characters",
        ));
    }
    // The existing bootstrap uses unquoted CQL identifiers.
    Ok(value.to_ascii_lowercase())
}

async fn replication(session: &Session, keyspace: &str) -> Result<HashMap<String, String>, PortError> {
    session
        .query_unpaged(
            "SELECT replication FROM system_schema.keyspaces WHERE keyspace_name = ?",
            (keyspace,),
        )
        .await
        .map_err(port_error)?
        .into_rows_result()
        .map_err(port_error)?
        .single_row::<(HashMap<String, String>,)>()
        .map(|row| row.0)
        .map_err(port_error)
}

fn quoted(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

pub(super) async fn bootstrap(session: &Session, configured: Option<&str>) -> Result<String, PortError> {
    let base = session
        .get_keyspace()
        .ok_or_else(|| PortError::new("read states require a bound Scylla keyspace"))?;
    let base = identifier(&base)?;
    let name = identifier(&configured.map(str::to_owned).unwrap_or_else(|| format!("{base}_reads")))?;
    if name == base {
        return Err(PortError::new(
            "SCYLLA_READ_STATE_KEYSPACE must differ from SCYLLA_KEYSPACE; existing data must not be recreated",
        ));
    }
    let expected = replication(session, &base).await?;
    let mut entries: Vec<_> = expected
        .iter()
        .map(|(key, value)| format!("{}: {}", quoted(key), quoted(value)))
        .collect();
    entries.sort();
    session
        .query_unpaged(
            format!(
                "CREATE KEYSPACE IF NOT EXISTS {name} WITH replication = {{{}}} AND tablets = {{'enabled': false}}",
                entries.join(", ")
            ),
            (),
        )
        .await
        .map_err(port_error)?;
    session.await_schema_agreement().await.map_err(port_error)?;
    if replication(session, &name).await? != expected {
        return Err(PortError::new(format!(
            "read-state keyspace {name} replication differs from {base}; reconcile both replication policies before starting"
        )));
    }

    // Do not change the shared session's current keyspace: all companion queries
    // are qualified, and legacy reads/index writes continue to use the original.
    let script = MIGRATION.replace("{{keyspace}}", &name);
    let script = script
        .lines()
        .filter(|line| !line.trim_start().starts_with("--"))
        .collect::<Vec<_>>()
        .join("\n");
    for statement in script.split(';').map(str::trim).filter(|s| !s.is_empty()) {
        session.query_unpaged(statement, ()).await.map_err(port_error)?;
    }
    session
        .query_unpaged(
            format!("CREATE TABLE IF NOT EXISTS {name}.schema_migrations (version text PRIMARY KEY)"),
            (),
        )
        .await
        .map_err(port_error)?;
    session.await_schema_agreement().await.map_err(port_error)?;
    // This real conditional write also fails startup if a pre-existing companion
    // is tablet-backed on a server that cannot support its required transactions.
    session
        .query_unpaged(
            format!("INSERT INTO {name}.schema_migrations (version) VALUES ('0008_read_accounting') IF NOT EXISTS"),
            (),
        )
        .await
        .map_err(|error| PortError::new(format!("read-state LWT capability check failed for {name}: {error}")))?;
    add_version(session, &name).await?;
    Ok(format!("{name}.read_state_accounting"))
}

async fn version_column(session: &Session, keyspace: &str) -> Result<Option<String>, PortError> {
    let result = session.query_unpaged(
        "SELECT type FROM system_schema.columns WHERE keyspace_name=? AND table_name='read_state_accounting' AND column_name='read_state_version'",
        (keyspace,),
    ).await.map_err(port_error)?.into_rows_result().map_err(port_error)?;
    result
        .rows::<(String,)>()
        .map_err(port_error)?
        .next()
        .transpose()
        .map(|row| row.map(|row| row.0))
        .map_err(port_error)
}

async fn add_version(session: &Session, keyspace: &str) -> Result<(), PortError> {
    if version_column(session, keyspace).await?.is_none() {
        let statement = VERSION_MIGRATION
            .lines()
            .filter(|line| !line.trim_start().starts_with("--"))
            .collect::<Vec<_>>()
            .join("\n")
            .replace("{{keyspace}}", keyspace);
        let result = session.query_unpaged(statement.trim().trim_end_matches(';'), ()).await;
        session.await_schema_agreement().await.map_err(port_error)?;
        // Another starting replica may have applied the same additive migration.
        if let Err(error) = result
            && version_column(session, keyspace).await?.as_deref() != Some("bigint")
        {
            return Err(port_error(error));
        }
    }
    if version_column(session, keyspace).await?.as_deref() != Some("bigint") {
        return Err(PortError::new("read-state version column must be bigint"));
    }
    session
        .query_unpaged(
            format!(
                "INSERT INTO {keyspace}.schema_migrations (version) VALUES ('0009_read_state_version') IF NOT EXISTS"
            ),
            (),
        )
        .await
        .map_err(port_error)?;
    Ok(())
}

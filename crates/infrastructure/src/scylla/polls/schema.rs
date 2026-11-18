use std::collections::HashMap;

use discordy_messages_application::PortError;
use scylla::client::session::Session;

use super::super::port_error;

const MIGRATION: &str = include_str!("../../../../../migrations/0012_poll_state.cql");

fn identifier(value: &str) -> Result<String, PortError> {
    if value.is_empty()
        || value.len() > 48
        || value.starts_with(|c: char| c.is_ascii_digit())
        || !value.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
    {
        return Err(PortError::new(
            "invalid poll keyspace identifier (maximum 48 characters)",
        ));
    }
    Ok(value.to_ascii_lowercase())
}

fn quoted(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

async fn replication(session: &Session, keyspace: &str) -> Result<HashMap<String, String>, PortError> {
    session
        .query_unpaged(
            "SELECT replication FROM system_schema.keyspaces WHERE keyspace_name=?",
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

pub(super) async fn bootstrap(session: &Session, configured: Option<&str>) -> Result<String, PortError> {
    let base = identifier(
        &session
            .get_keyspace()
            .ok_or_else(|| PortError::new("polls require a bound main keyspace"))?,
    )?;
    let name = identifier(&configured.map(str::to_owned).unwrap_or_else(|| format!("{base}_polls")))?;
    if name == base {
        return Err(PortError::new(
            "SCYLLA_POLL_KEYSPACE must differ from the main keyspace",
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
                "CREATE KEYSPACE IF NOT EXISTS {name} WITH replication={{{}}} AND tablets={{'enabled':false}}",
                entries.join(",")
            ),
            (),
        )
        .await
        .map_err(port_error)?;
    session.await_schema_agreement().await.map_err(port_error)?;
    if replication(session, &name).await? != expected {
        return Err(PortError::new(
            "poll companion replication differs from the main keyspace",
        ));
    }
    let script = MIGRATION
        .lines()
        .filter(|line| !line.trim_start().starts_with("--"))
        .collect::<Vec<_>>()
        .join("\n")
        .replace("{{keyspace}}", &name);
    for statement in script.split(';').map(str::trim).filter(|line| !line.is_empty()) {
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
    session
        .query_unpaged(
            format!("INSERT INTO {name}.schema_migrations (version) VALUES ('0012_poll_state') IF NOT EXISTS"),
            (),
        )
        .await
        .map_err(|error| PortError::new(format!("poll LWT capability check failed: {error}")))?;
    Ok(name)
}

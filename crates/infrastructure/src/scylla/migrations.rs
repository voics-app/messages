//! Applies the CQL migrations in `messages/migrations` in order and records
//! each applied version in `schema_migrations`.

use scylla::client::session::Session;
use scylla::value::CqlTimestamp;

use super::port_error;
use discordy_messages_application::PortError;

/// Embedded migrations, ordered by version.
const MIGRATIONS: &[(&str, &str)] = &[
    ("0001_initial", include_str!("../../../../migrations/0001_initial.cql")),
    (
        "0002_message_origin",
        include_str!("../../../../migrations/0002_message_origin.cql"),
    ),
    (
        "0003_attachment_description_flags",
        include_str!("../../../../migrations/0003_attachment_description_flags.cql"),
    ),
    (
        "0004_message_forward",
        include_str!("../../../../migrations/0004_message_forward.cql"),
    ),
    (
        "0005_system_message_payloads",
        include_str!("../../../../migrations/0005_system_message_payloads.cql"),
    ),
    (
        "0006_mention_here",
        include_str!("../../../../migrations/0006_mention_here.cql"),
    ),
    (
        "0007_message_tts",
        include_str!("../../../../migrations/0007_message_tts.cql"),
    ),
    (
        "0013_announcement_publication",
        include_str!("../../../../migrations/0013_announcement_publication.cql"),
    ),
    (
        "0014_guild_message_activity",
        include_str!("../../../../migrations/0014_guild_message_activity.cql"),
    ),
];

/// Creates the keyspace when missing (replication factor 1: the platform
/// runs a single ScyllaDB node, tablets off because the publication marker and
/// read states use lightweight transactions), switches to it and applies pending
/// migrations.
pub async fn migrate(session: &Session, keyspace: &str) -> Result<(), PortError> {
    validate_identifier(keyspace)?;
    let create_keyspace = format!(
        "CREATE KEYSPACE IF NOT EXISTS {keyspace} WITH replication = \
         {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}} AND tablets = {{'enabled': false}}"
    );
    session.query_unpaged(create_keyspace, ()).await.map_err(port_error)?;
    session.await_schema_agreement().await.map_err(port_error)?;
    session.use_keyspace(keyspace, false).await.map_err(port_error)?;
    session
        .query_unpaged(
            "CREATE TABLE IF NOT EXISTS schema_migrations (version text PRIMARY KEY, applied_at timestamp)",
            (),
        )
        .await
        .map_err(port_error)?;
    for (version, script) in MIGRATIONS {
        if is_applied(session, version).await? {
            continue;
        }
        for statement in statements(script) {
            session.query_unpaged(statement, ()).await.map_err(port_error)?;
        }
        session.await_schema_agreement().await.map_err(port_error)?;
        record(session, version).await?;
        tracing::info!(version, "applied CQL migration");
    }
    Ok(())
}

async fn is_applied(session: &Session, version: &str) -> Result<bool, PortError> {
    let rows = session
        .query_unpaged("SELECT version FROM schema_migrations WHERE version = ?", (version,))
        .await
        .map_err(port_error)?
        .into_rows_result()
        .map_err(port_error)?;
    Ok(rows.rows_num() > 0)
}

async fn record(session: &Session, version: &str) -> Result<(), PortError> {
    let now = CqlTimestamp(chrono::Utc::now().timestamp_millis());
    session
        .query_unpaged(
            "INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)",
            (version, now),
        )
        .await
        .map_err(port_error)?;
    Ok(())
}

/// Splits a script into statements, dropping `--` comment lines.
fn statements(script: &str) -> Vec<String> {
    let without_comments: String = script
        .lines()
        .filter(|line| !line.trim_start().starts_with("--"))
        .collect::<Vec<_>>()
        .join("\n");
    without_comments
        .split(';')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .collect()
}

fn validate_identifier(keyspace: &str) -> Result<(), PortError> {
    let valid = !keyspace.is_empty()
        && keyspace.len() <= 48
        && keyspace.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
        && !keyspace.starts_with(|c: char| c.is_ascii_digit());
    if valid {
        Ok(())
    } else {
        Err(PortError::new(format!("invalid keyspace name: {keyspace}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_statements_and_drops_comments() {
        let script = "-- header\nCREATE TABLE a (x int PRIMARY KEY);\n\n-- note\nCREATE TABLE b (y int PRIMARY KEY);\n";
        assert_eq!(
            statements(script),
            vec![
                "CREATE TABLE a (x int PRIMARY KEY)",
                "CREATE TABLE b (y int PRIMARY KEY)"
            ]
        );
    }

    #[test]
    fn initial_migration_creates_every_table() {
        let all = statements(MIGRATIONS[0].1).join("\n");
        for table in [
            "messages",
            "reactions",
            "pins",
            "read_states",
            "channel_last_message",
            "message_activity",
        ] {
            assert!(all.contains(&format!("CREATE TABLE IF NOT EXISTS {table} ")), "{table}");
        }
    }

    #[test]
    fn keyspace_names_are_validated() {
        assert!(validate_identifier("discordy").is_ok());
        assert!(validate_identifier("discordy; DROP").is_err());
        assert!(validate_identifier("1abc").is_err());
    }
}

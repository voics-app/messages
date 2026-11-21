use std::str::FromStr;
use std::time::Duration;

use async_trait::async_trait;
use discordy_messages_application::PortError;
use discordy_messages_application::ports::{
    BackfillState, Checkpoint, IndexOp, PinnedFilter, SearchHit, SearchIndex, SearchPage, SearchRequest,
};
use discordy_messages_domain::{ChannelId, GuildId, SearchFilters, Snowflake, SortBy, SortOrder};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{PgPool, Postgres, QueryBuilder, Row};

const MIGRATIONS: &[(&str, &str)] = &[(
    "0001_search_messages",
    include_str!("../../../../migrations/search/0001_search_messages.sql"),
)];

/// Rows counted for `total_results` are capped here (Discord reports at most 10 000 too).
const COUNT_CAP: i64 = 10_000;
const STATEMENT_TIMEOUT_MS: &str = "5000";
const BACKFILL_KEY: &str = "backfill";

fn port_error(error: impl std::fmt::Display) -> PortError {
    PortError::new(format!("search: {error}"))
}

fn db_id(id: Snowflake) -> i64 {
    id.value() as i64
}

fn snowflake(raw: i64) -> Snowflake {
    Snowflake::new(raw as u64)
}

/// A lazily connected pool: the service starts, and keeps serving messages,
/// while PostgreSQL is still coming up.
pub fn connect(url: &str) -> Result<PgPool, PortError> {
    let options = PgConnectOptions::from_str(url)
        .map_err(port_error)?
        .options([("statement_timeout", STATEMENT_TIMEOUT_MS)]);
    Ok(PgPoolOptions::new()
        .max_connections(5)
        .acquire_timeout(Duration::from_secs(5))
        .connect_lazy_with(options))
}

pub struct PgSearchIndex {
    pool: PgPool,
}

impl PgSearchIndex {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Applies pending migrations under an advisory lock.
    /// Applies pending migrations. Scripts are idempotent and run as one
    /// implicit transaction (simple query protocol), so a concurrent replica
    /// applying the same script is harmless.
    pub async fn migrate(&self) -> Result<(), PortError> {
        sqlx::query("CREATE TABLE IF NOT EXISTS search_schema_migrations (version text PRIMARY KEY)")
            .execute(&self.pool)
            .await
            .map_err(port_error)?;
        for (version, script) in MIGRATIONS {
            let applied = sqlx::query("SELECT 1 FROM search_schema_migrations WHERE version = $1")
                .bind(*version)
                .fetch_optional(&self.pool)
                .await
                .map_err(port_error)?
                .is_some();
            if applied {
                continue;
            }
            sqlx::raw_sql(script).execute(&self.pool).await.map_err(port_error)?;
            sqlx::query("INSERT INTO search_schema_migrations (version) VALUES ($1) ON CONFLICT DO NOTHING")
                .bind(*version)
                .execute(&self.pool)
                .await
                .map_err(port_error)?;
            tracing::info!(version, "applied search migration");
        }
        Ok(())
    }

    pub async fn ping(&self) -> Result<(), PortError> {
        sqlx::query("SELECT 1")
            .execute(&self.pool)
            .await
            .map(|_| ())
            .map_err(port_error)
    }
}

/// The shared `WHERE` clause of the page and the count query.
fn push_filters(qb: &mut QueryBuilder<'_, Postgres>, request: &SearchRequest) {
    let f: &SearchFilters = &request.filters;
    qb.push(" WHERE channel_id = ANY(");
    qb.push_bind(request.channels.iter().map(|c| db_id(*c)).collect::<Vec<i64>>());
    qb.push(")");
    if let Some(text) = &f.content {
        qb.push(" AND tsv @@ websearch_to_tsquery('simple', ");
        qb.push_bind(text.clone());
        qb.push(")");
    }
    if !f.author_ids.is_empty() {
        qb.push(" AND author_id = ANY(");
        qb.push_bind(f.author_ids.iter().map(|u| db_id(*u)).collect::<Vec<i64>>());
        qb.push(")");
    }
    if !f.mentions.is_empty() {
        qb.push(" AND mentions @> ");
        qb.push_bind(f.mentions.iter().map(|u| db_id(*u)).collect::<Vec<i64>>());
    }
    if f.has.bits() != 0 {
        qb.push(" AND (has & ");
        qb.push_bind(i16::from(f.has.bits()));
        qb.push("::smallint) = ");
        qb.push_bind(i16::from(f.has.bits()));
    }
    if let Some(min) = f.min_id {
        qb.push(" AND message_id > ");
        qb.push_bind(db_id(min));
    }
    if let Some(max) = f.max_id {
        qb.push(" AND message_id < ");
        qb.push_bind(db_id(max));
    }
    match &request.pinned {
        None => {}
        Some(PinnedFilter::Only(ids)) => {
            qb.push(" AND message_id = ANY(");
            qb.push_bind(ids.iter().map(|i| db_id(*i)).collect::<Vec<i64>>());
            qb.push(")");
        }
        Some(PinnedFilter::Except(ids)) => {
            qb.push(" AND message_id <> ALL(");
            qb.push_bind(ids.iter().map(|i| db_id(*i)).collect::<Vec<i64>>());
            qb.push(")");
        }
    }
}

#[async_trait]
impl SearchIndex for PgSearchIndex {
    async fn apply(&self, ops: &[IndexOp], checkpoint: Option<&Checkpoint>) -> Result<(), PortError> {
        let mut tx = self.pool.begin().await.map_err(port_error)?;
        if let Some(checkpoint) = checkpoint {
            // Every replica reads every partition. Serialize each partition's batches,
            // including the first batch before an offset row exists.
            sqlx::query("SELECT pg_advisory_xact_lock(hashtext($1), $2)")
                .bind(&checkpoint.topic)
                .bind(checkpoint.partition)
                .execute(&mut *tx)
                .await
                .map_err(port_error)?;
            let current: Option<i64> =
                sqlx::query_scalar("SELECT next_offset FROM search_offsets WHERE topic = $1 AND partition = $2")
                    .bind(&checkpoint.topic)
                    .bind(checkpoint.partition)
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(port_error)?;
            if current.is_some_and(|offset| offset >= checkpoint.next_offset) {
                return tx.commit().await.map_err(port_error);
            }
        }
        for op in ops {
            match op {
                IndexOp::Upsert(entry) => {
                    sqlx::query(
                        "INSERT INTO search_messages \
                         (channel_id, message_id, guild_id, author_id, tsv, mentions, has, edited_ms) \
                         VALUES ($1, $2, $3, $4, to_tsvector('simple', $5), $6, $7, $8) \
                         ON CONFLICT (channel_id, message_id) DO UPDATE SET \
                           guild_id = EXCLUDED.guild_id, author_id = EXCLUDED.author_id, tsv = EXCLUDED.tsv, \
                           mentions = EXCLUDED.mentions, has = EXCLUDED.has, edited_ms = EXCLUDED.edited_ms \
                         WHERE search_messages.edited_ms <= EXCLUDED.edited_ms",
                    )
                    .bind(db_id(entry.channel_id))
                    .bind(db_id(entry.message_id))
                    .bind(entry.guild_id.map(db_id))
                    .bind(db_id(entry.author_id))
                    .bind(&entry.content)
                    .bind(entry.mentions.iter().map(|u| db_id(*u)).collect::<Vec<i64>>())
                    .bind(i16::from(entry.has.bits()))
                    .bind(entry.edited_at_ms as i64)
                    .execute(&mut *tx)
                    .await
                    .map_err(port_error)?;
                }
                IndexOp::Delete {
                    channel_id,
                    message_ids,
                } => {
                    sqlx::query("DELETE FROM search_messages WHERE channel_id = $1 AND message_id = ANY($2)")
                        .bind(db_id(*channel_id))
                        .bind(message_ids.iter().map(|i| db_id(*i)).collect::<Vec<i64>>())
                        .execute(&mut *tx)
                        .await
                        .map_err(port_error)?;
                }
            }
        }
        if let Some(checkpoint) = checkpoint {
            sqlx::query(
                "INSERT INTO search_offsets (topic, partition, next_offset) VALUES ($1, $2, $3) \
                 ON CONFLICT (topic, partition) DO UPDATE SET next_offset = EXCLUDED.next_offset",
            )
            .bind(&checkpoint.topic)
            .bind(checkpoint.partition)
            .bind(checkpoint.next_offset)
            .execute(&mut *tx)
            .await
            .map_err(port_error)?;
        }
        tx.commit().await.map_err(port_error)
    }

    async fn checkpoint(&self, topic: &str, partition: i32) -> Result<Option<i64>, PortError> {
        let row = sqlx::query("SELECT next_offset FROM search_offsets WHERE topic = $1 AND partition = $2")
            .bind(topic)
            .bind(partition)
            .fetch_optional(&self.pool)
            .await
            .map_err(port_error)?;
        row.map(|r| r.try_get::<i64, _>(0).map_err(port_error)).transpose()
    }

    async fn guild_channels(&self, guild_id: GuildId, only: &[ChannelId]) -> Result<Vec<ChannelId>, PortError> {
        let rows = sqlx::query(
            "SELECT DISTINCT channel_id FROM search_messages \
             WHERE guild_id = $1 AND (cardinality($2::bigint[]) = 0 OR channel_id = ANY($2))",
        )
        .bind(db_id(guild_id))
        .bind(only.iter().map(|c| db_id(*c)).collect::<Vec<i64>>())
        .fetch_all(&self.pool)
        .await
        .map_err(port_error)?;
        rows.iter()
            .map(|r| r.try_get::<i64, _>(0).map(snowflake).map_err(port_error))
            .collect()
    }

    async fn search(&self, request: &SearchRequest) -> Result<SearchPage, PortError> {
        let f = &request.filters;
        let mut page = QueryBuilder::<Postgres>::new("SELECT channel_id, message_id FROM search_messages");
        push_filters(&mut page, request);
        page.push(" ORDER BY ");
        let direction = if f.sort_order == SortOrder::Asc { "ASC" } else { "DESC" };
        match (f.sort_by, &f.content) {
            (SortBy::Relevance, Some(text)) => {
                page.push("ts_rank_cd(tsv, websearch_to_tsquery('simple', ");
                page.push_bind(text.clone());
                page.push(format!(")) {direction}, message_id {direction}"));
            }
            _ => {
                page.push(format!("message_id {direction}"));
            }
        }
        page.push(" OFFSET ");
        page.push_bind(i64::from(f.offset));
        page.push(" LIMIT ");
        page.push_bind(i64::from(f.limit));
        let rows = page.build().fetch_all(&self.pool).await.map_err(port_error)?;
        let hits = rows
            .iter()
            .map(|r| {
                Ok(SearchHit {
                    channel_id: snowflake(r.try_get::<i64, _>(0).map_err(port_error)?),
                    message_id: snowflake(r.try_get::<i64, _>(1).map_err(port_error)?),
                })
            })
            .collect::<Result<Vec<_>, PortError>>()?;

        let mut count = QueryBuilder::<Postgres>::new("SELECT count(*) FROM (SELECT 1 FROM search_messages");
        push_filters(&mut count, request);
        count.push(" LIMIT ");
        count.push_bind(COUNT_CAP);
        count.push(") capped");
        let total: i64 = count
            .build()
            .fetch_one(&self.pool)
            .await
            .map_err(port_error)?
            .try_get(0)
            .map_err(port_error)?;
        Ok(SearchPage {
            total: total.max(0) as u64,
            hits,
        })
    }

    async fn backfill_state(&self) -> Result<BackfillState, PortError> {
        let row = sqlx::query("SELECT value FROM search_state WHERE key = $1")
            .bind(BACKFILL_KEY)
            .fetch_optional(&self.pool)
            .await
            .map_err(port_error)?;
        let value: Option<String> = row.map(|r| r.try_get(0)).transpose().map_err(port_error)?;
        Ok(match value.as_deref() {
            Some("complete") => BackfillState::Complete,
            Some("running") => BackfillState::Running,
            _ => BackfillState::Idle,
        })
    }

    async fn set_backfill_state(&self, state: BackfillState) -> Result<(), PortError> {
        let value = match state {
            BackfillState::Idle => "idle",
            BackfillState::Running => "running",
            BackfillState::Complete => "complete",
        };
        sqlx::query(
            "INSERT INTO search_state (key, value) VALUES ($1, $2) \
             ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
        )
        .bind(BACKFILL_KEY)
        .bind(value)
        .execute(&self.pool)
        .await
        .map(|_| ())
        .map_err(port_error)
    }
}

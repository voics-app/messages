//! Full-text message search on PostgreSQL (`tsvector` + GIN), fed from
//! `discordy.message.v1` by [`consumer`]. See ADR 0018.

pub mod consumer;
pub mod events;
mod postgres;

pub use postgres::{PgSearchIndex, connect};

use async_trait::async_trait;
use discordy_messages_application::PortError;
use discordy_messages_application::ports::{
    BackfillState, Checkpoint, IndexOp, SearchIndex, SearchPage, SearchRequest,
};
use discordy_messages_domain::{ChannelId, GuildId};

/// Stands in when `SEARCH_DATABASE_URL` is not configured: every search
/// answers "unavailable" and nothing is indexed.
pub struct DisabledSearchIndex;

fn disabled() -> PortError {
    PortError::new("search: no search database is configured")
}

#[async_trait]
impl SearchIndex for DisabledSearchIndex {
    async fn apply(&self, _ops: &[IndexOp], _checkpoint: Option<&Checkpoint>) -> Result<(), PortError> {
        Err(disabled())
    }

    async fn checkpoint(&self, _topic: &str, _partition: i32) -> Result<Option<i64>, PortError> {
        Err(disabled())
    }

    async fn guild_channels(&self, _guild_id: GuildId, _only: &[ChannelId]) -> Result<Vec<ChannelId>, PortError> {
        Err(disabled())
    }

    async fn search(&self, _request: &SearchRequest) -> Result<SearchPage, PortError> {
        Err(disabled())
    }

    async fn backfill_state(&self) -> Result<BackfillState, PortError> {
        Err(disabled())
    }

    async fn set_backfill_state(&self, _state: BackfillState) -> Result<(), PortError> {
        Err(disabled())
    }
}

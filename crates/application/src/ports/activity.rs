use async_trait::async_trait;
use discordy_messages_domain::MessageId;

use crate::error::PortError;

/// Message counts for the admin dashboard. Stores ids and timestamps only,
/// never content.
#[async_trait]
pub trait ActivityRepository: Send + Sync {
    async fn record_standard_message(&self, message_id: MessageId) -> Result<(), PortError>;

    /// Standard messages created at or after `since_unix_ms`.
    async fn count_standard_since(&self, since_unix_ms: u64) -> Result<u64, PortError>;
}

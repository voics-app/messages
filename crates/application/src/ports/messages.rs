use async_trait::async_trait;
use discordy_messages_domain::{Bucket, ChannelId, Message, MessageId, ScanDirection};

use crate::error::PortError;

/// One query against one `(channel_id, bucket)` partition. Hashable so that
/// identical concurrent queries can be coalesced.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BucketQuery {
    pub channel_id: ChannelId,
    pub bucket: Bucket,
    pub direction: ScanDirection,
    /// Exclusive bound; `None` reads from the top of the partition.
    pub bound: Option<MessageId>,
    pub limit: usize,
}

#[async_trait]
pub trait MessageRepository: Send + Sync {
    /// Stores a new message and advances the channel's last message id.
    async fn insert(&self, message: &Message) -> Result<(), PortError>;

    /// Atomically stores an opener and its durable marker in the same partition.
    async fn insert_forum_once(&self, _marker: &Message, _message: &Message) -> Result<bool, PortError> {
        Err(PortError::new("forum reservation unavailable"))
    }
    /// Marker survives deletion of the visible opening message.
    async fn forum_marker(
        &self,
        _channel_id: ChannelId,
    ) -> Result<Option<(discordy_messages_domain::UserId, MessageId, String)>, PortError> {
        Err(PortError::new("forum reservation unavailable"))
    }

    /// Overwrites the mutable fields of an existing message.
    async fn update(&self, message: &Message) -> Result<(), PortError>;

    async fn find(&self, channel_id: ChannelId, message_id: MessageId) -> Result<Option<Message>, PortError>;

    async fn delete(&self, channel_id: ChannelId, message_ids: &[MessageId]) -> Result<(), PortError>;

    async fn set_pinned(&self, channel_id: ChannelId, message_id: MessageId, pinned: bool) -> Result<(), PortError>;

    /// Messages of one bucket: newest first for `Older`, oldest first for
    /// `Newer`.
    async fn scan_bucket(&self, query: BucketQuery) -> Result<Vec<Message>, PortError>;

    /// Exact count in one partition. Adapters may override the paged fallback.
    async fn count_bucket(&self, channel_id: ChannelId, bucket: Bucket) -> Result<u64, PortError> {
        let mut count = 0u64;
        let mut bound = None;
        loop {
            let page = self
                .scan_bucket(BucketQuery {
                    channel_id,
                    bucket,
                    direction: ScanDirection::Newer,
                    bound,
                    limit: 100,
                })
                .await?;
            if page.is_empty() {
                break;
            }
            count += page.len() as u64;
            bound = page.last().map(|m| m.id);
            if page.len() < 100 {
                break;
            }
        }
        Ok(count)
    }

    async fn last_message_id(&self, channel_id: ChannelId) -> Result<Option<MessageId>, PortError>;

    /// Every channel that has ever held a message, with its newest message id
    /// (a full scan of a small table: used by the search backfill only).
    async fn channels_with_messages(&self) -> Result<Vec<(ChannelId, MessageId)>, PortError>;
}

//! Coalesced read path for history. Every partition read goes through a
//! [`Coalescer`], so a burst of identical reads (everyone opening the same
//! busy channel at once) costs a single ScyllaDB query per partition.

use std::sync::Arc;

use discordy_messages_domain::{Bucket, ChannelId, Message, MessageId, ReactionRow};

use crate::coalescer::Coalescer;
use crate::error::PortError;
use crate::ports::{BucketQuery, MessageRepository, ReactionRepository};

type Shared<T> = Result<Arc<T>, PortError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct ReactionRangeKey {
    channel_id: ChannelId,
    bucket: Bucket,
    oldest: MessageId,
    newest: MessageId,
}

pub struct HistoryReader {
    messages: Arc<dyn MessageRepository>,
    reactions: Arc<dyn ReactionRepository>,
    bucket_reads: Coalescer<BucketQuery, Shared<Vec<Message>>>,
    newest_reads: Coalescer<ChannelId, Result<Option<MessageId>, PortError>>,
    reaction_reads: Coalescer<ReactionRangeKey, Shared<Vec<ReactionRow>>>,
}

impl HistoryReader {
    pub fn new(messages: Arc<dyn MessageRepository>, reactions: Arc<dyn ReactionRepository>) -> Self {
        Self {
            messages,
            reactions,
            bucket_reads: Coalescer::new(),
            newest_reads: Coalescer::new(),
            reaction_reads: Coalescer::new(),
        }
    }

    pub async fn scan_bucket(&self, query: BucketQuery) -> Shared<Vec<Message>> {
        let messages = Arc::clone(&self.messages);
        self.bucket_reads
            .run(query, move || async move { messages.scan_bucket(query).await.map(Arc::new) })
            .await
    }

    pub async fn last_message_id(&self, channel_id: ChannelId) -> Result<Option<MessageId>, PortError> {
        let messages = Arc::clone(&self.messages);
        self.newest_reads
            .run(channel_id, move || async move { messages.last_message_id(channel_id).await })
            .await
    }

    pub async fn reactions_in_range(
        &self,
        channel_id: ChannelId,
        bucket: Bucket,
        oldest: MessageId,
        newest: MessageId,
    ) -> Shared<Vec<ReactionRow>> {
        let key = ReactionRangeKey {
            channel_id,
            bucket,
            oldest,
            newest,
        };
        let reactions = Arc::clone(&self.reactions);
        self.reaction_reads
            .run(key, move || async move {
                reactions
                    .for_range(channel_id, bucket, oldest, newest)
                    .await
                    .map(Arc::new)
            })
            .await
    }
}

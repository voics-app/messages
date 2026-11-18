//! Snapshot of a user's channel read position and unread mention count.
//!
//! Mutations require the atomic ReadStateRepository operations. This aggregate
//! cannot safely implement ACK or increment without the delivery ledger.

use crate::{ChannelId, MessageId, UserId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadState {
    pub user_id: UserId,
    pub channel_id: ChannelId,
    pub last_read_id: Option<MessageId>,
    pub mention_count: u32,
}

impl ReadState {
    pub fn unread(user_id: UserId, channel_id: ChannelId) -> Self {
        Self {
            user_id,
            channel_id,
            last_read_id: None,
            mention_count: 0,
        }
    }
}

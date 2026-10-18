//! Per-user, per-channel read position and unread mention count.

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

    /// Marks everything up to `message_id` as read. The read position never
    /// moves backwards; acknowledging clears the mention count.
    pub fn acknowledge(&mut self, message_id: MessageId) {
        self.last_read_id = Some(self.last_read_id.map_or(message_id, |last| last.max(message_id)));
        self.mention_count = 0;
    }

    /// Counts a new mention unless the user has already read past it.
    pub fn record_mention(&mut self, message_id: MessageId) {
        if self.last_read_id.is_none_or(|last| message_id > last) {
            self.mention_count = self.mention_count.saturating_add(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Snowflake;

    fn state() -> ReadState {
        ReadState::unread(Snowflake::new(1), Snowflake::new(2))
    }

    #[test]
    fn acknowledge_moves_forward_and_clears_mentions() {
        let mut s = state();
        s.record_mention(Snowflake::new(10));
        s.acknowledge(Snowflake::new(10));
        assert_eq!(s.last_read_id, Some(Snowflake::new(10)));
        assert_eq!(s.mention_count, 0);
    }

    #[test]
    fn acknowledge_never_moves_backwards() {
        let mut s = state();
        s.acknowledge(Snowflake::new(20));
        s.acknowledge(Snowflake::new(10));
        assert_eq!(s.last_read_id, Some(Snowflake::new(20)));
    }

    #[test]
    fn mentions_before_the_read_position_are_ignored() {
        let mut s = state();
        s.acknowledge(Snowflake::new(20));
        s.record_mention(Snowflake::new(15));
        assert_eq!(s.mention_count, 0);
        s.record_mention(Snowflake::new(25));
        s.record_mention(Snowflake::new(26));
        assert_eq!(s.mention_count, 2);
    }
}

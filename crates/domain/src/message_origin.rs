//! What produced a message besides a plain user: a webhook (with optional
//! display overrides) or an interaction response.

use crate::Snowflake;

/// Discord's ephemeral message flag: delivered only to the invoking user.
pub const FLAG_EPHEMERAL: u32 = 1 << 6;

/// Set on messages posted through a webhook. The message's author is the
/// webhook itself, so `username` and `avatar_url` override its display.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebhookAuthor {
    pub webhook_id: Snowflake,
    pub username: Option<String>,
    pub avatar_url: Option<String>,
}

/// Links a message to the interaction whose response it is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InteractionLink {
    pub interaction_id: Snowflake,
    pub application_id: Snowflake,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ephemeral_is_discords_flag_64() {
        assert_eq!(FLAG_EPHEMERAL, 64);
    }
}

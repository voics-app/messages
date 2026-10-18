//! The subset of Discord's permission bitfield that governs messages.
//!
//! Bit values are Discord's documented ones (see `contracts/permissions.json`).
//! The api service computes the effective bitfield for a channel; this module
//! only interprets it.

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Permissions(u64);

impl Permissions {
    pub const ADMINISTRATOR: u64 = 1 << 3;
    pub const ADD_REACTIONS: u64 = 1 << 6;
    pub const VIEW_CHANNEL: u64 = 1 << 10;
    pub const SEND_MESSAGES: u64 = 1 << 11;
    pub const MANAGE_MESSAGES: u64 = 1 << 13;
    pub const ATTACH_FILES: u64 = 1 << 15;
    pub const READ_MESSAGE_HISTORY: u64 = 1 << 16;
    pub const MENTION_EVERYONE: u64 = 1 << 17;
    pub const SEND_MESSAGES_IN_THREADS: u64 = 1 << 38;
    pub const PIN_MESSAGES: u64 = 1 << 51;

    pub const fn new(bits: u64) -> Self {
        Self(bits)
    }

    pub const fn bits(self) -> u64 {
        self.0
    }

    /// True when every bit of `required` is granted. ADMINISTRATOR grants all.
    pub const fn has(self, required: u64) -> bool {
        self.0 & Self::ADMINISTRATOR != 0 || self.0 & required == required
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_discord_bit_values() {
        assert_eq!(Permissions::ADMINISTRATOR, 8);
        assert_eq!(Permissions::VIEW_CHANNEL, 1024);
        assert_eq!(Permissions::SEND_MESSAGES, 2048);
        assert_eq!(Permissions::READ_MESSAGE_HISTORY, 65536);
    }

    #[test]
    fn requires_every_requested_bit() {
        let p = Permissions::new(Permissions::VIEW_CHANNEL);
        assert!(p.has(Permissions::VIEW_CHANNEL));
        assert!(!p.has(Permissions::VIEW_CHANNEL | Permissions::SEND_MESSAGES));
    }

    #[test]
    fn administrator_grants_everything() {
        let p = Permissions::new(Permissions::ADMINISTRATOR);
        assert!(p.has(Permissions::MANAGE_MESSAGES | Permissions::ATTACH_FILES));
    }
}

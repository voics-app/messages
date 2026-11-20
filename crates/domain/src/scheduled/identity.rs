use crate::DomainError;
use std::{fmt, str::FromStr};

/// Canonical lowercase UUIDv4. Randomness is supplied by the client/adapter, not this pure type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ScheduleId(u128);

impl ScheduleId {
    pub fn bits(self) -> u128 {
        self.0
    }
}

impl FromStr for ScheduleId {
    type Err = DomainError;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        let invalid = || DomainError::invalid("request_id", "must be a lowercase UUIDv4");
        if raw.len() != 36
            || !raw.bytes().enumerate().all(|(index, byte)| {
                if [8, 13, 18, 23].contains(&index) {
                    byte == b'-'
                } else {
                    byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)
                }
            })
            || raw.as_bytes()[14] != b'4'
            || !b"89ab".contains(&raw.as_bytes()[19])
        {
            return Err(invalid());
        }
        u128::from_str_radix(&raw.replace('-', ""), 16)
            .map(Self)
            .map_err(|_| invalid())
    }
}

impl fmt::Display for ScheduleId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
            self.0 >> 96,
            (self.0 >> 80) & 0xffff,
            (self.0 >> 64) & 0xffff,
            (self.0 >> 48) & 0xffff,
            self.0 & 0xffffffffffff
        )
    }
}

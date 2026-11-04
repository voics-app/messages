//! Snowflake identifiers (pinned interface P5).
//!
//! Layout: 42 bits of milliseconds since the Voics epoch, 10 bits of worker
//! id and 12 bits of per-millisecond sequence.

use std::fmt;
use std::str::FromStr;

use crate::error::DomainError;

/// 2026-01-01T00:00:00Z in Unix milliseconds.
pub const DISCORDY_EPOCH_MS: u64 = 1_767_225_600_000;

const TIMESTAMP_SHIFT: u32 = 22;
const WORKER_SHIFT: u32 = 12;
const MAX_SEQUENCE: u16 = 0x0FFF;

/// The worker ids reserved for the messages service.
pub const MESSAGES_WORKER_RANGE: std::ops::RangeInclusive<u16> = 64..=127;

/// A 64-bit, time-ordered identifier.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Snowflake(u64);

impl Snowflake {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn value(self) -> u64 {
        self.0
    }

    /// Milliseconds since the Voics epoch encoded in this id.
    pub const fn epoch_millis(self) -> u64 {
        self.0 >> TIMESTAMP_SHIFT
    }

    /// Unix milliseconds encoded in this id.
    pub const fn unix_millis(self) -> u64 {
        self.epoch_millis() + DISCORDY_EPOCH_MS
    }

    /// The smallest possible id created at `unix_ms`. Useful as a range bound.
    pub const fn first_at_unix_millis(unix_ms: u64) -> Self {
        Self(unix_ms.saturating_sub(DISCORDY_EPOCH_MS) << TIMESTAMP_SHIFT)
    }

    /// The id immediately after this one, used to turn an inclusive bound
    /// into an exclusive one.
    pub const fn successor(self) -> Self {
        Self(self.0.saturating_add(1))
    }
}

impl fmt::Display for Snowflake {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for Snowflake {
    type Err = DomainError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse::<u64>()
            .map(Self)
            .map_err(|_| DomainError::invalid("id", "must be a decimal snowflake"))
    }
}

/// A worker id validated against the messages service range (64–127).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkerId(u16);

impl WorkerId {
    pub fn for_messages(value: u16) -> Result<Self, DomainError> {
        if MESSAGES_WORKER_RANGE.contains(&value) {
            Ok(Self(value))
        } else {
            Err(DomainError::invalid(
                "SNOWFLAKE_WORKER_ID",
                "must be between 64 and 127 for the messages service",
            ))
        }
    }

    pub const fn value(self) -> u16 {
        self.0
    }
}

/// Issues strictly increasing Snowflakes for one worker.
///
/// The generator is deterministic: the caller supplies the current time, which
/// keeps the domain free of clocks. When the clock moves backwards, or more
/// than 4096 ids are requested in one millisecond, the generator borrows from
/// the next millisecond instead of blocking, so ids never repeat.
#[derive(Debug)]
pub struct SnowflakeGenerator {
    worker: WorkerId,
    last_epoch_ms: u64,
    sequence: u16,
}

impl SnowflakeGenerator {
    pub fn new(worker: WorkerId) -> Self {
        Self {
            worker,
            last_epoch_ms: 0,
            sequence: 0,
        }
    }

    pub fn next_id(&mut self, now_unix_ms: u64) -> Snowflake {
        let now = now_unix_ms.saturating_sub(DISCORDY_EPOCH_MS);
        if now > self.last_epoch_ms {
            self.last_epoch_ms = now;
            self.sequence = 0;
        } else if self.sequence == MAX_SEQUENCE {
            self.last_epoch_ms += 1;
            self.sequence = 0;
        } else {
            self.sequence += 1;
        }
        Snowflake(
            (self.last_epoch_ms << TIMESTAMP_SHIFT)
                | (u64::from(self.worker.value()) << WORKER_SHIFT)
                | u64::from(self.sequence),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOW: u64 = DISCORDY_EPOCH_MS + 1_000_000;

    fn generator() -> SnowflakeGenerator {
        SnowflakeGenerator::new(WorkerId::for_messages(64).unwrap())
    }

    #[test]
    fn worker_id_must_be_in_messages_range() {
        assert!(WorkerId::for_messages(63).is_err());
        assert!(WorkerId::for_messages(64).is_ok());
        assert!(WorkerId::for_messages(127).is_ok());
        assert!(WorkerId::for_messages(128).is_err());
    }

    #[test]
    fn encodes_timestamp_worker_and_sequence() {
        let id = generator().next_id(NOW);
        assert_eq!(id.epoch_millis(), 1_000_000);
        assert_eq!(id.unix_millis(), NOW);
        assert_eq!((id.value() >> 12) & 0x3FF, 64);
        assert_eq!(id.value() & 0xFFF, 0);
    }

    #[test]
    fn ids_within_one_millisecond_increase_by_sequence() {
        let mut g = generator();
        let a = g.next_id(NOW);
        let b = g.next_id(NOW);
        assert_eq!(b.value(), a.value() + 1);
    }

    #[test]
    fn sequence_overflow_borrows_the_next_millisecond() {
        let mut g = generator();
        let mut last = g.next_id(NOW);
        for _ in 0..5000 {
            let next = g.next_id(NOW);
            assert!(next > last);
            last = next;
        }
        assert_eq!(last.epoch_millis(), 1_000_001);
    }

    #[test]
    fn clock_going_backwards_never_repeats_ids() {
        let mut g = generator();
        let a = g.next_id(NOW);
        let b = g.next_id(NOW - 500);
        assert!(b > a);
    }

    #[test]
    fn parses_and_displays_decimal_strings() {
        let id: Snowflake = "175928847299117063".parse().unwrap();
        assert_eq!(id.to_string(), "175928847299117063");
        assert!("abc".parse::<Snowflake>().is_err());
    }

    #[test]
    fn first_at_unix_millis_is_a_lower_bound() {
        let id = generator().next_id(NOW);
        let bound = Snowflake::first_at_unix_millis(NOW);
        assert!(bound <= id);
        assert_eq!(bound.unix_millis(), NOW);
    }
}

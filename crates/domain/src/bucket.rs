//! Time buckets that partition a channel's messages (pinned interface P5).
//!
//! `bucket = floor((snowflake >> 22) / 864_000_000)`: every ten days of
//! messages in a channel share one ScyllaDB partition `(channel_id, bucket)`.

use crate::snowflake::Snowflake;

/// Ten days in milliseconds.
pub const BUCKET_SIZE_MS: u64 = 864_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Bucket(i32);

impl Bucket {
    pub const fn new(value: i32) -> Self {
        Self(value)
    }

    pub const fn value(self) -> i32 {
        self.0
    }

    /// The bucket that a Snowflake (message, channel or bound) falls into.
    pub fn of(id: Snowflake) -> Self {
        let index = id.epoch_millis() / BUCKET_SIZE_MS;
        Self(i32::try_from(index).unwrap_or(i32::MAX))
    }

    /// Buckets from `self` down to `floor`, newest first. Empty when `floor`
    /// is newer than `self`.
    pub fn descending_to(self, floor: Bucket) -> Vec<Bucket> {
        (floor.0..=self.0).rev().map(Bucket).collect()
    }

    /// Buckets from `self` up to `ceiling`, oldest first. Empty when
    /// `ceiling` is older than `self`.
    pub fn ascending_to(self, ceiling: Bucket) -> Vec<Bucket> {
        (self.0..=ceiling.0).map(Bucket).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id_at_epoch_ms(ms: u64) -> Snowflake {
        Snowflake::new(ms << 22)
    }

    #[test]
    fn first_ten_days_are_bucket_zero() {
        assert_eq!(Bucket::of(id_at_epoch_ms(0)), Bucket::new(0));
        assert_eq!(Bucket::of(id_at_epoch_ms(BUCKET_SIZE_MS - 1)), Bucket::new(0));
    }

    #[test]
    fn bucket_boundaries_are_every_ten_days() {
        assert_eq!(Bucket::of(id_at_epoch_ms(BUCKET_SIZE_MS)), Bucket::new(1));
        assert_eq!(Bucket::of(id_at_epoch_ms(BUCKET_SIZE_MS * 37 + 5)), Bucket::new(37));
    }

    #[test]
    fn worker_and_sequence_bits_do_not_change_the_bucket() {
        let base = id_at_epoch_ms(BUCKET_SIZE_MS * 3 - 1).value();
        assert_eq!(Bucket::of(Snowflake::new(base | 0x3F_FFFF)), Bucket::new(2));
    }

    #[test]
    fn descending_walk_is_newest_first_and_inclusive() {
        let walk = Bucket::new(5).descending_to(Bucket::new(3));
        assert_eq!(walk, vec![Bucket::new(5), Bucket::new(4), Bucket::new(3)]);
        assert!(Bucket::new(2).descending_to(Bucket::new(3)).is_empty());
    }

    #[test]
    fn ascending_walk_is_oldest_first_and_inclusive() {
        let walk = Bucket::new(3).ascending_to(Bucket::new(4));
        assert_eq!(walk, vec![Bucket::new(3), Bucket::new(4)]);
        assert!(Bucket::new(5).ascending_to(Bucket::new(4)).is_empty());
    }
}

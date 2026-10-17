//! History paging across buckets.
//!
//! A page request is turned into one or two *scans*. Each scan lists the
//! buckets to visit in order; the caller queries them one at a time and stops
//! as soon as the scan's limit is reached. Older scans walk backwards and stop
//! at the channel's creation bucket (a channel can't hold messages older than
//! itself); newer scans walk forwards up to the newest message's bucket.

use crate::bucket::Bucket;
use crate::error::DomainError;
use crate::{ChannelId, MessageId};

const DEFAULT_LIMIT: u32 = 50;
const MAX_LIMIT: u32 = 100;

/// Where a page starts (Discord's `before` / `after` / `around`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HistoryCursor {
    Latest,
    Before(MessageId),
    After(MessageId),
    Around(MessageId),
}

/// Number of messages per page: 1–100, default 50.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PageLimit(usize);

impl PageLimit {
    pub fn parse(raw: Option<u32>) -> Result<Self, DomainError> {
        match raw.unwrap_or(DEFAULT_LIMIT) {
            limit @ 1..=MAX_LIMIT => Ok(Self(limit as usize)),
            _ => Err(DomainError::invalid("limit", "must be between 1 and 100")),
        }
    }

    pub const fn value(self) -> usize {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ScanDirection {
    /// Newest first, ids strictly below `bound` (or all, when unbounded).
    Older,
    /// Oldest first, ids strictly above `bound`.
    Newer,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Scan {
    pub direction: ScanDirection,
    /// Exclusive bound; `None` means "from the newest message".
    pub bound: Option<MessageId>,
    pub limit: usize,
    pub buckets: Vec<Bucket>,
}

/// Plans the scans for one page. `newest` is the channel's last message id,
/// or `None` when the channel has never had a message.
pub fn plan(cursor: HistoryCursor, limit: PageLimit, channel_id: ChannelId, newest: Option<MessageId>) -> Vec<Scan> {
    let Some(newest) = newest else {
        return Vec::new();
    };
    let floor = Bucket::of(channel_id);
    let limit = limit.value();
    let scans = match cursor {
        HistoryCursor::Latest => vec![older(None, limit, newest, floor)],
        HistoryCursor::Before(id) => vec![older(Some(id), limit, newest, floor)],
        HistoryCursor::After(id) => vec![newer(id, limit, newest, floor)],
        HistoryCursor::Around(id) => {
            let after = limit / 2;
            vec![
                older(Some(id.successor()), limit - after, newest, floor),
                newer(id, after, newest, floor),
            ]
        }
    };
    scans
        .into_iter()
        .filter(|scan| scan.limit > 0 && !scan.buckets.is_empty())
        .collect()
}

fn older(bound: Option<MessageId>, limit: usize, newest: MessageId, floor: Bucket) -> Scan {
    let start = bound.map_or(Bucket::of(newest), |b| Bucket::of(b).min(Bucket::of(newest)));
    Scan {
        direction: ScanDirection::Older,
        bound,
        limit,
        buckets: start.descending_to(floor),
    }
}

fn newer(bound: MessageId, limit: usize, newest: MessageId, floor: Bucket) -> Scan {
    let buckets = if bound >= newest {
        Vec::new()
    } else {
        Bucket::of(bound).max(floor).ascending_to(Bucket::of(newest))
    };
    Scan {
        direction: ScanDirection::Newer,
        bound: Some(bound),
        limit,
        buckets,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Snowflake;
    use crate::bucket::BUCKET_SIZE_MS;

    /// An id in bucket `b`, `offset` ms into it.
    fn id_in(b: u64, offset: u64) -> Snowflake {
        Snowflake::new((b * BUCKET_SIZE_MS + offset) << 22)
    }

    fn buckets(values: &[i32]) -> Vec<Bucket> {
        values.iter().copied().map(Bucket::new).collect()
    }

    fn limit(n: u32) -> PageLimit {
        PageLimit::parse(Some(n)).unwrap()
    }

    const CHANNEL_BUCKET: u64 = 3;

    fn channel() -> Snowflake {
        id_in(CHANNEL_BUCKET, 0)
    }

    #[test]
    fn limit_defaults_to_50_and_is_bounded() {
        assert_eq!(PageLimit::parse(None).unwrap().value(), 50);
        assert!(PageLimit::parse(Some(0)).is_err());
        assert!(PageLimit::parse(Some(101)).is_err());
        assert_eq!(limit(100).value(), 100);
    }

    #[test]
    fn empty_channels_need_no_scans() {
        assert!(plan(HistoryCursor::Latest, limit(50), channel(), None).is_empty());
    }

    #[test]
    fn latest_walks_back_from_the_newest_bucket_to_the_creation_bucket() {
        let scans = plan(HistoryCursor::Latest, limit(50), channel(), Some(id_in(6, 1)));
        assert_eq!(
            scans,
            vec![Scan {
                direction: ScanDirection::Older,
                bound: None,
                limit: 50,
                buckets: buckets(&[6, 5, 4, 3]),
            }]
        );
    }

    #[test]
    fn before_starts_at_the_cursor_bucket() {
        let before = id_in(5, 9);
        let scans = plan(HistoryCursor::Before(before), limit(10), channel(), Some(id_in(8, 0)));
        assert_eq!(scans[0].buckets, buckets(&[5, 4, 3]));
        assert_eq!(scans[0].bound, Some(before));
    }

    #[test]
    fn before_a_future_id_skips_empty_recent_buckets() {
        let scans = plan(
            HistoryCursor::Before(id_in(40, 0)),
            limit(10),
            channel(),
            Some(id_in(4, 0)),
        );
        assert_eq!(scans[0].buckets, buckets(&[4, 3]));
    }

    #[test]
    fn after_walks_forward_to_the_newest_bucket() {
        let after = id_in(4, 2);
        let scans = plan(HistoryCursor::After(after), limit(25), channel(), Some(id_in(6, 0)));
        assert_eq!(
            scans,
            vec![Scan {
                direction: ScanDirection::Newer,
                bound: Some(after),
                limit: 25,
                buckets: buckets(&[4, 5, 6]),
            }]
        );
    }

    #[test]
    fn after_the_newest_message_is_empty() {
        let newest = id_in(6, 0);
        assert!(plan(HistoryCursor::After(newest), limit(25), channel(), Some(newest)).is_empty());
    }

    #[test]
    fn after_an_id_older_than_the_channel_starts_at_the_creation_bucket() {
        let scans = plan(
            HistoryCursor::After(id_in(0, 0)),
            limit(5),
            channel(),
            Some(id_in(4, 0)),
        );
        assert_eq!(scans[0].buckets, buckets(&[3, 4]));
    }

    #[test]
    fn around_splits_the_limit_and_includes_the_target() {
        let target = id_in(5, 0);
        let scans = plan(HistoryCursor::Around(target), limit(5), channel(), Some(id_in(6, 0)));
        assert_eq!(scans.len(), 2);
        assert_eq!(scans[0].direction, ScanDirection::Older);
        assert_eq!(scans[0].bound, Some(target.successor()));
        assert_eq!(scans[0].limit, 3);
        assert_eq!(scans[1].direction, ScanDirection::Newer);
        assert_eq!(scans[1].bound, Some(target));
        assert_eq!(scans[1].limit, 2);
    }

    #[test]
    fn around_with_limit_one_only_fetches_the_target() {
        let target = id_in(5, 0);
        let scans = plan(HistoryCursor::Around(target), limit(1), channel(), Some(id_in(6, 0)));
        assert_eq!(scans.len(), 1);
        assert_eq!(scans[0].direction, ScanDirection::Older);
    }
}

//! A small fixed-window limiter keyed by user, for expensive endpoints.

use std::sync::Arc;

use dashmap::DashMap;
use discordy_messages_domain::UserId;

use crate::ports::Clock;

/// Entries older than this many windows are dropped when the map grows.
const SWEEP_THRESHOLD: usize = 10_000;

pub struct RateLimiter {
    limit: u32,
    window_ms: u64,
    clock: Arc<dyn Clock>,
    windows: DashMap<UserId, (u64, u32)>,
}

impl RateLimiter {
    pub fn new(clock: Arc<dyn Clock>, limit: u32, window_ms: u64) -> Self {
        Self {
            limit,
            window_ms,
            clock,
            windows: DashMap::new(),
        }
    }

    /// Counts one request. `Err` carries the milliseconds until the window
    /// resets.
    pub fn check(&self, user: UserId) -> Result<(), u64> {
        let now = self.clock.now_unix_ms();
        if self.windows.len() > SWEEP_THRESHOLD {
            self.windows
                .retain(|_, (start, _)| now.saturating_sub(*start) < self.window_ms);
        }
        let mut entry = self.windows.entry(user).or_insert((now, 0));
        let (start, count) = entry.value_mut();
        if now.saturating_sub(*start) >= self.window_ms {
            *start = now;
            *count = 0;
        }
        if *count >= self.limit {
            return Err((*start + self.window_ms).saturating_sub(now).max(1));
        }
        *count += 1;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU64;

    use discordy_messages_domain::Snowflake;

    use super::*;
    use crate::fakes::FakeClock;

    #[test]
    fn allows_the_limit_then_blocks_until_the_window_resets() {
        let clock = Arc::new(FakeClock(AtomicU64::new(1_000)));
        let limiter = RateLimiter::new(clock.clone(), 2, 10_000);
        let user = Snowflake::new(1);
        assert!(limiter.check(user).is_ok());
        assert!(limiter.check(user).is_ok());
        clock.advance_ms(4_000);
        assert_eq!(limiter.check(user), Err(6_000));
        assert!(limiter.check(Snowflake::new(2)).is_ok());
        clock.advance_ms(6_000);
        assert!(limiter.check(user).is_ok());
    }
}

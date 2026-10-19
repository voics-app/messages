//! System clock and the process-wide Snowflake generator.

use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use discordy_messages_application::ports::{Clock, IdGenerator};
use discordy_messages_domain::{Snowflake, SnowflakeGenerator, WorkerId};

pub struct SystemClock;

impl Clock for SystemClock {
    fn now_unix_ms(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_millis() as u64)
    }
}

/// Thread-safe wrapper around the domain generator, reading the system clock.
pub struct SnowflakeIds {
    generator: Mutex<SnowflakeGenerator>,
}

impl SnowflakeIds {
    pub fn new(worker: WorkerId) -> Self {
        Self {
            generator: Mutex::new(SnowflakeGenerator::new(worker)),
        }
    }
}

impl IdGenerator for SnowflakeIds {
    fn next_id(&self) -> Snowflake {
        let now = SystemClock.now_unix_ms();
        self.generator
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .next_id(now)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::sync::Arc;

    #[test]
    fn concurrent_ids_are_unique() {
        let ids = Arc::new(SnowflakeIds::new(WorkerId::for_messages(100).unwrap()));
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let ids = Arc::clone(&ids);
                std::thread::spawn(move || (0..1000).map(|_| ids.next_id()).collect::<Vec<_>>())
            })
            .collect();
        let all: HashSet<_> = handles.into_iter().flat_map(|h| h.join().unwrap()).collect();
        assert_eq!(all.len(), 8000);
    }
}

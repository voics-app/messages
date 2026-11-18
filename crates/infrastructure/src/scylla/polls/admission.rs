//! Bound local hot-poll contention. Independent repositories still contend in
//! Scylla; this optimization is never the mutation's correctness boundary.
use std::sync::Arc;
use std::time::Duration;

use discordy_messages_application::PortError;
use discordy_messages_application::ports::PollKey;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

pub(super) struct Admission {
    outstanding: Arc<Semaphore>,
    stripes: Vec<Arc<Semaphore>>,
}

pub(super) struct Permit {
    _outstanding: OwnedSemaphorePermit,
    _partition: OwnedSemaphorePermit,
}

impl Admission {
    pub fn new() -> Self {
        Self {
            outstanding: Arc::new(Semaphore::new(256)),
            stripes: (0..256).map(|_| Arc::new(Semaphore::new(1))).collect(),
        }
    }

    pub async fn acquire(&self, key: PollKey) -> Result<Permit, PortError> {
        let outstanding = self
            .outstanding
            .clone()
            .try_acquire_owned()
            .map_err(|_| PortError::new("poll admission queue is full"))?;
        let index = ((key.channel_id.value() ^ key.message_id.value()) % self.stripes.len() as u64) as usize;
        let partition = tokio::time::timeout(Duration::from_secs(5), self.stripes[index].clone().acquire_owned())
            .await
            .map_err(|_| PortError::new("poll admission wait exceeded five seconds"))?
            .map_err(|_| PortError::new("poll admission is closed"))?;
        Ok(Permit {
            _outstanding: outstanding,
            _partition: partition,
        })
    }
}

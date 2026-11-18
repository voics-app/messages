//! Bounded buffered expiry; completion refills a slot without another scan.
use super::Polls;
use crate::ports::{PollDueEntry, PollKey};
use crate::{AppResult, PortError};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::{Id, JoinSet};

const MAX_IN_FLIGHT: usize = 4;
const MAX_OUTSTANDING: usize = 400;

pub(super) struct WorkQueue {
    service: Arc<Polls>,
    waiting: VecDeque<PollDueEntry>,
    retained: HashSet<PollKey>,
    running: HashMap<Id, PollKey>,
    tasks: JoinSet<AppResult<bool>>,
    stopped: bool,
}

impl WorkQueue {
    pub fn new(service: Arc<Polls>) -> Self {
        Self {
            service,
            waiting: VecDeque::new(),
            retained: HashSet::new(),
            running: HashMap::new(),
            tasks: JoinSet::new(),
            stopped: false,
        }
    }
    pub fn in_flight(&self) -> usize {
        self.tasks.len()
    }
    pub fn queued(&self) -> usize {
        self.waiting.len()
    }
    pub fn stopped(&self) -> bool {
        self.stopped
    }

    pub fn enqueue(&mut self, entry: PollDueEntry) -> bool {
        if self.retained.contains(&entry.key) {
            return true;
        }
        if self.stopped || self.retained.len() >= MAX_OUTSTANDING {
            return false;
        }
        self.retained.insert(entry.key);
        self.waiting.push_back(entry);
        true
    }
    pub fn fill(&mut self) {
        while !self.stopped && self.tasks.len() < MAX_IN_FLIGHT {
            let Some(entry) = self.waiting.pop_front() else {
                break;
            };
            let service = self.service.clone();
            let task = self.tasks.spawn(async move {
                tokio::time::timeout(Duration::from_secs(10), service.finalize_due(entry))
                    .await
                    .unwrap_or_else(|_| Err(PortError::new("poll finalization timed out").into()))
            });
            self.running.insert(task.id(), entry.key);
        }
    }
    pub async fn next(&mut self) -> Option<AppResult<bool>> {
        let (id, outcome) = match self.tasks.join_next_with_id().await? {
            Ok(result) => result,
            Err(error) => (error.id(), Err(PortError::new("poll finalization task stopped").into())),
        };
        if let Some(key) = self.running.remove(&id) {
            self.retained.remove(&key);
        }
        self.fill();
        Some(outcome)
    }
    pub fn stop(&mut self) {
        self.stopped = true;
        for entry in self.waiting.drain(..) {
            self.retained.remove(&entry.key);
        }
    }
}

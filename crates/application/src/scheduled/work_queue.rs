//! Bounded discovery buffering lets completed dispatches free a slot without waiting for another scan.
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use discordy_messages_domain::scheduled::DispatchOutcome;
use tokio::task::{Id, JoinSet};

use super::ScheduledDispatcher;
use crate::{AppResult, PortError, ports::scheduled::ScheduleKey};

const MAX_IN_FLIGHT: usize = 4;
const MAX_OUTSTANDING: usize = 400;

pub(super) struct WorkQueue {
    dispatcher: Arc<ScheduledDispatcher>,
    waiting: VecDeque<ScheduleKey>,
    scheduled: HashSet<ScheduleKey>,
    running: HashMap<Id, ScheduleKey>,
    tasks: JoinSet<AppResult<Option<DispatchOutcome>>>,
    stopped: bool,
}

impl WorkQueue {
    pub fn new(dispatcher: ScheduledDispatcher) -> Self {
        Self {
            dispatcher: Arc::new(dispatcher),
            waiting: VecDeque::new(),
            scheduled: HashSet::new(),
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

    pub fn enqueue(&mut self, key: ScheduleKey) -> bool {
        if self.scheduled.contains(&key) {
            return true;
        }
        if self.scheduled.len() >= MAX_OUTSTANDING || self.stopped {
            return false;
        }
        self.scheduled.insert(key);
        self.waiting.push_back(key);
        true
    }

    pub fn fill(&mut self) {
        while !self.stopped && self.tasks.len() < MAX_IN_FLIGHT {
            let Some(key) = self.waiting.pop_front() else {
                break;
            };
            let dispatcher = self.dispatcher.clone();
            let task = self.tasks.spawn(async move { dispatcher.execute(key).await });
            self.running.insert(task.id(), key);
        }
    }

    pub async fn next(&mut self) -> Option<AppResult<Option<DispatchOutcome>>> {
        let result = self.tasks.join_next_with_id().await?;
        let (id, outcome) = match result {
            Ok((id, outcome)) => (id, outcome),
            Err(error) => (
                error.id(),
                Err(PortError::new("scheduled dispatch task did not complete").into()),
            ),
        };
        if let Some(key) = self.running.remove(&id) {
            self.scheduled.remove(&key);
        }
        self.fill();
        Some(outcome)
    }

    pub fn stop(&mut self) {
        self.stopped = true;
        for key in self.waiting.drain(..) {
            self.scheduled.remove(&key);
        }
    }
}

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use discordy_messages_domain::UserId;
use discordy_messages_domain::poll::{PollAggregate, PollDefinition, PollError, PollTransition, PollVote};

use crate::PortError;
use crate::ports::{Clock, PollDueEntry, PollKey, PollRepository, PollSnapshotState, PollStoreResult, VoteCommand};

struct State {
    aggregate: PollAggregate,
    voters: HashMap<UserId, PollVote>,
    indexed: bool,
}

pub struct InMemoryPolls {
    rows: Mutex<HashMap<PollKey, State>>,
    clock: Arc<dyn Clock>,
    pub mutations: AtomicUsize,
    pub failure: Mutex<Option<PortError>>,
}

impl InMemoryPolls {
    pub fn new(clock: Arc<dyn Clock>) -> Self {
        Self {
            rows: Mutex::default(),
            clock,
            mutations: AtomicUsize::new(0),
            failure: Mutex::default(),
        }
    }

    fn fail_if_requested(&self) -> PollStoreResult<()> {
        match self.failure.lock().unwrap().clone() {
            Some(error) => Err(error.into()),
            None => Ok(()),
        }
    }

    fn now(&self) -> PollStoreResult<i64> {
        i64::try_from(self.clock.now_unix_ms()).map_err(|_| PollError::Exhausted.into())
    }
}

#[async_trait]
impl PollRepository for InMemoryPolls {
    async fn initialize(&self, key: PollKey, definition: &PollDefinition) -> PollStoreResult<()> {
        self.fail_if_requested()?;
        let mut rows = self.rows.lock().unwrap();
        if rows.contains_key(&key) {
            return Err(PortError::new("poll already reserved").into());
        }
        rows.insert(
            key,
            State {
                aggregate: PollAggregate::new(definition),
                voters: HashMap::new(),
                indexed: true,
            },
        );
        self.mutations.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn snapshot(&self, key: PollKey, viewer: UserId) -> PollStoreResult<Option<PollSnapshotState>> {
        self.fail_if_requested()?;
        Ok(self.rows.lock().unwrap().get(&key).map(|state| PollSnapshotState {
            aggregate: state.aggregate.clone(),
            voter: state.voters.get(&viewer).cloned(),
        }))
    }

    async fn vote(&self, command: VoteCommand) -> PollStoreResult<PollTransition> {
        self.fail_if_requested()?;
        let mut rows = self.rows.lock().unwrap();
        let state = rows.get_mut(&command.key).ok_or(PollError::Deleted)?;
        let next = state.aggregate.vote(
            state.voters.get(&command.user_id),
            command.answer_id,
            command.selected,
            command.expected_vote_version,
            self.now()?,
        )?;
        if next.changed {
            state.aggregate = next.aggregate.clone();
            state.voters.insert(command.user_id, next.voter.clone());
            self.mutations.fetch_add(1, Ordering::SeqCst);
        }
        Ok(next)
    }

    async fn close(&self, key: PollKey, early: bool) -> PollStoreResult<Option<(PollAggregate, bool)>> {
        self.fail_if_requested()?;
        let mut rows = self.rows.lock().unwrap();
        let Some(state) = rows.get_mut(&key) else {
            return Ok(None);
        };
        match state.aggregate.close(self.now()?, early)? {
            None => Ok(Some((state.aggregate.clone(), false))),
            Some(closed) => {
                state.aggregate = closed.clone();
                state.indexed = false;
                self.mutations.fetch_add(1, Ordering::SeqCst);
                Ok(Some((closed, true)))
            }
        }
    }

    async fn delete(&self, key: PollKey) -> PollStoreResult<()> {
        self.fail_if_requested()?;
        if let Some(state) = self.rows.lock().unwrap().get_mut(&key) {
            if let Some(deleted) = state.aggregate.delete()? {
                state.aggregate = deleted;
                self.mutations.fetch_add(1, Ordering::SeqCst);
            }
            state.voters.clear();
            state.indexed = false;
        }
        Ok(())
    }

    async fn due_page(&self, shard: u8, after: Option<PollDueEntry>, limit: u16) -> PollStoreResult<Vec<PollDueEntry>> {
        self.fail_if_requested()?;
        let now = self.now()?;
        let order = |entry: &PollDueEntry| (entry.expires_at_ms, entry.key.channel_id, entry.key.message_id);
        let mut entries: Vec<_> = self
            .rows
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, state)| state.indexed && state.aggregate.expires_at_ms <= now)
            .map(|(key, state)| PollDueEntry {
                key: *key,
                expires_at_ms: state.aggregate.expires_at_ms,
            })
            .filter(|entry| entry.shard() == shard && after.is_none_or(|after| order(entry) > order(&after)))
            .collect();
        entries.sort_by_key(order);
        entries.truncate(usize::from(limit));
        Ok(entries)
    }

    async fn remove_due(&self, entry: PollDueEntry) -> PollStoreResult<()> {
        self.fail_if_requested()?;
        if let Some(state) = self.rows.lock().unwrap().get_mut(&entry.key) {
            state.indexed = false;
        }
        Ok(())
    }
}

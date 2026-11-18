//! In-memory journal/index implementing the same atomic quota and transition contract.
use crate::ports::scheduled::*;
use crate::{AppResult, PortError};
use async_trait::async_trait;
use discordy_messages_domain::scheduled::{
    AUTHOR_SCHEDULE_LIMIT, CHANNEL_SCHEDULE_LIMIT, ScheduleChange, ScheduleId, ScheduledMessage, conflict,
};
use discordy_messages_domain::{DomainError, UserId};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicU64, Ordering},
};

#[derive(Default)]
pub struct ClaimGate {
    pub entered: tokio::sync::Notify,
    pub release: tokio::sync::Notify,
}

#[derive(Default)]
pub struct MemorySchedules {
    pub jobs: Mutex<BTreeMap<ScheduleKey, ScheduledMessage>>,
    pub due: Mutex<BTreeSet<DueSchedule>>,
    pub cursors: Mutex<BTreeMap<u8, DueSchedule>>,
    pub fail_next_stage: AtomicBool,
    pub claim_gate: Mutex<Option<Arc<ClaimGate>>>,
    pub fail_after_claim: AtomicBool,
}

#[async_trait]
impl ScheduleJournal for MemorySchedules {
    async fn find(&self, key: ScheduleKey) -> Result<Option<ScheduledMessage>, PortError> {
        Ok(self.jobs.lock().unwrap().get(&key).cloned())
    }

    async fn reserve(&self, job: ScheduledMessage) -> AppResult<ScheduleReservation> {
        let mut jobs = self.jobs.lock().unwrap();
        let key = (&job).into();
        if let Some(existing) = jobs.get(&key) {
            if existing.channel_id != job.channel_id || existing.original_fingerprint != job.original_fingerprint {
                return Err(conflict(
                    "request_id",
                    "request key already identifies a different original request",
                )
                .into());
            }
            return Ok(ScheduleReservation {
                job: existing.clone(),
                created: false,
            });
        }
        let active = jobs
            .values()
            .filter(|item| item.author_id == job.author_id && item.occupies_quota())
            .collect::<Vec<_>>();
        if active.len() >= AUTHOR_SCHEDULE_LIMIT as usize {
            return Err(conflict("request_id", "author has 100 active schedules").into());
        }
        if active.iter().filter(|item| item.channel_id == job.channel_id).count() >= CHANNEL_SCHEDULE_LIMIT as usize {
            return Err(conflict("channel_id", "channel has 25 active schedules").into());
        }
        jobs.insert(key, job.clone());
        Ok(ScheduleReservation { job, created: true })
    }

    async fn change(&self, key: ScheduleKey, change: ScheduleChange, now: u64) -> AppResult<ScheduledMessage> {
        let claim = matches!(&change, ScheduleChange::Claim { .. });
        let gate = if claim {
            self.claim_gate.lock().unwrap().take()
        } else {
            None
        };
        if let Some(gate) = gate {
            gate.entered.notify_one();
            gate.release.notified().await;
        }
        let result = {
            let mut jobs = self.jobs.lock().unwrap();
            let job = jobs.get_mut(&key).ok_or(DomainError::UnknownSchedule)?;
            job.transition(change, now)?;
            job.clone()
        };
        if claim && self.fail_after_claim.swap(false, Ordering::SeqCst) {
            return Err(PortError::new("fixture lost claim response").into());
        }
        Ok(result)
    }

    async fn active(&self, author: UserId) -> Result<Vec<ScheduledMessage>, PortError> {
        Ok(self
            .jobs
            .lock()
            .unwrap()
            .values()
            .filter(|job| job.author_id == author && job.occupies_quota())
            .cloned()
            .collect())
    }

    async fn list(&self, query: ScheduleListQuery) -> Result<SchedulePage, PortError> {
        let jobs = self.jobs.lock().unwrap();
        let rows = jobs
            .values()
            .filter(|job| job.author_id == query.author_id && query.after.is_none_or(|after| job.id > after))
            .take(usize::from(query.limit))
            .cloned()
            .collect::<Vec<_>>();
        let next_after = (rows.len() == usize::from(query.limit)).then(|| rows.last().unwrap().id);
        let items = rows
            .into_iter()
            .filter(|job| job.channel_id == query.channel_id && job.visible_in_list(query.now_ms))
            .collect();
        Ok(SchedulePage { items, next_after })
    }
}

#[async_trait]
impl ScheduleIndex for MemorySchedules {
    async fn stage(&self, due: DueSchedule) -> Result<(), PortError> {
        if self.fail_next_stage.swap(false, Ordering::SeqCst) {
            return Err(PortError::new("fixture unavailable index"));
        }
        self.due.lock().unwrap().insert(due);
        Ok(())
    }

    async fn remove(&self, due: DueSchedule) -> Result<(), PortError> {
        self.due.lock().unwrap().remove(&due);
        Ok(())
    }

    async fn page(&self, shard: u8, after: Option<DueSchedule>, now: u64) -> Result<Vec<DueSchedule>, PortError> {
        Ok(self
            .due
            .lock()
            .unwrap()
            .iter()
            .filter(|due| due.shard() == shard && due.wake_at_ms <= now && after.is_none_or(|after| **due > after))
            .take(usize::from(SCHEDULE_PAGE_SIZE))
            .copied()
            .collect())
    }

    async fn cursor(&self, shard: u8) -> Result<Option<DueSchedule>, PortError> {
        Ok(self.cursors.lock().unwrap().get(&shard).copied())
    }

    async fn save_cursor(&self, shard: u8, after: Option<DueSchedule>) -> Result<(), PortError> {
        let mut cursors = self.cursors.lock().unwrap();
        match after {
            Some(value) => {
                cursors.insert(shard, value);
            }
            None => {
                cursors.remove(&shard);
            }
        }
        Ok(())
    }
}

#[derive(Default)]
pub struct SequenceAttempts(AtomicU64);
impl ScheduleAttempts for SequenceAttempts {
    fn next_attempt(&self) -> Result<ScheduleId, PortError> {
        format!(
            "00000000-0000-4000-8000-{:012x}",
            self.0.fetch_add(1, Ordering::SeqCst) + 1
        )
        .parse()
        .map_err(PortError::new)
    }
}

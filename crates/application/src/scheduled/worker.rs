use discordy_messages_domain::scheduled::{DispatchOutcome, MAX_OVERDUE_MS, ScheduleChange, ScheduledStatus};

use super::{ScheduledDispatcher, ScheduledPorts, work_queue::WorkQueue};
use crate::{AppResult, PortError, ports::scheduled::*};

const PAGES_PER_SCAN: usize = 4;

#[derive(Default, Debug)]
pub struct ScheduleScan {
    pub pages: usize,
    pub examined: usize,
    pub removed: usize,
    pub recovered: usize,
    pub oldest_due_age_ms: u64,
    pub shard_due_ages: Vec<(u8, u64)>,
    /// Oldest observed preparing/dispatching age on this shard page, before recovery.
    pub shard_claim_ages: Vec<(u8, [u64; 2])>,
    /// Observed candidates, not a whole-database census: preparing/pending/dispatching/sent/failed/cancelled/unknown.
    pub states: [u64; 7],
    pub wrapped_shards: Vec<u8>,
}

pub struct ScheduledWorker {
    ports: ScheduledPorts,
    next_shard: u8,
    work: WorkQueue,
}

impl ScheduledWorker {
    pub fn new(ports: ScheduledPorts) -> Self {
        Self {
            work: WorkQueue::new(ScheduledDispatcher::new(ports.clone())),
            ports,
            next_shard: 0,
        }
    }

    pub fn in_flight(&self) -> usize {
        self.work.in_flight()
    }
    pub fn queued(&self) -> usize {
        self.work.queued()
    }
    pub fn stop(&mut self) {
        self.work.stop();
    }

    /// Poll at most once per second. A completion immediately refills an insertion slot independently.
    pub async fn scan(&mut self) -> AppResult<ScheduleScan> {
        if self.work.stopped() {
            return Err(PortError::new("scheduled worker has stopped").into());
        }
        let mut report = ScheduleScan::default();
        for _ in 0..PAGES_PER_SCAN {
            let shard = self.next_shard;
            self.next_shard = (shard + 1) % SCHEDULE_SHARDS;
            self.scan_shard(shard, &mut report).await?;
        }
        self.work.fill();
        Ok(report)
    }

    pub async fn next_completion(&mut self) -> Option<AppResult<Option<DispatchOutcome>>> {
        self.work.next().await
    }

    pub async fn drain(&mut self) {
        self.work.fill();
        while let Some(outcome) = self.next_completion().await {
            if let Err(error) = outcome {
                tracing::warn!(%error, "scheduled dispatch did not complete");
            }
        }
    }

    async fn scan_shard(&mut self, shard: u8, report: &mut ScheduleScan) -> AppResult<()> {
        let cursor = self.ports.index.cursor(shard).await?;
        let page = self
            .ports
            .index
            .page(shard, cursor, self.ports.clock.now_unix_ms())
            .await?;
        if page.len() > usize::from(SCHEDULE_PAGE_SIZE) {
            return Err(PortError::new("scheduled index exceeded its page bound").into());
        }
        report.pages += 1;
        let short_page = page.len() < usize::from(SCHEDULE_PAGE_SIZE);
        let mut last = cursor;
        let previous_oldest = report.oldest_due_age_ms;
        report.oldest_due_age_ms = 0;
        report.shard_claim_ages.push((shard, [0; 2]));
        let mut blocked = false;
        for candidate in page {
            report.examined += 1;
            if !self.examine(candidate, report).await? {
                blocked = true;
                break;
            }
            last = Some(candidate);
        }
        // A full queue never advances past the candidate it could not retain.
        // Buffered keys remain in the index, so restart/rewind safely rediscovers them.
        if short_page && !blocked {
            self.ports.index.save_cursor(shard, None).await?;
            report.wrapped_shards.push(shard);
        } else {
            self.ports.index.save_cursor(shard, last).await?;
        }
        report.shard_due_ages.push((shard, report.oldest_due_age_ms));
        report.oldest_due_age_ms = report.oldest_due_age_ms.max(previous_oldest);
        Ok(())
    }

    async fn examine(&mut self, candidate: DueSchedule, report: &mut ScheduleScan) -> AppResult<bool> {
        let Some(mut job) = self.ports.journal.find(candidate.key).await? else {
            self.remove(candidate, report).await?;
            return Ok(true);
        };
        let now = self.ports.clock.now_unix_ms();
        if matches!(job.status, ScheduledStatus::Preparing | ScheduledStatus::Dispatching) {
            let age_index = usize::from(job.status == ScheduledStatus::Dispatching);
            let ages = &mut report.shard_claim_ages.last_mut().expect("scan page initialized").1;
            ages[age_index] = ages[age_index].max(now.saturating_sub(job.updated_at_ms));
            let previous = job.status;
            job = self
                .ports
                .journal
                .change(candidate.key, ScheduleChange::Recover, now)
                .await?;
            report.recovered += usize::from(job.status != previous);
        }
        report.states[state_index(job.status)] += 1;
        match job.status {
            ScheduledStatus::Preparing | ScheduledStatus::Dispatching => {}
            ScheduledStatus::Pending => {
                report.oldest_due_age_ms = report.oldest_due_age_ms.max(now.saturating_sub(job.scheduled_at_ms));
                if candidate.revision < job.revision
                    || (candidate.revision == job.revision && candidate.wake_at_ms != job.scheduled_at_ms)
                {
                    self.remove(candidate, report).await?;
                } else if candidate.revision == job.revision
                    && (job.due(now) || now.saturating_sub(job.scheduled_at_ms) >= MAX_OVERDUE_MS)
                {
                    return Ok(self.work.enqueue(candidate.key));
                }
                // A future revision may be an edit paused between staging and its CAS; retain it.
            }
            _ => self.remove(candidate, report).await?,
        }
        Ok(true)
    }

    async fn remove(&self, candidate: DueSchedule, report: &mut ScheduleScan) -> AppResult<()> {
        self.ports.index.remove(candidate).await?;
        report.removed += 1;
        Ok(())
    }
}

fn state_index(status: ScheduledStatus) -> usize {
    match status {
        ScheduledStatus::Preparing => 0,
        ScheduledStatus::Pending => 1,
        ScheduledStatus::Dispatching => 2,
        ScheduledStatus::Sent => 3,
        ScheduledStatus::Failed => 4,
        ScheduledStatus::Cancelled => 5,
        ScheduledStatus::DeliveryUnknown => 6,
    }
}

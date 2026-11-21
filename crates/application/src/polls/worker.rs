use super::{Polls, work_queue::WorkQueue};
use crate::ports::{Clock, PollDueEntry};
use crate::{AppResult, PortError};
use std::sync::Arc;

const PAGE_SIZE: u16 = 100;
const PAGES_PER_SCAN: usize = 4;

#[derive(Default, Debug)]
pub struct PollScan {
    pub pages: usize,
    pub examined: usize,
    /// Ages observed on bounded pages, not a complete database census.
    pub shard_due_ages_ms: Vec<(u8, u64)>,
    pub wrapped_shards: Vec<u8>,
}

pub struct PollWorker {
    service: Arc<Polls>,
    clock: Arc<dyn Clock>,
    next_shard: u8,
    cursors: [Option<PollDueEntry>; 64],
    work: WorkQueue,
}

impl PollWorker {
    pub fn new(service: Arc<Polls>, clock: Arc<dyn Clock>) -> Self {
        Self {
            work: WorkQueue::new(service.clone()),
            service,
            clock,
            next_shard: 0,
            cursors: [None; 64],
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
    pub async fn next_completion(&mut self) -> Option<AppResult<bool>> {
        self.work.next().await
    }

    /// Four shard pages per scan; call at most once per second.
    pub async fn scan(&mut self) -> AppResult<PollScan> {
        if self.work.stopped() {
            return Err(PortError::new("poll worker stopped").into());
        }
        let mut report = PollScan::default();
        for _ in 0..PAGES_PER_SCAN {
            let shard = self.next_shard;
            self.next_shard = (shard + 1) % 64;
            let cursor = &mut self.cursors[usize::from(shard)];
            let page = self.service.repository.due_page(shard, *cursor, PAGE_SIZE).await?;
            if page.len() > usize::from(PAGE_SIZE) {
                return Err(PortError::new("poll index exceeded page bound").into());
            }
            report.pages += 1;
            let short = page.len() < usize::from(PAGE_SIZE);
            let mut blocked = false;
            let mut oldest = 0;
            for entry in page {
                if entry.shard() != shard || entry.expires_at_ms < 0 {
                    return Err(PortError::new("poll index returned invalid candidate").into());
                }
                report.examined += 1;
                oldest = oldest.max(self.clock.now_unix_ms().saturating_sub(entry.expires_at_ms as u64));
                if !self.work.enqueue(entry) {
                    blocked = true;
                    break;
                }
                *cursor = Some(entry);
            }
            // Never advance over work that could not be retained. Restart safely
            // starts at each shard head; failed entries remain in the durable index.
            if short && !blocked {
                *cursor = None;
                report.wrapped_shards.push(shard);
            }
            report.shard_due_ages_ms.push((shard, oldest));
            self.work.fill();
        }
        Ok(report)
    }
    pub async fn drain(&mut self) {
        while let Some(result) = self.next_completion().await {
            if let Err(error) = result {
                tracing::warn!(%error, "poll finalization did not complete");
            }
        }
    }
}

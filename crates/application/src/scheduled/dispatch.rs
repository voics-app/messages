use super::{ScheduledPorts, failure_reason};
use crate::{AppError, AppResult, PortError, ports::scheduled::*};
use discordy_messages_domain::scheduled::{
    DispatchOutcome, MAX_OVERDUE_MS, ScheduleChange, ScheduledFailure, ScheduledMessage, ScheduledStatus,
};
use std::time::Duration;

const PREPARE_TIMEOUT: Duration = Duration::from_secs(5);
const COMPLETION_ATTEMPTS: u32 = 3;

pub struct ScheduledDispatcher {
    ports: ScheduledPorts,
}

impl ScheduledDispatcher {
    pub fn new(ports: ScheduledPorts) -> Self {
        Self { ports }
    }

    pub async fn execute(&self, key: ScheduleKey) -> AppResult<Option<DispatchOutcome>> {
        let Some(job) = self.ports.journal.find(key).await? else {
            return Ok(None);
        };
        let now = self.ports.clock.now_unix_ms();
        if matches!(job.status, ScheduledStatus::Preparing | ScheduledStatus::Dispatching) {
            self.ports.journal.change(key, ScheduleChange::Recover, now).await?;
            return Ok(None);
        }
        if !job.due(now) {
            return Ok(None);
        }
        if now.saturating_sub(job.scheduled_at_ms) >= MAX_OVERDUE_MS {
            self.reject(&job, ScheduledFailure::DependencyUnavailable).await?;
            return Ok(None);
        }
        self.prepare_and_claim(job).await
    }

    async fn prepare_and_claim(&self, job: ScheduledMessage) -> AppResult<Option<DispatchOutcome>> {
        let draft = super::author_draft(&job)?;
        let result = tokio::time::timeout(PREPARE_TIMEOUT, self.ports.delivery.prepare(&draft))
            .await
            .unwrap_or_else(|_| Err(PortError::new("scheduled preparation timed out").into()));
        let mut message = match result {
            Ok(message) => message,
            Err(error) => {
                self.preclaim_failure(&job, error).await?;
                return Ok(None);
            }
        };
        let attempt = match self.ports.attempts.next_attempt() {
            Ok(attempt) => attempt,
            Err(error) => {
                self.preclaim_failure(&job, error.into()).await?;
                return Ok(None);
            }
        };
        let change = ScheduleChange::Claim {
            expected_revision: job.revision,
            attempt_id: attempt,
            message_id: message.message.id,
        };
        match self
            .ports
            .journal
            .change((&job).into(), change, self.ports.clock.now_unix_ms())
            .await
        {
            Ok(_) => {}
            Err(AppError::Domain(_)) => return Ok(None),
            // An ambiguous claim result never authorizes this process to insert.
            Err(error) => return Err(error),
        }
        let outcome = self.ports.delivery.insert(&mut message).await;
        let completion = self
            .complete(
                (&job).into(),
                ScheduleChange::Finish {
                    attempt_id: attempt,
                    outcome,
                },
            )
            .await;
        if outcome == DispatchOutcome::Sent {
            self.ports.delivery.publish(&message).await;
        }
        completion?;
        Ok(Some(outcome))
    }

    async fn complete(&self, key: ScheduleKey, change: ScheduleChange) -> AppResult<()> {
        for attempt in 0..COMPLETION_ATTEMPTS {
            match self
                .ports
                .journal
                .change(key, change.clone(), self.ports.clock.now_unix_ms())
                .await
            {
                Ok(_) => return Ok(()),
                Err(AppError::Unavailable(_)) if attempt + 1 < COMPLETION_ATTEMPTS => {
                    // Only certification is retried; insertion and publication are not replayed.
                    tokio::time::sleep(Duration::from_millis(100 << attempt)).await;
                }
                Err(error) => return Err(error),
            }
        }
        unreachable!("the last completion result is returned")
    }

    async fn preclaim_failure(&self, job: &ScheduledMessage, error: AppError) -> AppResult<()> {
        if matches!(error, AppError::Unavailable(_)) {
            let result = self
                .ports
                .journal
                .change(
                    job.into(),
                    ScheduleChange::Defer {
                        expected_revision: job.revision,
                    },
                    self.ports.clock.now_unix_ms(),
                )
                .await;
            match result {
                Ok(_) | Err(AppError::Domain(_)) => Ok(()),
                Err(error) => Err(error),
            }
        } else {
            self.reject(job, failure_reason(&error)).await
        }
    }

    async fn reject(&self, job: &ScheduledMessage, reason: ScheduledFailure) -> AppResult<()> {
        match self
            .ports
            .journal
            .change(
                job.into(),
                ScheduleChange::Reject {
                    expected_revision: job.revision,
                    reason,
                },
                self.ports.clock.now_unix_ms(),
            )
            .await
        {
            Ok(_) | Err(AppError::Domain(_)) => Ok(()),
            Err(error) => Err(error),
        }
    }
}

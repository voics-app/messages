use super::{
    MAX_OVERDUE_MS, PREPARATION_TIMEOUT_MS, ScheduleId, ScheduledDraft, ScheduledFailure, ScheduledMessage,
    ScheduledStatus, conflict,
};
use crate::{DomainError, MessageId};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DispatchOutcome {
    Sent,
    Failed(ScheduledFailure),
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScheduleChange {
    Activate,
    Edit {
        expected_revision: u32,
        scheduled_at_ms: Option<u64>,
        draft: ScheduledDraft,
    },
    Cancel {
        expected_revision: u32,
    },
    Claim {
        expected_revision: u32,
        attempt_id: ScheduleId,
        message_id: MessageId,
    },
    Reject {
        expected_revision: u32,
        reason: ScheduledFailure,
    },
    Defer {
        expected_revision: u32,
    },
    Finish {
        attempt_id: ScheduleId,
        outcome: DispatchOutcome,
    },
    Recover,
}

impl ScheduledMessage {
    /// A repository commits the returned mutation and its quota delta atomically.
    pub fn transition(&mut self, change: ScheduleChange, now: u64) -> Result<bool, DomainError> {
        // Failed validation, including revision exhaustion, must never partially mutate a record.
        let mut next = self.clone();
        next.apply(change, now)?;
        let changed = next != *self;
        *self = next;
        Ok(changed)
    }

    fn apply(&mut self, change: ScheduleChange, now: u64) -> Result<(), DomainError> {
        match change {
            ScheduleChange::Activate => self.activate(now),
            ScheduleChange::Edit {
                expected_revision,
                scheduled_at_ms,
                draft,
            } => self.edit(expected_revision, (scheduled_at_ms, draft), now),
            ScheduleChange::Cancel { expected_revision } => self.cancel(expected_revision, now),
            ScheduleChange::Claim {
                expected_revision,
                attempt_id,
                message_id,
            } => self.claim(expected_revision, (attempt_id, message_id), now),
            ScheduleChange::Reject {
                expected_revision,
                reason,
            } => {
                self.pending_revision(expected_revision)?;
                self.advance(now)?;
                self.failed(reason);
                Ok(())
            }
            ScheduleChange::Defer { expected_revision } => self.defer(expected_revision, now),
            ScheduleChange::Finish { attempt_id, outcome } => self.finish(attempt_id, outcome, now),
            ScheduleChange::Recover => self.recover(now),
        }
    }

    fn activate(&mut self, now: u64) -> Result<(), DomainError> {
        if self.status != ScheduledStatus::Preparing {
            return Err(conflict("request_id", "reservation is no longer preparing"));
        }
        if now.saturating_sub(self.created_at_ms) >= PREPARATION_TIMEOUT_MS {
            return Err(conflict("request_id", "reservation expired"));
        }
        self.advance(now)?;
        self.status = ScheduledStatus::Pending;
        Ok(())
    }

    fn edit(&mut self, revision: u32, replacement: (Option<u64>, ScheduledDraft), now: u64) -> Result<(), DomainError> {
        self.pending_revision(revision)?;
        if let Some(due) = replacement.0 {
            Self::validate_time(due, now)?;
        }
        replacement.1.validate()?;
        self.advance(now)?;
        self.scheduled_at_ms = replacement.0.unwrap_or(self.scheduled_at_ms);
        self.next_attempt_at_ms = self.scheduled_at_ms;
        self.dependency_failures = 0;
        self.draft = Some(replacement.1);
        Ok(())
    }

    fn cancel(&mut self, revision: u32, now: u64) -> Result<(), DomainError> {
        if self.status == ScheduledStatus::Cancelled {
            return Ok(());
        }
        self.pending_revision(revision)?;
        self.advance(now)?;
        self.status = ScheduledStatus::Cancelled;
        self.clear_draft();
        Ok(())
    }

    fn claim(&mut self, revision: u32, selected: (ScheduleId, MessageId), now: u64) -> Result<(), DomainError> {
        self.pending_revision(revision)?;
        if !self.due(now) || now.saturating_sub(self.scheduled_at_ms) >= MAX_OVERDUE_MS {
            return Err(conflict("scheduled_at", "schedule is not eligible for dispatch"));
        }
        self.advance(now)?;
        self.status = ScheduledStatus::Dispatching;
        self.attempt_id = Some(selected.0);
        self.message_id = Some(selected.1);
        Ok(())
    }

    fn defer(&mut self, revision: u32, now: u64) -> Result<(), DomainError> {
        self.pending_revision(revision)?;
        if now.saturating_sub(self.scheduled_at_ms) >= MAX_OVERDUE_MS {
            self.advance(now)?;
            self.failed(ScheduledFailure::DependencyUnavailable);
            return Ok(());
        }
        let base = (1_000_u64 << self.dependency_failures.min(6)).min(60_000);
        let jitter = ((self.id.bits() as u64) ^ now) % (base / 4 + 1);
        self.next_attempt_at_ms = now.saturating_add((base + jitter).min(60_000));
        self.dependency_failures = self.dependency_failures.saturating_add(1);
        // This internal retry deadline does not change the public resource/revision or due index.
        Ok(())
    }

    fn finish(&mut self, attempt: ScheduleId, outcome: DispatchOutcome, now: u64) -> Result<(), DomainError> {
        if self.attempt_id != Some(attempt) {
            return Err(conflict("status", "dispatch attempt changed"));
        }
        if self.status == ScheduledStatus::Sent && outcome == DispatchOutcome::Sent {
            return Ok(());
        }
        if self.status == ScheduledStatus::DeliveryUnknown && outcome == DispatchOutcome::Unknown {
            return Ok(());
        }
        if self.status != ScheduledStatus::Dispatching
            && !(self.status == ScheduledStatus::DeliveryUnknown && outcome == DispatchOutcome::Sent)
        {
            return Err(conflict("status", "dispatch outcome cannot be replaced"));
        }
        self.advance(now)?;
        match outcome {
            DispatchOutcome::Sent => {
                self.status = ScheduledStatus::Sent;
                self.failure = None;
                self.clear_draft();
            }
            DispatchOutcome::Failed(reason) => self.failed(reason),
            DispatchOutcome::Unknown => self.uncertain(),
        }
        Ok(())
    }

    fn recover(&mut self, now: u64) -> Result<(), DomainError> {
        if now.saturating_sub(self.updated_at_ms) < PREPARATION_TIMEOUT_MS {
            return Ok(());
        }
        match self.status {
            ScheduledStatus::Preparing => {
                self.advance(now)?;
                self.failed(ScheduledFailure::DependencyUnavailable);
            }
            ScheduledStatus::Dispatching => {
                self.advance(now)?;
                self.uncertain();
            }
            _ => {}
        }
        Ok(())
    }
}

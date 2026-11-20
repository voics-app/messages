//! Author-private scheduling policy. Time is supplied by callers; no IO or timer lives here.

mod identity;
mod transitions;

use crate::{ChannelId, DomainError, FieldError, MessageId, UserId};

pub use identity::ScheduleId;
pub use transitions::{DispatchOutcome, ScheduleChange};

pub const AUTHOR_SCHEDULE_LIMIT: u32 = 100;
pub const CHANNEL_SCHEDULE_LIMIT: u32 = 25;
pub const MIN_SCHEDULE_DELAY_MS: u64 = 60_000;
pub const MAX_SCHEDULE_DELAY_MS: u64 = 30 * 86_400_000;
pub const PREPARATION_TIMEOUT_MS: u64 = 120_000;
pub const MAX_OVERDUE_MS: u64 = 86_400_000;
pub const TERMINAL_LIST_RETENTION_MS: u64 = 7 * 86_400_000;
pub const MAX_SCHEDULE_REVISION: u32 = i32::MAX as u32;

pub fn conflict(field: &str, reason: &str) -> DomainError {
    DomainError::ScheduleConflict(vec![FieldError {
        field: field.into(),
        reason: reason.into(),
    }])
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduledDraft {
    pub content: String,
    pub tts: bool,
    pub reply_to: Option<MessageId>,
    pub replied_user: bool,
    pub silent: bool,
}

impl ScheduledDraft {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.content.trim().is_empty() || self.content.chars().count() > crate::message::MAX_CONTENT_CHARS {
            return Err(DomainError::invalid(
                "message.content",
                "must contain 1..2000 characters, including non-whitespace text",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScheduledStatus {
    Preparing,
    Pending,
    Dispatching,
    Sent,
    Failed,
    Cancelled,
    DeliveryUnknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScheduledFailure {
    AccessDenied,
    ChannelMissing,
    DeliveryModeChanged,
    InvalidMessage,
    DependencyUnavailable,
    DeliveryUnknown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduleRequest {
    pub id: ScheduleId,
    pub author_id: UserId,
    pub channel_id: ChannelId,
    pub scheduled_at_ms: u64,
    pub draft: ScheduledDraft,
    pub original_fingerprint: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduledMessage {
    pub id: ScheduleId,
    pub author_id: UserId,
    pub channel_id: ChannelId,
    pub original_fingerprint: String,
    pub revision: u32,
    pub scheduled_at_ms: u64,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
    pub status: ScheduledStatus,
    pub draft: Option<ScheduledDraft>,
    pub message_id: Option<MessageId>,
    pub attempt_id: Option<ScheduleId>,
    pub failure: Option<ScheduledFailure>,
    pub next_attempt_at_ms: u64,
    pub dependency_failures: u8,
}

impl ScheduledMessage {
    pub fn prepare(request: ScheduleRequest, now: u64) -> Result<Self, DomainError> {
        request.draft.validate()?;
        Self::validate_time(request.scheduled_at_ms, now)?;
        Ok(Self {
            id: request.id,
            author_id: request.author_id,
            channel_id: request.channel_id,
            original_fingerprint: request.original_fingerprint,
            revision: 0,
            scheduled_at_ms: request.scheduled_at_ms,
            created_at_ms: now,
            updated_at_ms: now,
            status: ScheduledStatus::Preparing,
            draft: Some(request.draft),
            message_id: None,
            attempt_id: None,
            failure: None,
            next_attempt_at_ms: request.scheduled_at_ms,
            dependency_failures: 0,
        })
    }

    pub fn validate_time(due: u64, now: u64) -> Result<(), DomainError> {
        if !(MIN_SCHEDULE_DELAY_MS..=MAX_SCHEDULE_DELAY_MS).contains(&due.saturating_sub(now)) {
            return Err(DomainError::invalid(
                "scheduled_at",
                "must be between one minute and 30 days ahead of the server clock",
            ));
        }
        Ok(())
    }

    pub fn occupies_quota(&self) -> bool {
        matches!(
            self.status,
            ScheduledStatus::Preparing | ScheduledStatus::Pending | ScheduledStatus::Dispatching
        )
    }

    pub fn visible_in_list(&self, now: u64) -> bool {
        self.status != ScheduledStatus::Preparing
            && (self.occupies_quota()
                || self.status == ScheduledStatus::DeliveryUnknown
                || now.saturating_sub(self.updated_at_ms) <= TERMINAL_LIST_RETENTION_MS)
    }

    pub fn due(&self, now: u64) -> bool {
        self.status == ScheduledStatus::Pending && now >= self.scheduled_at_ms && now >= self.next_attempt_at_ms
    }

    fn advance(&mut self, now: u64) -> Result<(), DomainError> {
        if self.revision >= MAX_SCHEDULE_REVISION {
            return Err(conflict("expected_revision", "revision exhausted"));
        }
        self.revision += 1;
        self.updated_at_ms = now;
        Ok(())
    }

    fn pending_revision(&self, expected: u32) -> Result<(), DomainError> {
        if self.status != ScheduledStatus::Pending || self.revision != expected {
            return Err(conflict(
                "expected_revision",
                "schedule changed or is no longer pending",
            ));
        }
        Ok(())
    }

    fn clear_draft(&mut self) {
        self.draft = None;
    }

    fn failed(&mut self, reason: ScheduledFailure) {
        self.status = ScheduledStatus::Failed;
        self.failure = Some(reason);
        self.message_id = None;
        self.clear_draft();
    }

    fn uncertain(&mut self) {
        self.status = ScheduledStatus::DeliveryUnknown;
        self.failure = Some(ScheduledFailure::DeliveryUnknown);
        self.clear_draft();
    }
}

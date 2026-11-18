//! Durable scheduling orchestration; adapters own persistence and external delivery.

mod create;
mod dispatch;
mod mutate;

use crate::{
    AppError, AppResult, PortError, RateLimiter,
    ports::{Clock, scheduled::*},
};
use discordy_messages_domain::scheduled::{
    ScheduleId, ScheduledDraft, ScheduledFailure, ScheduledMessage, ScheduledStatus,
};
use discordy_messages_domain::{ChannelId, DomainError, UserId};
pub use dispatch::ScheduledDispatcher;
use std::sync::Arc;

#[derive(Clone)]
pub struct ScheduledPorts {
    pub journal: Arc<dyn ScheduleJournal>,
    pub index: Arc<dyn ScheduleIndex>,
    pub delivery: Arc<dyn ScheduledDelivery>,
    pub attempts: Arc<dyn ScheduleAttempts>,
    pub clock: Arc<dyn Clock>,
}

#[derive(Clone)]
pub struct CreateSchedule {
    pub id: ScheduleId,
    pub author_id: UserId,
    pub channel_id: ChannelId,
    pub scheduled_at_ms: u64,
    pub draft: ScheduledDraft,
}

#[derive(Clone, Copy)]
pub struct ScheduleAddress {
    pub key: ScheduleKey,
    pub channel_id: ChannelId,
}

pub struct EditSchedule {
    pub address: ScheduleAddress,
    pub expected_revision: u32,
    pub scheduled_at_ms: Option<u64>,
    pub draft: Option<ScheduledDraft>,
}

pub struct ScheduleAcceptance {
    pub job: ScheduledMessage,
    pub created: bool,
}

pub struct ScheduledMessages {
    ports: ScheduledPorts,
    mutations: RateLimiter,
}

impl ScheduledMessages {
    pub fn new(ports: ScheduledPorts) -> Self {
        Self {
            mutations: RateLimiter::new(ports.clock.clone(), 120, 60_000),
            ports,
        }
    }

    pub async fn get(&self, address: ScheduleAddress) -> AppResult<ScheduledMessage> {
        let job = self.owned(address).await?;
        if job.status == ScheduledStatus::Preparing {
            return Err(PortError::new("schedule acceptance is incomplete; retry the original request").into());
        }
        Ok(job)
    }

    pub async fn list(&self, mut query: ScheduleListQuery) -> AppResult<SchedulePage> {
        if !(1..=SCHEDULE_PAGE_SIZE).contains(&query.limit) {
            return Err(DomainError::invalid("limit", "must be 1..100").into());
        }
        query.now_ms = self.ports.clock.now_unix_ms();
        Ok(self.ports.journal.list(query).await?)
    }

    async fn owned(&self, address: ScheduleAddress) -> AppResult<ScheduledMessage> {
        self.ports
            .journal
            .find(address.key)
            .await?
            .filter(|job| job.channel_id == address.channel_id)
            .ok_or_else(|| DomainError::UnknownSchedule.into())
    }

    fn rate_limit(&self, author: UserId) -> AppResult<()> {
        self.mutations
            .check(author)
            .map_err(|retry_after_ms| AppError::RateLimited { retry_after_ms })
    }
}

pub(crate) fn failure_reason(error: &AppError) -> ScheduledFailure {
    match error {
        AppError::Domain(DomainError::UnknownChannel) => ScheduledFailure::ChannelMissing,
        AppError::Domain(DomainError::DeliveryModeConflict(_)) => ScheduledFailure::DeliveryModeChanged,
        AppError::Domain(DomainError::MissingAccess | DomainError::MissingPermissions) => {
            ScheduledFailure::AccessDenied
        }
        AppError::Domain(_) => ScheduledFailure::InvalidMessage,
        _ => ScheduledFailure::DependencyUnavailable,
    }
}

fn author_draft(job: &ScheduledMessage) -> AppResult<ScheduledAuthorDraft> {
    Ok(ScheduledAuthorDraft {
        author_id: job.author_id,
        channel_id: job.channel_id,
        draft: job
            .draft
            .clone()
            .ok_or_else(|| PortError::new("active schedule has no draft"))?,
    })
}

#[cfg(any(test, feature = "test-support"))]
pub mod fakes;
#[cfg(test)]
mod tests;

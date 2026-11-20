use super::{CreateSchedule, ScheduleAcceptance, ScheduledMessages};
use crate::{AppError, AppResult, PortError, ports::scheduled::*};
use discordy_messages_domain::scheduled::{
    AUTHOR_SCHEDULE_LIMIT, PREPARATION_TIMEOUT_MS, ScheduleChange, ScheduleRequest, ScheduledMessage, ScheduledStatus,
    conflict,
};
use sha2::{Digest, Sha256};

fn fingerprint(command: &CreateSchedule) -> String {
    let mut digest = Sha256::new();
    digest.update(b"scheduled-message-v1");
    digest.update(command.channel_id.value().to_be_bytes());
    digest.update(command.scheduled_at_ms.to_be_bytes());
    digest.update((command.draft.content.len() as u64).to_be_bytes());
    digest.update(command.draft.content.as_bytes());
    digest.update([
        u8::from(command.draft.tts),
        u8::from(command.draft.replied_user),
        u8::from(command.draft.silent),
    ]);
    digest.update([u8::from(command.draft.reply_to.is_some())]);
    digest.update(command.draft.reply_to.map_or(0, |id| id.value()).to_be_bytes());
    format!("{:x}", digest.finalize())
}

impl ScheduledMessages {
    pub async fn create(&self, command: CreateSchedule) -> AppResult<ScheduleAcceptance> {
        self.rate_limit(command.author_id)?;
        self.reconcile_author(command.author_id).await?;
        let original_fingerprint = fingerprint(&command);
        let key = ScheduleKey {
            author_id: command.author_id,
            id: command.id,
        };
        if let Some(existing) = self.ports.journal.find(key).await? {
            if existing.channel_id != command.channel_id || existing.original_fingerprint != original_fingerprint {
                return Err(conflict(
                    "request_id",
                    "request key already identifies a different original request",
                )
                .into());
            }
            return self.accept_reservation(existing).await;
        }
        let request = ScheduleRequest {
            id: command.id,
            author_id: command.author_id,
            channel_id: command.channel_id,
            scheduled_at_ms: command.scheduled_at_ms,
            draft: command.draft,
            original_fingerprint,
        };
        let job = ScheduledMessage::prepare(request, self.ports.clock.now_unix_ms())?;
        self.ports.delivery.validate(&super::author_draft(&job)?).await?;
        let reserved = self.ports.journal.reserve(job).await?;
        self.accept_reservation(reserved.job).await
    }

    async fn accept_reservation(&self, job: ScheduledMessage) -> AppResult<ScheduleAcceptance> {
        if job.status != ScheduledStatus::Preparing {
            return Ok(ScheduleAcceptance { job, created: false });
        }
        // Incomplete reservations have never been accepted. Resuming one needs current access;
        // replay of an already accepted resource above deliberately does not.
        self.ports.delivery.validate(&super::author_draft(&job)?).await?;
        let key = (&job).into();
        // Recovery can find an indexed but abandoned preparation before a far-future due time.
        self.ports
            .index
            .stage(DueSchedule {
                wake_at_ms: job.created_at_ms.saturating_add(PREPARATION_TIMEOUT_MS),
                key,
                revision: 0,
            })
            .await?;
        self.ports
            .index
            .stage(DueSchedule {
                wake_at_ms: job.scheduled_at_ms,
                key,
                revision: 1,
            })
            .await?;
        match self
            .ports
            .journal
            .change(key, ScheduleChange::Activate, self.ports.clock.now_unix_ms())
            .await
        {
            Ok(job) => Ok(ScheduleAcceptance { job, created: true }),
            Err(AppError::Domain(_)) => {
                let current = self
                    .ports
                    .journal
                    .find(key)
                    .await?
                    .ok_or_else(|| PortError::new("schedule reservation disappeared"))?;
                if current.status == ScheduledStatus::Preparing {
                    return Err(PortError::new("schedule acceptance is incomplete; retry the original request").into());
                }
                Ok(ScheduleAcceptance {
                    job: current,
                    created: false,
                })
            }
            Err(error) => Err(error),
        }
    }

    /// Recover abandoned work for one authenticated author, bounded by their active quota.
    /// Call explicitly before author inspection; `get` and `list` never perform recovery writes.
    pub async fn reconcile_author(&self, author: discordy_messages_domain::UserId) -> AppResult<()> {
        let active = self.ports.journal.active(author).await?;
        if active.len() > AUTHOR_SCHEDULE_LIMIT as usize {
            return Err(PortError::new("active schedule discovery exceeds quota").into());
        }
        let now = self.ports.clock.now_unix_ms();
        for job in active {
            if matches!(job.status, ScheduledStatus::Preparing | ScheduledStatus::Dispatching)
                && now.saturating_sub(job.updated_at_ms) >= PREPARATION_TIMEOUT_MS
            {
                match self
                    .ports
                    .journal
                    .change((&job).into(), ScheduleChange::Recover, now)
                    .await
                {
                    Ok(_) | Err(AppError::Domain(_)) => {}
                    Err(error) => return Err(error),
                }
            }
        }
        Ok(())
    }
}

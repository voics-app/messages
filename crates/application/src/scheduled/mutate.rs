use super::{EditSchedule, ScheduleAddress, ScheduledMessages};
use crate::{AppResult, ports::scheduled::DueSchedule};
use discordy_messages_domain::DomainError;
use discordy_messages_domain::scheduled::{ScheduleChange, ScheduledMessage, conflict};

impl ScheduledMessages {
    pub async fn edit(&self, command: EditSchedule) -> AppResult<ScheduledMessage> {
        self.rate_limit(command.address.key.author_id)?;
        if command.scheduled_at_ms.is_none() && command.draft.is_none() {
            return Err(DomainError::invalid("message", "at least one replacement is required").into());
        }
        let mut next = self.owned(command.address).await?;
        let replacement = command
            .draft
            .or_else(|| next.draft.clone())
            .ok_or_else(|| conflict("expected_revision", "schedule changed or is no longer pending"))?;
        let change = ScheduleChange::Edit {
            expected_revision: command.expected_revision,
            scheduled_at_ms: command.scheduled_at_ms,
            draft: replacement,
        };
        next.transition(change.clone(), self.ports.clock.now_unix_ms())?;
        self.ports.delivery.validate(&super::author_draft(&next)?).await?;
        // The old index remains until the new index and revision are durable.
        // A failed/lost CAS leaves a candidate, never an accepted undiscoverable job.
        self.ports.index.stage(DueSchedule::pending(&next)).await?;
        self.ports
            .journal
            .change(command.address.key, change, self.ports.clock.now_unix_ms())
            .await
    }

    pub async fn cancel(&self, address: ScheduleAddress, expected_revision: u32) -> AppResult<ScheduledMessage> {
        self.rate_limit(address.key.author_id)?;
        self.reconcile_author(address.key.author_id).await?;
        self.owned(address).await?;
        self.ports
            .journal
            .change(
                address.key,
                ScheduleChange::Cancel { expected_revision },
                self.ports.clock.now_unix_ms(),
            )
            .await
    }
}

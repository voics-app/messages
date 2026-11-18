//! Expiry is durable even if the message was never accepted or its channel vanished.
use super::Polls;
use crate::ports::{AccessError, MessageEventKind, PollDueEntry, PollStoreError};
use crate::use_cases::support::{announce, event};
use crate::{AppResult, PortError};
use discordy_messages_domain::DeliveryMode;
use discordy_messages_domain::poll::{PollError, PollStatus};

impl Polls {
    pub(super) async fn finalize_due(&self, due: PollDueEntry) -> AppResult<bool> {
        let closed = match self.repository.close(due.key, false).await {
            Err(PollStoreError::Rule(PollError::Deleted)) => {
                // A prior delete may have installed its fence before cleanup failed.
                self.repository.delete(due.key).await?;
                self.repository.remove_due(due).await?;
                return Ok(false);
            }
            other => other?,
        };
        let Some((results, changed)) = closed else {
            self.repository.remove_due(due).await?;
            return Ok(false);
        };
        if results.expires_at_ms != due.expires_at_ms || results.status != PollStatus::Finalized {
            return Err(PortError::new("poll expiry index differs from persisted metadata").into());
        }
        if !changed {
            return Ok(false);
        }
        let Some(message) = self.messages.find(due.key.channel_id, due.key.message_id).await? else {
            return Ok(true); // Content-free reservation; never recreate its message.
        };
        Self::check_state(&message, &results)?;
        let access = match self.access.channel(due.key.channel_id).await {
            Err(AccessError::UnknownChannel | AccessError::NotMember) => return Ok(true),
            other => other?,
        };
        if access.delivery_mode == DeliveryMode::Standard {
            announce(
                self.events.as_ref(),
                event(
                    &access,
                    due.key.channel_id,
                    MessageEventKind::PollFinalized { message, results },
                ),
            )
            .await;
        }
        // Missing publication is recovered by authoritative foreground/reconnect GET.
        Ok(true)
    }
}

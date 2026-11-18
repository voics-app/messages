//! Poll commands authorize fresh channel access before reading or mutating.
mod expiry;
#[cfg(test)]
mod tests;
mod work_queue;
mod worker;
#[cfg(test)]
mod worker_tests;
use std::sync::Arc;
pub use worker::{PollScan, PollWorker};

use discordy_messages_domain::poll::{PollAggregate, PollStatus, PollTransition};
use discordy_messages_domain::{ChannelAccess, DomainError, Message};

use crate::ports::{
    ChannelAccessProvider, EventPublisher, MessageEventKind, MessageRepository, PollKey, PollRepository,
    PollSnapshotState, Principal, VoteCommand,
};
use crate::use_cases::MessagingPorts;
use crate::use_cases::support::{announce, event, find_message};
use crate::{AppResult, PortError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PollView {
    pub message: Message,
    pub state: PollSnapshotState,
}

#[derive(Clone)]
pub struct Polls {
    access: Arc<dyn ChannelAccessProvider>,
    messages: Arc<dyn MessageRepository>,
    pub(crate) repository: Arc<dyn PollRepository>,
    events: Arc<dyn EventPublisher>,
}

impl Polls {
    pub fn new(ports: &MessagingPorts) -> Option<Self> {
        Some(Self {
            repository: ports.polls.clone()?,
            access: ports.access.clone(),
            messages: ports.messages.clone(),
            events: ports.events.clone(),
        })
    }

    async fn accessible(&self, key: PollKey, actor: Principal) -> AppResult<(ChannelAccess, Message)> {
        let access = self.access.refresh(key.channel_id, actor.user_id).await?;
        access.ensure_can_read_poll()?;
        let message = find_message(self.messages.as_ref(), key.channel_id, key.message_id).await?;
        if message.poll.is_none() {
            return Err(DomainError::UnknownMessage.into());
        }
        Ok((access, message))
    }

    fn check_state(message: &Message, state: &PollAggregate) -> AppResult<()> {
        if state.status == PollStatus::Deleted {
            return Err(DomainError::UnknownMessage.into());
        }
        let definition = message.poll.as_ref().ok_or(DomainError::UnknownMessage)?;
        if definition.expires_at_ms != state.expires_at_ms
            || definition.allow_multiselect != state.allow_multiselect
            || definition.answers.len() != state.counts.len()
        {
            return Err(PortError::new("stored poll state differs from its immutable message definition").into());
        }
        Ok(())
    }

    pub async fn get(&self, key: PollKey, actor: Principal) -> AppResult<PollView> {
        let (_, message) = self.accessible(key, actor).await?;
        let state = self.inspect(key, actor, &message).await?;
        Ok(PollView { message, state })
    }

    async fn inspect(&self, key: PollKey, actor: Principal, message: &Message) -> AppResult<PollSnapshotState> {
        let state = self
            .repository
            .snapshot(key, actor.user_id)
            .await?
            .ok_or_else(|| PortError::new("poll metadata is missing; refusing to invent empty results"))?;
        Self::check_state(message, &state.aggregate)?;
        Ok(state)
    }

    pub async fn vote(&self, actor: Principal, command: VoteCommand) -> AppResult<PollView> {
        if actor.is_bot || actor.user_id != command.user_id {
            return Err(DomainError::MissingPermissions.into());
        }
        let (access, message) = self.accessible(command.key, actor).await?;
        self.inspect(command.key, actor, &message).await?;
        let transition = match self.repository.vote(command).await {
            Err(crate::ports::PollStoreError::Rule(discordy_messages_domain::poll::PollError::Closed)) => {
                self.finish(&access, &message, command.key, false).await?;
                return Err(discordy_messages_domain::poll::PollError::Closed.into());
            }
            other => other?,
        };
        Self::check_state(&message, &transition.aggregate)?;
        if transition.changed {
            self.announce_vote(&access, command, &transition).await;
        }
        Ok(PollView {
            message,
            state: PollSnapshotState {
                aggregate: transition.aggregate,
                voter: Some(transition.voter),
            },
        })
    }

    async fn announce_vote(&self, access: &ChannelAccess, command: VoteCommand, transition: &PollTransition) {
        let changes = transition
            .removed()
            .into_iter()
            .map(|id| (id, false))
            .chain(transition.added().into_iter().map(|id| (id, true)));
        for (answer_id, selected) in changes {
            announce(
                self.events.as_ref(),
                event(
                    access,
                    command.key.channel_id,
                    MessageEventKind::PollVoteChanged {
                        message_id: command.key.message_id,
                        user_id: command.user_id,
                        answer_id,
                        selected,
                        voter: transition.voter.clone(),
                        results: transition.aggregate.clone(),
                    },
                ),
            )
            .await;
        }
    }

    pub async fn expire(&self, key: PollKey, actor: Principal) -> AppResult<PollView> {
        let (access, message) = self.accessible(key, actor).await?;
        if message.author_id != actor.user_id {
            return Err(DomainError::MissingPermissions.into());
        }
        self.inspect(key, actor, &message).await?;
        self.finish(&access, &message, key, true).await?;
        self.get(key, actor).await
    }

    async fn finish(&self, access: &ChannelAccess, message: &Message, key: PollKey, early: bool) -> AppResult<()> {
        let (results, changed) = self
            .repository
            .close(key, early)
            .await?
            .ok_or_else(|| PortError::new("poll metadata is missing"))?;
        Self::check_state(message, &results)?;
        if changed {
            announce(
                self.events.as_ref(),
                event(
                    access,
                    key.channel_id,
                    MessageEventKind::PollFinalized {
                        message: message.clone(),
                        results,
                    },
                ),
            )
            .await;
        }
        Ok(())
    }
}

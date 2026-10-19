use std::sync::Arc;

use discordy_messages_domain::{ReadState, UserId};

use super::MessagingPorts;
use crate::error::AppResult;
use crate::ports::ReadStateRepository;

/// Internal: every read state of a user, for the gateway's READY payload.
pub struct ListReadStates {
    read_states: Arc<dyn ReadStateRepository>,
}

impl ListReadStates {
    pub fn new(ports: &MessagingPorts) -> Self {
        Self {
            read_states: ports.read_states.clone(),
        }
    }

    pub async fn execute(&self, user: UserId) -> AppResult<Vec<ReadState>> {
        Ok(self.read_states.for_user(user).await?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fakes::*;

    #[tokio::test]
    async fn lists_only_the_users_states() {
        let h = Harness::new();
        h.read_states.save(&ReadState::unread(ALICE, CHANNEL)).await.unwrap();
        h.read_states.save(&ReadState::unread(BOB, CHANNEL)).await.unwrap();
        let states = ListReadStates::new(&h.ports).execute(BOB).await.unwrap();
        assert_eq!(states.len(), 1);
        assert_eq!(states[0].user_id, BOB);
    }
}

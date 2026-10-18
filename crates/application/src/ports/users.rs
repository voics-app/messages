use std::collections::HashMap;

use async_trait::async_trait;
use discordy_messages_domain::UserId;

/// The public part of a user, as the api service knows it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserProfile {
    pub id: UserId,
    pub username: String,
    pub global_name: Option<String>,
    pub avatar: Option<String>,
    pub bot: bool,
    pub system: bool,
    pub flags: u64,
}

/// Resolves message authors to profiles (the api service's internal batch
/// lookup). Hydration is best effort: ids that cannot be resolved, or an
/// unavailable api, simply leave the id out of the answer. It never fails.
#[async_trait]
pub trait UserDirectory: Send + Sync {
    async fn lookup(&self, ids: &[UserId]) -> HashMap<UserId, UserProfile>;
}

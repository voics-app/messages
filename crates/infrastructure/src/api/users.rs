//! `POST /internal/v1/users/lookup` with a 60 second cache.
//!
//! Misses are fetched in one batch per call. Hydration is best effort: when
//! the api is down or answers badly, the ids stay unresolved and nothing is
//! cached, so the next call tries again.

use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;
use contracts::{LookedUpUser, UsersLookupRequest};
use discordy_messages_application::PortError;
use discordy_messages_application::ports::{UserDirectory, UserProfile};
use discordy_messages_domain::{Snowflake, UserId};
use moka::future::Cache;

use super::{InternalApi, port_error};

pub const USER_CACHE_TTL: Duration = Duration::from_secs(60);
const USER_CACHE_CAPACITY: u64 = 50_000;
/// The api accepts at most this many ids per lookup.
const LOOKUP_BATCH: usize = 100;

pub struct HttpUserDirectory {
    api: InternalApi,
    cache: Cache<UserId, UserProfile>,
}

impl HttpUserDirectory {
    pub fn new(api: InternalApi) -> Self {
        Self::with_ttl(api, USER_CACHE_TTL)
    }

    pub fn with_ttl(api: InternalApi, ttl: Duration) -> Self {
        Self {
            api,
            cache: Cache::builder()
                .max_capacity(USER_CACHE_CAPACITY)
                .time_to_live(ttl)
                .build(),
        }
    }

    async fn fetch(&self, ids: &[UserId]) -> Result<Vec<UserProfile>, PortError> {
        let request = UsersLookupRequest {
            ids: ids.iter().map(ToString::to_string).collect(),
        };
        let response = self
            .api
            .post("/internal/v1/users/lookup")
            .json(&request)
            .send()
            .await
            .map_err(port_error)?;
        if !response.status().is_success() {
            return Err(port_error(format!("users lookup returned {}", response.status())));
        }
        let users: Vec<LookedUpUser> = response.json().await.map_err(port_error)?;
        Ok(users.into_iter().filter_map(to_profile).collect())
    }
}

fn to_profile(user: LookedUpUser) -> Option<UserProfile> {
    Some(UserProfile {
        id: user.id.parse::<Snowflake>().ok()?,
        username: user.username,
        global_name: user.global_name,
        avatar: user.avatar,
        bot: user.bot,
        system: user.system,
        flags: user.flags,
    })
}

#[async_trait]
impl UserDirectory for HttpUserDirectory {
    async fn lookup(&self, ids: &[UserId]) -> HashMap<UserId, UserProfile> {
        let mut found = HashMap::new();
        let mut missing = Vec::new();
        for id in ids {
            match self.cache.get(id).await {
                Some(profile) => {
                    found.insert(*id, profile);
                }
                None => missing.push(*id),
            }
        }
        missing.sort_unstable();
        missing.dedup();
        for batch in missing.chunks(LOOKUP_BATCH) {
            match self.fetch(batch).await {
                Ok(profiles) => {
                    for profile in profiles {
                        self.cache.insert(profile.id, profile.clone()).await;
                        found.insert(profile.id, profile);
                    }
                }
                Err(error) => tracing::warn!(%error, "author lookup failed; serving ids only"),
            }
        }
        found
    }
}

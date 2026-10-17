//! `GET /internal/v1/channels/{id}/access/{user}` with a short-lived cache.
//!
//! Answers (including "not a member" and "unknown channel") are cached for
//! five seconds; concurrent misses for the same key share one request.
//! Transport failures are never cached.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use contracts::ChannelAccessResponse;
use discordy_messages_application::PortError;
use discordy_messages_application::ports::{AccessError, ChannelAccessProvider};
use discordy_messages_domain::{ChannelAccess, ChannelId, DeliveryMode, Permissions, Snowflake, UserId};
use moka::future::Cache;
use reqwest::StatusCode;

use super::{InternalApi, port_error};

pub const ACCESS_CACHE_TTL: Duration = Duration::from_secs(5);
const ACCESS_CACHE_CAPACITY: u64 = 100_000;

type Answer = Result<ChannelAccess, AccessError>;

pub struct HttpChannelAccess {
    api: InternalApi,
    cache: Cache<(ChannelId, UserId), Answer>,
}

impl HttpChannelAccess {
    pub fn new(api: InternalApi) -> Self {
        Self::with_ttl(api, ACCESS_CACHE_TTL)
    }

    pub fn with_ttl(api: InternalApi, ttl: Duration) -> Self {
        Self {
            api,
            cache: Cache::builder()
                .max_capacity(ACCESS_CACHE_CAPACITY)
                .time_to_live(ttl)
                .build(),
        }
    }

    async fn fetch(&self, channel_id: ChannelId, user_id: UserId) -> Result<Answer, PortError> {
        let response = self
            .api
            .get(&format!("/internal/v1/channels/{channel_id}/access/{user_id}"))
            .send()
            .await
            .map_err(port_error)?;
        match response.status() {
            StatusCode::FORBIDDEN => Ok(Err(AccessError::NotMember)),
            StatusCode::NOT_FOUND => Ok(Err(AccessError::UnknownChannel)),
            status if status.is_success() => {
                let body: ChannelAccessResponse = response.json().await.map_err(port_error)?;
                Ok(Ok(to_domain(body)?))
            }
            status => Err(port_error(format!("access endpoint returned {status}"))),
        }
    }
}

fn to_domain(body: ChannelAccessResponse) -> Result<ChannelAccess, PortError> {
    let parse_id = |raw: &str| raw.parse::<Snowflake>().map_err(port_error);
    Ok(ChannelAccess {
        permissions: Permissions::new(body.permissions.parse().map_err(port_error)?),
        guild_id: body.guild_id.as_deref().map(parse_id).transpose()?,
        channel_type: body.channel_type,
        delivery_mode: body.delivery_mode.parse::<DeliveryMode>().map_err(port_error)?,
        recipient_ids: body
            .recipient_ids
            .iter()
            .map(|raw| parse_id(raw))
            .collect::<Result<_, _>>()?,
    })
}

#[async_trait]
impl ChannelAccessProvider for HttpChannelAccess {
    async fn access(&self, channel_id: ChannelId, user_id: UserId) -> Result<ChannelAccess, AccessError> {
        self.cache
            .try_get_with((channel_id, user_id), self.fetch(channel_id, user_id))
            .await
            .map_err(|error: Arc<PortError>| AccessError::Unavailable((*error).clone()))?
    }
}

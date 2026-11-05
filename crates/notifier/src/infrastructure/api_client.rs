//! The api service's internal endpoints the notifier uses: who may be pushed for a channel, and
//! removal of tokens the provider rejected.

use std::time::Duration;

use async_trait::async_trait;
use serde::Deserialize;

use crate::application::{
    ChannelTargets, DeviceToken, Environment, Platform, PortError, Target, TargetDirectory, TokenRegistry,
};
use crate::domain::{ChannelContext, Level, Recipient};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

pub struct ApiClient {
    http: reqwest::Client,
    base_url: String,
    internal_token: String,
}

impl ApiClient {
    pub fn new(base_url: &str, internal_token: String) -> Result<Self, PortError> {
        let http = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .build()
            .map_err(PortError::new)?;
        Ok(Self {
            http,
            base_url: base_url.trim_end_matches('/').to_owned(),
            internal_token,
        })
    }
}

#[derive(Deserialize)]
struct TargetsBody {
    guild_name: Option<String>,
    channel_name: Option<String>,
    delivery_mode: String,
    targets: Vec<TargetBody>,
}

#[derive(Deserialize)]
struct TargetBody {
    user_id: String,
    role_ids: Vec<String>,
    online: bool,
    level: u8,
    muted: bool,
    suppress_everyone: bool,
    suppress_roles: bool,
    tokens: Vec<TokenBody>,
}

#[derive(Deserialize)]
struct TokenBody {
    platform: String,
    token: String,
    environment: String,
    previews: bool,
}

impl TokenBody {
    /// A token of a platform or environment this version does not know is left out.
    fn into_token(self) -> Option<DeviceToken> {
        let platform = match self.platform.as_str() {
            "apns" => Platform::Apns,
            "fcm" => Platform::Fcm,
            _ => return None,
        };
        let environment = match self.environment.as_str() {
            "production" => Environment::Production,
            "sandbox" => Environment::Sandbox,
            _ => return None,
        };
        Some(DeviceToken {
            platform,
            token: self.token,
            environment,
            previews: self.previews,
        })
    }
}

impl From<TargetsBody> for ChannelTargets {
    fn from(body: TargetsBody) -> Self {
        ChannelTargets {
            context: ChannelContext {
                guild_name: body.guild_name,
                channel_name: body.channel_name,
                delivery_mode: body.delivery_mode,
            },
            targets: body
                .targets
                .into_iter()
                .map(|target| Target {
                    recipient: Recipient {
                        user_id: target.user_id,
                        role_ids: target.role_ids,
                        online: target.online,
                        level: Level::from_code(target.level),
                        muted: target.muted,
                        suppress_everyone: target.suppress_everyone,
                        suppress_roles: target.suppress_roles,
                    },
                    tokens: target.tokens.into_iter().filter_map(TokenBody::into_token).collect(),
                })
                .collect(),
        }
    }
}

#[async_trait]
impl TargetDirectory for ApiClient {
    async fn targets(&self, channel_id: &str, author_id: &str) -> Result<ChannelTargets, PortError> {
        let response = self
            .http
            .get(format!(
                "{}/internal/v1/channels/{channel_id}/push-targets",
                self.base_url
            ))
            .query(&[("author_id", author_id)])
            .header("x-internal-token", &self.internal_token)
            .send()
            .await
            .map_err(|error| PortError::new(format!("api: {error}")))?;
        let status = response.status();
        if !status.is_success() {
            return Err(PortError::new(format!("api answered {status} for push-targets")));
        }
        let body: TargetsBody = response
            .json()
            .await
            .map_err(|error| PortError::new(format!("api: {error}")))?;
        Ok(body.into())
    }
}

#[async_trait]
impl TokenRegistry for ApiClient {
    async fn forget(&self, token: &DeviceToken) -> Result<(), PortError> {
        let response = self
            .http
            .delete(format!("{}/internal/v1/push-tokens", self.base_url))
            .header("x-internal-token", &self.internal_token)
            .json(&serde_json::json!({"platform": token.platform.label(), "token": token.token}))
            .send()
            .await
            .map_err(|error| PortError::new(format!("api: {error}")))?;
        let status = response.status();
        if status.is_success() {
            Ok(())
        } else {
            Err(PortError::new(format!("api answered {status} removing a push token")))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use axum::Router;
    use axum::extract::{Path, Query, State};
    use axum::http::HeaderMap;
    use axum::routing::{delete, get};
    use serde_json::{Value, json};

    use super::*;

    #[derive(Clone, Default)]
    struct Log(Arc<Mutex<Vec<String>>>);

    async fn serve(log: Log) -> String {
        async fn targets(
            State(log): State<Log>,
            Path(channel): Path<String>,
            Query(query): Query<std::collections::HashMap<String, String>>,
            headers: HeaderMap,
        ) -> axum::Json<Value> {
            log.0.lock().unwrap().push(format!(
                "GET {channel} author={} auth={}",
                query["author_id"],
                headers["x-internal-token"].to_str().unwrap()
            ));
            axum::Json(json!({
                "guild_id": "1", "guild_name": "Test Guild", "channel_name": "general",
                "delivery_mode": "standard",
                "targets": [{
                    "user_id": "42", "role_ids": ["9"], "online": false, "level": 1, "muted": true,
                    "suppress_everyone": true, "suppress_roles": false,
                    "tokens": [
                        {"platform": "apns", "token": "abc", "environment": "sandbox", "previews": false},
                        {"platform": "wns", "token": "x", "environment": "production", "previews": true},
                        {"platform": "fcm", "token": "f", "environment": "staging", "previews": true}
                    ]
                }]
            }))
        }
        async fn forget(State(log): State<Log>, axum::Json(body): axum::Json<Value>) {
            log.0.lock().unwrap().push(format!("DELETE {body}"));
        }
        let app = Router::new()
            .route("/internal/v1/channels/{id}/push-targets", get(targets))
            .route("/internal/v1/push-tokens", delete(forget))
            .with_state(log);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        url
    }

    #[tokio::test]
    async fn targets_are_fetched_with_the_internal_token_and_mapped() {
        let log = Log::default();
        let client = ApiClient::new(&serve(log.clone()).await, "secret".into()).unwrap();

        let found = client.targets("10", "7").await.unwrap();

        assert_eq!(log.0.lock().unwrap()[0], "GET 10 author=7 auth=secret");
        assert_eq!(found.context.delivery_mode, "standard");
        assert_eq!(found.context.guild_name.as_deref(), Some("Test Guild"));
        let target = &found.targets[0];
        assert_eq!(target.recipient.level, Level::OnlyMentions);
        assert!(target.recipient.muted && target.recipient.suppress_everyone && !target.recipient.online);
        assert_eq!(target.recipient.role_ids, vec!["9"]);
        assert_eq!(
            target.tokens,
            vec![DeviceToken {
                platform: Platform::Apns,
                token: "abc".into(),
                environment: Environment::Sandbox,
                previews: false
            }],
            "unknown platforms and environments are dropped"
        );
    }

    #[tokio::test]
    async fn a_rejected_token_is_deleted_by_platform_and_value() {
        let log = Log::default();
        let client = ApiClient::new(&serve(log.clone()).await, "secret".into()).unwrap();
        let token = DeviceToken {
            platform: Platform::Apns,
            token: "abc".into(),
            environment: Environment::Sandbox,
            previews: true,
        };
        client.forget(&token).await.unwrap();
        assert_eq!(log.0.lock().unwrap()[0], r#"DELETE {"platform":"apns","token":"abc"}"#);
    }

    #[tokio::test]
    async fn an_api_error_is_reported_not_swallowed() {
        let client = ApiClient::new("http://127.0.0.1:1", "secret".into()).unwrap();
        assert!(client.targets("10", "7").await.is_err());
        let token = DeviceToken {
            platform: Platform::Apns,
            token: "abc".into(),
            environment: Environment::Sandbox,
            previews: true,
        };
        assert!(client.forget(&token).await.is_err());
    }
}

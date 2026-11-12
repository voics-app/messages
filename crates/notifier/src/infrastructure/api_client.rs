//! The api service's internal endpoints the notifier uses: who may be pushed for a channel, and
//! removal of tokens the provider rejected.

use std::time::Duration;

use async_trait::async_trait;
use serde::Deserialize;

use crate::application::{
    ChannelTargets, DeviceDirectory, DevicePage, DeviceToken, Environment, Platform, PortError, Target,
    TargetDirectory, TokenRegistry,
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
    #[serde(default)]
    voip_tokens: Vec<VoipTokenBody>,
}

#[derive(Deserialize)]
struct VoipTokenBody {
    token: String,
    environment: String,
}

impl VoipTokenBody {
    /// A PushKit token is an APNs token: it never previews text and has no mention preference.
    fn into_token(self) -> Option<DeviceToken> {
        let environment = match self.environment.as_str() {
            "production" => Environment::Production,
            "sandbox" => Environment::Sandbox,
            _ => return None,
        };
        Some(DeviceToken {
            platform: Platform::Apns,
            token: self.token,
            environment,
            previews: false,
            only_mentions: false,
        })
    }
}

#[derive(Deserialize)]
struct TokenBody {
    platform: String,
    token: String,
    environment: String,
    previews: bool,
    #[serde(default)]
    only_mentions: bool,
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
            only_mentions: self.only_mentions,
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
                    voip_tokens: target
                        .voip_tokens
                        .into_iter()
                        .filter_map(VoipTokenBody::into_token)
                        .collect(),
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

#[derive(Deserialize)]
struct ManualBody {
    users: Vec<ManualUserBody>,
    next_after: Option<String>,
}

#[derive(Deserialize)]
struct ManualUserBody {
    tokens: Vec<TokenBody>,
}

impl ManualBody {
    fn into_page(self) -> DevicePage {
        DevicePage {
            devices: self
                .users
                .into_iter()
                .flat_map(|user| user.tokens)
                .filter_map(TokenBody::into_token)
                .collect(),
            next_after: self.next_after,
        }
    }
}

impl ApiClient {
    async fn manual(&self, path: &str, query: &[(&str, &str)]) -> Result<DevicePage, PortError> {
        let response = self
            .http
            .get(format!("{}{path}", self.base_url))
            .query(query)
            .header("x-internal-token", &self.internal_token)
            .send()
            .await
            .map_err(|error| PortError::new(format!("api: {error}")))?;
        let status = response.status();
        if !status.is_success() {
            return Err(PortError::new(format!("api answered {status} for {path}")));
        }
        let body: ManualBody = response
            .json()
            .await
            .map_err(|error| PortError::new(format!("api: {error}")))?;
        Ok(body.into_page())
    }
}

#[async_trait]
impl DeviceDirectory for ApiClient {
    async fn user_devices(&self, user_id: &str) -> Result<Vec<DeviceToken>, PortError> {
        Ok(self
            .manual(&format!("/internal/v1/users/{user_id}/push-tokens"), &[])
            .await?
            .devices)
    }

    async fn device_page(&self, after: Option<&str>) -> Result<DevicePage, PortError> {
        let query: Vec<(&str, &str)> = after.map(|a| ("after", a)).into_iter().collect();
        self.manual("/internal/v1/push-tokens", &query).await
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
                        {"platform": "apns", "token": "abc", "environment": "sandbox", "previews": false, "only_mentions": true},
                        {"platform": "wns", "token": "x", "environment": "production", "previews": true},
                        {"platform": "fcm", "token": "f", "environment": "staging", "previews": true}
                    ],
                    "voip_tokens": [
                        {"token": "voip1", "environment": "production"},
                        {"token": "voip2", "environment": "staging"}
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
                previews: false,
                only_mentions: true
            }],
            "unknown platforms and environments are dropped"
        );
        assert_eq!(
            target.voip_tokens,
            vec![DeviceToken {
                platform: Platform::Apns,
                token: "voip1".into(),
                environment: Environment::Production,
                previews: false,
                only_mentions: false
            }],
            "VoIP tokens are APNs tokens; an unknown environment is dropped"
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
            only_mentions: false,
        };
        client.forget(&token).await.unwrap();
        assert_eq!(log.0.lock().unwrap()[0], r#"DELETE {"platform":"apns","token":"abc"}"#);
    }

    #[tokio::test]
    async fn manual_push_devices_are_read_from_the_internal_endpoints() {
        async fn user(headers: HeaderMap, Path(id): Path<String>) -> axum::Json<Value> {
            assert_eq!(headers["x-internal-token"], "secret");
            assert_eq!(id, "42");
            axum::Json(json!({"users": [{"user_id": "42", "tokens": [
                {"platform": "fcm", "token": "f1", "environment": "production", "previews": true, "only_mentions": false},
                {"platform": "wns", "token": "x", "environment": "production", "previews": true, "only_mentions": false}
            ]}], "next_after": null}))
        }
        async fn page(Query(query): Query<std::collections::HashMap<String, String>>) -> axum::Json<Value> {
            match query.get("after").map(String::as_str) {
                None => axum::Json(json!({"users": [{"user_id": "1", "tokens": [
                    {"platform": "apns", "token": "a1", "environment": "sandbox", "previews": true, "only_mentions": false}
                ]}], "next_after": "1"})),
                _ => axum::Json(json!({"users": [], "next_after": null})),
            }
        }
        let app = Router::new()
            .route("/internal/v1/users/{id}/push-tokens", get(user))
            .route("/internal/v1/push-tokens", get(page));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = ApiClient::new(&url, "secret".into()).unwrap();

        let devices = client.user_devices("42").await.unwrap();
        assert_eq!(devices.len(), 1, "a platform this version does not know is left out");
        assert_eq!(devices[0].token, "f1");
        let first = client.device_page(None).await.unwrap();
        assert_eq!((first.devices.len(), first.next_after.as_deref()), (1, Some("1")));
        let last = client.device_page(Some("1")).await.unwrap();
        assert_eq!((last.devices.len(), last.next_after), (0, None));
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
            only_mentions: false,
        };
        assert!(client.forget(&token).await.is_err());
    }
}

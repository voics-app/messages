//! APNs over HTTP/2 with token-based authentication (a `.p8` key, its Key ID and the Team ID).
//!
//! The host is chosen per token: a token issued to a development build belongs to the sandbox
//! host and a store build's to the production host, unless the configuration forces one. The same
//! sender carries PushKit VoIP pushes (`OutboundPush::Voip`), which use the `.voip` topic.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use serde::Serialize;
use serde_json::{Value, json};

use crate::application::{DeviceToken, Environment, PortError, PushSender, SendError};
use crate::domain::{AlertContent, OutboundPush};

pub const PRODUCTION_URL: &str = "https://api.push.apple.com";
pub const SANDBOX_URL: &str = "https://api.sandbox.push.apple.com";

/// Apple asks for a new provider token no more often than every 20 minutes and rejects one older
/// than an hour.
const JWT_LIFETIME: Duration = Duration::from_secs(50 * 60);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Clone)]
pub struct ApnsConfig {
    /// The PKCS#8 PEM text of the `.p8` key.
    pub key_pem: String,
    pub key_id: String,
    pub team_id: String,
    /// The app's bundle identifier, which is the `apns-topic`.
    pub topic: String,
    pub production_url: String,
    pub sandbox_url: String,
    /// Sends every token to this environment instead of the one stored with it.
    pub environment_override: Option<Environment>,
    /// How long APNs keeps trying to deliver to an unreachable device.
    pub ttl: Duration,
}

impl ApnsConfig {
    pub fn new(key_pem: String, key_id: String, team_id: String, topic: String) -> Self {
        Self {
            key_pem,
            key_id,
            team_id,
            topic,
            production_url: PRODUCTION_URL.to_owned(),
            sandbox_url: SANDBOX_URL.to_owned(),
            environment_override: None,
            ttl: Duration::from_secs(3600),
        }
    }
}

#[derive(Serialize)]
struct Claims<'a> {
    iss: &'a str,
    iat: u64,
}

struct CachedJwt {
    token: String,
    issued: Instant,
}

pub struct ApnsSender {
    http: reqwest::Client,
    config: ApnsConfig,
    key: EncodingKey,
    jwt: Mutex<Option<CachedJwt>>,
    /// Tokens found to belong to the other host than the one registered with them.
    learned: Mutex<HashMap<String, Environment>>,
}

fn unix_seconds() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs())
}

impl ApnsSender {
    /// Fails when the key is not an EC PEM, so a wrong secret stops the service at startup.
    pub fn new(config: ApnsConfig) -> Result<Self, PortError> {
        let key = EncodingKey::from_ec_pem(config.key_pem.as_bytes())
            .map_err(|error| PortError::new(format!("APNs key is not a valid .p8 (PKCS#8 EC) key: {error}")))?;
        let http = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .build()
            .map_err(PortError::new)?;
        let sender = Self {
            http,
            config,
            key,
            jwt: Mutex::new(None),
            learned: Mutex::new(HashMap::new()),
        };
        sender
            .provider_token()
            .map_err(|error| PortError::new(format!("{error:?}")))?;
        Ok(sender)
    }

    fn provider_token(&self) -> Result<String, SendError> {
        let mut cached = self
            .jwt
            .lock()
            .map_err(|_| SendError::Failed("jwt cache poisoned".into()))?;
        if let Some(current) = cached.as_ref().filter(|jwt| jwt.issued.elapsed() < JWT_LIFETIME) {
            return Ok(current.token.clone());
        }
        let mut header = Header::new(Algorithm::ES256);
        header.kid = Some(self.config.key_id.clone());
        let claims = Claims {
            iss: &self.config.team_id,
            iat: unix_seconds(),
        };
        let token = jsonwebtoken::encode(&header, &claims, &self.key)
            .map_err(|error| SendError::Failed(format!("cannot sign the APNs provider token: {error}")))?;
        *cached = Some(CachedJwt {
            token: token.clone(),
            issued: Instant::now(),
        });
        Ok(token)
    }

    fn forget_provider_token(&self) {
        if let Ok(mut cached) = self.jwt.lock() {
            *cached = None;
        }
    }

    fn base_url(&self, environment: Environment) -> &str {
        match environment {
            Environment::Production => &self.config.production_url,
            Environment::Sandbox => &self.config.sandbox_url,
        }
    }
}

fn alert_payload(alert: &AlertContent) -> Value {
    let mut content = json!({"title": alert.title});
    if !alert.body.is_empty() {
        content["body"] = json!(alert.body);
    }
    let mut payload = json!({"aps": {"alert": content, "sound": "default", "thread-id": alert.thread_id}});
    for (key, value) in &alert.data {
        payload[key] = json!(value);
    }
    payload
}

#[derive(serde::Deserialize)]
struct ErrorBody {
    reason: String,
}

/// Maps an APNs answer to what the dispatcher does next.
pub(crate) fn classify(status: u16, reason: Option<&str>) -> Result<(), SendError> {
    let reason_text = reason.unwrap_or("unknown").to_owned();
    match (status, reason) {
        (200, _) => Ok(()),
        (410, _) | (400, Some("BadDeviceToken" | "DeviceTokenNotForTopic")) => {
            Err(SendError::InvalidToken(reason_text))
        }
        (403, Some("ExpiredProviderToken")) | (429, _) | (500..=599, _) => Err(SendError::Retry {
            reason: format!("{status} {reason_text}"),
            after: None,
        }),
        _ => Err(SendError::Failed(format!("{status} {reason_text}"))),
    }
}

/// The most tokens whose corrected host is remembered; the map is cleared when it fills up.
const LEARNED_LIMIT: usize = 10_000;

fn other(environment: Environment) -> Environment {
    match environment {
        Environment::Production => Environment::Sandbox,
        Environment::Sandbox => Environment::Production,
    }
}

impl ApnsSender {
    fn learned_host(&self, token: &str) -> Option<Environment> {
        self.learned.lock().ok()?.get(token).copied()
    }

    fn learn_host(&self, token: &str, environment: Environment) {
        if let Ok(mut learned) = self.learned.lock() {
            if learned.len() >= LEARNED_LIMIT {
                learned.clear();
            }
            learned.insert(token.to_owned(), environment);
        }
    }

    /// One request to the APNs host of `environment`.
    async fn post(&self, environment: Environment, token: &str, push: &OutboundPush) -> Result<(), SendError> {
        let (push_type, topic, payload) = match push {
            OutboundPush::Alert(alert) => ("alert", self.config.topic.clone(), alert_payload(alert)),
            OutboundPush::Voip(payload) => ("voip", format!("{}.voip", self.config.topic), payload.clone()),
        };
        // A call that is not delivered at once is not delivered at all: APNs tries once and drops it, so a ring never arrives after the caller hung up.
        let expiration = match push {
            OutboundPush::Alert(_) => unix_seconds() + self.config.ttl.as_secs(),
            OutboundPush::Voip(_) => 0,
        };
        let mut request = self
            .http
            .post(format!("{}/3/device/{token}", self.base_url(environment)))
            .header("authorization", format!("bearer {}", self.provider_token()?))
            .header("apns-topic", topic)
            .header("apns-push-type", push_type)
            .header("apns-priority", "10")
            .header("apns-expiration", expiration.to_string());
        if let OutboundPush::Alert(alert) = push {
            if alert
                .data
                .iter()
                .any(|(key, value)| key == "type" && value == "scheduled_event")
                && alert.thread_id.len() <= 64
                && alert.thread_id.is_ascii()
            {
                request = request.header("apns-collapse-id", &alert.thread_id);
            }
        }
        let response = request.json(&payload).send().await.map_err(|error| SendError::Retry {
            reason: format!("transport: {error}"),
            after: None,
        })?;
        let status = response.status().as_u16();
        if status == 200 {
            return Ok(());
        }
        let reason = response.json::<ErrorBody>().await.ok().map(|body| body.reason);
        let label: String = reason
            .as_deref()
            .unwrap_or("unknown")
            .chars()
            .filter(char::is_ascii_alphanumeric)
            .take(40)
            .collect();
        metrics::counter!("notifier_apns_errors_total", "status" => status.to_string(), "reason" => label).increment(1);
        let result = classify(status, reason.as_deref());
        if matches!((status, reason.as_deref()), (403, Some("ExpiredProviderToken"))) {
            self.forget_provider_token();
        }
        result
    }
}

#[async_trait]
impl PushSender for ApnsSender {
    /// Sends to the host the token was issued for. `BadDeviceToken` is what APNs answers when a
    /// sandbox token reaches the production host or the other way round (an ad-hoc build can carry
    /// either, depending on how it was signed), so the other host gets one try before the token is
    /// declared dead, and a token that works there is remembered. A forced environment never falls back.
    async fn send(&self, token: &DeviceToken, push: &OutboundPush) -> Result<(), SendError> {
        if let Some(forced) = self.config.environment_override {
            return self.post(forced, &token.token, push).await;
        }
        let first = self.learned_host(&token.token).unwrap_or(token.environment);
        match self.post(first, &token.token, push).await {
            Err(SendError::InvalidToken(reason)) if reason == "BadDeviceToken" => {
                let second = other(first);
                let result = self.post(second, &token.token, push).await;
                if result.is_ok() {
                    metrics::counter!("notifier_apns_host_corrections_total").increment(1);
                    self.learn_host(&token.token, second);
                }
                result
            }
            result => result,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::Router;
    use axum::extract::{Path, State};
    use axum::http::{HeaderMap, StatusCode};
    use axum::routing::post;
    use jsonwebtoken::{DecodingKey, Validation};
    use p256::pkcs8::{EncodePrivateKey, EncodePublicKey, LineEnding};

    use super::*;
    use crate::application::Platform;

    #[derive(Clone, Default)]
    struct Seen(Arc<Mutex<Vec<(String, HeaderMap, Value)>>>);

    struct Keys {
        private: String,
        public: String,
    }

    fn keys() -> Keys {
        let secret = p256::SecretKey::random(&mut rand_core::OsRng);
        Keys {
            private: secret.to_pkcs8_pem(LineEnding::LF).unwrap().to_string(),
            public: secret.public_key().to_public_key_pem(LineEnding::LF).unwrap(),
        }
    }

    /// A fake APNs host: records every request, answers `status` with `reason` for tokens that
    /// start with `bad-`, `Unregistered` (410) for `gone-`, and 200 otherwise.
    async fn fake_apns(seen: Seen, reply: (StatusCode, &'static str)) -> String {
        async fn handle(
            State((seen, reply)): State<(Seen, (StatusCode, &'static str))>,
            Path(token): Path<String>,
            headers: HeaderMap,
            axum::Json(body): axum::Json<Value>,
        ) -> (StatusCode, String) {
            seen.0.lock().unwrap().push((token.clone(), headers, body));
            if token.starts_with("gone-") {
                return (StatusCode::GONE, json!({"reason": "Unregistered"}).to_string());
            }
            if token.starts_with("bad-") {
                return (reply.0, json!({"reason": reply.1}).to_string());
            }
            (StatusCode::OK, String::new())
        }
        let app = Router::new()
            .route("/3/device/{token}", post(handle))
            .with_state((seen, reply));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        url
    }

    fn sender(keys: &Keys, production: &str, sandbox: &str) -> ApnsSender {
        let mut config = ApnsConfig::new(
            keys.private.clone(),
            "KEYID12345".into(),
            "TEAMID1234".into(),
            "nl.example.app".into(),
        );
        config.production_url = production.into();
        config.sandbox_url = sandbox.into();
        ApnsSender::new(config).unwrap()
    }

    fn device(token: &str, environment: Environment) -> DeviceToken {
        DeviceToken {
            platform: Platform::Apns,
            token: token.into(),
            environment,
            previews: true,
            only_mentions: false,
        }
    }

    fn alert() -> OutboundPush {
        OutboundPush::Alert(AlertContent {
            title: "Alice".into(),
            body: "hi".into(),
            thread_id: "10".into(),
            data: vec![("channel_id".into(), "10".into()), ("message_id".into(), "100".into())],
        })
    }

    #[tokio::test]
    async fn an_alert_is_posted_with_token_auth_to_the_host_of_the_tokens_environment() {
        let keys = keys();
        let (production, sandbox) = (Seen::default(), Seen::default());
        let production_url = fake_apns(production.clone(), (StatusCode::OK, "")).await;
        let sandbox_url = fake_apns(sandbox.clone(), (StatusCode::OK, "")).await;
        let sender = sender(&keys, &production_url, &sandbox_url);

        sender
            .send(&device("dev-token", Environment::Sandbox), &alert())
            .await
            .unwrap();
        sender
            .send(&device("store-token", Environment::Production), &alert())
            .await
            .unwrap();

        assert_eq!(sandbox.0.lock().unwrap().len(), 1);
        assert_eq!(production.0.lock().unwrap().len(), 1);
        let seen = sandbox.0.lock().unwrap();
        let (path_token, headers, body) = &seen[0];
        assert_eq!(path_token, "dev-token");
        assert_eq!(headers["apns-topic"], "nl.example.app");
        assert_eq!(headers["apns-push-type"], "alert");
        assert_eq!(headers["apns-priority"], "10");
        assert!(headers.contains_key("apns-expiration"));
        assert_eq!(body["aps"]["alert"]["title"], "Alice");
        assert_eq!(body["aps"]["alert"]["body"], "hi");
        assert_eq!(body["aps"]["thread-id"], "10");
        assert_eq!(body["channel_id"], "10");
        assert_eq!(body["message_id"], "100");

        let bearer = headers["authorization"]
            .to_str()
            .unwrap()
            .strip_prefix("bearer ")
            .unwrap()
            .to_owned();
        let mut validation = Validation::new(Algorithm::ES256);
        validation.required_spec_claims.clear();
        validation.validate_exp = false;
        let decoded = jsonwebtoken::decode::<Value>(
            &bearer,
            &DecodingKey::from_ec_pem(keys.public.as_bytes()).unwrap(),
            &validation,
        )
        .unwrap();
        assert_eq!(decoded.header.kid.as_deref(), Some("KEYID12345"));
        assert_eq!(decoded.claims["iss"], "TEAMID1234");
        assert!(decoded.claims["iat"].as_u64().unwrap() > 1_700_000_000);
        let other = production.0.lock().unwrap()[0].1["authorization"].clone();
        assert_eq!(headers["authorization"], other, "the provider token is reused");
    }

    #[tokio::test]
    async fn the_environment_override_wins_over_the_stored_one() {
        let keys = keys();
        let (production, sandbox) = (Seen::default(), Seen::default());
        let production_url = fake_apns(production.clone(), (StatusCode::OK, "")).await;
        let sandbox_url = fake_apns(sandbox.clone(), (StatusCode::OK, "")).await;
        let mut config = ApnsConfig::new(keys.private.clone(), "K".into(), "T".into(), "nl.example.app".into());
        config.production_url = production_url;
        config.sandbox_url = sandbox_url;
        config.environment_override = Some(Environment::Sandbox);
        let sender = ApnsSender::new(config).unwrap();

        sender
            .send(&device("t", Environment::Production), &alert())
            .await
            .unwrap();

        assert_eq!(sandbox.0.lock().unwrap().len(), 1);
        assert!(production.0.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_token_on_the_wrong_host_is_retried_on_the_other_one_and_remembered() {
        let keys = keys();
        let (production, sandbox) = (Seen::default(), Seen::default());
        let production_url = fake_apns(production.clone(), (StatusCode::BAD_REQUEST, "BadDeviceToken")).await;
        let sandbox_url = fake_apns(sandbox.clone(), (StatusCode::OK, "")).await;
        let sender = sender(&keys, &production_url, &sandbox_url);
        let registered_as_production = device("bad-1", Environment::Production);

        sender.send(&registered_as_production, &alert()).await.unwrap();
        sender.send(&registered_as_production, &alert()).await.unwrap();

        assert_eq!(
            production.0.lock().unwrap().len(),
            1,
            "only the first send tried production"
        );
        assert_eq!(sandbox.0.lock().unwrap().len(), 2, "the host that worked is remembered");
    }

    #[tokio::test]
    async fn a_token_both_hosts_reject_is_invalid_and_a_forced_host_never_falls_back() {
        let keys = keys();
        let (production, sandbox) = (Seen::default(), Seen::default());
        let rejecting = (StatusCode::BAD_REQUEST, "BadDeviceToken");
        let production_url = fake_apns(production.clone(), rejecting).await;
        let sandbox_url = fake_apns(sandbox.clone(), rejecting).await;
        let sender = sender(&keys, &production_url, &sandbox_url);
        assert_eq!(
            sender.send(&device("bad-1", Environment::Production), &alert()).await,
            Err(SendError::InvalidToken("BadDeviceToken".into()))
        );
        assert_eq!(
            (production.0.lock().unwrap().len(), sandbox.0.lock().unwrap().len()),
            (1, 1)
        );

        let mut config = ApnsConfig::new(keys.private.clone(), "K".into(), "T".into(), "nl.example.app".into());
        config.production_url = production_url;
        config.sandbox_url = sandbox_url;
        config.environment_override = Some(Environment::Production);
        let forced = ApnsSender::new(config).unwrap();
        let before = sandbox.0.lock().unwrap().len();
        let _ = forced.send(&device("bad-2", Environment::Sandbox), &alert()).await;
        assert_eq!(
            sandbox.0.lock().unwrap().len(),
            before,
            "no fallback when the host is forced"
        );
    }

    #[tokio::test]
    async fn a_title_only_alert_has_no_body_key() {
        let keys = keys();
        let seen = Seen::default();
        let url = fake_apns(seen.clone(), (StatusCode::OK, "")).await;
        let sender = sender(&keys, &url, &url);
        let generic = OutboundPush::Alert(AlertContent {
            title: "New message".into(),
            body: String::new(),
            thread_id: "10".into(),
            data: Vec::new(),
        });
        sender.send(&device("t", Environment::Sandbox), &generic).await.unwrap();
        let body = seen.0.lock().unwrap()[0].2.clone();
        assert_eq!(
            body,
            json!({"aps": {"alert": {"title": "New message"}, "sound": "default", "thread-id": "10"}})
        );
    }

    #[tokio::test]
    async fn voip_pushes_use_the_voip_topic_and_type() {
        let keys = keys();
        let seen = Seen::default();
        let url = fake_apns(seen.clone(), (StatusCode::OK, "")).await;
        let sender = sender(&keys, &url, &url);
        let call = OutboundPush::Voip(json!({"call_id": "9"}));
        sender.send(&device("t", Environment::Production), &call).await.unwrap();
        let seen = seen.0.lock().unwrap();
        assert_eq!(seen[0].1["apns-topic"], "nl.example.app.voip");
        assert_eq!(seen[0].1["apns-push-type"], "voip");
        assert_eq!(
            seen[0].1["apns-expiration"], "0",
            "a call is tried once and never delivered late"
        );
        assert_eq!(seen[0].2, json!({"call_id": "9"}));
    }

    #[tokio::test]
    async fn provider_answers_map_to_what_the_dispatcher_should_do() {
        let keys = keys();
        let seen = Seen::default();
        let url = fake_apns(seen.clone(), (StatusCode::BAD_REQUEST, "BadDeviceToken")).await;
        let sender = sender(&keys, &url, &url);
        let sandbox = |t: &str| device(t, Environment::Sandbox);

        assert_eq!(
            sender.send(&sandbox("gone-1"), &alert()).await,
            Err(SendError::InvalidToken("Unregistered".into()))
        );
        assert_eq!(
            sender.send(&sandbox("bad-1"), &alert()).await,
            Err(SendError::InvalidToken("BadDeviceToken".into()))
        );
    }

    #[test]
    fn classification_covers_the_documented_failures() {
        assert!(classify(200, None).is_ok());
        assert!(matches!(
            classify(410, Some("Unregistered")),
            Err(SendError::InvalidToken(_))
        ));
        assert!(matches!(
            classify(400, Some("DeviceTokenNotForTopic")),
            Err(SendError::InvalidToken(_))
        ));
        assert!(matches!(
            classify(429, Some("TooManyRequests")),
            Err(SendError::Retry { .. })
        ));
        assert!(matches!(
            classify(503, Some("ServiceUnavailable")),
            Err(SendError::Retry { .. })
        ));
        assert!(matches!(classify(500, None), Err(SendError::Retry { .. })));
        assert!(matches!(
            classify(403, Some("ExpiredProviderToken")),
            Err(SendError::Retry { .. })
        ));
        assert!(matches!(
            classify(403, Some("InvalidProviderToken")),
            Err(SendError::Failed(_))
        ));
        assert!(matches!(classify(400, Some("PayloadEmpty")), Err(SendError::Failed(_))));
        assert!(matches!(
            classify(413, Some("PayloadTooLarge")),
            Err(SendError::Failed(_))
        ));
    }

    #[tokio::test]
    async fn an_expired_provider_token_is_dropped_so_the_retry_signs_a_new_one() {
        let keys = keys();
        let url = fake_apns(Seen::default(), (StatusCode::FORBIDDEN, "ExpiredProviderToken")).await;
        let sender = sender(&keys, &url, &url);
        let result = sender.send(&device("bad-1", Environment::Sandbox), &alert()).await;
        assert!(matches!(result, Err(SendError::Retry { .. })));
        assert!(sender.jwt.lock().unwrap().is_none());
        sender
            .send(&device("ok-1", Environment::Sandbox), &alert())
            .await
            .unwrap();
        assert!(sender.jwt.lock().unwrap().is_some());
    }

    #[test]
    fn a_key_that_is_not_a_p8_stops_the_service_at_startup() {
        let config = ApnsConfig::new("not a key".into(), "K".into(), "T".into(), "nl.example.app".into());
        let error = ApnsSender::new(config).err().expect("must fail");
        assert!(error.to_string().contains("not a valid .p8"), "{error}");
    }

    #[tokio::test]
    async fn an_unreachable_host_is_a_retryable_failure() {
        let keys = keys();
        let sender = sender(&keys, "http://127.0.0.1:1", "http://127.0.0.1:1");
        let result = sender.send(&device("t", Environment::Sandbox), &alert()).await;
        assert!(matches!(result, Err(SendError::Retry { .. })), "{result:?}");
    }
}

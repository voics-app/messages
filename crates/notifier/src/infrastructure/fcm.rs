//! Firebase Cloud Messaging HTTP v1 for Android.
//!
//! Authentication: the service-account JSON (Firebase console, Project settings, Service
//! accounts) signs a short-lived RS256 JWT that is exchanged at the token endpoint for an OAuth2
//! access token, which is cached until shortly before it expires. Messages go to
//! `POST {base}/v1/projects/{project_id}/messages:send`.
//!
//! An alert becomes a notification plus data message (the system shows it while the app is
//! closed, the data lets a tap open the chat). A call ring (`OutboundPush::Voip`) becomes a
//! data-only high-priority message with a short time to live, so the app wakes and shows its own
//! full-screen incoming-call screen. Neither carries more text than the shared composer allows.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::application::{DeviceToken, PortError, PushSender, SendError};
use crate::domain::{AlertContent, OutboundPush};

pub const FCM_URL: &str = "https://fcm.googleapis.com";
pub const DEFAULT_TOKEN_URI: &str = "https://oauth2.googleapis.com/token";
pub const SCOPE: &str = "https://www.googleapis.com/auth/firebase.messaging";
/// The Android notification channel the app creates for message notifications.
pub const ANDROID_CHANNEL: &str = "messages";

const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const ASSERTION_LIFETIME_SECS: u64 = 3600;
/// An access token is renewed this long before Google says it expires.
const EXPIRY_MARGIN_SECS: u64 = 120;
/// A call ring is useless once the phone stopped ringing.
const CALL_TTL: &str = "30s";

/// The fields of the service-account key file the sender uses.
#[derive(Deserialize)]
struct ServiceAccount {
    project_id: String,
    client_email: String,
    private_key: String,
    #[serde(default)]
    token_uri: Option<String>,
}

pub struct FcmConfig {
    /// The whole service-account JSON.
    pub service_account_json: String,
    pub fcm_url: String,
    /// Overrides the key file's `token_uri` (tests only).
    pub token_uri_override: Option<String>,
    /// How long FCM keeps a message for an unreachable device.
    pub ttl: Duration,
}

impl FcmConfig {
    pub fn new(service_account_json: String) -> Self {
        Self {
            service_account_json,
            fcm_url: FCM_URL.to_owned(),
            token_uri_override: None,
            ttl: Duration::from_secs(3600),
        }
    }
}

#[derive(Serialize)]
struct Claims<'a> {
    iss: &'a str,
    scope: &'a str,
    aud: &'a str,
    iat: u64,
    exp: u64,
}

struct CachedToken {
    access_token: String,
    /// Unix seconds from which the token must be renewed.
    renew_at: u64,
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
    #[serde(default)]
    expires_in: Option<u64>,
}

pub struct FcmSender {
    http: reqwest::Client,
    project_id: String,
    client_email: String,
    token_uri: String,
    key: EncodingKey,
    fcm_url: String,
    ttl: Duration,
    cached: tokio::sync::Mutex<Option<CachedToken>>,
    clock: Box<dyn Fn() -> u64 + Send + Sync>,
}

fn unix_seconds() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs())
}

impl FcmSender {
    /// Fails when the JSON or its private key is unusable, so a wrong secret stops the service at startup.
    pub fn new(config: FcmConfig) -> Result<Self, PortError> {
        Self::with_clock(config, Box::new(unix_seconds))
    }

    pub fn with_clock(config: FcmConfig, clock: Box<dyn Fn() -> u64 + Send + Sync>) -> Result<Self, PortError> {
        let account: ServiceAccount = serde_json::from_str(&config.service_account_json)
            .map_err(|error| PortError::new(format!("FCM service account is not valid JSON: {error}")))?;
        if account.project_id.is_empty() || account.client_email.is_empty() {
            return Err(PortError::new("FCM service account lacks project_id or client_email"));
        }
        let key = EncodingKey::from_rsa_pem(account.private_key.as_bytes()).map_err(|error| {
            PortError::new(format!(
                "FCM service account private_key is not a valid RSA key: {error}"
            ))
        })?;
        let http = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .build()
            .map_err(PortError::new)?;
        Ok(Self {
            http,
            project_id: account.project_id,
            client_email: account.client_email,
            token_uri: config
                .token_uri_override
                .or(account.token_uri)
                .unwrap_or_else(|| DEFAULT_TOKEN_URI.to_owned()),
            key,
            fcm_url: config.fcm_url,
            ttl: config.ttl,
            cached: tokio::sync::Mutex::new(None),
            clock,
        })
    }

    /// A valid OAuth2 access token, fetched when none is cached or it is about to expire.
    async fn access_token(&self) -> Result<String, SendError> {
        let mut cached = self.cached.lock().await;
        let now = (self.clock)();
        if let Some(current) = cached.as_ref().filter(|token| now < token.renew_at) {
            return Ok(current.access_token.clone());
        }
        let claims = Claims {
            iss: &self.client_email,
            scope: SCOPE,
            aud: &self.token_uri,
            iat: now,
            exp: now + ASSERTION_LIFETIME_SECS,
        };
        let assertion = jsonwebtoken::encode(&Header::new(Algorithm::RS256), &claims, &self.key)
            .map_err(|error| SendError::Failed(format!("cannot sign the FCM assertion: {error}")))?;
        let response = self
            .http
            .post(&self.token_uri)
            .form(&[
                ("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"),
                ("assertion", assertion.as_str()),
            ])
            .send()
            .await
            .map_err(|error| SendError::Retry {
                reason: format!("oauth transport: {error}"),
                after: None,
            })?;
        let status = response.status().as_u16();
        if status != 200 {
            metrics::counter!("notifier_fcm_errors_total", "status" => status.to_string(), "reason" => "oauth")
                .increment(1);
            // A rejected key is permanent until the owner replaces it; anything else is transient.
            return Err(if (400..500).contains(&status) && status != 429 {
                SendError::Failed(format!(
                    "FCM token endpoint answered {status}: the service account key is rejected"
                ))
            } else {
                SendError::Retry {
                    reason: format!("FCM token endpoint answered {status}"),
                    after: None,
                }
            });
        }
        let body: TokenResponse = response.json().await.map_err(|error| SendError::Retry {
            reason: format!("oauth body: {error}"),
            after: None,
        })?;
        let lifetime = body.expires_in.unwrap_or(3600);
        *cached = Some(CachedToken {
            access_token: body.access_token.clone(),
            renew_at: now + lifetime.saturating_sub(EXPIRY_MARGIN_SECS),
        });
        Ok(body.access_token)
    }

    async fn forget_access_token(&self) {
        *self.cached.lock().await = None;
    }
}

/// FCM data values are strings; non-string JSON values are rendered as their JSON text.
fn stringify(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        other => other.to_string(),
    }
}

pub(crate) fn alert_message(token: &str, alert: &AlertContent, ttl: Duration) -> Value {
    let mut notification = json!({"title": alert.title});
    if !alert.body.is_empty() {
        notification["body"] = json!(alert.body);
    }
    let mut data = serde_json::Map::new();
    for (key, value) in &alert.data {
        data.insert(key.clone(), json!(value));
    }
    data.entry("type".to_owned()).or_insert_with(|| json!("message"));
    json!({"message": {
        "token": token,
        "notification": notification,
        "data": data,
        "android": {
            "priority": "HIGH",
            "ttl": format!("{}s", ttl.as_secs()),
            "notification": {"channel_id": ANDROID_CHANNEL, "tag": alert.thread_id},
        },
    }})
}

/// A call ring: data only, high priority, short lifetime. Carries exactly the fields of the
/// PushKit payload (which already omits the caller's name outside `standard` channels), plus
/// `call_id`, an alias of `call_message_id`.
pub(crate) fn call_message(token: &str, payload: &Value) -> Value {
    let mut data = serde_json::Map::new();
    if let Some(fields) = payload.as_object() {
        for (key, value) in fields {
            data.insert(key.clone(), json!(stringify(value)));
        }
    }
    if let Some(id) = data.get("call_message_id").cloned() {
        data.insert("call_id".to_owned(), id);
    }
    json!({"message": {
        "token": token,
        "data": data,
        "android": {"priority": "HIGH", "ttl": CALL_TTL},
    }})
}

fn message_for(token: &str, push: &OutboundPush, ttl: Duration) -> Value {
    match push {
        OutboundPush::Alert(alert) => alert_message(token, alert, ttl),
        OutboundPush::Voip(payload) => call_message(token, payload),
    }
}

/// The error code FCM names in `error.details[].errorCode`, else `error.status`.
fn error_code(body: &Value) -> String {
    let error = &body["error"];
    error["details"]
        .as_array()
        .and_then(|details| details.iter().find_map(|d| d["errorCode"].as_str()))
        .or_else(|| error["status"].as_str())
        .unwrap_or("UNKNOWN")
        .to_owned()
}

/// Maps an FCM answer to what the dispatcher does next.
pub(crate) fn classify(status: u16, code: &str, retry_after: Option<Duration>) -> Result<(), SendError> {
    match (status, code) {
        (200, _) => Ok(()),
        (_, "UNREGISTERED" | "INVALID_ARGUMENT" | "SENDER_ID_MISMATCH") | (404, _) => {
            Err(SendError::InvalidToken(code.to_owned()))
        }
        (401, _) | (429, _) | (500..=599, _) => Err(SendError::Retry {
            reason: format!("{status} {code}"),
            after: retry_after,
        }),
        _ => Err(SendError::Failed(format!("{status} {code}"))),
    }
}

#[async_trait]
impl PushSender for FcmSender {
    async fn send(&self, token: &DeviceToken, push: &OutboundPush) -> Result<(), SendError> {
        let access = self.access_token().await?;
        let response = self
            .http
            .post(format!(
                "{}/v1/projects/{}/messages:send",
                self.fcm_url, self.project_id
            ))
            .bearer_auth(access)
            .json(&message_for(&token.token, push, self.ttl))
            .send()
            .await
            .map_err(|error| SendError::Retry {
                reason: format!("transport: {error}"),
                after: None,
            })?;
        let status = response.status().as_u16();
        if status == 200 {
            return Ok(());
        }
        let retry_after = response
            .headers()
            .get("retry-after")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok())
            .map(Duration::from_secs);
        let body: Value = response.json().await.unwrap_or(Value::Null);
        let code = error_code(&body);
        let label: String = code
            .chars()
            .filter(|c| c.is_ascii_alphanumeric() || *c == '_')
            .take(40)
            .collect();
        metrics::counter!("notifier_fcm_errors_total", "status" => status.to_string(), "reason" => label).increment(1);
        if status == 401 {
            // The cached access token was refused: fetch a new one on the retry.
            self.forget_access_token().await;
        }
        classify(status, &code, retry_after)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn event_alert_keeps_its_type_in_fcm_data() {
        let alert = crate::domain::AlertContent {
            title: "Event starting".into(),
            body: String::new(),
            thread_id: "1".into(),
            data: vec![
                ("type".into(), "scheduled_event".into()),
                ("dedupe_key".into(), "key".into()),
            ],
        };
        let payload = super::alert_message("token", &alert, std::time::Duration::from_secs(60));
        assert_eq!(payload["message"]["data"]["type"], "scheduled_event");
        assert_eq!(payload["message"]["data"]["dedupe_key"], "key");
    }
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicU64, Ordering};

    use axum::Router;
    use axum::extract::{Path, State};
    use axum::http::{HeaderMap, StatusCode};
    use axum::routing::post;
    use jsonwebtoken::{DecodingKey, Validation};
    use rsa::pkcs8::{EncodePrivateKey, EncodePublicKey, LineEnding};

    use super::*;
    use crate::application::{Environment, Platform};

    #[derive(Default)]
    struct Seen {
        oauth: Mutex<Vec<String>>,
        sends: Mutex<Vec<(String, HeaderMap, Value)>>,
        /// Token expiry the fake OAuth server announces.
        expires_in: AtomicU64,
    }

    struct Keys {
        private: String,
        public: String,
    }

    fn keys() -> Keys {
        let private = rsa::RsaPrivateKey::new(&mut rand_core::OsRng, 2048).unwrap();
        Keys {
            private: private.to_pkcs8_pem(LineEnding::LF).unwrap().to_string(),
            public: private.to_public_key().to_public_key_pem(LineEnding::LF).unwrap(),
        }
    }

    /// A fake Google: `/token` hands out `token-N`; `/v1/projects/{p}/messages:send` answers per
    /// the device token prefix (`gone-` 404 UNREGISTERED, `bad-` 400 INVALID_ARGUMENT, `busy-` 503,
    /// `expired-` 401 once for a stale access token, else 200).
    async fn fake_google(seen: Arc<Seen>) -> String {
        async fn token(State(seen): State<Arc<Seen>>, body: String) -> axum::Json<Value> {
            let mut oauth = seen.oauth.lock().unwrap();
            oauth.push(body);
            axum::Json(json!({
                "access_token": format!("token-{}", oauth.len()),
                "expires_in": seen.expires_in.load(Ordering::SeqCst),
                "token_type": "Bearer",
            }))
        }
        async fn send(
            State(seen): State<Arc<Seen>>,
            Path(_project): Path<String>,
            headers: HeaderMap,
            axum::Json(body): axum::Json<Value>,
        ) -> (StatusCode, String) {
            let device = body["message"]["token"].as_str().unwrap_or_default().to_owned();
            let auth = headers["authorization"].to_str().unwrap().to_owned();
            seen.sends.lock().unwrap().push((device.clone(), headers, body));
            if device.starts_with("gone-") {
                return (
                    StatusCode::NOT_FOUND,
                    json!({"error": {"code": 404, "status": "NOT_FOUND", "details": [
                        {"@type": "type.googleapis.com/google.firebase.fcm.v1.FcmError", "errorCode": "UNREGISTERED"}]}})
                    .to_string(),
                );
            }
            if device.starts_with("bad-") {
                return (
                    StatusCode::BAD_REQUEST,
                    json!({"error": {"code": 400, "status": "INVALID_ARGUMENT"}}).to_string(),
                );
            }
            if device.starts_with("busy-") {
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    json!({"error": {"code": 503, "status": "UNAVAILABLE"}}).to_string(),
                );
            }
            if device.starts_with("expired-") && auth == "Bearer token-1" {
                return (
                    StatusCode::UNAUTHORIZED,
                    json!({"error": {"code": 401, "status": "UNAUTHENTICATED"}}).to_string(),
                );
            }
            (StatusCode::OK, json!({"name": "projects/p/messages/1"}).to_string())
        }
        let app = Router::new()
            .route("/token", post(token))
            .route("/v1/projects/{project}/messages:send", post(send))
            .with_state(seen);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        url
    }

    fn sender(keys: &Keys, url: &str, clock: Arc<AtomicU64>) -> FcmSender {
        let account = json!({
            "type": "service_account",
            "project_id": "voics-test",
            "client_email": "push@voics-test.iam.gserviceaccount.com",
            "private_key": keys.private,
        });
        let mut config = FcmConfig::new(account.to_string());
        config.fcm_url = url.to_owned();
        config.token_uri_override = Some(format!("{url}/token"));
        FcmSender::with_clock(config, Box::new(move || clock.load(Ordering::SeqCst))).unwrap()
    }

    fn device(token: &str) -> DeviceToken {
        DeviceToken {
            platform: Platform::Fcm,
            token: token.into(),
            environment: Environment::Production,
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

    fn rig() -> (Arc<Seen>, Arc<AtomicU64>) {
        let seen = Arc::new(Seen::default());
        seen.expires_in.store(3600, Ordering::SeqCst);
        (seen, Arc::new(AtomicU64::new(1_800_000_000)))
    }

    #[tokio::test]
    async fn an_alert_is_a_notification_with_data_and_a_signed_oauth_exchange() {
        let (seen, clock) = rig();
        let keys = keys();
        let url = fake_google(seen.clone()).await;
        sender(&keys, &url, clock)
            .send(&device("phone"), &alert())
            .await
            .unwrap();

        let oauth = seen.oauth.lock().unwrap();
        assert_eq!(oauth.len(), 1);
        let form: std::collections::HashMap<String, String> = oauth[0]
            .split('&')
            .filter_map(|pair| pair.split_once('='))
            .map(|(k, v)| (k.to_owned(), v.to_owned()))
            .collect();
        assert_eq!(
            form["grant_type"],
            "urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer"
        );
        let mut validation = Validation::new(Algorithm::RS256);
        validation.set_audience(&[format!("{url}/token")]);
        validation.validate_exp = false;
        validation.required_spec_claims.clear();
        let decoded = jsonwebtoken::decode::<Value>(
            &form["assertion"],
            &DecodingKey::from_rsa_pem(keys.public.as_bytes()).unwrap(),
            &validation,
        )
        .unwrap();
        assert_eq!(decoded.claims["iss"], "push@voics-test.iam.gserviceaccount.com");
        assert_eq!(decoded.claims["scope"], SCOPE);
        assert_eq!(decoded.claims["iat"], 1_800_000_000u64);
        assert_eq!(decoded.claims["exp"], 1_800_003_600u64);

        let sends = seen.sends.lock().unwrap();
        let (_, headers, body) = &sends[0];
        assert_eq!(headers["authorization"], "Bearer token-1");
        let message = &body["message"];
        assert_eq!(message["token"], "phone");
        assert_eq!(message["notification"]["title"], "Alice");
        assert_eq!(message["notification"]["body"], "hi");
        assert_eq!(message["data"]["channel_id"], "10");
        assert_eq!(message["data"]["message_id"], "100");
        assert_eq!(message["data"]["type"], "message");
        assert_eq!(message["android"]["priority"], "HIGH");
        assert_eq!(message["android"]["notification"]["channel_id"], "messages");
    }

    #[tokio::test]
    async fn the_access_token_is_cached_until_shortly_before_it_expires() {
        let (seen, clock) = rig();
        let url = fake_google(seen.clone()).await;
        let sender = sender(&keys(), &url, clock.clone());
        for _ in 0..3 {
            sender.send(&device("phone"), &alert()).await.unwrap();
        }
        assert_eq!(seen.oauth.lock().unwrap().len(), 1, "one exchange serves many sends");

        clock.fetch_add(3600 - EXPIRY_MARGIN_SECS - 1, Ordering::SeqCst);
        sender.send(&device("phone"), &alert()).await.unwrap();
        assert_eq!(seen.oauth.lock().unwrap().len(), 1, "still valid inside the margin");

        clock.fetch_add(1, Ordering::SeqCst);
        sender.send(&device("phone"), &alert()).await.unwrap();
        assert_eq!(seen.oauth.lock().unwrap().len(), 2, "renewed at the margin");
        assert_eq!(
            seen.sends.lock().unwrap().last().unwrap().1["authorization"],
            "Bearer token-2"
        );
    }

    #[tokio::test]
    async fn a_refused_access_token_is_dropped_and_the_retry_uses_a_new_one() {
        let (seen, clock) = rig();
        let url = fake_google(seen.clone()).await;
        let sender = sender(&keys(), &url, clock);
        let first = sender.send(&device("expired-1"), &alert()).await;
        assert!(matches!(first, Err(SendError::Retry { .. })), "{first:?}");
        sender.send(&device("expired-1"), &alert()).await.unwrap();
        assert_eq!(seen.oauth.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn dead_tokens_are_invalid_and_server_trouble_is_retried() {
        let (seen, clock) = rig();
        let url = fake_google(seen).await;
        let sender = sender(&keys(), &url, clock);
        assert_eq!(
            sender.send(&device("gone-1"), &alert()).await,
            Err(SendError::InvalidToken("UNREGISTERED".into()))
        );
        assert_eq!(
            sender.send(&device("bad-1"), &alert()).await,
            Err(SendError::InvalidToken("INVALID_ARGUMENT".into()))
        );
        assert!(matches!(
            sender.send(&device("busy-1"), &alert()).await,
            Err(SendError::Retry { .. })
        ));
    }

    #[test]
    fn answers_map_to_dispatcher_outcomes() {
        assert_eq!(classify(200, "", None), Ok(()));
        assert_eq!(
            classify(403, "SENDER_ID_MISMATCH", None),
            Err(SendError::InvalidToken("SENDER_ID_MISMATCH".into()))
        );
        assert_eq!(
            classify(429, "QUOTA_EXCEEDED", Some(Duration::from_secs(7))),
            Err(SendError::Retry {
                reason: "429 QUOTA_EXCEEDED".into(),
                after: Some(Duration::from_secs(7))
            })
        );
        assert!(matches!(
            classify(403, "PERMISSION_DENIED", None),
            Err(SendError::Failed(_))
        ));
        assert!(matches!(
            classify(400, "FAILED_PRECONDITION", None),
            Err(SendError::Failed(_))
        ));
        assert!(matches!(classify(500, "INTERNAL", None), Err(SendError::Retry { .. })));
    }

    #[tokio::test]
    async fn a_call_ring_is_data_only_high_priority_with_a_short_ttl() {
        let (seen, clock) = rig();
        let url = fake_google(seen.clone()).await;
        let payload = json!({"type": "call", "channel_id": "10", "call_message_id": "100",
            "caller_id": "7", "private": false, "ring_seconds": 30, "caller_name": "Alice"});
        sender(&keys(), &url, clock)
            .send(&device("phone"), &OutboundPush::Voip(payload))
            .await
            .unwrap();
        let sends = seen.sends.lock().unwrap();
        let message = &sends[0].2["message"];
        assert!(
            message.get("notification").is_none(),
            "data only: the app builds the UI"
        );
        assert_eq!(
            message["data"],
            json!({"type": "call", "channel_id": "10", "call_message_id": "100", "call_id": "100",
                   "caller_id": "7", "private": "false", "ring_seconds": "30", "caller_name": "Alice"})
        );
        assert_eq!(message["android"]["priority"], "HIGH");
        assert_eq!(message["android"]["ttl"], "30s");
    }

    #[test]
    fn a_private_call_has_no_caller_name_and_alerts_keep_the_composers_privacy() {
        use crate::domain::{ChannelContext, compose, compose_call};
        let message = crate::domain::message::fixtures::message(json!({
            "type": 3, "guild_id": null, "content": "the launch code is 1234",
            "call": {"participants": ["7"]}}));
        let ctx = ChannelContext {
            guild_name: Some("Test Guild".into()),
            channel_name: Some("general".into()),
            delivery_mode: "ephemeral".into(),
        };
        let ring = call_message("t", &compose_call(&message, &ctx)).to_string();
        let text = message_for(
            "t",
            &OutboundPush::Alert(compose(&message, &ctx, true)),
            Duration::from_secs(60),
        )
        .to_string();
        for leaked in ["Alice", "general", "Test Guild", "launch code"] {
            assert!(!ring.contains(leaked), "ring leaked {leaked}");
            assert!(!text.contains(leaked), "alert leaked {leaked}");
        }
        assert!(ring.contains(r#""private":"true""#));
    }

    #[test]
    fn a_broken_service_account_is_rejected_at_startup() {
        assert!(FcmSender::new(FcmConfig::new("not json".into())).is_err());
        let no_key = json!({"project_id": "p", "client_email": "e", "private_key": "nope"});
        assert!(FcmSender::new(FcmConfig::new(no_key.to_string())).is_err());
    }
}

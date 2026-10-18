//! The api-service clients against a stub of its internal endpoints.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use discordy_messages_application::ports::{AccessError, AuthError, Authenticator, ChannelAccessProvider};
use discordy_messages_domain::{DeliveryMode, Permissions, Snowflake};
use discordy_messages_infrastructure::api::{HttpChannelAccess, InternalApi, TokenAuthenticator};
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use serde_json::{Value, json};

// Throwaway Ed25519 pair generated for these tests only; it signs nothing real.
const TEST_PUBLIC_KEY: &str = "-----BEGIN PUBLIC KEY-----\n\
MCowBQYDK2VwAyEAH7bM1DHgEhoNQirWKvmob23fq3r4gErpc/RivIp3I1c=\n\
-----END PUBLIC KEY-----\n";
const TEST_PRIVATE_KEY: &str = "-----BEGIN PRIVATE KEY-----\n\
MC4CAQAwBQYDK2VwBCIEIEhBt6mYt22syDZO6sHVwFypBk5+rhPGVRCsGXpaofgo\n\
-----END PRIVATE KEY-----\n";
const TOKEN: &str = "stub-internal-token";

#[derive(Default)]
struct Calls {
    access: AtomicUsize,
    introspect: AtomicUsize,
}

type Stub = Arc<Calls>;

fn authorised(headers: &HeaderMap) -> bool {
    headers.get("x-internal-token").and_then(|v| v.to_str().ok()) == Some(TOKEN)
}

async fn access(
    State(calls): State<Stub>,
    headers: HeaderMap,
    Path((channel, user)): Path<(String, String)>,
) -> impl IntoResponse {
    calls.access.fetch_add(1, Ordering::SeqCst);
    if !authorised(&headers) {
        return (StatusCode::UNAUTHORIZED, Json(Value::Null));
    }
    match (channel.as_str(), user.as_str()) {
        ("1", "10") => (
            StatusCode::OK,
            Json(json!({"permissions": "3072", "guild_id": "500", "channel_type": 0,
                        "delivery_mode": "standard", "recipient_ids": []})),
        ),
        ("2", "10") => (
            StatusCode::OK,
            Json(json!({"permissions": "3072", "guild_id": null, "channel_type": 1,
                        "delivery_mode": "p2p", "recipient_ids": ["10", "11"]})),
        ),
        ("1", _) => (StatusCode::FORBIDDEN, Json(Value::Null)),
        ("3", _) => (StatusCode::INTERNAL_SERVER_ERROR, Json(Value::Null)),
        _ => (StatusCode::NOT_FOUND, Json(Value::Null)),
    }
}

async fn delivery(State(calls): State<Stub>, headers: HeaderMap, Path(channel): Path<String>) -> impl IntoResponse {
    calls.access.fetch_add(1, Ordering::SeqCst);
    if !authorised(&headers) {
        return (StatusCode::UNAUTHORIZED, Json(Value::Null));
    }
    match channel.as_str() {
        "1" => (
            StatusCode::OK,
            Json(json!({"guild_id": "500", "channel_type": 0,
                        "delivery_mode": "ephemeral", "recipient_ids": []})),
        ),
        _ => (StatusCode::NOT_FOUND, Json(Value::Null)),
    }
}

#[tokio::test]
async fn channel_lookups_need_no_user() {
    let (api, _) = start_stub().await;
    let provider = HttpChannelAccess::new(api);

    let channel = provider.channel(Snowflake::new(1)).await.unwrap();
    assert_eq!(channel.guild_id, Some(Snowflake::new(500)));
    assert_eq!(channel.delivery_mode, DeliveryMode::Ephemeral);
    assert_eq!(channel.permissions, Permissions::new(0));
    assert_eq!(
        provider.channel(Snowflake::new(9)).await.unwrap_err(),
        AccessError::UnknownChannel
    );
}

async fn introspect(State(calls): State<Stub>, headers: HeaderMap, Json(body): Json<Value>) -> impl IntoResponse {
    calls.introspect.fetch_add(1, Ordering::SeqCst);
    if !authorised(&headers) {
        return (StatusCode::UNAUTHORIZED, Json(Value::Null));
    }
    if body["token"] == "12.valid" {
        (
            StatusCode::OK,
            Json(json!({"user_id": "12", "kind": "bot", "application_id": "99", "session_id": null})),
        )
    } else {
        (StatusCode::UNAUTHORIZED, Json(Value::Null))
    }
}

async fn start_stub() -> (InternalApi, Stub) {
    let calls = Stub::default();
    let app = Router::new()
        .route("/internal/v1/channels/{channel}/access/{user}", get(access))
        .route("/internal/v1/channels/{channel}/delivery", get(delivery))
        .route("/internal/v1/auth/introspect", post(introspect))
        .with_state(calls.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let api = InternalApi::new(reqwest::Client::new(), &format!("http://{addr}/"), TOKEN.into());
    (api, calls)
}

#[tokio::test]
async fn access_answers_map_to_domain_values() {
    let (api, _) = start_stub().await;
    let provider = HttpChannelAccess::new(api);

    let guild = provider.access(Snowflake::new(1), Snowflake::new(10)).await.unwrap();
    assert_eq!(guild.permissions, Permissions::new(3072));
    assert_eq!(guild.guild_id, Some(Snowflake::new(500)));
    assert_eq!(guild.delivery_mode, DeliveryMode::Standard);

    let direct = provider.access(Snowflake::new(2), Snowflake::new(10)).await.unwrap();
    assert_eq!(direct.delivery_mode, DeliveryMode::P2p);
    assert_eq!(direct.recipient_ids, vec![Snowflake::new(10), Snowflake::new(11)]);

    assert_eq!(
        provider
            .access(Snowflake::new(1), Snowflake::new(77))
            .await
            .unwrap_err(),
        AccessError::NotMember
    );
    assert_eq!(
        provider
            .access(Snowflake::new(9), Snowflake::new(10))
            .await
            .unwrap_err(),
        AccessError::UnknownChannel
    );
    assert!(matches!(
        provider
            .access(Snowflake::new(3), Snowflake::new(10))
            .await
            .unwrap_err(),
        AccessError::Unavailable(_)
    ));
}

#[tokio::test]
async fn access_answers_are_cached_and_coalesced_then_expire() {
    let (api, calls) = start_stub().await;
    let provider = Arc::new(HttpChannelAccess::with_ttl(api, Duration::from_millis(300)));

    let lookups: Vec<_> = (0..20)
        .map(|_| {
            let provider = provider.clone();
            tokio::spawn(async move { provider.access(Snowflake::new(1), Snowflake::new(10)).await })
        })
        .collect();
    for lookup in lookups {
        lookup.await.unwrap().unwrap();
    }
    provider.access(Snowflake::new(1), Snowflake::new(10)).await.unwrap();
    assert_eq!(
        calls.access.load(Ordering::SeqCst),
        1,
        "concurrent and repeat lookups share one call"
    );

    tokio::time::sleep(Duration::from_millis(450)).await;
    provider.access(Snowflake::new(1), Snowflake::new(10)).await.unwrap();
    assert_eq!(
        calls.access.load(Ordering::SeqCst),
        2,
        "the entry expires after its ttl"
    );

    // Failures are not cached.
    for _ in 0..2 {
        assert!(provider.access(Snowflake::new(3), Snowflake::new(10)).await.is_err());
    }
    assert_eq!(calls.access.load(Ordering::SeqCst), 4);
}

fn mint(sub: &str, kind: &str, issuer: &str, expires_in: i64) -> String {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
    let claims = json!({"iss": issuer, "sub": sub, "sid": "s", "did": "1", "kind": kind,
                        "iat": now, "exp": now + expires_in});
    let key = EncodingKey::from_ed_pem(TEST_PRIVATE_KEY.as_bytes()).unwrap();
    encode(&Header::new(Algorithm::EdDSA), &claims, &key).unwrap()
}

#[tokio::test]
async fn user_tokens_are_verified_locally() {
    let (api, calls) = start_stub().await;
    let auth = TokenAuthenticator::new(TEST_PUBLIC_KEY, api).unwrap();

    let ok = auth
        .authenticate(&format!("Bearer {}", mint("10", "user", "discordy-api", 900)))
        .await
        .unwrap();
    assert_eq!((ok.user_id, ok.is_bot), (Snowflake::new(10), false));

    for bad in [
        mint("10", "user", "someone-else", 900),
        mint("10", "user", "discordy-api", -3600),
        mint("10", "bot", "discordy-api", 900),
        mint("not-a-snowflake", "user", "discordy-api", 900),
        "garbage".to_owned(),
    ] {
        let result = auth.authenticate(&format!("Bearer {bad}")).await;
        assert_eq!(result.unwrap_err(), AuthError::Invalid);
    }
    assert_eq!(auth.authenticate("Basic abc").await.unwrap_err(), AuthError::Invalid);
    assert_eq!(
        calls.introspect.load(Ordering::SeqCst),
        0,
        "user tokens never hit the api"
    );
}

#[tokio::test]
async fn bot_tokens_are_introspected_and_cached() {
    let (api, calls) = start_stub().await;
    let auth = TokenAuthenticator::new(TEST_PUBLIC_KEY, api).unwrap();

    for _ in 0..3 {
        let bot = auth.authenticate("Bot 12.valid").await.unwrap();
        assert_eq!((bot.user_id, bot.is_bot), (Snowflake::new(12), true));
    }
    assert_eq!(calls.introspect.load(Ordering::SeqCst), 1);
    assert_eq!(
        auth.authenticate("Bot 12.revoked").await.unwrap_err(),
        AuthError::Invalid
    );
}

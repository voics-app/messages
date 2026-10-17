//! Authenticates public requests (pinned interface P3).
//!
//! * `Bearer <jwt>`: an EdDSA access token from the api service, verified
//!   locally with `JWT_PUBLIC_KEY` (issuer `discordy-api`, kind `user`).
//! * `Bot <token>`: introspected via `POST /internal/v1/auth/introspect`,
//!   with the answer cached for 60 seconds.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use contracts::{IntrospectRequest, IntrospectResponse};
use discordy_messages_application::PortError;
use discordy_messages_application::ports::{AuthError, Authenticator, Principal};
use discordy_messages_domain::Snowflake;
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode};
use moka::future::Cache;
use reqwest::StatusCode;
use serde::Deserialize;

use super::{InternalApi, port_error};

const ISSUER: &str = "discordy-api";
const BOT_CACHE_TTL: Duration = Duration::from_secs(60);
const BOT_CACHE_CAPACITY: u64 = 10_000;

#[derive(Debug, Deserialize)]
struct AccessClaims {
    sub: String,
    kind: String,
}

pub struct TokenAuthenticator {
    key: DecodingKey,
    validation: Validation,
    api: InternalApi,
    bots: Cache<String, Option<Principal>>,
}

impl TokenAuthenticator {
    pub fn new(public_key_pem: &str, api: InternalApi) -> Result<Self, PortError> {
        let key = DecodingKey::from_ed_pem(public_key_pem.as_bytes()).map_err(port_error)?;
        let mut validation = Validation::new(Algorithm::EdDSA);
        validation.set_issuer(&[ISSUER]);
        validation.set_required_spec_claims(&["exp", "iss", "sub"]);
        Ok(Self {
            key,
            validation,
            api,
            bots: Cache::builder()
                .max_capacity(BOT_CACHE_CAPACITY)
                .time_to_live(BOT_CACHE_TTL)
                .build(),
        })
    }

    fn verify_user_token(&self, jwt: &str) -> Result<Principal, AuthError> {
        let claims = decode::<AccessClaims>(jwt, &self.key, &self.validation)
            .map_err(|_| AuthError::Invalid)?
            .claims;
        if claims.kind != "user" {
            return Err(AuthError::Invalid);
        }
        let user_id: Snowflake = claims.sub.parse().map_err(|_| AuthError::Invalid)?;
        Ok(Principal { user_id, is_bot: false })
    }

    async fn introspect(&self, token: &str) -> Result<Option<Principal>, PortError> {
        let response = self
            .api
            .post("/internal/v1/auth/introspect")
            .json(&IntrospectRequest {
                token: token.to_owned(),
            })
            .send()
            .await
            .map_err(port_error)?;
        match response.status() {
            StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN | StatusCode::NOT_FOUND => Ok(None),
            status if status.is_success() => {
                let body: IntrospectResponse = response.json().await.map_err(port_error)?;
                let user_id = body.user_id.parse::<Snowflake>().map_err(port_error)?;
                Ok(Some(Principal {
                    user_id,
                    is_bot: body.kind == "bot",
                }))
            }
            status => Err(port_error(format!("introspect returned {status}"))),
        }
    }

    async fn verify_bot_token(&self, token: &str) -> Result<Principal, AuthError> {
        self.bots
            .try_get_with(token.to_owned(), self.introspect(token))
            .await
            .map_err(|error: Arc<PortError>| AuthError::Unavailable((*error).clone()))?
            .ok_or(AuthError::Invalid)
    }
}

#[async_trait]
impl Authenticator for TokenAuthenticator {
    async fn authenticate(&self, authorization: &str) -> Result<Principal, AuthError> {
        match authorization.split_once(' ') {
            Some(("Bearer", jwt)) => self.verify_user_token(jwt.trim()),
            Some(("Bot", token)) => self.verify_bot_token(token.trim()).await,
            _ => Err(AuthError::Invalid),
        }
    }
}

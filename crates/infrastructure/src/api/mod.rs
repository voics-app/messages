//! Clients for the api service's internal endpoints (pinned interface P4).

mod authenticator;
mod channel_access;
mod id_reservations;
mod notification_recipients;
mod users;

pub use authenticator::TokenAuthenticator;
pub use channel_access::HttpChannelAccess;
pub use notification_recipients::HttpNotificationRecipients;
pub use users::HttpUserDirectory;

use discordy_messages_application::PortError;

/// Base URL plus the shared `X-Internal-Token` secret.
#[derive(Clone)]
pub struct InternalApi {
    http: reqwest::Client,
    base_url: String,
    token: String,
}

pub const INTERNAL_TOKEN_HEADER: &str = "X-Internal-Token";

impl InternalApi {
    pub fn new(http: reqwest::Client, base_url: &str, token: String) -> Self {
        Self {
            http,
            base_url: base_url.trim_end_matches('/').to_owned(),
            token,
        }
    }

    fn get(&self, path: &str) -> reqwest::RequestBuilder {
        self.http
            .get(format!("{}{path}", self.base_url))
            .header(INTERNAL_TOKEN_HEADER, &self.token)
    }

    fn post(&self, path: &str) -> reqwest::RequestBuilder {
        self.http
            .post(format!("{}{path}", self.base_url))
            .header(INTERNAL_TOKEN_HEADER, &self.token)
    }
}

fn port_error(error: impl std::fmt::Display) -> PortError {
    PortError::new(format!("api: {error}"))
}

//! The one HTTP client that talks to third-party sites on behalf of the link
//! unfurler and the media proxy.
//!
//! Every hop of every request is checked by [`SsrfPolicy`]; DNS goes through
//! [`GuardedResolver`]; redirects are followed by hand (at most
//! [`MAX_REDIRECTS`]); total time and body size are capped; no proxy, no
//! cookies, no credentials.

use std::sync::Arc;
use std::time::Duration;

use reqwest::header::{ACCEPT, ACCEPT_LANGUAGE, CONTENT_LENGTH, CONTENT_TYPE, LOCATION, RANGE};
use reqwest::redirect::Policy;
use reqwest::{Client, Response, StatusCode};
use url::Url;

use super::ssrf::{Blocked, GuardedResolver, SsrfPolicy};

/// Redirect hops followed before giving up.
pub const MAX_REDIRECTS: usize = 3;
/// Time to establish a connection.
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
/// Time for a whole request including the body.
pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(8);
const BOT_USER_AGENT: &str = "Mozilla/5.0 (compatible; DiscordyBot/1.0; +https://discordy.voics.nl)";

#[derive(Debug, thiserror::Error)]
pub enum FetchError {
    #[error("blocked: {0}")]
    Blocked(#[from] Blocked),
    #[error("too many redirects")]
    TooManyRedirects,
    #[error("redirect without a usable Location")]
    BadRedirect,
    #[error("remote answered {0}")]
    Status(u16),
    #[error("{0}")]
    Transport(String),
}

impl FetchError {
    /// Whether the guard (rather than the remote site) refused the request.
    pub fn is_blocked(&self) -> bool {
        matches!(self, Self::Blocked(_))
    }
}

/// A response whose body was read up to a limit.
#[derive(Debug)]
pub struct Fetched {
    pub final_url: Url,
    pub content_type: Option<String>,
    pub body: Vec<u8>,
}

#[derive(Clone)]
pub struct SafeHttp {
    client: Client,
    policy: SsrfPolicy,
}

impl SafeHttp {
    pub fn new(policy: SsrfPolicy) -> Result<Self, reqwest::Error> {
        let client = Client::builder()
            .redirect(Policy::none())
            .no_proxy()
            .dns_resolver(Arc::new(GuardedResolver::new(policy)))
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(REQUEST_TIMEOUT)
            .pool_max_idle_per_host(2)
            .user_agent(BOT_USER_AGENT)
            .build()?;
        Ok(Self { client, policy })
    }

    pub fn policy(&self) -> SsrfPolicy {
        self.policy
    }

    /// Sends a GET and follows redirects, validating every hop. Returns the
    /// last response and the URL it came from. Non-2xx answers are errors.
    pub async fn open(&self, url: &Url, accept: &str, range: Option<&str>) -> Result<(Url, Response), FetchError> {
        let mut current = url.clone();
        for _ in 0..=MAX_REDIRECTS {
            self.policy.check_url(&current)?;
            let mut request = self
                .client
                .get(current.clone())
                .header(ACCEPT, accept)
                .header(ACCEPT_LANGUAGE, "en");
            if let Some(range) = range {
                request = request.header(RANGE, range);
            }
            let response = request
                .send()
                .await
                .map_err(|error| FetchError::Transport(describe(&error)))?;
            if response.status().is_redirection() {
                current = redirect_target(&current, &response)?;
                continue;
            }
            return match response.status() {
                StatusCode::OK | StatusCode::PARTIAL_CONTENT => Ok((current, response)),
                other => Err(FetchError::Status(other.as_u16())),
            };
        }
        Err(FetchError::TooManyRedirects)
    }

    /// Reads at most `limit_for(content_type)` bytes of the body and stops
    /// there; the rest is never downloaded. Truncation is not an error.
    pub async fn get_prefix(
        &self,
        url: &Url,
        accept: &str,
        limit_for: impl Fn(&str) -> usize,
    ) -> Result<Fetched, FetchError> {
        let (final_url, mut response) = self.open(url, accept, None).await?;
        let content_type = content_type_of(&response);
        let limit = limit_for(content_type.as_deref().unwrap_or(""));
        let declared = response
            .headers()
            .get(CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(0);
        let mut body = Vec::with_capacity(declared.min(limit).min(64 * 1024));
        while body.len() < limit {
            match response.chunk().await {
                Ok(Some(chunk)) => body.extend_from_slice(&chunk),
                Ok(None) => break,
                Err(error) => return Err(FetchError::Transport(describe(&error))),
            }
        }
        body.truncate(limit);
        Ok(Fetched {
            final_url,
            content_type,
            body,
        })
    }
}

/// The media type of a response without parameters, lowercased.
pub fn content_type_of(response: &Response) -> Option<String> {
    response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|v| v.split(';').next().unwrap_or("").trim().to_ascii_lowercase())
        .filter(|v| !v.is_empty())
}

fn redirect_target(from: &Url, response: &Response) -> Result<Url, FetchError> {
    response
        .headers()
        .get(LOCATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|location| from.join(location).ok())
        .ok_or(FetchError::BadRedirect)
}

/// Error text without the URL (which may hold user-supplied query strings).
fn describe(error: &reqwest::Error) -> String {
    if error.is_timeout() {
        "timed out".to_owned()
    } else if error.is_connect() {
        "connection failed".to_owned()
    } else {
        "request failed".to_owned()
    }
}

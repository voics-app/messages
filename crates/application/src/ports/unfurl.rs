use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;

use crate::error::PortError;

/// Turns a URL into an embed. The adapter owns every network concern: SSRF
/// protection, redirects, size and time limits, caching.
#[async_trait]
pub trait LinkUnfurler: Send + Sync {
    /// The embed as a JSON object, or `None` when the link has nothing to
    /// preview or must not be fetched. Failures of the remote site are not
    /// errors: they also yield `None`.
    async fn unfurl(&self, url: &str) -> Option<String>;
}

/// A remote media file opened for the media proxy.
pub struct RemoteMedia {
    pub status: u16,
    pub content_type: String,
    pub content_length: Option<u64>,
    pub content_range: Option<String>,
    pub body: BoxStream<'static, Result<Bytes, PortError>>,
}

/// Why the media proxy refused or failed a request.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum MediaError {
    /// The URL points somewhere the proxy must never go (private ranges, bad scheme or port).
    #[error("address is not allowed")]
    Forbidden,
    /// Not an image or video type the proxy serves.
    #[error("unsupported media type")]
    UnsupportedType,
    /// Larger than the proxy limit.
    #[error("media is too large")]
    TooLarge,
    /// The remote site failed, timed out, or answered with an error.
    #[error("remote unavailable: {0}")]
    Unavailable(String),
}

/// Streams remote images and videos for the media proxy, with the same SSRF
/// guard and limits as the unfurler.
#[async_trait]
pub trait RemoteMediaFetcher: Send + Sync {
    async fn open(&self, url: &str, range: Option<&str>) -> Result<RemoteMedia, MediaError>;
}

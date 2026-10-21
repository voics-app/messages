//! The media proxy's upstream side: streams remote images and videos to
//! clients so they never contact third-party hosts themselves.

use std::sync::Arc;

use async_trait::async_trait;
use discordy_messages_application::PortError;
use discordy_messages_application::ports::{MediaError, RemoteMedia, RemoteMediaFetcher};
use futures::StreamExt;
use reqwest::header::{CONTENT_LENGTH, CONTENT_RANGE};
use tokio::sync::Semaphore;
use url::Url;

use super::fetch::{FetchError, SafeHttp, content_type_of};

/// Largest media file the proxy relays.
pub const MAX_MEDIA_BYTES: u64 = 32 * 1024 * 1024;
/// Simultaneous proxied streams per process.
const MAX_CONCURRENT_STREAMS: usize = 64;
const MEDIA_ACCEPT: &str = "image/*,video/*";

/// Media types the proxy serves. SVG is excluded on purpose: it can carry script.
const ALLOWED_TYPES: [&str; 14] = [
    "image/png",
    "image/jpeg",
    "image/gif",
    "image/webp",
    "image/avif",
    "image/apng",
    "image/bmp",
    "image/x-icon",
    "image/vnd.microsoft.icon",
    "video/mp4",
    "video/webm",
    "video/ogg",
    "video/quicktime",
    "video/x-m4v",
];

pub struct HttpMediaFetcher {
    http: SafeHttp,
    streams: Arc<Semaphore>,
}

impl HttpMediaFetcher {
    pub fn new(http: SafeHttp) -> Self {
        Self {
            http,
            streams: Arc::new(Semaphore::new(MAX_CONCURRENT_STREAMS)),
        }
    }
}

#[async_trait]
impl RemoteMediaFetcher for HttpMediaFetcher {
    async fn open(&self, url: &str, range: Option<&str>) -> Result<RemoteMedia, MediaError> {
        let url = Url::parse(url).map_err(|_| MediaError::Forbidden)?;
        let permit = Arc::clone(&self.streams)
            .try_acquire_owned()
            .map_err(|_| MediaError::Unavailable("proxy is busy".into()))?;
        let (_, response) = self.http.open(&url, MEDIA_ACCEPT, range).await.map_err(media_error)?;
        let content_type = content_type_of(&response).unwrap_or_default();
        if !ALLOWED_TYPES.contains(&content_type.as_str()) {
            return Err(MediaError::UnsupportedType);
        }
        let content_length = response
            .headers()
            .get(CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok());
        if content_length.is_some_and(|length| length > MAX_MEDIA_BYTES) {
            return Err(MediaError::TooLarge);
        }
        let content_range = response
            .headers()
            .get(CONTENT_RANGE)
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned);
        let status = response.status().as_u16();
        // Declared lengths can lie: count what actually arrives.
        let mut seen = 0u64;
        let body = response
            .bytes_stream()
            .map(move |chunk| {
                let _keep_permit_until_the_stream_ends = &permit;
                let chunk = chunk.map_err(|_| PortError::new("remote stream failed"))?;
                seen += chunk.len() as u64;
                if seen > MAX_MEDIA_BYTES {
                    return Err(PortError::new("media exceeds the proxy limit"));
                }
                Ok(chunk)
            })
            .boxed();
        Ok(RemoteMedia {
            status,
            content_type,
            content_length,
            content_range,
            body,
        })
    }
}

fn media_error(error: FetchError) -> MediaError {
    match error {
        FetchError::Blocked(_) => MediaError::Forbidden,
        other => MediaError::Unavailable(other.to_string()),
    }
}

//! The link unfurler adapter: fetch, parse, enrich, cache.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use discordy_messages_application::ports::LinkUnfurler;
use moka::Expiry;
use moka::future::Cache;
use tokio::sync::Semaphore;
use url::Url;

use super::embed::{EmbedDraft, OEmbed, page_embed};
use super::fetch::{FetchError, SafeHttp};
use super::html::PageMeta;

/// How long a successful preview is reused.
const HIT_TTL: Duration = Duration::from_secs(60 * 60);
/// How long a failure or empty page is remembered, so a dead link is not refetched per message.
const MISS_TTL: Duration = Duration::from_secs(5 * 60);
const CACHE_CAPACITY: u64 = 10_000;
/// Whole-unfurl budget of one URL, across its page, oEmbed and image requests.
const UNFURL_DEADLINE: Duration = Duration::from_secs(12);
/// Simultaneous outbound unfurls per process.
const MAX_CONCURRENT: usize = 16;
/// HTML read for metadata.
const HTML_LIMIT: usize = 1024 * 1024;
/// Bytes of an image read to learn its size.
const IMAGE_PROBE_LIMIT: usize = 256 * 1024;
const OEMBED_LIMIT: usize = 64 * 1024;
const PAGE_ACCEPT: &str = "text/html,application/xhtml+xml,image/*;q=0.8,video/*;q=0.8,*/*;q=0.1";

/// Hosts with a known oEmbed endpoint: `(host suffix, endpoint)`. The page URL
/// is appended as the `url` query parameter.
const OEMBED_PROVIDERS: [(&str, &str); 5] = [
    ("youtube.com", "https://www.youtube.com/oembed?format=json"),
    ("youtu.be", "https://www.youtube.com/oembed?format=json"),
    ("vimeo.com", "https://vimeo.com/api/oembed.json"),
    ("open.spotify.com", "https://open.spotify.com/oembed"),
    ("soundcloud.com", "https://soundcloud.com/oembed?format=json"),
];

struct ByOutcome;

impl Expiry<String, Option<String>> for ByOutcome {
    fn expire_after_create(
        &self,
        _key: &String,
        value: &Option<String>,
        _created_at: std::time::Instant,
    ) -> Option<Duration> {
        Some(if value.is_some() { HIT_TTL } else { MISS_TTL })
    }
}

pub struct HttpLinkUnfurler {
    http: SafeHttp,
    cache: Cache<String, Option<String>>,
    permits: Arc<Semaphore>,
    extra_providers: Vec<(String, String)>,
}

impl HttpLinkUnfurler {
    pub fn new(http: SafeHttp) -> Self {
        Self {
            http,
            cache: Cache::builder()
                .max_capacity(CACHE_CAPACITY)
                .expire_after(ByOutcome)
                .build(),
            permits: Arc::new(Semaphore::new(MAX_CONCURRENT)),
            extra_providers: Vec::new(),
        }
    }

    /// Registers an additional oEmbed endpoint for `host` (tests point it at a fixture).
    #[cfg(any(test, feature = "test-support"))]
    pub fn with_oembed_provider(mut self, host: &str, endpoint: &str) -> Self {
        self.extra_providers.push((host.to_owned(), endpoint.to_owned()));
        self
    }

    async fn build(&self, requested: &str) -> Option<String> {
        let url = Url::parse(requested).ok()?;
        self.http.policy().check_url(&url).ok()?;
        let _permit = self.permits.acquire().await.ok()?;
        tokio::time::timeout(UNFURL_DEADLINE, self.fetch_embed(&url))
            .await
            .ok()?
    }

    async fn fetch_embed(&self, url: &Url) -> Option<String> {
        let known = self.known_oembed(url);
        let page = self.http.get_prefix(url, PAGE_ACCEPT, limit_for_page);
        let (page, known) = match known {
            Some(endpoint) => {
                let (page, oembed) = tokio::join!(page, self.fetch_oembed(&endpoint));
                (page, oembed)
            }
            None => (page.await, None),
        };
        let page = match page {
            Ok(page) => page,
            Err(error) => {
                log_failure(url, &error);
                return known.map(|o| self.draft_from_oembed(url, &o)).map(|d| d.to_json());
            }
        };
        let kind = page.content_type.clone().unwrap_or_default();
        let draft = if kind.starts_with("image/") && kind != "image/svg+xml" {
            let (width, height) = image_size(&page.body);
            EmbedDraft::direct_image(&page.final_url, width, height)
        } else if kind.starts_with("video/") {
            EmbedDraft::direct_video(&page.final_url)
        } else if kind == "text/html" || kind == "application/xhtml+xml" {
            let mut draft = self.draft_from_html(url, &page.final_url, &page.body, known).await;
            self.probe_images(&mut draft).await;
            draft
        } else {
            return None;
        };
        draft.has_preview().then(|| draft.to_json())
    }

    async fn draft_from_html(
        &self,
        requested: &Url,
        final_url: &Url,
        body: &[u8],
        known: Option<OEmbed>,
    ) -> EmbedDraft {
        let meta = PageMeta::parse(&String::from_utf8_lossy(body));
        let oembed = match known {
            Some(oembed) => Some(oembed),
            None => match meta.link_href("alternate", "application/json+oembed") {
                Some(href) => match final_url.join(href) {
                    Ok(endpoint) => self.fetch_oembed(endpoint.as_str()).await,
                    Err(_) => None,
                },
                None => None,
            },
        };
        let mut draft = page_embed(final_url, &meta, oembed.as_ref());
        draft.url = requested.to_string();
        draft
    }

    fn draft_from_oembed(&self, url: &Url, oembed: &OEmbed) -> EmbedDraft {
        page_embed(url, &PageMeta::default(), Some(oembed))
    }

    fn known_oembed(&self, url: &Url) -> Option<String> {
        let host = url.host_str()?;
        let matches = |suffix: &str| host == suffix || host.ends_with(&format!(".{suffix}"));
        let endpoint = self
            .extra_providers
            .iter()
            .find(|(suffix, _)| matches(suffix))
            .map(|(_, endpoint)| endpoint.as_str())
            .or_else(|| {
                OEMBED_PROVIDERS
                    .iter()
                    .find(|(suffix, _)| matches(suffix))
                    .map(|(_, e)| *e)
            })?;
        let separator = if endpoint.contains('?') { '&' } else { '?' };
        let query = url::form_urlencoded::Serializer::new(String::new())
            .append_pair("url", url.as_str())
            .finish();
        Some(format!("{endpoint}{separator}{query}"))
    }

    async fn fetch_oembed(&self, endpoint: &str) -> Option<OEmbed> {
        let endpoint = Url::parse(endpoint).ok()?;
        let fetched = self
            .http
            .get_prefix(&endpoint, "application/json", |_| OEMBED_LIMIT)
            .await
            .map_err(|error| log_failure(&endpoint, &error))
            .ok()?;
        OEmbed::parse(&fetched.body)
    }

    /// Fetches the head of each embed image: drops images that are not real
    /// raster images and fills in missing dimensions.
    async fn probe_images(&self, draft: &mut EmbedDraft) {
        for media in draft.images_mut() {
            let Ok(url) = Url::parse(&media.url) else {
                media.url.clear();
                continue;
            };
            match self.http.get_prefix(&url, "image/*", |_| IMAGE_PROBE_LIMIT).await {
                Ok(image) if is_raster_image(image.content_type.as_deref()) => {
                    let (width, height) = image_size(&image.body);
                    media.width = width.or(media.width);
                    media.height = height.or(media.height);
                }
                _ => media.url.clear(),
            }
        }
        if draft.image.as_ref().is_some_and(|m| m.url.is_empty()) {
            draft.image = None;
        }
        if draft.thumbnail.as_ref().is_some_and(|m| m.url.is_empty()) {
            draft.thumbnail = None;
        }
    }
}

#[async_trait]
impl LinkUnfurler for HttpLinkUnfurler {
    async fn unfurl(&self, url: &str) -> Option<String> {
        self.cache
            .get_with(url.to_owned(), async { self.build(url).await })
            .await
    }
}

fn limit_for_page(content_type: &str) -> usize {
    if content_type.starts_with("text/html") || content_type == "application/xhtml+xml" {
        HTML_LIMIT
    } else {
        IMAGE_PROBE_LIMIT
    }
}

fn is_raster_image(content_type: Option<&str>) -> bool {
    content_type.is_some_and(|t| t.starts_with("image/") && t != "image/svg+xml")
}

fn image_size(body: &[u8]) -> (Option<u32>, Option<u32>) {
    match imagesize::blob_size(body) {
        Ok(size) if size.width > 0 && size.height > 0 => {
            (u32::try_from(size.width).ok(), u32::try_from(size.height).ok())
        }
        _ => (None, None),
    }
}

fn log_failure(url: &Url, error: &FetchError) {
    if error.is_blocked() {
        tracing::warn!(host = url.host_str().unwrap_or(""), %error, "unfurl blocked by the SSRF guard");
    } else {
        tracing::debug!(host = url.host_str().unwrap_or(""), %error, "unfurl fetch failed");
    }
}

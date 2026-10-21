//! Building an embed from page metadata and oEmbed answers.

use discordy_messages_domain::is_player_iframe_host;
use serde_json::{Value, json};
use url::Url;

use super::html::{PageMeta, decode_entities};

/// Longest title and description kept (Discord shows far less).
pub const MAX_TITLE_CHARS: usize = 256;
pub const MAX_DESCRIPTION_CHARS: usize = 1024;
const MAX_URL_CHARS: usize = 2048;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MediaRef {
    pub url: String,
    pub width: Option<u32>,
    pub height: Option<u32>,
}

impl MediaRef {
    fn new(url: String) -> Self {
        Self {
            url,
            width: None,
            height: None,
        }
    }

    fn to_json(&self) -> Value {
        let mut media = json!({ "url": self.url });
        if let Some(width) = self.width {
            media["width"] = width.into();
        }
        if let Some(height) = self.height {
            media["height"] = height.into();
        }
        media
    }
}

/// An embed before image sizes are probed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmbedDraft {
    pub kind: &'static str,
    pub url: String,
    pub title: Option<String>,
    pub description: Option<String>,
    pub color: Option<u32>,
    pub provider: Option<(String, Option<String>)>,
    pub author: Option<(String, Option<String>)>,
    pub image: Option<MediaRef>,
    pub thumbnail: Option<MediaRef>,
    pub video: Option<MediaRef>,
}

impl EmbedDraft {
    fn new(kind: &'static str, url: &Url) -> Self {
        Self {
            kind,
            url: url.to_string(),
            title: None,
            description: None,
            color: None,
            provider: None,
            author: None,
            image: None,
            thumbnail: None,
            video: None,
        }
    }

    /// An embed with nothing to show is not worth sending.
    pub fn has_preview(&self) -> bool {
        self.title.is_some()
            || self.description.is_some()
            || self.image.is_some()
            || self.thumbnail.is_some()
            || self.video.is_some()
    }

    /// A direct link to an image file.
    pub fn direct_image(url: &Url, width: Option<u32>, height: Option<u32>) -> Self {
        let mut draft = Self::new("image", url);
        draft.thumbnail = Some(MediaRef {
            url: url.to_string(),
            width,
            height,
        });
        draft
    }

    /// A direct link to a video file.
    pub fn direct_video(url: &Url) -> Self {
        let mut draft = Self::new("video", url);
        draft.video = Some(MediaRef::new(url.to_string()));
        draft
    }

    pub fn to_json(&self) -> String {
        let mut embed = json!({ "type": self.kind, "url": self.url });
        let mut put = |key: &str, value: Option<Value>| {
            if let Some(value) = value {
                embed[key] = value;
            }
        };
        put("title", self.title.clone().map(Value::from));
        put("description", self.description.clone().map(Value::from));
        put("color", self.color.map(Value::from));
        put("provider", self.provider.as_ref().map(|(n, u)| named("name", n, u)));
        put("author", self.author.as_ref().map(|(n, u)| named("name", n, u)));
        put("image", self.image.as_ref().map(MediaRef::to_json));
        put("thumbnail", self.thumbnail.as_ref().map(MediaRef::to_json));
        put("video", self.video.as_ref().map(MediaRef::to_json));
        embed.to_string()
    }

    /// Every remote image whose size is still unknown, for probing.
    pub fn images_mut(&mut self) -> impl Iterator<Item = &mut MediaRef> {
        self.image.iter_mut().chain(self.thumbnail.iter_mut())
    }
}

fn named(key: &str, name: &str, url: &Option<String>) -> Value {
    let mut value = json!({ key: name });
    if let Some(url) = url {
        value["url"] = url.clone().into();
    }
    value
}

/// Collapses whitespace, drops control characters, decodes entities and cuts
/// at `max` characters. `None` when nothing is left.
pub fn clean_text(raw: &str, max: usize) -> Option<String> {
    let decoded = decode_entities(raw);
    let collapsed = decoded
        .split(|c: char| c.is_whitespace() || c.is_control())
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join(" ");
    if collapsed.is_empty() {
        return None;
    }
    if collapsed.chars().count() <= max {
        return Some(collapsed);
    }
    let cut: String = collapsed.chars().take(max.saturating_sub(1)).collect();
    Some(format!("{}\u{2026}", cut.trim_end()))
}

/// Resolves `raw` against `base`, keeping only http(s) URLs of sane length.
pub fn resolve_http(base: &Url, raw: &str) -> Option<String> {
    let resolved = base.join(raw.trim()).ok()?;
    let ok = matches!(resolved.scheme(), "http" | "https") && resolved.as_str().len() <= MAX_URL_CHARS;
    ok.then(|| resolved.to_string())
}

fn parse_color(raw: &str) -> Option<u32> {
    let hex = raw.trim().strip_prefix('#')?;
    match hex.len() {
        6 => u32::from_str_radix(hex, 16).ok(),
        3 => {
            let expanded: String = hex.chars().flat_map(|c| [c, c]).collect();
            u32::from_str_radix(&expanded, 16).ok()
        }
        _ => None,
    }
}

fn dimension(raw: Option<&str>) -> Option<u32> {
    raw.and_then(|v| v.trim().parse::<u32>().ok())
        .filter(|v| (1..=16_384).contains(v))
}

/// A player iframe address taken from oEmbed `html`, only for allow-listed hosts over https.
pub fn iframe_src(html: &str) -> Option<String> {
    let src = attribute_after(html, "<iframe", "src")?;
    let url = Url::parse(&src).ok()?;
    (url.scheme() == "https" && url.host_str().is_some_and(is_player_iframe_host)).then(|| url.to_string())
}

fn attribute_after(html: &str, tag: &str, attr: &str) -> Option<String> {
    let lower = html.to_ascii_lowercase();
    let start = lower.find(tag)?;
    let end = lower[start..].find('>').map_or(html.len(), |i| start + i);
    let needle = format!("{attr}=");
    let at = lower[start..end].find(&needle)? + start + needle.len();
    let rest = &html[at..end];
    let quote = rest.chars().next().filter(|c| *c == '"' || *c == '\'')?;
    let value = &rest[1..];
    value.find(quote).map(|i| decode_entities(&value[..i]))
}

/// The subset of an oEmbed answer that previews use.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OEmbed {
    pub kind: String,
    pub title: Option<String>,
    pub author_name: Option<String>,
    pub author_url: Option<String>,
    pub provider_name: Option<String>,
    pub provider_url: Option<String>,
    pub thumbnail_url: Option<String>,
    pub photo_url: Option<String>,
    pub html: Option<String>,
    pub width: Option<u32>,
    pub height: Option<u32>,
}

impl OEmbed {
    pub fn parse(body: &[u8]) -> Option<Self> {
        let value: Value = serde_json::from_slice(body).ok()?;
        let text = |key: &str| value.get(key).and_then(Value::as_str).map(str::to_owned);
        let number = |key: &str| {
            value
                .get(key)
                .and_then(|v| v.as_u64().or_else(|| v.as_str()?.parse().ok()))
        };
        Some(Self {
            kind: text("type")?,
            title: text("title"),
            author_name: text("author_name"),
            author_url: text("author_url"),
            provider_name: text("provider_name"),
            provider_url: text("provider_url"),
            thumbnail_url: text("thumbnail_url"),
            photo_url: text("url"),
            html: text("html"),
            width: number("width").and_then(|v| u32::try_from(v).ok()).filter(|v| *v > 0),
            height: number("height").and_then(|v| u32::try_from(v).ok()).filter(|v| *v > 0),
        })
    }
}

/// Builds the embed of an HTML page, merged with its oEmbed answer if any.
pub fn page_embed(page_url: &Url, meta: &PageMeta, oembed: Option<&OEmbed>) -> EmbedDraft {
    let mut draft = EmbedDraft::new("link", page_url);
    draft.title = meta
        .first_of(&["og:title", "twitter:title"])
        .map(str::to_owned)
        .or_else(|| meta.title.clone())
        .or_else(|| oembed.and_then(|o| o.title.clone()))
        .and_then(|t| clean_text(&t, MAX_TITLE_CHARS));
    draft.description = meta
        .first_of(&["og:description", "twitter:description", "description"])
        .and_then(|d| clean_text(d, MAX_DESCRIPTION_CHARS));
    draft.color = meta.first("theme-color").and_then(parse_color);
    draft.provider = provider_of(meta, oembed);
    draft.author = oembed.and_then(|o| {
        o.author_name
            .as_deref()
            .and_then(|n| clean_text(n, MAX_TITLE_CHARS))
            .map(|n| (n, o.author_url.as_deref().and_then(|u| resolve_http(page_url, u))))
    });

    let image_url = meta
        .first_of(&[
            "og:image:secure_url",
            "og:image",
            "og:image:url",
            "twitter:image",
            "twitter:image:src",
        ])
        .and_then(|raw| resolve_http(page_url, raw))
        .or_else(|| {
            oembed.and_then(|o| {
                o.photo_url
                    .as_deref()
                    .or(o.thumbnail_url.as_deref())
                    .and_then(|u| resolve_http(page_url, u))
            })
        });
    if let Some(url) = image_url {
        let mut media = MediaRef::new(url);
        media.width = dimension(meta.first("og:image:width"));
        media.height = dimension(meta.first("og:image:height"));
        let large =
            meta.first("twitter:card") == Some("summary_large_image") || oembed.is_some_and(|o| o.kind == "photo");
        if large {
            draft.image = Some(media);
        } else {
            draft.thumbnail = Some(media);
        }
    }

    draft.video = player_video(page_url, meta, oembed);
    draft.kind = if draft.video.is_some() {
        if is_gif_host(page_url) { "gifv" } else { "video" }
    } else if meta.first("og:type") == Some("article") {
        "article"
    } else {
        "link"
    };
    draft
}

fn provider_of(meta: &PageMeta, oembed: Option<&OEmbed>) -> Option<(String, Option<String>)> {
    let name = meta
        .first("og:site_name")
        .map(str::to_owned)
        .or_else(|| oembed.and_then(|o| o.provider_name.clone()))
        .and_then(|n| clean_text(&n, 64))?;
    let url = oembed
        .and_then(|o| o.provider_url.clone())
        .filter(|u| u.starts_with("http"));
    Some((name, url))
}

fn is_gif_host(url: &Url) -> bool {
    url.host_str().is_some_and(|h| {
        ["tenor.com", "giphy.com"]
            .iter()
            .any(|g| h == *g || h.ends_with(&format!(".{g}")))
    })
}

/// A playable video: an allow-listed player iframe (oEmbed html or
/// `og:video` of type text/html) or a direct video file.
fn player_video(page_url: &Url, meta: &PageMeta, oembed: Option<&OEmbed>) -> Option<MediaRef> {
    if let Some(o) = oembed.filter(|o| o.kind == "video" || o.kind == "rich")
        && let Some(src) = o.html.as_deref().and_then(iframe_src)
    {
        return Some(MediaRef {
            url: src,
            width: o.width,
            height: o.height,
        });
    }
    let raw = meta.first_of(&[
        "og:video:secure_url",
        "og:video:url",
        "og:video",
        "twitter:player:stream",
    ])?;
    let url = resolve_http(page_url, raw)?;
    let parsed = Url::parse(&url).ok()?;
    let kind = meta.first("og:video:type").unwrap_or("").to_ascii_lowercase();
    let playable = if kind.starts_with("video/") {
        true
    } else if kind == "text/html" || kind.is_empty() {
        parsed.scheme() == "https" && parsed.host_str().is_some_and(is_player_iframe_host)
    } else {
        false
    };
    playable.then(|| MediaRef {
        url,
        width: dimension(meta.first("og:video:width")),
        height: dimension(meta.first("og:video:height")),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn page(html: &str) -> EmbedDraft {
        let url = Url::parse("https://site.example/post/1").unwrap();
        page_embed(&url, &PageMeta::parse(html), None)
    }

    #[test]
    fn an_article_page_becomes_an_article_with_a_thumbnail() {
        let draft = page(
            r##"<head><title>ignored</title>
            <meta property="og:type" content="article">
            <meta property="og:title" content="  Big   news&nbsp;today ">
            <meta property="og:description" content="Line one
            line two">
            <meta property="og:site_name" content="Site">
            <meta property="og:image" content="/img/a.jpg">
            <meta property="og:image:width" content="800"><meta property="og:image:height" content="600">
            <meta name="theme-color" content="#f80"></head>"##,
        );
        assert_eq!(draft.kind, "article");
        assert_eq!(draft.title.as_deref(), Some("Big news today"));
        assert_eq!(draft.description.as_deref(), Some("Line one line two"));
        assert_eq!(draft.color, Some(0xff8800));
        assert_eq!(draft.provider.as_ref().unwrap().0, "Site");
        let thumb = draft.thumbnail.as_ref().unwrap();
        assert_eq!(
            (thumb.url.as_str(), thumb.width, thumb.height),
            ("https://site.example/img/a.jpg", Some(800), Some(600))
        );
        assert!(draft.image.is_none());
    }

    #[test]
    fn a_large_summary_card_uses_the_image_slot() {
        let draft = page(
            r#"<meta name="twitter:card" content="summary_large_image"><meta name="twitter:image" content="https://i.example/x.png"><meta property="og:title" content="t">"#,
        );
        assert!(draft.image.is_some() && draft.thumbnail.is_none());
        assert_eq!(draft.kind, "link");
    }

    #[test]
    fn the_title_tag_is_the_fallback_and_empty_pages_have_no_preview() {
        assert_eq!(page("<title>Only title</title>").title.as_deref(), Some("Only title"));
        assert!(!page("<head></head>").has_preview());
    }

    #[test]
    fn long_text_is_cut_and_markup_never_survives_as_tags() {
        let cut = clean_text(&"é".repeat(300), MAX_TITLE_CHARS).unwrap();
        assert_eq!(cut.chars().count(), MAX_TITLE_CHARS);
        assert!(cut.ends_with('\u{2026}'));
        assert_eq!(clean_text("a\u{0}b\u{7}c", 10).as_deref(), Some("a b c"));
        assert_eq!(clean_text("   ", 10), None);
    }

    #[test]
    fn only_http_urls_are_resolved() {
        let base = Url::parse("https://a.example/dir/page").unwrap();
        assert_eq!(
            resolve_http(&base, "x.png").as_deref(),
            Some("https://a.example/dir/x.png")
        );
        assert_eq!(
            resolve_http(&base, "//cdn.example/x.png").as_deref(),
            Some("https://cdn.example/x.png")
        );
        assert_eq!(resolve_http(&base, "javascript:alert(1)"), None);
        assert_eq!(resolve_http(&base, "data:image/png;base64,AAAA"), None);
        assert_eq!(resolve_http(&base, "file:///etc/passwd"), None);
    }

    #[test]
    fn og_video_players_are_limited_to_allow_listed_hosts() {
        let ok = page(
            r#"<meta property="og:video:url" content="https://www.youtube.com/embed/abc"><meta property="og:video:type" content="text/html"><meta property="og:video:width" content="1280"><meta property="og:video:height" content="720"><meta property="og:title" content="v">"#,
        );
        assert_eq!(ok.kind, "video");
        assert_eq!(ok.video.as_ref().unwrap().width, Some(1280));
        let evil = page(
            r#"<meta property="og:video:url" content="https://evil.example/player"><meta property="og:video:type" content="text/html"><meta property="og:title" content="v">"#,
        );
        assert!(evil.video.is_none());
        assert_eq!(evil.kind, "link");
        let file = page(
            r#"<meta property="og:video" content="https://cdn.example/a.mp4"><meta property="og:video:type" content="video/mp4"><meta property="og:title" content="v">"#,
        );
        assert_eq!(file.video.unwrap().url, "https://cdn.example/a.mp4");
    }

    #[test]
    fn oembed_adds_author_provider_player_and_thumbnail() {
        let o = OEmbed::parse(
            br#"{"type":"video","title":"Clip","author_name":"Chan","author_url":"https://www.youtube.com/@chan","provider_name":"YouTube","provider_url":"https://www.youtube.com/","thumbnail_url":"https://i.ytimg.com/vi/abc/hq.jpg","width":480,"height":270,"html":"<iframe width=\"480\" height=\"270\" src=\"https://www.youtube.com/embed/abc?feature=oembed\" frameborder=\"0\"></iframe>"}"#,
        )
        .unwrap();
        let url = Url::parse("https://youtu.be/abc").unwrap();
        let draft = page_embed(&url, &PageMeta::default(), Some(&o));
        assert_eq!(draft.kind, "video");
        assert_eq!(draft.title.as_deref(), Some("Clip"));
        assert_eq!(draft.author.as_ref().unwrap().0, "Chan");
        assert_eq!(draft.provider.as_ref().unwrap().0, "YouTube");
        assert_eq!(
            draft.video.as_ref().unwrap().url,
            "https://www.youtube.com/embed/abc?feature=oembed"
        );
        assert!(draft.thumbnail.is_some());
    }

    #[test]
    fn oembed_iframes_on_unknown_hosts_are_dropped() {
        let o = OEmbed::parse(br#"{"type":"rich","html":"<iframe src=\"https://evil.example/x\"></iframe>"}"#).unwrap();
        let url = Url::parse("https://rich.example/p").unwrap();
        assert!(page_embed(&url, &PageMeta::default(), Some(&o)).video.is_none());
        assert_eq!(
            iframe_src(r#"<iframe src="http://www.youtube.com/embed/a"></iframe>"#),
            None,
            "https only"
        );
        assert_eq!(iframe_src("<p>no frame</p>"), None);
    }

    #[test]
    fn json_output_carries_media_with_sizes() {
        let url = Url::parse("https://i.example/cat.png").unwrap();
        let json: Value = serde_json::from_str(&EmbedDraft::direct_image(&url, Some(10), Some(20)).to_json()).unwrap();
        assert_eq!(json["type"], "image");
        assert_eq!(json["thumbnail"]["width"], 10);
        let video: Value = serde_json::from_str(&EmbedDraft::direct_video(&url).to_json()).unwrap();
        assert_eq!(video["video"]["url"], "https://i.example/cat.png");
        assert!(video.get("thumbnail").is_none());
    }
}

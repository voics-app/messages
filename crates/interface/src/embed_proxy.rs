//! Rewrites the media of embeds to signed media-proxy URLs on serialization,
//! so clients never load third-party images or video directly.
//!
//! The stored embed keeps the original remote address in `url`; the wire embed
//! adds `proxy_url` (`proxy_icon_url` for icons). Player iframes on the
//! allow-listed hosts are left alone: they are embedded, not proxied.

use discordy_messages_domain::is_player_iframe_host;
use serde_json::Value;
use url::Url;

use crate::attachment_urls::AttachmentUrls;

/// `(object, url field, proxy field)` of every media reference in an embed.
const MEDIA_FIELDS: [(&str, &str, &str); 5] = [
    ("image", "url", "proxy_url"),
    ("thumbnail", "url", "proxy_url"),
    ("video", "url", "proxy_url"),
    ("footer", "icon_url", "proxy_icon_url"),
    ("author", "icon_url", "proxy_icon_url"),
];

/// Adds proxy URLs to every http(s) media reference of `embed`. A client- or
/// bot-supplied `proxy_url` is replaced: only this service may name one.
pub fn with_proxied_media(mut embed: Value, urls: &AttachmentUrls) -> Value {
    for (object, url_field, proxy_field) in MEDIA_FIELDS {
        let Some(media) = embed.get_mut(object).and_then(Value::as_object_mut) else {
            continue;
        };
        media.remove(proxy_field);
        let remote = media.get(url_field).and_then(Value::as_str).map(str::to_owned);
        let Some(remote) = remote.filter(|remote| is_proxyable(object, remote)) else {
            continue;
        };
        media.insert(proxy_field.to_owned(), Value::String(urls.proxy_url(&remote)));
    }
    embed
}

fn is_proxyable(object: &str, remote: &str) -> bool {
    let Ok(url) = Url::parse(remote) else {
        return false;
    };
    if !matches!(url.scheme(), "http" | "https") {
        return false;
    }
    let is_player = object == "video" && url.host_str().is_some_and(is_player_iframe_host);
    !is_player
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicU64;
    use std::time::Duration;

    use discordy_messages_application::fakes::FakeClock;
    use serde_json::json;

    use super::*;

    fn urls() -> AttachmentUrls {
        AttachmentUrls::new(
            "https://cdn.test",
            "unit-test-signing-key-0123456789abcdef",
            Duration::from_secs(60),
            Arc::new(FakeClock(AtomicU64::new(0))),
        )
    }

    #[test]
    fn remote_media_gets_signed_proxy_urls() {
        let embed = json!({
            "type": "link",
            "thumbnail": {"url": "https://img.example/a.png", "width": 4},
            "image": {"url": "https://img.example/b.png"},
            "author": {"name": "n", "icon_url": "https://img.example/i.png"},
            "footer": {"text": "f", "icon_url": "https://img.example/f.png"},
            "video": {"url": "https://cdn.example/v.mp4"}
        });
        let proxied = with_proxied_media(embed, &urls());
        for (object, field) in [
            ("thumbnail", "proxy_url"),
            ("image", "proxy_url"),
            ("author", "proxy_icon_url"),
            ("footer", "proxy_icon_url"),
            ("video", "proxy_url"),
        ] {
            let proxy = proxied[object][field].as_str().unwrap_or_else(|| panic!("{object}"));
            assert!(proxy.starts_with("https://cdn.test/external/"), "{proxy}");
        }
        assert_eq!(proxied["thumbnail"]["url"], "https://img.example/a.png");
        assert_eq!(proxied["thumbnail"]["width"], 4);
    }

    #[test]
    fn player_iframes_and_non_http_urls_are_not_proxied() {
        let embed = json!({
            "video": {"url": "https://www.youtube.com/embed/abc"},
            "image": {"url": "attachment://cat.png"},
            "thumbnail": {"url": "javascript:alert(1)"}
        });
        let proxied = with_proxied_media(embed, &urls());
        assert!(proxied["video"].get("proxy_url").is_none());
        assert!(proxied["image"].get("proxy_url").is_none());
        assert!(proxied["thumbnail"].get("proxy_url").is_none());
    }

    #[test]
    fn a_forged_proxy_url_is_overwritten_or_removed() {
        let embed = json!({
            "image": {"url": "https://img.example/b.png", "proxy_url": "https://evil.example/x"},
            "thumbnail": {"url": "ftp://x/y", "proxy_url": "https://evil.example/y"}
        });
        let proxied = with_proxied_media(embed, &urls());
        assert!(
            proxied["image"]["proxy_url"]
                .as_str()
                .unwrap()
                .starts_with("https://cdn.test/external/")
        );
        assert!(proxied["thumbnail"].get("proxy_url").is_none());
    }

    #[test]
    fn embeds_without_media_pass_through() {
        let embed = json!({"title": "t", "fields": [{"name": "a", "value": "b"}]});
        assert_eq!(with_proxied_media(embed.clone(), &urls()), embed);
    }
}

//! The unfurler and media proxy upstream against a local HTTP fixture server
//! that plays the role of a third-party site.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use axum::Router;
use axum::body::Body;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Redirect, Response};
use axum::routing::get;
use discordy_messages_application::ports::{LinkUnfurler, MediaError, RemoteMediaFetcher};
use discordy_messages_infrastructure::unfurl::{HttpLinkUnfurler, HttpMediaFetcher, SafeHttp, SsrfPolicy};
use futures::StreamExt;
use serde_json::Value;

#[derive(Clone, Default)]
struct Hits(Arc<AtomicUsize>);

/// A PNG signature plus IHDR for a 20x10 image: enough for a size probe.
fn png_header() -> Vec<u8> {
    let mut png = vec![
        0x89, b'P', b'N', b'G', 0x0d, 0x0a, 0x1a, 0x0a, 0, 0, 0, 13, b'I', b'H', b'D', b'R',
    ];
    png.extend_from_slice(&20u32.to_be_bytes());
    png.extend_from_slice(&10u32.to_be_bytes());
    png.extend_from_slice(&[8, 2, 0, 0, 0]);
    png
}

fn typed(content_type: &'static str, body: impl Into<Body>) -> Response {
    ([(header::CONTENT_TYPE, content_type)], body.into()).into_response()
}

async fn page(State(hits): State<Hits>) -> Response {
    hits.0.fetch_add(1, Ordering::SeqCst);
    typed(
        "text/html; charset=utf-8",
        r#"<html><head><title>Fallback</title>
        <meta property="og:title" content="Fixture &amp; Co">
        <meta property="og:description" content="A page about fixtures">
        <meta property="og:site_name" content="Fixture Site">
        <meta property="og:image" content="/img.png">
        <meta property="og:type" content="article"></head><body>hi</body></html>"#,
    )
}

async fn oembed(headers: HeaderMap, axum::extract::RawQuery(query): axum::extract::RawQuery) -> Response {
    let _ = headers;
    if !query.unwrap_or_default().contains("clip") {
        return StatusCode::NOT_FOUND.into_response();
    }
    typed(
        "application/json",
        r#"{"type":"video","title":"A clip","author_name":"Chan","provider_name":"Players","width":640,"height":360,
        "thumbnail_url":"/img.png",
        "html":"<iframe src=\"https://www.youtube.com/embed/abc\" width=\"640\"></iframe>"}"#,
    )
}

async fn endless_png() -> Response {
    let mut body = png_header();
    body.resize(40 * 1024 * 1024, 0);
    typed("image/png", body)
}

/// Chunked stream with no Content-Length that never stops.
async fn lying_png() -> Response {
    let chunks =
        futures::stream::repeat_with(|| Ok::<_, std::convert::Infallible>(bytes::Bytes::from(vec![0u8; 1 << 20])));
    let first = futures::stream::once(async { Ok::<_, std::convert::Infallible>(bytes::Bytes::from(png_header())) });
    typed("image/png", Body::from_stream(first.chain(chunks).take(64)))
}

async fn fixture() -> (SocketAddr, Hits) {
    let hits = Hits::default();
    let app = Router::new()
        .route("/page", get(page))
        .route("/img.png", get(|| async { typed("image/png", png_header()) }))
        .route("/clip.mp4", get(|| async { typed("video/mp4", vec![0u8; 16]) }))
        .route("/notes.txt", get(|| async { typed("text/plain", "just text") }))
        .route(
            "/evil.svg",
            get(|| async { typed("image/svg+xml", "<svg onload=alert(1)/>") }),
        )
        .route("/oembed", get(oembed))
        .route("/redirect", get(|| async { Redirect::temporary("/page") }))
        .route("/loop", get(|| async { Redirect::temporary("/loop") }))
        .route(
            "/to-metadata",
            get(|| async { Redirect::temporary("http://169.254.169.254/latest/meta-data/") }),
        )
        .route(
            "/to-private",
            get(|| async { Redirect::temporary("http://10.0.0.1/admin") }),
        )
        .route("/missing", get(|| async { StatusCode::NOT_FOUND }))
        .route("/huge.png", get(endless_png))
        .route("/lying.png", get(lying_png))
        .with_state(hits.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (addr, hits)
}

fn lenient_http() -> SafeHttp {
    SafeHttp::new(SsrfPolicy::allow_loopback_for_tests()).unwrap()
}

fn unfurler(addr: SocketAddr) -> HttpLinkUnfurler {
    HttpLinkUnfurler::new(lenient_http()).with_oembed_provider("127.0.0.1", &format!("http://{addr}/oembed"))
}

async fn embed(unfurler: &HttpLinkUnfurler, url: &str) -> Option<Value> {
    unfurler
        .unfurl(url)
        .await
        .map(|json| serde_json::from_str(&json).unwrap())
}

#[tokio::test]
async fn an_open_graph_page_becomes_an_article_embed_with_a_probed_thumbnail() {
    let (addr, _) = fixture().await;
    let embed = embed(&unfurler(addr), &format!("http://{addr}/page")).await.unwrap();
    assert_eq!(embed["type"], "article");
    assert_eq!(embed["title"], "Fixture & Co");
    assert_eq!(embed["description"], "A page about fixtures");
    assert_eq!(embed["provider"]["name"], "Fixture Site");
    assert_eq!(embed["url"], format!("http://{addr}/page"));
    assert_eq!(embed["thumbnail"]["url"], format!("http://{addr}/img.png"));
    assert_eq!(
        (
            embed["thumbnail"]["width"].as_u64(),
            embed["thumbnail"]["height"].as_u64()
        ),
        (Some(20), Some(10))
    );
}

#[tokio::test]
async fn direct_media_links_become_image_and_video_embeds() {
    let (addr, _) = fixture().await;
    let unfurler = unfurler(addr);
    let image = embed(&unfurler, &format!("http://{addr}/img.png")).await.unwrap();
    assert_eq!(image["type"], "image");
    assert_eq!(image["thumbnail"]["width"], 20);
    let video = embed(&unfurler, &format!("http://{addr}/clip.mp4")).await.unwrap();
    assert_eq!(video["type"], "video");
    assert_eq!(video["video"]["url"], format!("http://{addr}/clip.mp4"));
}

#[tokio::test]
async fn oembed_supplies_the_player_author_and_thumbnail() {
    let (addr, _) = fixture().await;
    let embed = embed(&unfurler(addr), &format!("http://{addr}/page?v=clip"))
        .await
        .unwrap();
    assert_eq!(embed["type"], "video");
    assert_eq!(embed["author"]["name"], "Chan");
    assert_eq!(embed["video"]["url"], "https://www.youtube.com/embed/abc");
    assert_eq!(embed["video"]["width"], 640);
}

#[tokio::test]
async fn non_previewable_and_failing_links_yield_nothing() {
    let (addr, _) = fixture().await;
    let unfurler = unfurler(addr);
    for path in ["notes.txt", "evil.svg", "missing", "loop"] {
        assert_eq!(embed(&unfurler, &format!("http://{addr}/{path}")).await, None, "{path}");
    }
}

#[tokio::test]
async fn redirects_are_followed_but_every_hop_is_guarded() {
    let (addr, _) = fixture().await;
    let unfurler = unfurler(addr);
    let followed = embed(&unfurler, &format!("http://{addr}/redirect")).await.unwrap();
    assert_eq!(followed["title"], "Fixture & Co");
    for path in ["to-metadata", "to-private"] {
        assert_eq!(embed(&unfurler, &format!("http://{addr}/{path}")).await, None, "{path}");
    }
}

#[tokio::test]
async fn results_are_cached_per_url() {
    let (addr, hits) = fixture().await;
    let unfurler = unfurler(addr);
    let url = format!("http://{addr}/page");
    let first = embed(&unfurler, &url).await;
    let second = embed(&unfurler, &url).await;
    assert_eq!(first, second);
    assert_eq!(hits.0.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn the_production_policy_never_touches_loopback_or_private_hosts() {
    let (addr, hits) = fixture().await;
    let strict = HttpLinkUnfurler::new(SafeHttp::new(SsrfPolicy::strict()).unwrap());
    for url in [
        format!("http://{addr}/page"),
        "http://127.0.0.1/".to_owned(),
        "http://localhost/".to_owned(),
        "http://[::1]/".to_owned(),
        "http://169.254.169.254/latest/meta-data/".to_owned(),
        "http://10.0.0.1/".to_owned(),
        "http://2130706433/".to_owned(),
        "ftp://example.com/".to_owned(),
        "file:///etc/passwd".to_owned(),
    ] {
        assert_eq!(strict.unfurl(&url).await, None, "{url}");
    }
    assert_eq!(hits.0.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn the_media_proxy_streams_images_and_refuses_everything_else() {
    let (addr, _) = fixture().await;
    let media = HttpMediaFetcher::new(lenient_http());
    let image = media.open(&format!("http://{addr}/img.png"), None).await.unwrap();
    assert_eq!((image.status, image.content_type.as_str()), (200, "image/png"));
    let body: Vec<u8> = image.body.map(|c| c.unwrap().to_vec()).concat().await;
    assert_eq!(body, png_header());

    let refused = |path: &str| {
        let url = format!("http://{addr}/{path}");
        let media = &media;
        async move { media.open(&url, None).await.err().unwrap() }
    };
    assert_eq!(refused("page").await, MediaError::UnsupportedType);
    assert_eq!(refused("evil.svg").await, MediaError::UnsupportedType);
    assert_eq!(refused("huge.png").await, MediaError::TooLarge);
    assert!(matches!(refused("missing").await, MediaError::Unavailable(_)));
    assert_eq!(refused("to-metadata").await, MediaError::Forbidden);
}

#[tokio::test]
async fn a_stream_that_outgrows_its_declared_size_is_cut_off() {
    let (addr, _) = fixture().await;
    let media = HttpMediaFetcher::new(lenient_http());
    let image = media.open(&format!("http://{addr}/lying.png"), None).await.unwrap();
    let mut total = 0usize;
    let mut failed = false;
    let mut body = image.body;
    while let Some(chunk) = body.next().await {
        match chunk {
            Ok(bytes) => total += bytes.len(),
            Err(_) => {
                failed = true;
                break;
            }
        }
    }
    assert!(failed, "the proxy must stop a stream above its limit");
    assert!(total <= 33 * 1024 * 1024);
}

#[tokio::test]
async fn the_proxy_guard_applies_in_production_mode_too() {
    let media = HttpMediaFetcher::new(SafeHttp::new(SsrfPolicy::strict()).unwrap());
    for url in [
        "http://127.0.0.1/a.png",
        "http://169.254.169.254/a.png",
        "http://[::ffff:10.0.0.1]/a.png",
        "not a url",
    ] {
        assert_eq!(media.open(url, None).await.err(), Some(MediaError::Forbidden), "{url}");
    }
}

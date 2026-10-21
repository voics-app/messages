//! Link unfurling and the media proxy upstream: everything that talks to third-party sites.

pub mod embed;
pub mod fetch;
pub mod html;
pub mod media;
pub mod ssrf;
pub mod unfurler;

pub use fetch::SafeHttp;
pub use media::HttpMediaFetcher;
pub use ssrf::SsrfPolicy;
pub use unfurler::HttpLinkUnfurler;

//! Signed, expiring CDN URLs of stored attachments (Discord CDN style).
//!
//! `{CDN_PUBLIC_URL}/attachments/{channel}/{attachment}/{filename}?ex={expiry}&is={issued}&hm={mac}`
//!
//! `ex` and `is` are unix seconds in hex; `hm` is the hex HMAC-SHA256 of
//! `v1\n{channel}/{attachment}/{filename}\n{ex}\n{is}` under the attachment
//! signing key. The object bucket stays private: `GET /attachments/...` on this
//! service checks the signature and streams the object. URLs are re-signed every
//! time a message is serialized, so a client refetches a message to refresh an
//! expired link.

use std::sync::Arc;
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use discordy_messages_application::ports::Clock;
use discordy_messages_domain::{Attachment, UploadKey};
use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

/// How long a freshly signed URL stays valid.
pub const DEFAULT_URL_TTL: Duration = Duration::from_secs(24 * 60 * 60);
/// Signature domain of media-proxy URLs.
const PROXY_DOMAIN: &str = "v1-media";
/// Minimum accepted signing key length in bytes.
pub const MIN_KEY_BYTES: usize = 32;

/// Why a signed URL was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VerifyError {
    /// Missing or malformed `ex`, `is` or `hm`.
    Malformed,
    /// Signature does not match: tampered path, parameters or wrong key.
    BadSignature,
    /// Signature is valid but `ex` has passed.
    Expired,
}

/// Parameters carried on the query string of a signed URL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignedParams {
    pub expires: u64,
    pub issued: u64,
    pub mac: String,
}

impl SignedParams {
    pub fn parse(ex: Option<&str>, is: Option<&str>, hm: Option<&str>) -> Result<Self, VerifyError> {
        let hex_u64 = |raw: Option<&str>| {
            raw.and_then(|v| u64::from_str_radix(v, 16).ok())
                .ok_or(VerifyError::Malformed)
        };
        Ok(Self {
            expires: hex_u64(ex)?,
            issued: hex_u64(is)?,
            mac: hm.filter(|v| !v.is_empty()).ok_or(VerifyError::Malformed)?.to_owned(),
        })
    }
}

#[derive(Clone)]
pub struct AttachmentUrls {
    base: String,
    key: Arc<[u8]>,
    ttl: Duration,
    clock: Arc<dyn Clock>,
}

impl AttachmentUrls {
    pub fn new(cdn_public_url: &str, signing_key: &str, ttl: Duration, clock: Arc<dyn Clock>) -> Self {
        Self {
            base: cdn_public_url.trim_end_matches('/').to_owned(),
            key: Arc::from(signing_key.as_bytes()),
            ttl,
            clock,
        }
    }

    /// `domain` keeps signatures of different URL kinds from being
    /// interchangeable under the one key.
    fn mac_in(&self, domain: &str, subject: &str, expires: u64, issued: u64) -> HmacSha256 {
        let mut mac = HmacSha256::new_from_slice(&self.key).expect("HMAC accepts any key length");
        mac.update(format!("{domain}\n{subject}\n{expires:x}\n{issued:x}").as_bytes());
        mac
    }

    fn mac(&self, path: &str, expires: u64, issued: u64) -> HmacSha256 {
        self.mac_in("v1", path, expires, issued)
    }

    /// The media-proxy URL of a remote image or video:
    /// `{CDN_PUBLIC_URL}/external/{base64url(remote)}?ex&is&hm`. Signed with
    /// the same key as attachments but under its own domain tag.
    pub fn proxy_url(&self, remote: &str) -> String {
        let subject = URL_SAFE_NO_PAD.encode(remote);
        let issued = self.now_s();
        let expires = issued + self.ttl.as_secs();
        let mac = hex::encode(
            self.mac_in(PROXY_DOMAIN, &subject, expires, issued)
                .finalize()
                .into_bytes(),
        );
        format!("{}/external/{subject}?ex={expires:x}&is={issued:x}&hm={mac}", self.base)
    }

    /// Checks a media-proxy request and returns the remote URL it names.
    pub fn verify_proxy(&self, subject: &str, params: &SignedParams) -> Result<String, VerifyError> {
        let provided = hex::decode(&params.mac).map_err(|_| VerifyError::Malformed)?;
        self.mac_in(PROXY_DOMAIN, subject, params.expires, params.issued)
            .verify_slice(&provided)
            .map_err(|_| VerifyError::BadSignature)?;
        if self.now_s() >= params.expires {
            return Err(VerifyError::Expired);
        }
        let bytes = URL_SAFE_NO_PAD.decode(subject).map_err(|_| VerifyError::Malformed)?;
        String::from_utf8(bytes).map_err(|_| VerifyError::Malformed)
    }

    pub(crate) fn remaining_s(&self, params: &SignedParams) -> u64 {
        params.expires.saturating_sub(self.now_s())
    }

    pub(crate) fn now_s(&self) -> u64 {
        self.clock.now_unix_ms() / 1000
    }

    /// The signed download URL of `attachment`, valid from now for the TTL.
    pub fn url(&self, channel_id: discordy_messages_domain::ChannelId, attachment: &Attachment) -> String {
        self.url_for(&attachment.key(channel_id))
    }

    /// The signed download URL of the object stored under `key`.
    pub fn url_for(&self, key: &UploadKey) -> String {
        let (channel_id, attachment_id) = (key.channel_id, key.attachment_id);
        let name = key.filename.as_str();
        let path = format!("{channel_id}/{attachment_id}/{name}");
        let issued = self.now_s();
        let expires = issued + self.ttl.as_secs();
        let mac = hex::encode(self.mac(&path, expires, issued).finalize().into_bytes());
        format!(
            "{}/attachments/{channel_id}/{attachment_id}/{}?ex={expires:x}&is={issued:x}&hm={mac}",
            self.base,
            encode_segment(name),
        )
    }

    /// Checks a request for `channel/attachment/filename` against its signed parameters.
    pub fn verify(
        &self,
        channel_id: &str,
        attachment_id: &str,
        filename: &str,
        params: &SignedParams,
    ) -> Result<(), VerifyError> {
        let provided = hex::decode(&params.mac).map_err(|_| VerifyError::Malformed)?;
        let path = format!("{channel_id}/{attachment_id}/{filename}");
        // `verify_slice` compares in constant time. The signature is checked
        // before expiry so a forged URL is never reported as merely expired.
        self.mac(&path, params.expires, params.issued)
            .verify_slice(&provided)
            .map_err(|_| VerifyError::BadSignature)?;
        if self.now_s() >= params.expires {
            return Err(VerifyError::Expired);
        }
        Ok(())
    }
}

/// Percent-encodes everything outside the URL-safe filename alphabet
/// (filenames may hold non-ASCII letters).
pub(crate) fn encode_segment(segment: &str) -> String {
    let mut out = String::with_capacity(segment.len());
    for byte in segment.bytes() {
        if byte.is_ascii_alphanumeric() || b"._-".contains(&byte) {
            out.push(char::from(byte));
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU64;

    use super::*;
    use discordy_messages_application::fakes::FakeClock;
    use discordy_messages_domain::{AttachmentFilename, Snowflake};

    const KEY: &str = "unit-test-signing-key-0123456789abcdef";

    fn attachment(name: &str) -> Attachment {
        Attachment {
            id: Snowflake::new(9),
            filename: AttachmentFilename::parse(name).unwrap(),
            content_type: None,
            size: 1,
            width: None,
            height: None,
            description: None,
            flags: 0,
        }
    }

    fn signer(key: &str, clock: &Arc<FakeClock>) -> AttachmentUrls {
        AttachmentUrls::new("http://localhost:8000/cdn/", key, DEFAULT_URL_TTL, clock.clone())
    }

    fn params_of(url: &str) -> SignedParams {
        let query = url.split_once('?').unwrap().1;
        let get = |name: &str| query.split('&').find_map(|pair| pair.strip_prefix(&format!("{name}=")));
        SignedParams::parse(get("ex"), get("is"), get("hm")).unwrap()
    }

    fn clock_at(seconds: u64) -> Arc<FakeClock> {
        Arc::new(FakeClock(AtomicU64::new(seconds * 1000)))
    }

    #[test]
    fn urls_sit_under_the_cdn_with_discord_style_parameters() {
        let clock = clock_at(1_800_000_000);
        let url = signer(KEY, &clock).url(Snowflake::new(5), &attachment("cat.png"));
        assert!(
            url.starts_with("http://localhost:8000/cdn/attachments/5/9/cat.png?ex=6b4b2380&is=6b49d200&hm="),
            "{url}"
        );
        let params = params_of(&url);
        assert_eq!(params.issued, 1_800_000_000);
        assert_eq!(params.expires, 1_800_000_000 + 86_400);
        assert_eq!(params.mac.len(), 64);
    }

    #[test]
    fn a_fresh_url_verifies_until_it_expires() {
        let clock = clock_at(1_800_000_000);
        let urls = signer(KEY, &clock);
        let params = params_of(&urls.url(Snowflake::new(5), &attachment("cat.png")));
        assert_eq!(urls.verify("5", "9", "cat.png", &params), Ok(()));
        clock.advance_ms(86_399_000);
        assert_eq!(urls.verify("5", "9", "cat.png", &params), Ok(()));
        clock.advance_ms(1_000);
        assert_eq!(urls.verify("5", "9", "cat.png", &params), Err(VerifyError::Expired));
    }

    #[test]
    fn tampering_is_rejected() {
        let clock = clock_at(1_800_000_000);
        let urls = signer(KEY, &clock);
        let params = params_of(&urls.url(Snowflake::new(5), &attachment("cat.png")));
        for (channel, attachment, name) in [("6", "9", "cat.png"), ("5", "8", "cat.png"), ("5", "9", "dog.png")] {
            assert_eq!(
                urls.verify(channel, attachment, name, &params),
                Err(VerifyError::BadSignature)
            );
        }
        let extended = SignedParams {
            expires: params.expires + 1,
            ..params.clone()
        };
        assert_eq!(
            urls.verify("5", "9", "cat.png", &extended),
            Err(VerifyError::BadSignature)
        );
        let other_key = signer("another-signing-key-0123456789abcdef", &clock);
        assert_eq!(
            other_key.verify("5", "9", "cat.png", &params),
            Err(VerifyError::BadSignature)
        );
    }

    #[test]
    fn a_forged_expired_url_is_a_bad_signature_not_an_expiry() {
        let clock = clock_at(1_800_000_000);
        let forged = SignedParams {
            expires: 1,
            issued: 0,
            mac: "00".repeat(32),
        };
        assert_eq!(
            signer(KEY, &clock).verify("5", "9", "cat.png", &forged),
            Err(VerifyError::BadSignature)
        );
    }

    #[test]
    fn malformed_parameters_are_reported() {
        assert_eq!(
            SignedParams::parse(None, Some("1"), Some("ab")),
            Err(VerifyError::Malformed)
        );
        assert_eq!(
            SignedParams::parse(Some("zz"), Some("1"), Some("ab")),
            Err(VerifyError::Malformed)
        );
        assert_eq!(
            SignedParams::parse(Some("1"), Some("1"), Some("")),
            Err(VerifyError::Malformed)
        );
        let clock = clock_at(10);
        let params = SignedParams::parse(Some("ff"), Some("1"), Some("not-hex")).unwrap();
        assert_eq!(
            signer(KEY, &clock).verify("5", "9", "cat.png", &params),
            Err(VerifyError::Malformed)
        );
    }

    #[test]
    fn proxy_urls_round_trip_and_expire() {
        let clock = clock_at(1_800_000_000);
        let urls = signer(KEY, &clock);
        let remote = "https://img.example/a b.png?x=1&y=2";
        let url = urls.proxy_url(remote);
        assert!(url.starts_with("http://localhost:8000/cdn/external/"), "{url}");
        let subject = url.split('?').next().unwrap().rsplit('/').next().unwrap().to_owned();
        let params = params_of(&url);
        assert_eq!(urls.verify_proxy(&subject, &params).as_deref(), Ok(remote));
        clock.advance_ms(86_400_000);
        assert_eq!(urls.verify_proxy(&subject, &params), Err(VerifyError::Expired));
    }

    #[test]
    fn proxy_urls_cannot_be_retargeted_or_borrowed_from_attachments() {
        let clock = clock_at(1_800_000_000);
        let urls = signer(KEY, &clock);
        let url = urls.proxy_url("https://img.example/a.png");
        let params = params_of(&url);
        let other = URL_SAFE_NO_PAD.encode("http://169.254.169.254/");
        assert_eq!(urls.verify_proxy(&other, &params), Err(VerifyError::BadSignature));
        // An attachment signature for the same string is not a proxy signature.
        let attachment_mac = hex::encode(
            urls.mac("5/9/cat.png", params.expires, params.issued)
                .finalize()
                .into_bytes(),
        );
        let borrowed = SignedParams {
            mac: attachment_mac,
            ..params.clone()
        };
        let subject = URL_SAFE_NO_PAD.encode("5/9/cat.png");
        assert_eq!(urls.verify_proxy(&subject, &borrowed), Err(VerifyError::BadSignature));
        let other_key = signer("another-signing-key-0123456789abcdef", &clock);
        let subject = URL_SAFE_NO_PAD.encode("https://img.example/a.png");
        assert_eq!(
            other_key.verify_proxy(&subject, &params),
            Err(VerifyError::BadSignature)
        );
    }

    #[test]
    fn non_ascii_filenames_are_percent_encoded_in_the_url() {
        let clock = clock_at(1_800_000_000);
        let url = signer(KEY, &clock).url(Snowflake::new(5), &attachment("café.png"));
        assert!(url.contains("/5/9/caf%C3%A9.png?"), "{url}");
    }
}

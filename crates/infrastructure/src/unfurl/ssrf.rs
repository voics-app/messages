//! Server-side request forgery guard shared by the link unfurler and the media
//! proxy.
//!
//! Two layers, both mandatory:
//!
//! 1. [`SsrfPolicy::check_url`] rejects a URL by shape: scheme, credentials,
//!    port, and IP-literal hosts in forbidden ranges (the `url` crate has
//!    already normalised decimal, octal and hex IPv4 forms by then).
//! 2. [`GuardedResolver`] is the HTTP client's only DNS resolver. It drops every
//!    forbidden address from the answer and fails when nothing is left, and the
//!    connection is made to exactly the addresses it returned. There is no
//!    second lookup between the check and the connect, so DNS rebinding cannot
//!    swap in a private address.
//!
//! Redirects are followed by hand and every hop goes through both layers.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use reqwest::dns::{Addrs, Name, Resolve, Resolving};
use url::{Host, Url};

/// Why a request was refused before it left the process.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum Blocked {
    #[error("only http and https are allowed")]
    Scheme,
    #[error("URLs with credentials are not allowed")]
    Credentials,
    #[error("the URL has no host")]
    NoHost,
    #[error("port is not allowed")]
    Port,
    #[error("address is in a forbidden range")]
    Address,
}

/// What the guard lets through.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SsrfPolicy {
    allow_loopback: bool,
}

impl SsrfPolicy {
    /// Production policy: public unicast addresses on ports 80 and 443 only.
    pub const fn strict() -> Self {
        Self { allow_loopback: false }
    }

    /// Tests that talk to a fixture server on 127.0.0.1 and an arbitrary port.
    #[cfg(any(test, feature = "test-support"))]
    pub const fn allow_loopback_for_tests() -> Self {
        Self { allow_loopback: true }
    }

    pub fn check_url(&self, url: &Url) -> Result<(), Blocked> {
        if !matches!(url.scheme(), "http" | "https") {
            return Err(Blocked::Scheme);
        }
        if !url.username().is_empty() || url.password().is_some() {
            return Err(Blocked::Credentials);
        }
        let host = url.host().ok_or(Blocked::NoHost)?;
        if !self.allow_loopback && !matches!(url.port_or_known_default(), Some(80 | 443)) {
            return Err(Blocked::Port);
        }
        match host {
            Host::Ipv4(ip) if self.is_forbidden(IpAddr::V4(ip)) => Err(Blocked::Address),
            Host::Ipv6(ip) if self.is_forbidden(IpAddr::V6(ip)) => Err(Blocked::Address),
            Host::Domain(name) if is_local_name(name) && !self.allow_loopback => Err(Blocked::Address),
            _ => Ok(()),
        }
    }

    pub fn is_forbidden(&self, ip: IpAddr) -> bool {
        if self.allow_loopback && ip.is_loopback() {
            return false;
        }
        is_forbidden_ip(ip)
    }
}

/// Names that are local by convention. DNS filtering would catch most of them
/// anyway; this rejects them without a lookup.
fn is_local_name(name: &str) -> bool {
    let name = name.trim_end_matches('.').to_ascii_lowercase();
    name == "localhost"
        || name.ends_with(".localhost")
        || name.ends_with(".local")
        || name.ends_with(".internal")
        || name.ends_with(".localdomain")
        || name.ends_with(".home.arpa")
        || !name.contains('.')
}

/// Whether `ip` must never be contacted: loopback, private, link-local
/// (including the 169.254.169.254 metadata address), carrier-grade NAT,
/// documentation, benchmarking, multicast, reserved and unspecified space,
/// and IPv6 forms that embed an IPv4 address in such a range.
pub fn is_forbidden_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => is_forbidden_v4(v4),
        IpAddr::V6(v6) => is_forbidden_v6(v6),
    }
}

fn is_forbidden_v4(ip: Ipv4Addr) -> bool {
    let [a, b, c, _] = ip.octets();
    ip.is_unspecified()
        || ip.is_loopback()
        || ip.is_private()
        || ip.is_link_local()
        || ip.is_broadcast()
        || ip.is_multicast()
        || a == 0 // 0.0.0.0/8 "this network"
        || (a == 100 && (64..=127).contains(&b)) // 100.64.0.0/10 carrier-grade NAT
        || (a == 192 && b == 0 && c == 0) // 192.0.0.0/24 IETF protocol assignments
        || (a == 192 && b == 0 && c == 2) // TEST-NET-1
        || (a == 192 && b == 88 && c == 99) // 6to4 relay anycast
        || (a == 198 && (b == 18 || b == 19)) // benchmarking
        || (a == 198 && b == 51 && c == 100) // TEST-NET-2
        || (a == 203 && b == 0 && c == 113) // TEST-NET-3
        || a >= 240 // reserved
}

fn is_forbidden_v6(ip: Ipv6Addr) -> bool {
    if let Some(v4) = embedded_v4(ip) {
        return is_forbidden_v4(v4);
    }
    let segments = ip.segments();
    ip.is_unspecified()
        || ip.is_loopback()
        || ip.is_multicast()
        || (segments[0] & 0xfe00) == 0xfc00 // fc00::/7 unique local
        || (segments[0] & 0xffc0) == 0xfe80 // fe80::/10 link-local
        || (segments[0] & 0xffc0) == 0xfec0 // fec0::/10 deprecated site-local
        || (segments[0] == 0x2001 && segments[1] == 0) // 2001::/32 Teredo
        || (segments[0] == 0x2001 && segments[1] == 0xdb8) // documentation
        || (segments[0] == 0x0100 && segments[1..4] == [0, 0, 0]) // 100::/64 discard-only
}

/// The IPv4 address an IPv6 address stands for, when it wraps one: IPv4-mapped
/// (`::ffff:a.b.c.d`), IPv4-compatible (`::a.b.c.d`), NAT64
/// (`64:ff9b::a.b.c.d`) and 6to4 (`2002:aabb:ccdd::`).
fn embedded_v4(ip: Ipv6Addr) -> Option<Ipv4Addr> {
    let s = ip.segments();
    let v4_from = |hi: u16, lo: u16| Ipv4Addr::new((hi >> 8) as u8, hi as u8, (lo >> 8) as u8, lo as u8);
    let high_zero = s[..5] == [0, 0, 0, 0, 0];
    if high_zero && s[5] == 0xffff {
        return Some(v4_from(s[6], s[7]));
    }
    if high_zero && s[5] == 0 && (s[6] != 0 || s[7] > 1) {
        return Some(v4_from(s[6], s[7]));
    }
    if s[0] == 0x0064 && s[1] == 0xff9b && s[2..6] == [0, 0, 0, 0] {
        return Some(v4_from(s[6], s[7]));
    }
    if s[0] == 0x2002 {
        return Some(v4_from(s[1], s[2]));
    }
    None
}

/// The DNS resolver of the unfurl HTTP client: resolves with the system
/// resolver, then keeps only addresses the policy allows.
#[derive(Debug, Clone, Copy)]
pub struct GuardedResolver {
    policy: SsrfPolicy,
}

impl GuardedResolver {
    pub const fn new(policy: SsrfPolicy) -> Self {
        Self { policy }
    }

    /// The allowed subset of `resolved`, or `None` when nothing is allowed.
    pub fn allowed(&self, resolved: impl IntoIterator<Item = SocketAddr>) -> Option<Vec<SocketAddr>> {
        let allowed: Vec<SocketAddr> = resolved
            .into_iter()
            .filter(|addr| !self.policy.is_forbidden(addr.ip()))
            .collect();
        (!allowed.is_empty()).then_some(allowed)
    }
}

impl Resolve for GuardedResolver {
    fn resolve(&self, name: Name) -> Resolving {
        let guard = *self;
        let host = name.as_str().to_owned();
        Box::pin(async move {
            let resolved = tokio::net::lookup_host((host.as_str(), 0)).await?;
            let allowed = guard
                .allowed(resolved)
                .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::PermissionDenied, Blocked::Address))?;
            Ok::<Addrs, Box<dyn std::error::Error + Send + Sync>>(Box::new(allowed.into_iter()))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ip(text: &str) -> IpAddr {
        text.parse().unwrap()
    }

    fn url(text: &str) -> Url {
        Url::parse(text).unwrap()
    }

    #[test]
    fn private_loopback_linklocal_and_metadata_ipv4_are_forbidden() {
        for text in [
            "0.0.0.0",
            "0.1.2.3",
            "10.0.0.1",
            "10.255.255.255",
            "100.64.0.1",
            "100.100.100.200",
            "100.127.255.255",
            "127.0.0.1",
            "127.255.255.254",
            "169.254.169.254",
            "169.254.0.1",
            "172.16.0.1",
            "172.31.255.255",
            "192.0.0.1",
            "192.0.2.1",
            "192.88.99.1",
            "192.168.1.1",
            "198.18.0.1",
            "198.19.255.255",
            "198.51.100.7",
            "203.0.113.9",
            "224.0.0.1",
            "239.255.255.250",
            "240.0.0.1",
            "255.255.255.255",
        ] {
            assert!(is_forbidden_ip(ip(text)), "{text} must be blocked");
        }
    }

    #[test]
    fn public_ipv4_is_allowed() {
        for text in ["1.1.1.1", "8.8.8.8", "93.184.216.34", "100.63.255.255", "100.128.0.1", "172.15.0.1", "172.32.0.1", "198.17.0.1", "198.20.0.1"] {
            assert!(!is_forbidden_ip(ip(text)), "{text} must be allowed");
        }
    }

    #[test]
    fn special_ipv6_ranges_are_forbidden() {
        for text in [
            "::",
            "::1",
            "fc00::1",
            "fd12:3456::1",
            "fd00:ec2::254",
            "fe80::1",
            "febf::1",
            "fec0::1",
            "ff02::1",
            "2001::1",
            "2001:db8::1",
            "100::1",
        ] {
            assert!(is_forbidden_ip(ip(text)), "{text} must be blocked");
        }
    }

    #[test]
    fn ipv6_wrapping_a_private_ipv4_is_forbidden() {
        for text in [
            "::ffff:127.0.0.1",
            "::ffff:10.1.2.3",
            "::ffff:169.254.169.254",
            "::ffff:7f00:1",
            "::127.0.0.1",
            "64:ff9b::7f00:1",
            "64:ff9b::a9fe:a9fe",
            "2002:7f00:1::",
            "2002:c0a8:101::1",
        ] {
            assert!(is_forbidden_ip(ip(text)), "{text} must be blocked");
        }
    }

    #[test]
    fn public_ipv6_is_allowed_even_when_it_wraps_a_public_ipv4() {
        for text in ["2606:4700:4700::1111", "2a00:1450:4001:81b::200e", "::ffff:8.8.8.8", "64:ff9b::808:808", "2002:808:808::1"] {
            assert!(!is_forbidden_ip(ip(text)), "{text} must be allowed");
        }
    }

    #[test]
    fn urls_are_checked_by_shape() {
        let strict = SsrfPolicy::strict();
        assert_eq!(strict.check_url(&url("https://example.com/a")), Ok(()));
        assert_eq!(strict.check_url(&url("http://example.com:80/a")), Ok(()));
        assert_eq!(strict.check_url(&url("ftp://example.com/a")), Err(Blocked::Scheme));
        assert_eq!(strict.check_url(&url("file:///etc/passwd")), Err(Blocked::Scheme));
        assert_eq!(strict.check_url(&url("https://user:pw@example.com/")), Err(Blocked::Credentials));
        assert_eq!(strict.check_url(&url("https://example.com:6379/")), Err(Blocked::Port));
        assert_eq!(strict.check_url(&url("http://127.0.0.1/")), Err(Blocked::Address));
        assert_eq!(strict.check_url(&url("http://[::1]/")), Err(Blocked::Address));
        assert_eq!(strict.check_url(&url("http://[::ffff:10.0.0.1]/")), Err(Blocked::Address));
        assert_eq!(strict.check_url(&url("http://169.254.169.254/latest/meta-data")), Err(Blocked::Address));
    }

    #[test]
    fn alternative_ipv4_notations_are_normalised_before_the_check() {
        let strict = SsrfPolicy::strict();
        for text in ["http://2130706433/", "http://0x7f.0.0.1/", "http://0177.0.0.1/", "http://127.1/", "http://0/", "http://0xA9FEA9FE/"] {
            assert_eq!(strict.check_url(&url(text)), Err(Blocked::Address), "{text}");
        }
    }

    #[test]
    fn local_hostnames_are_rejected_without_a_lookup() {
        let strict = SsrfPolicy::strict();
        for text in [
            "http://localhost/",
            "http://LOCALHOST./",
            "http://app.localhost/",
            "http://printer.local/",
            "http://vault.internal/",
            "http://intranet/",
            "http://router.home.arpa/",
        ] {
            assert_eq!(strict.check_url(&url(text)), Err(Blocked::Address), "{text}");
        }
    }

    #[test]
    fn the_loopback_exception_is_limited_to_the_test_policy() {
        let lenient = SsrfPolicy::allow_loopback_for_tests();
        assert_eq!(lenient.check_url(&url("http://127.0.0.1:41234/x")), Ok(()));
        assert_eq!(lenient.check_url(&url("http://10.0.0.1:41234/x")), Err(Blocked::Address));
        assert_eq!(lenient.check_url(&url("http://169.254.169.254/")), Err(Blocked::Address));
        assert!(SsrfPolicy::strict().is_forbidden(ip("127.0.0.1")));
    }

    #[test]
    fn dns_answers_are_filtered_and_an_all_private_answer_is_refused() {
        let resolver = GuardedResolver::new(SsrfPolicy::strict());
        let addr = |text: &str| SocketAddr::new(ip(text), 0);
        // A rebinding attacker returns a public and a private record.
        let mixed = resolver.allowed([addr("10.0.0.5"), addr("93.184.216.34"), addr("::1")]);
        assert_eq!(mixed, Some(vec![addr("93.184.216.34")]));
        assert_eq!(resolver.allowed([addr("127.0.0.1"), addr("169.254.169.254")]), None);
        assert_eq!(resolver.allowed([]), None);
    }
}

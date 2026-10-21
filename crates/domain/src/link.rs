//! Finding the links of a message that deserve a preview.
//!
//! Discord rules: a bare `http(s)://` URL is previewed, `<https://...>` is
//! not, and nothing inside code spans or code blocks is. At most
//! [`MAX_UNFURLED_LINKS`] distinct links are previewed per message.

/// Links previewed per message.
pub const MAX_UNFURLED_LINKS: usize = 5;
/// Longest URL accepted for a preview.
pub const MAX_LINK_CHARS: usize = 2048;

/// Hosts whose pages may be embedded as a player iframe in a client. The
/// unfurler only emits an iframe `video.url` for these, the media proxy never
/// proxies them, and clients render a `video.url` on one of them in a sandboxed
/// iframe instead of a `<video>` element.
pub const PLAYER_IFRAME_HOSTS: [&str; 6] = [
    "www.youtube.com",
    "www.youtube-nocookie.com",
    "player.vimeo.com",
    "open.spotify.com",
    "w.soundcloud.com",
    "www.dailymotion.com",
];

/// Whether `host` may serve an embedded player.
pub fn is_player_iframe_host(host: &str) -> bool {
    PLAYER_IFRAME_HOSTS
        .iter()
        .any(|allowed| host.eq_ignore_ascii_case(allowed))
}

const TRAILING_PUNCTUATION: &[char] = &['.', ',', ';', ':', '!', '?', '\'', '"', '*', '_', '~', '|', '`'];

/// The distinct previewable links of `content`, in order of appearance.
pub fn extract_links(content: &str) -> Vec<String> {
    let visible = without_code(content);
    let mut links: Vec<String> = Vec::new();
    let mut resume_at = 0;
    for (start, _) in visible.match_indices("http") {
        if start < resume_at || !starts_a_link(&visible[start..]) || is_suppressed(&visible[..start]) {
            continue;
        }
        if start > 0 && visible[..start].chars().next_back().is_some_and(char::is_alphanumeric) {
            continue;
        }
        let raw = link_text(&visible[start..]);
        resume_at = start + raw.len();
        let link = trim_link(raw);
        if link.len() > "https://".len() && link.chars().count() <= MAX_LINK_CHARS && !links.iter().any(|l| l == link) {
            links.push(link.to_owned());
        }
        if links.len() == MAX_UNFURLED_LINKS {
            break;
        }
    }
    links
}

fn starts_a_link(text: &str) -> bool {
    text.starts_with("http://") || text.starts_with("https://")
}

/// `<https://example.com>` opts out of the preview.
fn is_suppressed(before: &str) -> bool {
    before.ends_with('<')
}

fn link_text(from: &str) -> &str {
    let end = from
        .find(|c: char| c.is_whitespace() || c == '<' || c == '>')
        .unwrap_or(from.len());
    &from[..end]
}

/// Drops sentence punctuation and an unbalanced closing bracket, so that
/// `(see https://a.example/x)` and `https://a.example/x.` yield the URL alone
/// while `https://en.wikipedia.org/wiki/Rust_(language)` stays intact.
fn trim_link(mut link: &str) -> &str {
    loop {
        let trimmed = link.trim_end_matches(TRAILING_PUNCTUATION);
        let closing = [(')', '('), (']', '['), ('}', '{')]
            .into_iter()
            .find(|(close, _)| trimmed.ends_with(*close));
        match closing {
            Some((close, open)) if trimmed.matches(close).count() > trimmed.matches(open).count() => {
                link = &trimmed[..trimmed.len() - 1];
            }
            _ if trimmed.len() == link.len() => return trimmed,
            _ => link = trimmed,
        }
    }
}

/// Replaces fenced blocks and inline code with spaces of the same length, so
/// byte offsets of the remaining text are unchanged.
fn without_code(content: &str) -> String {
    let mut out = String::with_capacity(content.len());
    let mut rest = content;
    while let Some(open) = rest.find('`') {
        out.push_str(&rest[..open]);
        let ticks = rest[open..].chars().take_while(|c| *c == '`').count();
        let fence = "`".repeat(ticks);
        let after = &rest[open + ticks..];
        match after.find(&fence) {
            Some(close) => {
                let span_len = ticks + close + ticks;
                out.extend(std::iter::repeat_n(' ', span_len));
                rest = &rest[open + span_len..];
            }
            None => {
                out.push_str(&rest[open..open + ticks]);
                rest = &rest[open + ticks..];
            }
        }
    }
    out.push_str(rest);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn links(text: &str) -> Vec<String> {
        extract_links(text)
    }

    #[test]
    fn finds_bare_links_in_order_without_duplicates() {
        assert_eq!(
            links("see https://a.example/x and http://b.example then https://a.example/x again"),
            vec!["https://a.example/x", "http://b.example"]
        );
    }

    #[test]
    fn angle_brackets_suppress_the_preview() {
        assert_eq!(
            links("<https://a.example> https://b.example"),
            vec!["https://b.example"]
        );
    }

    #[test]
    fn code_is_never_previewed() {
        assert!(links("`https://a.example`").is_empty());
        assert!(links("```\nhttps://a.example\n```").is_empty());
        assert_eq!(links("`x` https://b.example"), vec!["https://b.example"]);
        assert_eq!(links("a ` https://b.example"), vec!["https://b.example"]);
    }

    #[test]
    fn punctuation_and_unbalanced_brackets_are_trimmed() {
        assert_eq!(links("go to https://a.example/x."), vec!["https://a.example/x"]);
        assert_eq!(links("(see https://a.example/x)"), vec!["https://a.example/x"]);
        assert_eq!(
            links("https://en.wikipedia.org/wiki/Rust_(language)"),
            vec!["https://en.wikipedia.org/wiki/Rust_(language)"]
        );
        assert_eq!(links("||https://a.example||"), vec!["https://a.example"]);
        assert_eq!(links("**https://a.example**"), vec!["https://a.example"]);
    }

    #[test]
    fn markdown_links_use_their_target() {
        assert_eq!(links("[docs](https://a.example/d)"), vec!["https://a.example/d"]);
    }

    #[test]
    fn only_http_and_https_count_and_words_do_not() {
        assert!(links("ftp://a.example javascript:alert(1) http").is_empty());
        assert!(links("xhttps://a.example http://").is_empty());
    }

    #[test]
    fn at_most_five_links_and_overlong_ones_are_ignored() {
        let many: String = (0..9).map(|i| format!("https://h{i}.example ")).collect();
        assert_eq!(links(&many).len(), MAX_UNFURLED_LINKS);
        let long = format!("https://a.example/{}", "x".repeat(MAX_LINK_CHARS));
        assert!(links(&long).is_empty());
    }

    #[test]
    fn urls_embedded_in_a_url_are_not_separate_links() {
        assert_eq!(
            links("https://web.archive.org/web/2020/https://a.example/x"),
            vec!["https://web.archive.org/web/2020/https://a.example/x"]
        );
    }

    #[test]
    fn player_hosts_are_an_exact_allow_list() {
        assert!(is_player_iframe_host("www.youtube.com"));
        assert!(is_player_iframe_host("PLAYER.vimeo.com"));
        assert!(!is_player_iframe_host("youtube.com.evil.example"));
        assert!(!is_player_iframe_host("evil.example"));
    }

    #[test]
    fn unicode_text_does_not_break_offsets() {
        assert_eq!(links("日本語 https://a.example/é ✓"), vec!["https://a.example/é"]);
    }
}

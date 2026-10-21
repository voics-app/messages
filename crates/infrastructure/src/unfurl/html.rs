//! A forgiving scanner for the `<head>` of an HTML page: `<title>`, `<meta>`
//! and `<link>` tags. It never builds a DOM and never executes anything; it
//! stops at `</head>` or `<body>`.

/// A `<link>` tag's interesting attributes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinkTag {
    pub rel: String,
    pub media_type: String,
    pub href: String,
}

/// Metadata found in a page head.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PageMeta {
    pub title: Option<String>,
    /// `(property|name, content)` pairs, keys lowercased, in document order.
    pub metas: Vec<(String, String)>,
    pub links: Vec<LinkTag>,
}

impl PageMeta {
    pub fn parse(html: &str) -> Self {
        Scanner::new(html).run()
    }

    /// The first value of `key` (`og:title`, `twitter:card`, `description`, ...).
    pub fn first(&self, key: &str) -> Option<&str> {
        self.metas
            .iter()
            .find(|(k, v)| k == key && !v.trim().is_empty())
            .map(|(_, v)| v.as_str())
    }

    /// The first present key out of `keys`.
    pub fn first_of(&self, keys: &[&str]) -> Option<&str> {
        keys.iter().find_map(|key| self.first(key))
    }

    /// The first `<link rel=... type=...>` href, for oEmbed discovery.
    pub fn link_href(&self, rel: &str, media_type: &str) -> Option<&str> {
        self.links
            .iter()
            .find(|l| l.rel.split_whitespace().any(|r| r == rel) && l.media_type == media_type)
            .map(|l| l.href.as_str())
    }
}

struct Scanner<'a> {
    html: &'a str,
    lower: String,
    pos: usize,
    out: PageMeta,
}

impl<'a> Scanner<'a> {
    fn new(html: &'a str) -> Self {
        Self {
            html,
            // ASCII lowercasing keeps every byte offset valid for `html`.
            lower: html.to_ascii_lowercase(),
            pos: 0,
            out: PageMeta::default(),
        }
    }

    fn run(mut self) -> PageMeta {
        while let Some(offset) = self.lower[self.pos..].find('<') {
            let start = self.pos + offset;
            self.pos = start + 1;
            let rest = &self.lower[start..];
            if rest.starts_with("<!--") {
                self.skip_past("-->", start + 4);
            } else if rest.starts_with("</head") || rest.starts_with("<body") {
                break;
            } else if let Some(name) = tag_name(rest) {
                self.handle_tag(name, start);
            }
        }
        self.out
    }

    fn skip_past(&mut self, needle: &str, from: usize) {
        self.pos = self.lower[from..]
            .find(needle)
            .map_or(self.lower.len(), |i| from + i + needle.len());
    }

    fn handle_tag(&mut self, name: &str, start: usize) {
        let body_start = start + 1 + name.len();
        match name {
            "script" | "style" | "template" | "svg" => self.skip_past(&format!("</{name}"), body_start),
            "title" => self.read_title(body_start),
            "meta" | "link" => {
                let end = tag_end(self.html, body_start);
                let attrs = attributes(&self.html[body_start..end]);
                self.pos = end;
                if name == "meta" {
                    self.record_meta(&attrs);
                } else {
                    self.record_link(&attrs);
                }
            }
            _ => {}
        }
    }

    fn read_title(&mut self, body_start: usize) {
        let Some(open_end) = self.lower[body_start..].find('>') else {
            self.pos = self.lower.len();
            return;
        };
        let text_start = body_start + open_end + 1;
        let text_end = self.lower[text_start..]
            .find("</title")
            .map_or(self.lower.len(), |i| text_start + i);
        if self.out.title.is_none() {
            self.out.title = Some(self.html[text_start..text_end].to_owned());
        }
        self.pos = text_end;
    }

    fn record_meta(&mut self, attrs: &[(String, String)]) {
        let get = |name: &str| attrs.iter().find(|(k, _)| k == name).map(|(_, v)| v.clone());
        let key = get("property").or_else(|| get("name")).or_else(|| get("itemprop"));
        if let (Some(key), Some(content)) = (key, get("content")) {
            self.out.metas.push((key.trim().to_ascii_lowercase(), content));
        }
    }

    fn record_link(&mut self, attrs: &[(String, String)]) {
        let get = |name: &str| attrs.iter().find(|(k, _)| k == name).map(|(_, v)| v.clone());
        if let Some(href) = get("href") {
            self.out.links.push(LinkTag {
                rel: get("rel").unwrap_or_default().to_ascii_lowercase(),
                media_type: get("type").unwrap_or_default().to_ascii_lowercase(),
                href,
            });
        }
    }
}

/// The tag name when `rest` starts with `<name` followed by a delimiter.
fn tag_name(rest: &str) -> Option<&str> {
    let after = rest.strip_prefix('<')?;
    let len = after
        .find(|c: char| !c.is_ascii_alphanumeric())
        .unwrap_or(after.len());
    (len > 0).then(|| &after[..len])
}

/// Index of the `>` that closes the tag whose attributes start at `from`,
/// ignoring `>` inside quoted values. The end of input when unterminated.
fn tag_end(html: &str, from: usize) -> usize {
    let mut quote: Option<u8> = None;
    for (i, byte) in html.as_bytes()[from..].iter().enumerate() {
        match (quote, *byte) {
            (Some(q), b) if b == q => quote = None,
            (None, b'"' | b'\'') => quote = Some(*byte),
            (None, b'>') => return from + i,
            _ => {}
        }
    }
    html.len()
}

/// `name=value` pairs of a tag's attribute text. Names are lowercased, values
/// have their entities decoded. Valueless attributes get an empty value.
fn attributes(text: &str) -> Vec<(String, String)> {
    let bytes = text.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        while i < bytes.len() && (bytes[i].is_ascii_whitespace() || bytes[i] == b'/') {
            i += 1;
        }
        let name_start = i;
        while i < bytes.len() && !bytes[i].is_ascii_whitespace() && bytes[i] != b'=' && bytes[i] != b'/' {
            i += 1;
        }
        if name_start == i {
            break;
        }
        let name = text[name_start..i].to_ascii_lowercase();
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        let mut value = String::new();
        if i < bytes.len() && bytes[i] == b'=' {
            i += 1;
            while i < bytes.len() && bytes[i].is_ascii_whitespace() {
                i += 1;
            }
            if i < bytes.len() && (bytes[i] == b'"' || bytes[i] == b'\'') {
                let quote = bytes[i];
                let start = i + 1;
                i = start;
                while i < bytes.len() && bytes[i] != quote {
                    i += 1;
                }
                value = text[start..i].to_owned();
                i = (i + 1).min(bytes.len());
            } else {
                let start = i;
                while i < bytes.len() && !bytes[i].is_ascii_whitespace() {
                    i += 1;
                }
                value = text[start..i].to_owned();
            }
        }
        out.push((name, decode_entities(&value)));
    }
    out
}

/// Decodes the named and numeric character references that occur in real
/// metadata. Unknown references are left as they are.
pub fn decode_entities(text: &str) -> String {
    if !text.contains('&') {
        return text.to_owned();
    }
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(amp) = rest.find('&') {
        out.push_str(&rest[..amp]);
        let candidate = &rest[amp..];
        match candidate.find(';').filter(|end| *end <= 10).and_then(|end| {
            decode_reference(&candidate[1..end]).map(|ch| (ch, end + 1))
        }) {
            Some((ch, consumed)) => {
                out.push(ch);
                rest = &candidate[consumed..];
            }
            None => {
                out.push('&');
                rest = &candidate[1..];
            }
        }
    }
    out.push_str(rest);
    out
}

fn decode_reference(name: &str) -> Option<char> {
    if let Some(number) = name.strip_prefix('#') {
        let code = match number.strip_prefix(['x', 'X']) {
            Some(hex) => u32::from_str_radix(hex, 16).ok()?,
            None => number.parse().ok()?,
        };
        return char::from_u32(code).filter(|c| *c != '\0');
    }
    Some(match name {
        "amp" => '&',
        "lt" => '<',
        "gt" => '>',
        "quot" => '"',
        "apos" => '\'',
        "nbsp" => '\u{a0}',
        "ndash" => '\u{2013}',
        "mdash" => '\u{2014}',
        "hellip" => '\u{2026}',
        "lsquo" => '\u{2018}',
        "rsquo" => '\u{2019}',
        "ldquo" => '\u{201c}',
        "rdquo" => '\u{201d}',
        "copy" => '\u{a9}',
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_open_graph_twitter_and_title() {
        let meta = PageMeta::parse(
            r#"<!doctype html><html><head><title>Fallback &amp; co</title>
            <meta property="og:title" content="The Title">
            <meta name='twitter:card' content='summary_large_image'/>
            <META PROPERTY=og:site_name CONTENT=Example>
            <meta property="og:description" content="Fish &quot;n&quot; chips &#8211; &#x2764;">
            <link rel="alternate" type="application/json+oembed" href="https://e.example/oembed?url=x&amp;format=json">
            </head><body><meta property="og:title" content="late"></body>"#,
        );
        assert_eq!(meta.title.as_deref(), Some("Fallback &amp; co"));
        assert_eq!(meta.first("og:title"), Some("The Title"));
        assert_eq!(meta.first("twitter:card"), Some("summary_large_image"));
        assert_eq!(meta.first("og:site_name"), Some("Example"));
        assert_eq!(meta.first("og:description"), Some("Fish \"n\" chips \u{2013} \u{2764}"));
        assert_eq!(
            meta.link_href("alternate", "application/json+oembed"),
            Some("https://e.example/oembed?url=x&format=json")
        );
        assert_eq!(meta.metas.iter().filter(|(k, _)| k == "og:title").count(), 1, "body is ignored");
    }

    #[test]
    fn scripts_styles_and_comments_hide_their_contents() {
        let meta = PageMeta::parse(
            r#"<head><script>var s = '<meta property="og:title" content="evil">';</script>
            <style>/* <meta name="description" content="evil"> */</style>
            <!-- <meta property="og:image" content="https://evil.example/x.png"> -->
            <meta property="og:title" content="good"></head>"#,
        );
        assert_eq!(meta.metas, vec![("og:title".to_owned(), "good".to_owned())]);
    }

    #[test]
    fn quoted_angle_brackets_do_not_end_a_tag() {
        let meta = PageMeta::parse(r#"<meta property="og:title" content="a > b"><meta name="description" content="d">"#);
        assert_eq!(meta.first("og:title"), Some("a > b"));
        assert_eq!(meta.first("description"), Some("d"));
    }

    #[test]
    fn garbage_and_truncated_input_never_panic() {
        for input in [
            "",
            "<",
            "<meta",
            "<meta property=",
            "<meta property=\"og:title",
            "<title>unterminated",
            "<!-- unterminated",
            "<script>",
            "<<<>>><meta>",
            "日本語<meta property=\"og:title\" content=\"é\">",
        ] {
            let _ = PageMeta::parse(input);
        }
        assert_eq!(
            PageMeta::parse("日本語<meta property=\"og:title\" content=\"é\">").first("og:title"),
            Some("é")
        );
    }

    #[test]
    fn entities_decode_and_unknown_ones_stay() {
        assert_eq!(decode_entities("a &amp; b &lt;c&gt; &copy; &unknown; & &#65; &#x42; &#0; &#xD800;"), "a & b <c> \u{a9} &unknown; & A B &#0; &#xD800;");
    }

    #[test]
    fn the_first_non_empty_value_wins() {
        let meta = PageMeta::parse(r#"<meta property="og:image" content=""><meta property="og:image" content="a.png"><meta property="og:image" content="b.png">"#);
        assert_eq!(meta.first("og:image"), Some("a.png"));
        assert_eq!(meta.first_of(&["twitter:image", "og:image"]), Some("a.png"));
    }
}

//! What a push says. Privacy rule: only a channel whose delivery mode is exactly `standard` may
//! put message text (or the names around it) into a push. Every other mode, including one this
//! service has never heard of, gets the generic "New message" and nothing else.

use serde_json::Value;

use super::message::IncomingMessage;

const MAX_BODY_CHARS: usize = 150;
const GENERIC_TITLE: &str = "New message";
const NO_PREVIEW_BODY: &str = "New message";
const ATTACHMENT_BODY: &str = "Sent an attachment";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelContext {
    pub guild_name: Option<String>,
    pub channel_name: Option<String>,
    /// The api service's effective delivery mode of the channel.
    pub delivery_mode: String,
}

impl ChannelContext {
    fn is_standard(&self) -> bool {
        self.delivery_mode == "standard"
    }
}

/// A visible notification, independent of the provider that carries it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlertContent {
    pub title: String,
    /// Empty for pushes that only carry the title.
    pub body: String,
    /// Groups notifications of one channel on the lock screen.
    pub thread_id: String,
    /// Custom keys the app reads when the notification is tapped.
    pub data: Vec<(String, String)>,
}

/// Something to send to one device. Alerts are what the notifier produces today; `Voip` is the
/// raw payload of a PushKit call push, so the sender serves the call ticket as well.
#[derive(Debug, Clone, PartialEq)]
pub enum OutboundPush {
    Alert(AlertContent),
    Voip(Value),
}

/// Mentions, custom emoji and line breaks as plain text, at most [`MAX_BODY_CHARS`] characters.
fn plain_text(content: &str) -> String {
    let mut text = String::with_capacity(content.len());
    let mut rest = content;
    while let Some(open) = rest.find('<') {
        text.push_str(&rest[..open]);
        let tail = &rest[open..];
        match tail.find('>') {
            Some(close) => {
                text.push_str(&token_text(&tail[1..close]).unwrap_or_else(|| tail[..=close].to_owned()));
                rest = &tail[close + 1..];
            }
            None => {
                text.push_str(tail);
                rest = "";
            }
        }
    }
    text.push_str(rest);
    let collapsed = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if collapsed.chars().count() <= MAX_BODY_CHARS {
        return collapsed;
    }
    let mut cut: String = collapsed.chars().take(MAX_BODY_CHARS - 1).collect();
    cut.push('\u{2026}');
    cut
}

/// The text for the inside of `<...>`: a user, role or channel mention, or a custom emoji.
fn token_text(inner: &str) -> Option<String> {
    let numeric = |s: &str| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit());
    if let Some(id) = inner.strip_prefix("@&") {
        return numeric(id).then(|| "@role".to_owned());
    }
    if let Some(id) = inner.strip_prefix("@!").or_else(|| inner.strip_prefix('@')) {
        return numeric(id).then(|| "@user".to_owned());
    }
    if let Some(id) = inner.strip_prefix('#') {
        return numeric(id).then(|| "#channel".to_owned());
    }
    let emoji = inner.strip_prefix("a:").or_else(|| inner.strip_prefix(':'))?;
    let (name, id) = emoji.trim_start_matches(':').split_once(':')?;
    (numeric(id) && !name.is_empty()).then(|| format!(":{name}:"))
}

fn context_title(message: &IncomingMessage, ctx: &ChannelContext) -> String {
    match (&ctx.channel_name, &ctx.guild_name) {
        _ if message.is_direct() => message.author_name.clone(),
        (Some(channel), Some(guild)) => format!("{} (#{channel}, {guild})", message.author_name),
        (Some(channel), None) => format!("{} (#{channel})", message.author_name),
        _ => message.author_name.clone(),
    }
}

/// How long the phone rings, which the gateway's ring timeout also is. The app ends a call nobody answered after this.
const RING_SECONDS: u64 = 30;

/// The PushKit payload that rings the phone for the call `message` reports. It never carries message
/// text. The caller's name is in it only for a channel whose delivery mode is exactly `standard`
/// (the same allow-list as alerts); every other mode, including one this service has never heard of,
/// gets a call without a name, and `private` tells the app to show it that way.
pub fn compose_call(message: &IncomingMessage, ctx: &ChannelContext) -> Value {
    let mut payload = serde_json::json!({
        "type": "call",
        "channel_id": message.channel_id,
        "call_message_id": message.id,
        "caller_id": message.author_id,
        "private": !ctx.is_standard(),
        "ring_seconds": RING_SECONDS,
    });
    if ctx.is_standard() {
        payload["caller_name"] = Value::String(message.author_name.clone());
    }
    payload
}

/// The notification for `message`. `previews` is the recipient device's setting.
pub fn compose(message: &IncomingMessage, ctx: &ChannelContext, previews: bool) -> AlertContent {
    let mut data = vec![
        ("channel_id".to_owned(), message.channel_id.clone()),
        ("message_id".to_owned(), message.id.clone()),
    ];
    if let Some(guild_id) = &message.guild_id {
        data.push(("guild_id".to_owned(), guild_id.clone()));
    }
    if !ctx.is_standard() {
        return AlertContent {
            title: GENERIC_TITLE.to_owned(),
            body: String::new(),
            thread_id: message.channel_id.clone(),
            data,
        };
    }
    let body = if !previews {
        NO_PREVIEW_BODY.to_owned()
    } else {
        let text = plain_text(&message.content);
        match (text.is_empty(), message.attachment_count) {
            (false, _) => text,
            (true, 0) => NO_PREVIEW_BODY.to_owned(),
            (true, _) => ATTACHMENT_BODY.to_owned(),
        }
    };
    AlertContent {
        title: context_title(message, ctx),
        body,
        thread_id: message.channel_id.clone(),
        data,
    }
}

#[cfg(test)]
mod tests {
    use super::super::message::fixtures::message;
    use super::*;
    use serde_json::json;

    fn standard() -> ChannelContext {
        ChannelContext {
            guild_name: Some("Test Guild".into()),
            channel_name: Some("general".into()),
            delivery_mode: "standard".into(),
        }
    }

    fn secret_message() -> IncomingMessage {
        message(json!({"content": "the launch code is 1234"}))
    }

    #[test]
    fn a_standard_guild_message_shows_author_place_and_text() {
        let alert = compose(&secret_message(), &standard(), true);
        assert_eq!(alert.title, "Alice (#general, Test Guild)");
        assert_eq!(alert.body, "the launch code is 1234");
        assert_eq!(alert.thread_id, "10");
        assert!(alert.data.contains(&("channel_id".into(), "10".into())));
        assert!(alert.data.contains(&("guild_id".into(), "1".into())));
    }

    #[test]
    fn a_direct_message_is_titled_with_the_author_only() {
        let dm = message(json!({"guild_id": null}));
        let ctx = ChannelContext {
            guild_name: None,
            channel_name: None,
            ..standard()
        };
        let alert = compose(&dm, &ctx, true);
        assert_eq!(alert.title, "Alice");
        assert!(!alert.data.iter().any(|(key, _)| key == "guild_id"));
    }

    fn call_message() -> IncomingMessage {
        message(json!({"type": 3, "guild_id": null, "content": "", "call": {"participants": ["7"]}}))
    }

    #[test]
    fn a_standard_call_names_the_caller_and_nothing_else() {
        let payload = compose_call(&call_message(), &standard());
        assert_eq!(
            payload,
            json!({"type": "call", "channel_id": "10", "call_message_id": "100", "caller_id": "7",
                   "private": false, "ring_seconds": 30, "caller_name": "Alice"})
        );
    }

    #[test]
    fn encrypted_and_unknown_modes_ring_without_a_name_a_place_or_text() {
        for mode in ["ephemeral", "p2p", "e2ee", "", "Standard", "something-new"] {
            let ctx = ChannelContext {
                delivery_mode: mode.into(),
                ..standard()
            };
            let payload = compose_call(&call_message(), &ctx);
            let everything = payload.to_string();
            for leaked in ["Alice", "general", "Test Guild"] {
                assert!(!everything.contains(leaked), "{mode} leaked {leaked}");
            }
            assert_eq!(payload["private"], true, "{mode}");
            assert!(payload.get("caller_name").is_none(), "{mode}");
            assert_eq!(payload["channel_id"], "10", "the app still opens the call");
        }
    }

    #[test]
    fn a_call_payload_never_carries_message_text() {
        let with_text = message(
            json!({"type": 3, "guild_id": null, "content": "the launch code is 1234", "call": {"participants": ["7"]}}),
        );
        assert!(
            !compose_call(&with_text, &standard())
                .to_string()
                .contains("launch code")
        );
    }

    #[test]
    fn encrypted_and_unknown_modes_never_carry_text_author_or_place() {
        for mode in ["ephemeral", "p2p", "e2ee", "", "Standard", "something-new"] {
            let ctx = ChannelContext {
                delivery_mode: mode.into(),
                ..standard()
            };
            for previews in [true, false] {
                let alert = compose(&secret_message(), &ctx, previews);
                let everything = format!("{} {} {:?}", alert.title, alert.body, alert.data);
                for leaked in ["launch code", "Alice", "general", "Test Guild"] {
                    assert!(!everything.contains(leaked), "{mode}/{previews} leaked {leaked}");
                }
                assert_eq!(alert.title, "New message");
                assert!(alert.body.is_empty());
                assert!(
                    alert.data.contains(&("channel_id".into(), "10".into())),
                    "tap still opens the chat"
                );
            }
        }
    }

    #[test]
    fn with_previews_off_the_text_is_replaced_but_the_place_stays() {
        let alert = compose(&secret_message(), &standard(), false);
        assert_eq!(alert.title, "Alice (#general, Test Guild)");
        assert_eq!(alert.body, "New message");
        assert!(!alert.body.contains("launch"));
    }

    #[test]
    fn attachments_without_text_are_announced() {
        let with_file = message(json!({"content": "", "attachments": [{
            "id": "1", "filename": "a.png", "size": 1, "url": "u", "proxy_url": "p"}]}));
        assert_eq!(compose(&with_file, &standard(), true).body, "Sent an attachment");
        assert_eq!(
            compose(&message(json!({"content": ""})), &standard(), true).body,
            "New message"
        );
    }

    #[test]
    fn mentions_and_emoji_become_plain_text() {
        let text = plain_text("hi <@123> and <@!456>, see <#789> <@&5> <:wave:99> <a:party:100> <b>bold</b> <@x>");
        assert_eq!(
            text,
            "hi @user and @user, see #channel @role :wave: :party: <b>bold</b> <@x>"
        );
    }

    #[test]
    fn whitespace_collapses_and_long_text_is_cut() {
        assert_eq!(plain_text("a \n\n  b\tc"), "a b c");
        let long = plain_text(&"x".repeat(400));
        assert_eq!(long.chars().count(), MAX_BODY_CHARS);
        assert!(long.ends_with('\u{2026}'));
        assert_eq!(plain_text("unterminated <@12"), "unterminated <@12");
    }
}

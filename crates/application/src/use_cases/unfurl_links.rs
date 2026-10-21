use std::sync::Arc;

use discordy_messages_domain::{ChannelId, FLAG_EPHEMERAL, MAX_UNFURLED_LINKS, Message, MessageId, extract_links};
use futures::future::join_all;

use super::MessagingPorts;
use super::support::{announce, event};
use crate::error::AppResult;
use crate::ports::{ChannelAccessProvider, EventPublisher, LinkUnfurler, MessageEventKind, MessageRepository};

/// Embed `type` values the unfurler generates. Anything else (`rich`, or no
/// type) was supplied by a bot or webhook and is never replaced.
const GENERATED_EMBED_TYPES: [&str; 5] = ["link", "image", "video", "gifv", "article"];

/// Whether `embed` (a JSON object) was produced by the unfurler.
pub(crate) fn is_generated_embed(embed: &str) -> bool {
    serde_json::from_str::<serde_json::Value>(embed)
        .ok()
        .and_then(|value| value.get("type")?.as_str().map(|t| GENERATED_EMBED_TYPES.contains(&t)))
        .unwrap_or(false)
}

/// Adds link previews to a stored message after the fact and announces them
/// as MESSAGE_UPDATE, like Discord. Runs in the background of create and edit.
///
/// Nothing is fetched for ephemeral relays (never stored), for channels that
/// are not Standard (Ephemeral and P2P content must not leave the clients),
/// or for messages with SUPPRESS_EMBEDS.
pub struct UnfurlLinks {
    access: Arc<dyn ChannelAccessProvider>,
    messages: Arc<dyn MessageRepository>,
    events: Arc<dyn EventPublisher>,
    unfurler: Arc<dyn LinkUnfurler>,
}

impl UnfurlLinks {
    pub fn new(ports: &MessagingPorts) -> Self {
        Self {
            access: ports.access.clone(),
            messages: ports.messages.clone(),
            events: ports.events.clone(),
            unfurler: ports.unfurler.clone(),
        }
    }

    /// Fire and forget: unfurls on a background task. Needs a Tokio runtime.
    pub fn spawn(self: &Arc<Self>, message: &Message) {
        let has_old_previews = message.embeds.iter().any(|e| is_generated_embed(e));
        if extract_links(&message.content).is_empty() && !has_old_previews {
            return;
        }
        let this = Arc::clone(self);
        let (channel_id, message_id) = (message.channel_id, message.id);
        tokio::spawn(async move {
            if let Err(error) = this.execute(channel_id, message_id).await {
                tracing::warn!(%channel_id, %message_id, %error, "link unfurl failed");
            }
        });
    }

    pub async fn execute(&self, channel_id: ChannelId, message_id: MessageId) -> AppResult<()> {
        let Some(message) = self.messages.find(channel_id, message_id).await? else {
            return Ok(());
        };
        if !may_unfurl(&message) {
            return Ok(());
        }
        let access = self.access.channel(channel_id).await?;
        if access.ensure_standard_delivery().is_err() {
            return Ok(());
        }
        let generated = self.generate(&message).await;
        // Re-read: the author may have edited or suppressed while we fetched.
        let Some(mut current) = self.messages.find(channel_id, message_id).await? else {
            return Ok(());
        };
        if !may_unfurl(&current) || current.content != message.content {
            return Ok(());
        }
        let Some(embeds) = merged_embeds(&current.embeds, generated) else {
            return Ok(());
        };
        current.embeds = embeds;
        self.messages.update(&current).await?;
        announce(
            self.events.as_ref(),
            event(&access, channel_id, MessageEventKind::Updated { message: current }),
        )
        .await;
        Ok(())
    }

    async fn generate(&self, message: &Message) -> Vec<String> {
        let links = extract_links(&message.content);
        let fetches = links
            .iter()
            .take(MAX_UNFURLED_LINKS)
            .map(|link| self.unfurler.unfurl(link));
        join_all(fetches).await.into_iter().flatten().collect()
    }
}

fn may_unfurl(message: &Message) -> bool {
    !message.embeds_suppressed()
        && message.flags & FLAG_EPHEMERAL == 0
        && message.webhook.is_none()
        && message.interaction.is_none()
}

/// Bot-supplied embeds stay, generated ones are replaced by `generated`.
/// `None` when nothing would change.
fn merged_embeds(existing: &[String], generated: Vec<String>) -> Option<Vec<String>> {
    let mut merged: Vec<String> = existing.iter().filter(|e| !is_generated_embed(e)).cloned().collect();
    merged.extend(generated);
    merged.truncate(discordy_messages_domain::message::MAX_EMBEDS);
    (merged != existing).then_some(merged)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fakes::*;
    use discordy_messages_domain::{DeliveryMode, FLAG_SUPPRESS_EMBEDS};

    const LINK_EMBED: &str = r#"{"type":"link","url":"https://a.example/","title":"A"}"#;

    async fn unfurl(h: &Harness, id: MessageId) {
        UnfurlLinks::new(&h.ports).execute(CHANNEL, id).await.unwrap();
    }

    #[tokio::test]
    async fn adds_embeds_and_announces_an_update() {
        let h = Harness::new();
        h.unfurler.set("https://a.example/", LINK_EMBED);
        let message = h.seed(ALICE, "look https://a.example/ now").await;
        unfurl(&h, message.id).await;
        let stored = h.messages.find(CHANNEL, message.id).await.unwrap().unwrap();
        assert_eq!(stored.embeds, vec![LINK_EMBED.to_owned()]);
        assert_eq!(stored.edited_at_ms, None, "a preview is not an edit");
        let kinds = h.events.kinds();
        assert!(matches!(&kinds[..], [MessageEventKind::Updated { message }] if message.embeds.len() == 1));
    }

    #[tokio::test]
    async fn links_without_a_preview_change_nothing() {
        let h = Harness::new();
        let message = h.seed(ALICE, "https://nothing.example/").await;
        unfurl(&h, message.id).await;
        assert!(h.events.kinds().is_empty());
    }

    #[tokio::test]
    async fn suppressed_messages_are_not_fetched() {
        let h = Harness::new();
        h.unfurler.set("https://a.example/", LINK_EMBED);
        let mut message = h.seed(ALICE, "https://a.example/").await;
        message.flags |= FLAG_SUPPRESS_EMBEDS;
        h.messages.update(&message).await.unwrap();
        unfurl(&h, message.id).await;
        assert_eq!(h.unfurler.calls(), 0);
        assert!(h.events.kinds().is_empty());
    }

    #[tokio::test]
    async fn angle_bracket_links_are_not_fetched() {
        let h = Harness::new();
        let message = h.seed(ALICE, "<https://a.example/>").await;
        unfurl(&h, message.id).await;
        assert_eq!(h.unfurler.calls(), 0);
    }

    #[tokio::test]
    async fn ephemeral_and_p2p_channels_are_never_fetched() {
        for mode in [DeliveryMode::Ephemeral, DeliveryMode::P2p] {
            let h = Harness::new();
            h.unfurler.set("https://a.example/", LINK_EMBED);
            let message = h.seed(ALICE, "https://a.example/").await;
            h.access.set(Ok(dm_access(mode)));
            unfurl(&h, message.id).await;
            assert_eq!(h.unfurler.calls(), 0, "{mode:?}");
            assert!(h.events.kinds().is_empty());
        }
    }

    #[tokio::test]
    async fn bot_embeds_stay_and_old_previews_are_replaced() {
        let h = Harness::new();
        let rich = r#"{"type":"rich","title":"bot"}"#;
        let stale = r#"{"type":"link","url":"https://old.example/"}"#;
        h.unfurler.set("https://a.example/", LINK_EMBED);
        let mut message = h.seed(ALICE, "https://a.example/").await;
        message.embeds = vec![rich.into(), stale.into()];
        h.messages.update(&message).await.unwrap();
        unfurl(&h, message.id).await;
        let stored = h.messages.find(CHANNEL, message.id).await.unwrap().unwrap();
        assert_eq!(stored.embeds, vec![rich.to_owned(), LINK_EMBED.to_owned()]);
    }

    #[tokio::test]
    async fn an_unchanged_result_publishes_nothing() {
        let h = Harness::new();
        h.unfurler.set("https://a.example/", LINK_EMBED);
        let message = h.seed(ALICE, "https://a.example/").await;
        unfurl(&h, message.id).await;
        unfurl(&h, message.id).await;
        assert_eq!(h.events.kinds().len(), 1);
    }

    #[tokio::test]
    async fn a_content_edit_during_the_fetch_discards_the_result() {
        let h = Harness::new();
        h.unfurler.set("https://a.example/", LINK_EMBED);
        let message = h.seed(ALICE, "https://a.example/").await;
        h.unfurler.edit_during_fetch(h.messages.clone(), message.clone());
        unfurl(&h, message.id).await;
        let stored = h.messages.find(CHANNEL, message.id).await.unwrap().unwrap();
        assert!(stored.embeds.is_empty());
    }

    #[tokio::test]
    async fn at_most_five_links_are_fetched() {
        let h = Harness::new();
        let text: String = (0..8).map(|i| format!("https://h{i}.example/ ")).collect();
        let message = h.seed(ALICE, &text).await;
        unfurl(&h, message.id).await;
        assert_eq!(h.unfurler.calls(), 5);
    }

    #[test]
    fn generated_embeds_are_recognised_by_type() {
        assert!(is_generated_embed(r#"{"type":"video"}"#));
        assert!(!is_generated_embed(r#"{"type":"rich"}"#));
        assert!(!is_generated_embed(r#"{"title":"no type"}"#));
        assert!(!is_generated_embed("not json"));
    }
}

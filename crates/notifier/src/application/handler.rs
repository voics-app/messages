//! Turns one bus event into pushes: parse, skip what is stale or repeated, ask the api service
//! who may be notified, apply the notification rules and hand the pushes to the dispatcher.

use std::collections::{HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use contracts::{Envelope, MessageDto, event_type};

use super::dispatcher::{Dispatcher, Submitted};
use super::ports::{Platform, TargetDirectory};
use crate::domain::{IncomingMessage, OutboundPush, Verdict, compose, decide, is_mentioned};

const DIRECTORY_ATTEMPTS: u32 = 3;
const DIRECTORY_BACKOFF: Duration = Duration::from_millis(200);

#[derive(Debug, Clone, Copy)]
pub struct HandlerConfig {
    /// Events older than this are not pushed: a notification for a message from before a restart
    /// or a long consumer stall only annoys.
    pub max_event_age: Duration,
    /// How many recent event ids are remembered for deduplication (delivery is at least once).
    pub dedupe_capacity: usize,
}

#[derive(Default)]
struct RecentIds {
    order: VecDeque<String>,
    set: HashSet<String>,
}

impl RecentIds {
    /// Records `id`; false when it was already there.
    fn insert(&mut self, id: &str, capacity: usize) -> bool {
        if !self.set.insert(id.to_owned()) {
            return false;
        }
        self.order.push_back(id.to_owned());
        while self.order.len() > capacity {
            if let Some(oldest) = self.order.pop_front() {
                self.set.remove(&oldest);
            }
        }
        true
    }
}

pub struct EventHandler {
    directory: Arc<dyn TargetDirectory>,
    dispatcher: Dispatcher,
    config: HandlerConfig,
    now_unix_ms: Arc<dyn Fn() -> i64 + Send + Sync>,
    seen: Mutex<RecentIds>,
}

fn event_result(result: &'static str) {
    metrics::counter!("notifier_events_total", "result" => result).increment(1);
}

fn unix_ms(rfc3339: &str) -> Option<i64> {
    let parsed = time::OffsetDateTime::parse(rfc3339, &time::format_description::well_known::Rfc3339).ok()?;
    i64::try_from(parsed.unix_timestamp_nanos() / 1_000_000).ok()
}

impl EventHandler {
    pub fn new(
        directory: Arc<dyn TargetDirectory>,
        dispatcher: Dispatcher,
        config: HandlerConfig,
        now_unix_ms: Arc<dyn Fn() -> i64 + Send + Sync>,
    ) -> Self {
        Self {
            directory,
            dispatcher,
            config,
            now_unix_ms,
            seen: Mutex::new(RecentIds::default()),
        }
    }

    pub async fn handle(&self, envelope: &Envelope) {
        if envelope.kind != event_type::MESSAGE_CREATE {
            return;
        }
        let first_time = self
            .seen
            .lock()
            .map(|mut seen| seen.insert(&envelope.id, self.config.dedupe_capacity))
            .unwrap_or(true);
        if !first_time {
            return event_result("duplicate");
        }
        let age_limit = i64::try_from(self.config.max_event_age.as_millis()).unwrap_or(i64::MAX);
        let age = unix_ms(&envelope.occurred_at).map(|at| (self.now_unix_ms)().saturating_sub(at));
        if age.is_none_or(|age| age > age_limit) {
            return event_result("stale");
        }
        let Ok(dto) = serde_json::from_value::<MessageDto>(envelope.data.clone()) else {
            return event_result("unparsable");
        };
        let message = IncomingMessage::from_dto(dto);
        if !message.is_pushable() {
            return event_result("not_pushable");
        }
        let Some(found) = self.targets(&message).await else {
            return event_result("directory_error");
        };
        event_result("processed");
        for target in &found.targets {
            match decide(&message, &target.recipient) {
                Verdict::Skip(reason) => {
                    metrics::counter!("notifier_decisions_total", "verdict" => reason.label()).increment(1);
                }
                Verdict::Notify => {
                    metrics::counter!("notifier_decisions_total", "verdict" => "notify").increment(1);
                    for token in &target.tokens {
                        if token.only_mentions && !is_mentioned(&message, &target.recipient) {
                            metrics::counter!("notifier_decisions_total", "verdict" => "device_only_mentions")
                                .increment(1);
                            continue;
                        }
                        if token.platform != Platform::Apns {
                            metrics::counter!(
                                "notifier_pushes_total",
                                "provider" => token.platform.label(),
                                "outcome" => "unsupported"
                            )
                            .increment(1);
                            continue;
                        }
                        let alert = compose(&message, &found.context, token.previews);
                        if self.dispatcher.submit(token.clone(), OutboundPush::Alert(alert)) == Submitted::Dropped {
                            tracing::debug!(channel = %message.channel_id, "push queue full, dropped");
                        }
                    }
                }
            }
        }
    }

    async fn targets(&self, message: &IncomingMessage) -> Option<super::ports::ChannelTargets> {
        for attempt in 1..=DIRECTORY_ATTEMPTS {
            match self.directory.targets(&message.channel_id, &message.author_id).await {
                Ok(found) => return Some(found),
                Err(error) => {
                    tracing::warn!(%error, attempt, channel = %message.channel_id, "push targets unavailable");
                    if attempt < DIRECTORY_ATTEMPTS {
                        tokio::time::sleep(DIRECTORY_BACKOFF * attempt).await;
                    }
                }
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use serde_json::json;

    use super::*;
    use crate::application::dispatcher::DispatchConfig;
    use crate::application::ports::{
        ChannelTargets, DeviceToken, Environment, PortError, PushSender, SendError, Target, TokenRegistry,
    };
    use crate::domain::{ChannelContext, Level, Recipient};

    const NOW_MS: i64 = 1_793_000_000_000;

    struct FakeDirectory {
        answer: Mutex<Result<ChannelTargets, PortError>>,
        calls: AtomicUsize,
    }

    #[async_trait]
    impl TargetDirectory for FakeDirectory {
        async fn targets(&self, _channel: &str, _author: &str) -> Result<ChannelTargets, PortError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.answer.lock().unwrap().clone()
        }
    }

    #[derive(Default)]
    struct Recording {
        sent: Mutex<Vec<(DeviceToken, OutboundPush)>>,
    }

    #[async_trait]
    impl PushSender for Recording {
        async fn send(&self, token: &DeviceToken, push: &OutboundPush) -> Result<(), SendError> {
            self.sent.lock().unwrap().push((token.clone(), push.clone()));
            Ok(())
        }
    }

    struct NoRegistry;

    #[async_trait]
    impl TokenRegistry for NoRegistry {
        async fn forget(&self, _token: &DeviceToken) -> Result<(), PortError> {
            Ok(())
        }
    }

    fn recipient(user: &str) -> Recipient {
        Recipient {
            user_id: user.into(),
            role_ids: Vec::new(),
            online: false,
            level: Level::AllMessages,
            muted: false,
            suppress_everyone: false,
            suppress_roles: false,
        }
    }

    fn apns(token: &str, previews: bool) -> DeviceToken {
        DeviceToken {
            platform: Platform::Apns,
            token: token.into(),
            environment: Environment::Sandbox,
            previews,
            only_mentions: false,
        }
    }

    fn context(mode: &str) -> ChannelContext {
        ChannelContext {
            guild_name: Some("Test Guild".into()),
            channel_name: Some("general".into()),
            delivery_mode: mode.into(),
        }
    }

    fn envelope(id: &str, at_ms: i64, overrides: serde_json::Value) -> Envelope {
        let mut data = json!({
            "id": "100", "channel_id": "10", "guild_id": "1",
            "author": {"id": "7", "username": "alice", "global_name": "Alice"},
            "content": "hello there", "timestamp": "2026-11-05T10:00:00Z",
            "edited_timestamp": null, "tts": false, "mention_everyone": false,
            "mentions": [], "mention_roles": [], "attachments": [], "embeds": [],
            "pinned": false, "type": 0, "flags": 0, "components": []
        });
        for (key, value) in overrides.as_object().cloned().unwrap_or_default() {
            data[key] = value;
        }
        let occurred_at = time::OffsetDateTime::from_unix_timestamp_nanos(i128::from(at_ms) * 1_000_000)
            .unwrap()
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap();
        Envelope {
            id: id.into(),
            kind: "MESSAGE_CREATE".into(),
            v: 1,
            occurred_at,
            guild_id: Some("1".into()),
            channel_id: Some("10".into()),
            target_user_ids: None,
            data,
        }
    }

    struct Rig {
        handler: EventHandler,
        directory: Arc<FakeDirectory>,
        sender: Arc<Recording>,
    }

    fn rig(targets: Vec<Target>, mode: &str) -> Rig {
        let directory = Arc::new(FakeDirectory {
            answer: Mutex::new(Ok(ChannelTargets {
                context: context(mode),
                targets,
            })),
            calls: AtomicUsize::new(0),
        });
        let sender = Arc::new(Recording::default());
        let dispatcher = Dispatcher::spawn(
            DispatchConfig {
                queue_capacity: 100,
                max_in_flight: 10,
                max_attempts: 1,
                base_backoff: Duration::from_millis(1),
                max_backoff: Duration::from_millis(1),
            },
            sender.clone(),
            Arc::new(NoRegistry),
        );
        let handler = EventHandler::new(
            directory.clone(),
            dispatcher,
            HandlerConfig {
                max_event_age: Duration::from_secs(120),
                dedupe_capacity: 2,
            },
            Arc::new(|| NOW_MS),
        );
        Rig {
            handler,
            directory,
            sender,
        }
    }

    async fn settle() {
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
    }

    fn sent_to(rig: &Rig) -> Vec<String> {
        let mut tokens: Vec<String> = rig
            .sender
            .sent
            .lock()
            .unwrap()
            .iter()
            .map(|(t, _)| t.token.clone())
            .collect();
        tokens.sort();
        tokens
    }

    #[tokio::test]
    async fn pushes_go_to_offline_recipients_the_rules_allow() {
        let online = Recipient {
            online: true,
            ..recipient("2")
        };
        let muted = Recipient {
            muted: true,
            ..recipient("3")
        };
        let nothing = Recipient {
            level: Level::NoMessages,
            ..recipient("4")
        };
        let rig = rig(
            vec![
                Target {
                    recipient: recipient("1"),
                    tokens: vec![apns("phone", true), apns("tablet", true)],
                },
                Target {
                    recipient: online,
                    tokens: vec![apns("online", true)],
                },
                Target {
                    recipient: muted,
                    tokens: vec![apns("muted", true)],
                },
                Target {
                    recipient: nothing,
                    tokens: vec![apns("nothing", true)],
                },
            ],
            "standard",
        );
        rig.handler.handle(&envelope("e1", NOW_MS - 1000, json!({}))).await;
        settle().await;
        assert_eq!(sent_to(&rig), vec!["phone", "tablet"]);
    }

    #[tokio::test]
    async fn a_device_that_wants_only_mentions_skips_plain_messages_but_not_pings() {
        let picky = DeviceToken {
            only_mentions: true,
            ..apns("picky", true)
        };
        let rig = rig(
            vec![Target {
                recipient: recipient("1"),
                tokens: vec![picky, apns("open", true)],
            }],
            "standard",
        );
        rig.handler.handle(&envelope("plain", NOW_MS, json!({}))).await;
        settle().await;
        assert_eq!(
            sent_to(&rig),
            vec!["open"],
            "level all, but this device wants mentions only"
        );

        let ping = json!({"mentions": [{"id": "1"}]});
        rig.handler.handle(&envelope("ping", NOW_MS, ping)).await;
        settle().await;
        assert_eq!(
            sent_to(&rig),
            vec!["open", "open", "picky"],
            "a direct mention reaches both devices"
        );
    }

    #[tokio::test]
    async fn the_preview_choice_is_per_device_and_standard_text_is_shown() {
        let rig = rig(
            vec![Target {
                recipient: recipient("1"),
                tokens: vec![apns("shows", true), apns("hides", false)],
            }],
            "standard",
        );
        rig.handler.handle(&envelope("e1", NOW_MS, json!({}))).await;
        settle().await;
        let sent = rig.sender.sent.lock().unwrap();
        let body = |name: &str| match &sent.iter().find(|(t, _)| t.token == name).unwrap().1 {
            OutboundPush::Alert(alert) => alert.body.clone(),
            OutboundPush::Voip(_) => unreachable!(),
        };
        assert_eq!(body("shows"), "hello there");
        assert_eq!(body("hides"), "New message");
    }

    #[tokio::test]
    async fn an_encrypted_channel_never_leaks_text_into_a_push() {
        let rig = rig(
            vec![Target {
                recipient: recipient("1"),
                tokens: vec![apns("phone", true)],
            }],
            "ephemeral",
        );
        rig.handler
            .handle(&envelope("e1", NOW_MS, json!({"content": "top secret"})))
            .await;
        settle().await;
        let sent = rig.sender.sent.lock().unwrap();
        let OutboundPush::Alert(alert) = &sent[0].1 else {
            unreachable!()
        };
        assert_eq!(alert.title, "New message");
        assert!(alert.body.is_empty());
        assert!(!format!("{alert:?}").contains("secret"));
        assert!(!format!("{alert:?}").contains("Alice"));
    }

    #[tokio::test]
    async fn fcm_tokens_are_skipped_until_a_sender_exists() {
        let fcm = DeviceToken {
            platform: Platform::Fcm,
            ..apns("android", true)
        };
        let rig = rig(
            vec![Target {
                recipient: recipient("1"),
                tokens: vec![fcm, apns("phone", true)],
            }],
            "standard",
        );
        rig.handler.handle(&envelope("e1", NOW_MS, json!({}))).await;
        settle().await;
        assert_eq!(sent_to(&rig), vec!["phone"]);
    }

    #[tokio::test]
    async fn stale_repeated_foreign_and_silent_events_send_nothing() {
        let rig = rig(
            vec![Target {
                recipient: recipient("1"),
                tokens: vec![apns("phone", true)],
            }],
            "standard",
        );
        rig.handler
            .handle(&envelope("old", NOW_MS - 10 * 60 * 1000, json!({})))
            .await;
        let mut other = envelope("other", NOW_MS, json!({}));
        other.kind = "MESSAGE_UPDATE".into();
        rig.handler.handle(&other).await;
        rig.handler
            .handle(&envelope("silent", NOW_MS, json!({"flags": 1 << 12})))
            .await;
        rig.handler
            .handle(&envelope("system", NOW_MS, json!({"type": 7})))
            .await;
        settle().await;
        assert!(sent_to(&rig).is_empty());
        assert_eq!(
            rig.directory.calls.load(Ordering::SeqCst),
            0,
            "no api call for events that cannot push"
        );

        rig.handler.handle(&envelope("fresh", NOW_MS, json!({}))).await;
        rig.handler.handle(&envelope("fresh", NOW_MS, json!({}))).await;
        settle().await;
        assert_eq!(sent_to(&rig), vec!["phone"], "a repeated event id pushes once");
    }

    #[tokio::test(start_paused = true)]
    async fn an_unavailable_directory_is_retried_then_the_event_is_given_up() {
        let rig = rig(Vec::new(), "standard");
        *rig.directory.answer.lock().unwrap() = Err(PortError::new("api down"));
        rig.handler.handle(&envelope("e1", NOW_MS, json!({}))).await;
        assert_eq!(rig.directory.calls.load(Ordering::SeqCst), 3);
        assert!(sent_to(&rig).is_empty());
    }

    #[test]
    fn the_dedupe_window_forgets_the_oldest_id() {
        let mut ids = RecentIds::default();
        assert!(ids.insert("a", 2));
        assert!(!ids.insert("a", 2));
        assert!(ids.insert("b", 2));
        assert!(ids.insert("c", 2));
        assert!(ids.insert("a", 2), "a was pushed out by b and c");
    }
}

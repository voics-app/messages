//! Turns one bus event into pushes: parse, skip what is stale or repeated, ask the api service
//! who may be notified, apply the notification rules and hand the pushes to the dispatcher.

use std::collections::{HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use contracts::{Envelope, MessageDto, event_type};

use super::dispatcher::{Dispatcher, Submitted};
use super::ports::{EventReminder, Platform, TargetDirectory};
use crate::domain::{
    AlertContent, IncomingMessage, OutboundPush, Verdict, compose, compose_call, decide, is_mentioned,
};

const DIRECTORY_ATTEMPTS: u32 = 3;
const DIRECTORY_BACKOFF: Duration = Duration::from_millis(200);
/// A call ring older than this is not pushed (the gateway rings for 30 seconds).
const CALL_MAX_AGE_MS: i64 = 20_000;

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
        if envelope.kind == "GUILD_SCHEDULED_EVENT_START" {
            return self.handle_event_reminder(envelope).await;
        }
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
        if message.is_call_ring() {
            // A phone rings for a few seconds: a call event that sat in a queue longer than that is over.
            if age.is_some_and(|age| age > CALL_MAX_AGE_MS) {
                return event_result("stale_call");
            }
            return self.ring(&message).await;
        }
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
                        let alert = compose(&message, &found.context, token.previews);
                        if self.dispatcher.submit(token.clone(), OutboundPush::Alert(alert)) == Submitted::Dropped {
                            tracing::debug!(channel = %message.channel_id, "push queue full, dropped");
                        }
                    }
                }
            }
        }
    }

    async fn handle_event_reminder(&self, envelope: &Envelope) {
        let Some(reminder) = parse_event_reminder(envelope) else {
            return event_result("unparsable_event_reminder");
        };
        let Some(start_ms) = unix_ms(&reminder.scheduled_start_time) else {
            return event_result("unparsable_event_reminder");
        };
        let age = (self.now_unix_ms)().saturating_sub(start_ms);
        if !(0..=300_000).contains(&age) {
            return event_result("stale_event_reminder");
        }
        let mut tokens = None;
        for attempt in 1..=DIRECTORY_ATTEMPTS {
            match self.directory.event_targets(&reminder).await {
                Ok(found) => {
                    tokens = Some(found);
                    break;
                }
                Err(error) => {
                    tracing::warn!(%error, attempt, "event reminder directory unavailable");
                    if attempt < DIRECTORY_ATTEMPTS {
                        tokio::time::sleep(DIRECTORY_BACKOFF * attempt).await;
                    }
                }
            }
        }
        let Some(tokens) = tokens else {
            return event_result("directory_error");
        };
        let first_time = self
            .seen
            .lock()
            .map(|mut seen| seen.insert(&reminder.dedupe_key, self.config.dedupe_capacity))
            .unwrap_or(true);
        if !first_time {
            return event_result("duplicate");
        }
        let alert = AlertContent {
            title: "Event starting".into(),
            body: "An event you are interested in is starting.".into(),
            thread_id: reminder.guild_id.clone(),
            data: vec![
                ("type".into(), "scheduled_event".into()),
                ("guild_id".into(), reminder.guild_id),
                ("event_id".into(), reminder.event_id),
                ("dedupe_key".into(), reminder.dedupe_key),
            ],
        };
        for token in tokens {
            if token.only_mentions {
                continue;
            }
            if self.dispatcher.submit(token, OutboundPush::Alert(alert.clone())) == Submitted::Dropped {
                tracing::warn!("scheduled event push queue full");
            }
        }
        event_result("event_reminder_processed");
    }

    /// Rings every device of the recipients of a DM call that has just started, with a PushKit push
    /// (`OutboundPush::Voip`). Who rings: members of the conversation other than the caller, who are not
    /// in the call yet and have not muted the conversation. Online status and the message level do not
    /// matter: a phone that went to the background keeps a live gateway session for a while, and a call
    /// that does not ring is worse than one that rings twice (the app shows one call, see
    /// `docs/handbook/13-mobile-client.md`).
    async fn ring(&self, message: &IncomingMessage) {
        let Some(found) = self.targets(message).await else {
            return event_result("directory_error");
        };
        event_result("call");
        let payload = compose_call(message, &found.context);
        let in_call = message
            .call
            .as_ref()
            .map(|call| call.participants.as_slice())
            .unwrap_or_default();
        for target in &found.targets {
            let verdict = if in_call.contains(&target.recipient.user_id) {
                "call_in_call"
            } else if target.recipient.muted {
                "call_muted"
            } else {
                "call_ring"
            };
            metrics::counter!("notifier_decisions_total", "verdict" => verdict).increment(1);
            if verdict != "call_ring" {
                continue;
            }
            // Android has no PushKit: its ring is a data-only FCM message to the ordinary token.
            let android = target.tokens.iter().filter(|token| token.platform == Platform::Fcm);
            for token in target.voip_tokens.iter().chain(android) {
                if self
                    .dispatcher
                    .submit(token.clone(), OutboundPush::Voip(payload.clone()))
                    == Submitted::Dropped
                {
                    tracing::debug!(channel = %message.channel_id, "push queue full, dropped a call ring");
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

fn parse_event_reminder(envelope: &Envelope) -> Option<EventReminder> {
    let [user_id] = envelope.target_user_ids.as_deref()? else {
        return None;
    };
    let guild_id = envelope.data.get("guild_id")?.as_str()?;
    let event_id = envelope.data.get("guild_scheduled_event_id")?.as_str()?;
    let start = envelope.data.get("scheduled_start_time")?.as_str()?;
    let revision = envelope.data.get("revision")?.as_i64()?;
    let key = envelope.data.get("dedupe_key")?.as_str()?;
    let numeric = |id: &str| !id.is_empty() && id.len() <= 20 && id.bytes().all(|b| b.is_ascii_digit());
    if !numeric(guild_id)
        || !numeric(event_id)
        || !numeric(user_id)
        || revision < 0
        || envelope.guild_id.as_deref() != Some(guild_id)
    {
        return None;
    }
    let expected = format!("{guild_id}:{event_id}:{}:{revision}:{user_id}", unix_ms(start)?);
    if key != expected {
        return None;
    }
    Some(EventReminder {
        guild_id: guild_id.into(),
        event_id: event_id.into(),
        user_id: user_id.clone(),
        scheduled_start_time: start.into(),
        revision,
        dedupe_key: key.into(),
    })
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
        async fn event_targets(&self, reminder: &EventReminder) -> Result<Vec<DeviceToken>, PortError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let found = self.answer.lock().unwrap().clone()?;
            Ok(found
                .targets
                .into_iter()
                .filter(|target| target.recipient.user_id == reminder.user_id)
                .flat_map(|target| target.tokens)
                .collect())
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
    async fn event_reminder_rechecks_target_and_deduplicates_the_stable_key() {
        let rig = rig(
            vec![Target {
                recipient: recipient("2"),
                tokens: vec![
                    apns("phone", true),
                    DeviceToken {
                        only_mentions: true,
                        ..apns("mentions-only", true)
                    },
                ],
                voip_tokens: vec![],
            }],
            "standard",
        );
        let start_ms = NOW_MS - 1000;
        let start = time::OffsetDateTime::from_unix_timestamp_nanos(i128::from(start_ms) * 1_000_000)
            .unwrap()
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap();
        let mut message = envelope("first", NOW_MS, json!({}));
        message.kind = "GUILD_SCHEDULED_EVENT_START".into();
        message.target_user_ids = Some(vec!["2".into()]);
        message.data = json!({ "guild_id":"1", "guild_scheduled_event_id":"9", "scheduled_start_time":start, "revision":0, "dedupe_key":format!("1:9:{start_ms}:0:2") });
        rig.handler.handle(&message).await;
        message.id = "replayed-with-different-envelope".into();
        rig.handler.handle(&message).await;
        settle().await;
        assert_eq!(sent_to(&rig), vec!["phone"]);
        let sent = rig.sender.sent.lock().unwrap();
        let OutboundPush::Alert(alert) = &sent[0].1 else {
            panic!("expected alert")
        };
        assert_eq!(alert.title, "Event starting");
        assert!(!alert.body.contains("Meetup"));
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
                    voip_tokens: Vec::new(),
                },
                Target {
                    recipient: online,
                    tokens: vec![apns("online", true)],
                    voip_tokens: Vec::new(),
                },
                Target {
                    recipient: muted,
                    tokens: vec![apns("muted", true)],
                    voip_tokens: Vec::new(),
                },
                Target {
                    recipient: nothing,
                    tokens: vec![apns("nothing", true)],
                    voip_tokens: Vec::new(),
                },
            ],
            "standard",
        );
        rig.handler.handle(&envelope("e1", NOW_MS - 1000, json!({}))).await;
        settle().await;
        assert_eq!(sent_to(&rig), vec!["online", "phone", "tablet"]);
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
                voip_tokens: Vec::new(),
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
                voip_tokens: Vec::new(),
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
                voip_tokens: Vec::new(),
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
    async fn fcm_tokens_get_the_same_alert_as_apns_tokens() {
        let fcm = DeviceToken {
            platform: Platform::Fcm,
            ..apns("android", true)
        };
        let rig = rig(
            vec![Target {
                recipient: recipient("1"),
                tokens: vec![fcm, apns("phone", true)],
                voip_tokens: Vec::new(),
            }],
            "standard",
        );
        rig.handler.handle(&envelope("e1", NOW_MS, json!({}))).await;
        settle().await;
        assert_eq!(sent_to(&rig), vec!["android", "phone"]);
        let sent = rig.sender.sent.lock().unwrap();
        let alerts: Vec<_> = sent
            .iter()
            .map(|(token, push)| match push {
                OutboundPush::Alert(alert) => (token.platform, alert.body.clone()),
                OutboundPush::Voip(_) => unreachable!(),
            })
            .collect();
        assert!(alerts.iter().all(|(_, body)| body == "hello there"));
        assert!(alerts.iter().any(|(platform, _)| *platform == Platform::Fcm));
    }

    #[tokio::test]
    async fn an_fcm_device_obeys_its_own_previews_and_only_mentions_settings() {
        let fcm = |token: &str, previews, only_mentions| DeviceToken {
            platform: Platform::Fcm,
            previews,
            only_mentions,
            ..apns(token, true)
        };
        let rig = rig(
            vec![Target {
                recipient: recipient("1"),
                tokens: vec![fcm("quiet", false, false), fcm("pings", true, true)],
                voip_tokens: Vec::new(),
            }],
            "standard",
        );
        rig.handler.handle(&envelope("e1", NOW_MS, json!({}))).await;
        settle().await;
        assert_eq!(sent_to(&rig), vec!["quiet"]);
        let sent = rig.sender.sent.lock().unwrap();
        match &sent[0].1 {
            OutboundPush::Alert(alert) => assert_eq!(alert.body, "New message"),
            OutboundPush::Voip(_) => unreachable!(),
        }
    }

    #[tokio::test]
    async fn a_call_rings_android_through_its_fcm_token_and_ios_through_voip_only() {
        let fcm = DeviceToken {
            platform: Platform::Fcm,
            ..apns("android", true)
        };
        let both = Target {
            recipient: recipient("1"),
            tokens: vec![fcm, apns("iphone-alert", true)],
            voip_tokens: vec![apns("iphone-voip", false)],
        };
        let rig = rig(vec![both], "standard");
        rig.handler.handle(&call_envelope("c1", NOW_MS, json!({}))).await;
        settle().await;
        assert_eq!(sent_to(&rig), vec!["android", "iphone-voip"]);
        for (_, push) in rig.sender.sent.lock().unwrap().iter() {
            assert!(matches!(push, OutboundPush::Voip(_)), "no alert for a ring");
        }
    }

    #[tokio::test]
    async fn stale_repeated_foreign_and_silent_events_send_nothing() {
        let rig = rig(
            vec![Target {
                recipient: recipient("1"),
                tokens: vec![apns("phone", true)],
                voip_tokens: Vec::new(),
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

    fn call_envelope(id: &str, at_ms: i64, overrides: serde_json::Value) -> Envelope {
        let mut data = json!({"type": 3, "guild_id": null, "content": "", "call": {"participants": ["7"]}});
        for (key, value) in overrides.as_object().cloned().unwrap_or_default() {
            data[key] = value;
        }
        envelope(id, at_ms, data)
    }

    fn with_voip(user: &str, phone: &str, voip: &str) -> Target {
        Target {
            recipient: recipient(user),
            tokens: vec![apns(phone, true)],
            voip_tokens: vec![apns(voip, false)],
        }
    }

    fn voip_payloads(rig: &Rig) -> Vec<(String, serde_json::Value)> {
        rig.sender
            .sent
            .lock()
            .unwrap()
            .iter()
            .map(|(token, push)| match push {
                OutboundPush::Voip(payload) => (token.token.clone(), payload.clone()),
                OutboundPush::Alert(alert) => panic!("a call must not be an alert: {alert:?}"),
            })
            .collect()
    }

    #[tokio::test]
    async fn a_new_dm_call_rings_every_voip_device_and_sends_no_alert() {
        let both = Target {
            recipient: recipient("1"),
            tokens: vec![apns("phone", true)],
            voip_tokens: vec![apns("voip-phone", false), apns("voip-tablet", false)],
        };
        let rig = rig(vec![both, with_voip("2", "other", "voip-other")], "standard");
        rig.handler.handle(&call_envelope("c1", NOW_MS - 1000, json!({}))).await;
        settle().await;
        let sent = voip_payloads(&rig);
        let mut tokens: Vec<&str> = sent.iter().map(|(token, _)| token.as_str()).collect();
        tokens.sort_unstable();
        assert_eq!(tokens, vec!["voip-other", "voip-phone", "voip-tablet"]);
        assert_eq!(sent[0].1["type"], "call");
        assert_eq!(sent[0].1["caller_name"], "Alice");
        assert_eq!(sent[0].1["channel_id"], "10");
    }

    #[tokio::test]
    async fn a_call_rings_a_user_who_has_only_a_voip_token() {
        let only_voip = Target {
            recipient: recipient("1"),
            tokens: Vec::new(),
            voip_tokens: vec![apns("voip", false)],
        };
        let rig = rig(vec![only_voip], "standard");
        rig.handler.handle(&call_envelope("c1", NOW_MS, json!({}))).await;
        settle().await;
        assert_eq!(sent_to(&rig), vec!["voip"]);
    }

    #[tokio::test]
    async fn a_call_does_not_ring_people_already_in_it_or_who_muted_the_conversation_but_ignores_online_and_level() {
        let muted = Recipient {
            muted: true,
            ..recipient("3")
        };
        let online_quiet = Recipient {
            online: true,
            level: Level::NoMessages,
            ..recipient("4")
        };
        let rig = rig(
            vec![
                with_voip("1", "p1", "v-joined"),
                Target {
                    recipient: muted,
                    tokens: Vec::new(),
                    voip_tokens: vec![apns("v-muted", false)],
                },
                Target {
                    recipient: online_quiet,
                    tokens: Vec::new(),
                    voip_tokens: vec![apns("v-online", false)],
                },
            ],
            "standard",
        );
        let call = json!({"call": {"participants": ["7", "1"]}});
        rig.handler.handle(&call_envelope("c1", NOW_MS, call)).await;
        settle().await;
        assert_eq!(sent_to(&rig), vec!["v-online"]);
    }

    #[tokio::test]
    async fn an_encrypted_dm_call_rings_without_the_name_and_with_no_message_text() {
        for mode in ["ephemeral", "p2p", "something-new"] {
            let rig = rig(vec![with_voip("1", "p", "v")], mode);
            let overrides = json!({"content": "top secret"});
            rig.handler.handle(&call_envelope("c1", NOW_MS, overrides)).await;
            settle().await;
            let sent = voip_payloads(&rig);
            assert_eq!(sent.len(), 1, "{mode}");
            let text = sent[0].1.to_string();
            assert!(
                !text.contains("Alice") && !text.contains("secret") && !text.contains("general"),
                "{mode}: {text}"
            );
            assert_eq!(sent[0].1["private"], true);
        }
    }

    #[tokio::test]
    async fn an_ended_a_server_a_stale_and_a_repeated_call_event_ring_nothing() {
        let rig = rig(vec![with_voip("1", "p", "v")], "standard");
        let ended = json!({"call": {"participants": ["7"], "ended_timestamp": "2026-11-05T10:05:00Z"}});
        rig.handler.handle(&call_envelope("ended", NOW_MS, ended)).await;
        rig.handler
            .handle(&call_envelope("server", NOW_MS, json!({"guild_id": "1"})))
            .await;
        rig.handler
            .handle(&call_envelope("stale", NOW_MS - 25_000, json!({})))
            .await;
        settle().await;
        assert!(sent_to(&rig).is_empty());

        rig.handler
            .handle(&call_envelope("once", NOW_MS - 5000, json!({})))
            .await;
        rig.handler
            .handle(&call_envelope("once", NOW_MS - 5000, json!({})))
            .await;
        settle().await;
        assert_eq!(sent_to(&rig), vec!["v"], "a repeated event id rings once");
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

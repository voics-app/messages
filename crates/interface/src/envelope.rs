//! Encodes message events into the P6 envelope and sends them to the bus.

use std::sync::Arc;

use async_trait::async_trait;
use contracts::{
    ChannelPinsUpdateData, ENVELOPE_VERSION, Envelope, MESSAGE_TOPIC, MessageAckData, MessageDeleteBulkData,
    MessageDeleteData, MessageReactionData, MessageReactionRemoveAllData, event_type,
};
use discordy_messages_application::PortError;
use discordy_messages_application::ports::{
    Clock, EventPublisher, EventSink, MessageEvent, MessageEventKind, UserDirectory,
};
use discordy_messages_domain::Audience;
use serde::Serialize;
use serde_json::Value;
use uuid::Uuid;

use crate::attachment_urls::AttachmentUrls;
use crate::authors::{Authors, resolve};
use crate::dto::{emoji_dto, message_dto, rfc3339};

pub struct EnvelopePublisher {
    sink: Arc<dyn EventSink>,
    clock: Arc<dyn Clock>,
    urls: AttachmentUrls,
    users: Arc<dyn UserDirectory>,
}

impl EnvelopePublisher {
    pub fn new(
        sink: Arc<dyn EventSink>,
        clock: Arc<dyn Clock>,
        urls: AttachmentUrls,
        users: Arc<dyn UserDirectory>,
    ) -> Self {
        Self {
            sink,
            clock,
            urls,
            users,
        }
    }

    /// Builds the envelope for `event`. Guild audiences are routed by
    /// `guild_id`; everything else names its recipients explicitly.
    pub fn envelope(&self, event: &MessageEvent, authors: &Authors) -> Result<Envelope, PortError> {
        let channel = event.channel_id.to_string();
        let guild = event.guild_id.map(|id| id.to_string());
        let (kind, data) = self.payload(event, &channel, guild.clone(), authors)?;
        let (guild_id, target_user_ids) = match &event.audience {
            Audience::Guild(id) => (Some(id.to_string()), None),
            Audience::Users(users) => (guild, Some(users.iter().map(ToString::to_string).collect())),
        };
        Ok(Envelope {
            id: Uuid::now_v7().to_string(),
            kind: kind.to_owned(),
            v: ENVELOPE_VERSION,
            occurred_at: rfc3339(self.clock.now_unix_ms()),
            guild_id,
            channel_id: Some(channel),
            target_user_ids,
            data,
        })
    }

    fn payload(
        &self,
        event: &MessageEvent,
        channel: &str,
        guild: Option<String>,
        authors: &Authors,
    ) -> Result<(&'static str, Value), PortError> {
        let channel_id = channel.to_owned();
        let reaction = |message_id: &discordy_messages_domain::MessageId,
                        user_id: &discordy_messages_domain::UserId,
                        emoji: &discordy_messages_domain::Emoji| MessageReactionData {
            user_id: user_id.to_string(),
            channel_id: channel_id.clone(),
            message_id: message_id.to_string(),
            guild_id: guild.clone(),
            emoji: emoji_dto(emoji),
        };
        let (kind, data) = match &event.kind {
            MessageEventKind::Created { message, nonce } => (
                event_type::MESSAGE_CREATE,
                encode(&message_dto(
                    message,
                    &[],
                    event.guild_id,
                    nonce.clone(),
                    &self.urls,
                    authors,
                ))?,
            ),
            MessageEventKind::Updated { message } => (
                event_type::MESSAGE_UPDATE,
                encode(&message_dto(message, &[], event.guild_id, None, &self.urls, authors))?,
            ),
            MessageEventKind::Deleted { message_id } => (
                event_type::MESSAGE_DELETE,
                encode(&MessageDeleteData {
                    id: message_id.to_string(),
                    channel_id: channel_id.clone(),
                    guild_id: guild.clone(),
                })?,
            ),
            MessageEventKind::DeletedBulk { message_ids } => (
                event_type::MESSAGE_DELETE_BULK,
                encode(&MessageDeleteBulkData {
                    ids: message_ids.iter().map(ToString::to_string).collect(),
                    channel_id: channel_id.clone(),
                    guild_id: guild.clone(),
                })?,
            ),
            MessageEventKind::ReactionAdded {
                message_id,
                user_id,
                emoji,
            } => (
                event_type::MESSAGE_REACTION_ADD,
                encode(&reaction(message_id, user_id, emoji))?,
            ),
            MessageEventKind::ReactionRemoved {
                message_id,
                user_id,
                emoji,
            } => (
                event_type::MESSAGE_REACTION_REMOVE,
                encode(&reaction(message_id, user_id, emoji))?,
            ),
            MessageEventKind::ReactionsCleared { message_id } => (
                event_type::MESSAGE_REACTION_REMOVE_ALL,
                encode(&MessageReactionRemoveAllData {
                    channel_id: channel_id.clone(),
                    message_id: message_id.to_string(),
                    guild_id: guild.clone(),
                })?,
            ),
            MessageEventKind::PinsUpdated { last_pin_timestamp_ms } => (
                event_type::CHANNEL_PINS_UPDATE,
                encode(&ChannelPinsUpdateData {
                    guild_id: guild.clone(),
                    channel_id: channel_id.clone(),
                    last_pin_timestamp: last_pin_timestamp_ms.map(rfc3339),
                })?,
            ),
            MessageEventKind::Acknowledged {
                message_id,
                mention_count,
            } => (
                event_type::MESSAGE_ACK,
                encode(&MessageAckData {
                    channel_id: channel_id.clone(),
                    message_id: message_id.to_string(),
                    mention_count: *mention_count,
                })?,
            ),
        };
        Ok((kind, data))
    }
}

fn encode(value: &impl Serialize) -> Result<Value, PortError> {
    serde_json::to_value(value).map_err(PortError::new)
}

#[async_trait]
impl EventPublisher for EnvelopePublisher {
    async fn publish(&self, event: MessageEvent) -> Result<(), PortError> {
        let authors = match &event.kind {
            MessageEventKind::Created { message, .. } | MessageEventKind::Updated { message } => {
                resolve(self.users.as_ref(), [message]).await
            }
            _ => Authors::new(),
        };
        let envelope = self.envelope(&event, &authors)?;
        let payload = serde_json::to_vec(&envelope).map_err(PortError::new)?;
        self.sink
            .send(MESSAGE_TOPIC, &event.channel_id.to_string(), payload)
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::AtomicU64;

    use discordy_messages_application::fakes::{ALICE, BOB, CHANNEL, FakeClock, FakeUsers, GUILD, START_MS};
    use discordy_messages_domain::{Emoji, Snowflake};

    use super::*;

    type Sent = Vec<(String, String, Vec<u8>)>;

    #[derive(Default)]
    struct Sink(Mutex<Sent>);

    #[async_trait]
    impl EventSink for Sink {
        async fn send(&self, topic: &str, key: &str, payload: Vec<u8>) -> Result<(), PortError> {
            self.0.lock().unwrap().push((topic.into(), key.into(), payload));
            Ok(())
        }
    }

    fn publisher() -> (EnvelopePublisher, Arc<Sink>) {
        let sink = Arc::new(Sink::default());
        let clock = Arc::new(FakeClock(AtomicU64::new(START_MS)));
        let users = Arc::new(FakeUsers::default());
        users.add(ALICE, "alice");
        let urls = AttachmentUrls::new(
            "http://cdn",
            "unit-test-signing-key-0123456789abcdef",
            std::time::Duration::from_secs(60),
            clock.clone(),
        );
        let publisher = EnvelopePublisher::new(sink.clone(), clock, urls, users);
        (publisher, sink)
    }

    fn event(audience: Audience, guild: Option<Snowflake>, kind: MessageEventKind) -> MessageEvent {
        MessageEvent {
            channel_id: CHANNEL,
            guild_id: guild,
            audience,
            kind,
        }
    }

    #[tokio::test]
    async fn guild_events_are_routed_by_guild_id_and_keyed_by_channel() {
        let (publisher, sink) = publisher();
        let kind = MessageEventKind::Deleted {
            message_id: Snowflake::new(77),
        };
        publisher
            .publish(event(Audience::Guild(GUILD), Some(GUILD), kind))
            .await
            .unwrap();
        let sent = sink.0.lock().unwrap();
        let (topic, key, payload) = &sent[0];
        assert_eq!(topic, "discordy.message.v1");
        assert_eq!(key, &CHANNEL.to_string());
        let envelope: Envelope = serde_json::from_slice(payload).unwrap();
        assert_eq!(envelope.kind, "MESSAGE_DELETE");
        assert_eq!(envelope.v, 1);
        assert_eq!(envelope.guild_id, Some(GUILD.to_string()));
        assert_eq!(envelope.target_user_ids, None);
        assert_eq!(envelope.data["id"], "77");
        assert_eq!(envelope.data["channel_id"], CHANNEL.to_string());
        assert!(Uuid::parse_str(&envelope.id).is_ok());
    }

    #[test]
    fn direct_messages_name_their_recipients() {
        let (publisher, _) = publisher();
        let kind = MessageEventKind::ReactionAdded {
            message_id: Snowflake::new(1),
            user_id: ALICE,
            emoji: Emoji::Unicode("👍".into()),
        };
        let envelope = publisher
            .envelope(&event(Audience::Users(vec![ALICE, BOB]), None, kind), &Authors::new())
            .unwrap();
        assert_eq!(envelope.kind, "MESSAGE_REACTION_ADD");
        assert_eq!(envelope.guild_id, None);
        assert_eq!(envelope.target_user_ids, Some(vec![ALICE.to_string(), BOB.to_string()]));
        assert_eq!(envelope.data["emoji"]["name"], "👍");
        assert_eq!(envelope.data["user_id"], ALICE.to_string());
    }

    #[tokio::test]
    async fn message_events_carry_the_full_author() {
        let (publisher, sink) = publisher();
        let draft = discordy_messages_domain::MessageDraft {
            content: "hi".into(),
            ..Default::default()
        };
        let message =
            discordy_messages_domain::Message::compose(Snowflake::new(1 << 22), CHANNEL, ALICE, draft, false).unwrap();
        let kind = MessageEventKind::Created {
            message: message.clone(),
            nonce: None,
        };
        publisher
            .publish(event(Audience::Guild(GUILD), Some(GUILD), kind))
            .await
            .unwrap();
        let updated = MessageEventKind::Updated { message };
        publisher
            .publish(event(Audience::Guild(GUILD), Some(GUILD), updated))
            .await
            .unwrap();
        for (_, _, payload) in sink.0.lock().unwrap().iter() {
            let envelope: Envelope = serde_json::from_slice(payload).unwrap();
            assert_eq!(envelope.data["author"]["id"], ALICE.to_string());
            assert_eq!(envelope.data["author"]["username"], "alice");
        }
    }

    #[test]
    fn pins_updates_carry_the_last_pin_timestamp() {
        let (publisher, _) = publisher();
        let kind = MessageEventKind::PinsUpdated {
            last_pin_timestamp_ms: Some(1_767_225_600_000),
        };
        let envelope = publisher
            .envelope(&event(Audience::Guild(GUILD), Some(GUILD), kind), &Authors::new())
            .unwrap();
        assert_eq!(envelope.data["last_pin_timestamp"], "2026-01-01T00:00:00Z");
    }
}

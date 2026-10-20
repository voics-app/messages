//! In-memory port implementations for use case tests.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use discordy_messages_domain::snowflake::DISCORDY_EPOCH_MS;
use discordy_messages_domain::{
    Bucket, ChannelAccess, ChannelId, DeliveryMode, Emoji, Message, MessageDraft, MessageId, Permissions, ReactionRow,
    ReadState, ScanDirection, SnowflakeGenerator, UploadKey, UserId, WorkerId,
};
use futures::StreamExt;

use crate::error::PortError;
use crate::history_reader::HistoryReader;
use crate::ports::*;
use crate::use_cases::MessagingPorts;

pub use discordy_messages_domain::Snowflake;

pub const CHANNEL: Snowflake = Snowflake::new(1 << 22);
pub const GUILD: Snowflake = Snowflake::new(500);
pub const ALICE: Snowflake = Snowflake::new(10);
pub const BOB: Snowflake = Snowflake::new(11);
/// 100 days after the Discordy epoch.
pub const START_MS: u64 = DISCORDY_EPOCH_MS + 100 * 86_400_000;

pub const MEMBER: u64 = Permissions::VIEW_CHANNEL
    | Permissions::SEND_MESSAGES
    | Permissions::READ_MESSAGE_HISTORY
    | Permissions::ADD_REACTIONS
    | Permissions::ATTACH_FILES;

#[derive(Default)]
pub struct InMemoryMessages {
    rows: Mutex<BTreeMap<(ChannelId, MessageId), Message>>,
    last: Mutex<HashMap<ChannelId, MessageId>>,
    pub scans: AtomicUsize,
}

#[async_trait]
impl MessageRepository for InMemoryMessages {
    async fn insert(&self, message: &Message) -> Result<(), PortError> {
        self.rows
            .lock()
            .unwrap()
            .insert((message.channel_id, message.id), message.clone());
        let mut last = self.last.lock().unwrap();
        let entry = last.entry(message.channel_id).or_insert(message.id);
        *entry = (*entry).max(message.id);
        Ok(())
    }

    async fn update(&self, message: &Message) -> Result<(), PortError> {
        self.rows
            .lock()
            .unwrap()
            .insert((message.channel_id, message.id), message.clone());
        Ok(())
    }

    async fn find(&self, channel_id: ChannelId, message_id: MessageId) -> Result<Option<Message>, PortError> {
        Ok(self.rows.lock().unwrap().get(&(channel_id, message_id)).cloned())
    }

    async fn delete(&self, channel_id: ChannelId, message_ids: &[MessageId]) -> Result<(), PortError> {
        let mut rows = self.rows.lock().unwrap();
        for id in message_ids {
            rows.remove(&(channel_id, *id));
        }
        Ok(())
    }

    async fn set_pinned(&self, channel_id: ChannelId, message_id: MessageId, pinned: bool) -> Result<(), PortError> {
        if let Some(message) = self.rows.lock().unwrap().get_mut(&(channel_id, message_id)) {
            message.pinned = pinned;
        }
        Ok(())
    }

    async fn scan_bucket(&self, query: BucketQuery) -> Result<Vec<Message>, PortError> {
        self.scans.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        let rows = self.rows.lock().unwrap();
        let in_bucket = rows
            .values()
            .filter(|m| m.channel_id == query.channel_id && Bucket::of(m.id) == query.bucket);
        let result: Vec<Message> = match query.direction {
            ScanDirection::Older => in_bucket
                .rev()
                .filter(|m| query.bound.is_none_or(|b| m.id < b))
                .take(query.limit)
                .cloned()
                .collect(),
            ScanDirection::Newer => in_bucket
                .filter(|m| query.bound.is_none_or(|b| m.id > b))
                .take(query.limit)
                .cloned()
                .collect(),
        };
        Ok(result)
    }

    async fn last_message_id(&self, channel_id: ChannelId) -> Result<Option<MessageId>, PortError> {
        Ok(self.last.lock().unwrap().get(&channel_id).copied())
    }
}

#[derive(Default)]
pub struct InMemoryReactions {
    rows: Mutex<Vec<(ChannelId, ReactionRow)>>,
}

#[async_trait]
impl ReactionRepository for InMemoryReactions {
    async fn add(
        &self,
        channel_id: ChannelId,
        message_id: MessageId,
        emoji: &Emoji,
        user_id: UserId,
    ) -> Result<(), PortError> {
        let row = ReactionRow {
            message_id,
            emoji: emoji.clone(),
            user_id,
        };
        let mut rows = self.rows.lock().unwrap();
        if !rows.iter().any(|(c, r)| *c == channel_id && *r == row) {
            rows.push((channel_id, row));
        }
        Ok(())
    }

    async fn remove(
        &self,
        channel_id: ChannelId,
        message_id: MessageId,
        emoji: &Emoji,
        user_id: UserId,
    ) -> Result<(), PortError> {
        self.rows.lock().unwrap().retain(|(c, r)| {
            !(*c == channel_id && r.message_id == message_id && r.emoji == *emoji && r.user_id == user_id)
        });
        Ok(())
    }

    async fn remove_all(&self, channel_id: ChannelId, message_id: MessageId) -> Result<(), PortError> {
        self.rows
            .lock()
            .unwrap()
            .retain(|(c, r)| !(*c == channel_id && r.message_id == message_id));
        Ok(())
    }

    async fn for_message(&self, channel_id: ChannelId, message_id: MessageId) -> Result<Vec<ReactionRow>, PortError> {
        Ok(self
            .rows
            .lock()
            .unwrap()
            .iter()
            .filter(|(c, r)| *c == channel_id && r.message_id == message_id)
            .map(|(_, r)| r.clone())
            .collect())
    }

    async fn for_range(
        &self,
        channel_id: ChannelId,
        bucket: Bucket,
        oldest: MessageId,
        newest: MessageId,
    ) -> Result<Vec<ReactionRow>, PortError> {
        Ok(self
            .rows
            .lock()
            .unwrap()
            .iter()
            .filter(|(c, r)| {
                *c == channel_id
                    && Bucket::of(r.message_id) == bucket
                    && r.message_id >= oldest
                    && r.message_id <= newest
            })
            .map(|(_, r)| r.clone())
            .collect())
    }
}

#[derive(Default)]
pub struct InMemoryPins {
    rows: Mutex<Vec<(ChannelId, Pin)>>,
}

#[async_trait]
impl PinRepository for InMemoryPins {
    async fn pin(&self, channel_id: ChannelId, pin: Pin) -> Result<(), PortError> {
        self.rows.lock().unwrap().push((channel_id, pin));
        Ok(())
    }

    async fn unpin(&self, channel_id: ChannelId, message_id: MessageId) -> Result<(), PortError> {
        self.rows
            .lock()
            .unwrap()
            .retain(|(c, p)| !(*c == channel_id && p.message_id == message_id));
        Ok(())
    }

    async fn list(&self, channel_id: ChannelId) -> Result<Vec<Pin>, PortError> {
        let mut pins: Vec<Pin> = self
            .rows
            .lock()
            .unwrap()
            .iter()
            .filter(|(c, _)| *c == channel_id)
            .map(|(_, p)| *p)
            .collect();
        pins.sort_by_key(|p| std::cmp::Reverse(p.pinned_at_ms));
        Ok(pins)
    }
}

#[derive(Default)]
pub struct InMemoryReadStates {
    rows: Mutex<HashMap<(UserId, ChannelId), ReadState>>,
}

#[async_trait]
impl ReadStateRepository for InMemoryReadStates {
    async fn find(&self, user_id: UserId, channel_id: ChannelId) -> Result<Option<ReadState>, PortError> {
        Ok(self.rows.lock().unwrap().get(&(user_id, channel_id)).cloned())
    }

    async fn save(&self, state: &ReadState) -> Result<(), PortError> {
        self.rows
            .lock()
            .unwrap()
            .insert((state.user_id, state.channel_id), state.clone());
        Ok(())
    }

    async fn for_user(&self, user_id: UserId) -> Result<Vec<ReadState>, PortError> {
        Ok(self
            .rows
            .lock()
            .unwrap()
            .values()
            .filter(|s| s.user_id == user_id)
            .cloned()
            .collect())
    }
}

#[derive(Default)]
pub struct InMemoryActivity {
    ids: Mutex<Vec<MessageId>>,
}

#[async_trait]
impl ActivityRepository for InMemoryActivity {
    async fn record_standard_message(&self, message_id: MessageId) -> Result<(), PortError> {
        self.ids.lock().unwrap().push(message_id);
        Ok(())
    }

    async fn count_standard_since(&self, since_unix_ms: u64) -> Result<u64, PortError> {
        Ok(self
            .ids
            .lock()
            .unwrap()
            .iter()
            .filter(|id| id.unix_millis() >= since_unix_ms)
            .count() as u64)
    }
}

pub struct FakeAccess {
    pub access: Mutex<Result<ChannelAccess, AccessError>>,
    /// The answer for user-less lookups (webhook posts).
    pub channel: Mutex<Result<ChannelAccess, AccessError>>,
    /// The uncached answer (`refresh`); falls back to `access` when unset.
    pub fresh: Mutex<Option<Result<ChannelAccess, AccessError>>>,
}

impl FakeAccess {
    pub fn set_permissions(&self, bits: u64) {
        if let Ok(access) = self.access.lock().unwrap().as_mut() {
            access.permissions = Permissions::new(bits);
        }
    }

    /// Sets both the member and the user-less answer.
    pub fn set(&self, access: Result<ChannelAccess, AccessError>) {
        *self.channel.lock().unwrap() = access.clone();
        *self.access.lock().unwrap() = access;
    }
}

#[async_trait]
impl ChannelAccessProvider for FakeAccess {
    async fn access(&self, _channel_id: ChannelId, _user_id: UserId) -> Result<ChannelAccess, AccessError> {
        self.access.lock().unwrap().clone()
    }

    async fn channel(&self, _channel_id: ChannelId) -> Result<ChannelAccess, AccessError> {
        self.channel.lock().unwrap().clone()
    }

    async fn refresh(&self, _channel_id: ChannelId, _user_id: UserId) -> Result<ChannelAccess, AccessError> {
        match self.fresh.lock().unwrap().clone() {
            Some(answer) => answer,
            None => self.access.lock().unwrap().clone(),
        }
    }
}

#[derive(Default)]
pub struct FakeStorage {
    pub objects: Mutex<HashMap<String, StoredObject>>,
    pub deleted: Mutex<Vec<String>>,
    /// Bytes served by `open`, keyed like `objects`.
    pub bodies: Mutex<HashMap<String, Vec<u8>>>,
}

impl FakeStorage {
    pub fn put_body(&self, key: &str, body: &[u8]) {
        self.put(key, body.len() as u64);
        self.bodies.lock().unwrap().insert(key.to_owned(), body.to_vec());
    }

    pub fn put(&self, key: &str, size: u64) {
        self.objects.lock().unwrap().insert(
            key.to_owned(),
            StoredObject {
                size,
                content_type: Some("image/png".into()),
            },
        );
    }
}

#[async_trait]
impl AttachmentStorage for FakeStorage {
    fn presign_upload(&self, key: &UploadKey) -> String {
        format!("https://upload.test/{}?signed", key.as_object_key())
    }

    async fn stat(&self, key: &UploadKey) -> Result<Option<StoredObject>, PortError> {
        Ok(self.objects.lock().unwrap().get(&key.as_object_key()).cloned())
    }

    async fn open(&self, key: &UploadKey, range: Option<&str>) -> Result<Option<ObjectRead>, PortError> {
        let name = key.as_object_key();
        let Some(body) = self.bodies.lock().unwrap().get(&name).cloned() else {
            return Ok(None);
        };
        let total = body.len() as u64;
        // Only `bytes=<start>-` is understood by the fake.
        let start = range
            .and_then(|r| r.strip_prefix("bytes="))
            .and_then(|r| r.strip_suffix('-'))
            .and_then(|r| r.parse::<usize>().ok());
        let (status, slice, content_range) = match start {
            Some(start) if start < body.len() => (
                206,
                body[start..].to_vec(),
                Some(format!("bytes {start}-{}/{total}", total - 1)),
            ),
            _ => (200, body, None),
        };
        Ok(Some(ObjectRead {
            status,
            content_type: Some("image/png".into()),
            content_length: Some(slice.len() as u64),
            content_range,
            etag: None,
            body: futures::stream::once(async move { Ok(bytes::Bytes::from(slice)) }).boxed(),
        }))
    }

    async fn delete(&self, key: &UploadKey) -> Result<(), PortError> {
        self.deleted.lock().unwrap().push(key.as_object_key());
        Ok(())
    }
}

#[derive(Default)]
pub struct RecordingEvents {
    pub events: Mutex<Vec<MessageEvent>>,
}

impl RecordingEvents {
    pub fn kinds(&self) -> Vec<MessageEventKind> {
        self.events.lock().unwrap().iter().map(|e| e.kind.clone()).collect()
    }
}

#[async_trait]
impl EventPublisher for RecordingEvents {
    async fn publish(&self, event: MessageEvent) -> Result<(), PortError> {
        self.events.lock().unwrap().push(event);
        Ok(())
    }
}

pub struct FakeClock(pub AtomicU64);

impl FakeClock {
    pub fn advance_ms(&self, ms: u64) {
        self.0.fetch_add(ms, Ordering::SeqCst);
    }
}

impl Clock for FakeClock {
    fn now_unix_ms(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}

pub struct FakeIds {
    clock: Arc<FakeClock>,
    generator: Mutex<SnowflakeGenerator>,
}

impl IdGenerator for FakeIds {
    fn next_id(&self) -> Snowflake {
        self.generator.lock().unwrap().next_id(self.clock.now_unix_ms())
    }
}

/// A user directory backed by a map, counting lookups and able to go down.
#[derive(Default)]
pub struct FakeUsers {
    profiles: Mutex<HashMap<UserId, UserProfile>>,
    calls: AtomicUsize,
    down: std::sync::atomic::AtomicBool,
}

impl FakeUsers {
    pub fn add(&self, id: UserId, username: &str) {
        let profile = UserProfile {
            id,
            username: username.to_owned(),
            global_name: None,
            avatar: None,
            bot: false,
            system: false,
            flags: 0,
        };
        self.profiles.lock().unwrap().insert(id, profile);
    }

    pub fn set_down(&self, down: bool) {
        self.down.store(down, Ordering::SeqCst);
    }

    /// How many batch lookups were made.
    pub fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl UserDirectory for FakeUsers {
    async fn lookup(&self, ids: &[UserId]) -> HashMap<UserId, UserProfile> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.down.load(Ordering::SeqCst) {
            return HashMap::new();
        }
        let profiles = self.profiles.lock().unwrap();
        ids.iter()
            .filter_map(|id| profiles.get(id).map(|profile| (*id, profile.clone())))
            .collect()
    }
}

/// All fakes, wired into a [`MessagingPorts`].
pub struct Harness {
    pub access: Arc<FakeAccess>,
    pub messages: Arc<InMemoryMessages>,
    pub reactions: Arc<InMemoryReactions>,
    pub pins: Arc<InMemoryPins>,
    pub read_states: Arc<InMemoryReadStates>,
    pub storage: Arc<FakeStorage>,
    pub events: Arc<RecordingEvents>,
    pub clock: Arc<FakeClock>,
    pub users: Arc<FakeUsers>,
    pub ports: MessagingPorts,
}

pub fn guild_access(bits: u64) -> ChannelAccess {
    ChannelAccess {
        permissions: Permissions::new(bits),
        guild_id: Some(GUILD),
        channel_type: 0,
        delivery_mode: DeliveryMode::Standard,
        recipient_ids: vec![],
    }
}

pub fn dm_access(mode: DeliveryMode) -> ChannelAccess {
    ChannelAccess {
        permissions: Permissions::new(MEMBER),
        guild_id: None,
        channel_type: 1,
        delivery_mode: mode,
        recipient_ids: vec![ALICE, BOB],
    }
}

impl Harness {
    pub fn new() -> Self {
        let access = Arc::new(FakeAccess {
            access: Mutex::new(Ok(guild_access(MEMBER))),
            channel: Mutex::new(Ok(guild_access(MEMBER))),
            fresh: Mutex::new(None),
        });
        let messages = Arc::new(InMemoryMessages::default());
        let reactions = Arc::new(InMemoryReactions::default());
        let pins = Arc::new(InMemoryPins::default());
        let read_states = Arc::new(InMemoryReadStates::default());
        let storage = Arc::new(FakeStorage::default());
        let events = Arc::new(RecordingEvents::default());
        let clock = Arc::new(FakeClock(AtomicU64::new(START_MS)));
        let ids = Arc::new(FakeIds {
            clock: Arc::clone(&clock),
            generator: Mutex::new(SnowflakeGenerator::new(WorkerId::for_messages(64).unwrap())),
        });
        let users = Arc::new(FakeUsers::default());
        users.add(ALICE, "alice");
        users.add(BOB, "bob");
        let ports = MessagingPorts {
            access: access.clone(),
            messages: messages.clone(),
            reactions: reactions.clone(),
            pins: pins.clone(),
            read_states: read_states.clone(),
            activity: Arc::new(InMemoryActivity::default()),
            storage: storage.clone(),
            events: events.clone(),
            ids,
            clock: clock.clone(),
            history: Arc::new(HistoryReader::new(messages.clone(), reactions.clone())),
            users: users.clone(),
        };
        Self {
            access,
            messages,
            reactions,
            pins,
            read_states,
            storage,
            events,
            clock,
            users,
            ports,
        }
    }

    /// Stores a message written by `author` at the current fake time.
    pub async fn seed(&self, author: UserId, content: &str) -> Message {
        let draft = MessageDraft {
            content: content.to_owned(),
            ..MessageDraft::default()
        };
        let message = Message::compose(self.ports.ids.next_id(), CHANNEL, author, draft, false).unwrap();
        self.messages.insert(&message).await.unwrap();
        message
    }
}

impl Default for Harness {
    fn default() -> Self {
        Self::new()
    }
}

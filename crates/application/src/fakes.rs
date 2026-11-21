//! In-memory port implementations for use case tests.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use discordy_messages_domain::snowflake::DISCORDY_EPOCH_MS;
use discordy_messages_domain::{
    Bucket, ChannelAccess, ChannelId, DeliveryMode, Emoji, GuildId, IndexedMessage, Message, MessageDraft, MessageId,
    Permissions, ReactionRow, ReadState, ScanDirection, SnowflakeGenerator, SortOrder, UploadKey, UserId, WorkerId,
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
/// 100 days after the Voics epoch.
pub const START_MS: u64 = DISCORDY_EPOCH_MS + 100 * 86_400_000;

pub const MEMBER: u64 = Permissions::VIEW_CHANNEL
    | Permissions::SEND_MESSAGES
    | Permissions::READ_MESSAGE_HISTORY
    | Permissions::ADD_REACTIONS
    | Permissions::ATTACH_FILES;

#[derive(Default)]
pub struct InMemoryMessages {
    rows: Mutex<BTreeMap<(ChannelId, MessageId), Message>>,
    guild_activity: Mutex<BTreeSet<(GuildId, i64, MessageId)>>,
    last: Mutex<HashMap<ChannelId, MessageId>>,
    pub scans: AtomicUsize,
}

#[async_trait]
impl MessageRepository for InMemoryMessages {
    async fn guild_activity_source_initialized_ms(&self) -> Result<Option<u64>, PortError> {
        Ok(None)
    }
    async fn guild_message_count(&self, guild_id: GuildId, utc_day: i64) -> Result<u64, PortError> {
        Ok(self
            .guild_activity
            .lock()
            .unwrap()
            .iter()
            .filter(|(guild, day, _)| *guild == guild_id && *day == utc_day)
            .count() as u64)
    }
    async fn insert_scoped(
        &self,
        message: &Message,
        guild_id: Option<discordy_messages_domain::GuildId>,
    ) -> Result<(), PortError> {
        self.insert(message).await?;
        if let Some(guild_id) = guild_id {
            self.guild_activity.lock().unwrap().insert((
                guild_id,
                (message.id.unix_millis() / 86_400_000) as i64,
                message.id,
            ));
        }
        Ok(())
    }
    async fn mark_crossposted(&self, channel_id: ChannelId, message_id: MessageId) -> Result<bool, PortError> {
        if let Some(message) = self.rows.lock().unwrap().get_mut(&(channel_id, message_id)) {
            message.flags |= discordy_messages_domain::FLAG_CROSSPOSTED;
            Ok(true)
        } else {
            Ok(false)
        }
    }
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

    async fn insert_forum_once(&self, marker: &Message, message: &Message) -> Result<bool, PortError> {
        let mut rows = self.rows.lock().unwrap();
        if rows.contains_key(&(marker.channel_id, marker.id)) {
            return Ok(false);
        }
        rows.insert((marker.channel_id, marker.id), marker.clone());
        rows.insert((message.channel_id, message.id), message.clone());
        drop(rows);
        let mut last = self.last.lock().unwrap();
        let entry = last.entry(message.channel_id).or_insert(message.id);
        *entry = (*entry).max(message.id);
        Ok(true)
    }
    async fn forum_marker(&self, channel_id: ChannelId) -> Result<Option<(UserId, MessageId, String)>, PortError> {
        let rows = self.rows.lock().unwrap();
        Ok(rows
            .get(&(channel_id, channel_id))
            .filter(|m| m.flags == discordy_messages_domain::FORUM_MARKER_FLAG)
            .and_then(|m| m.reply_to.map(|id| (m.author_id, id, m.content.clone()))))
    }

    async fn update(&self, message: &Message) -> Result<(), PortError> {
        self.rows
            .lock()
            .unwrap()
            .insert((message.channel_id, message.id), message.clone());
        Ok(())
    }

    async fn find(&self, channel_id: ChannelId, message_id: MessageId) -> Result<Option<Message>, PortError> {
        Ok(self
            .rows
            .lock()
            .unwrap()
            .get(&(channel_id, message_id))
            .filter(|m| m.flags != discordy_messages_domain::FORUM_MARKER_FLAG)
            .cloned())
    }

    async fn delete(&self, channel_id: ChannelId, message_ids: &[MessageId]) -> Result<(), PortError> {
        let mut rows = self.rows.lock().unwrap();
        for id in message_ids {
            if *id != channel_id {
                rows.remove(&(channel_id, *id));
            }
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
        let in_bucket = rows.values().filter(|m| {
            m.channel_id == query.channel_id
                && Bucket::of(m.id) == query.bucket
                && m.flags != discordy_messages_domain::FORUM_MARKER_FLAG
        });
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

    async fn message_count(&self, channel_id: ChannelId) -> Result<u64, PortError> {
        Ok(self
            .rows
            .lock()
            .unwrap()
            .keys()
            .filter(|(stored_channel, _)| *stored_channel == channel_id)
            .count() as u64)
    }

    async fn channels_with_messages(&self) -> Result<Vec<(ChannelId, MessageId)>, PortError> {
        Ok(self.last.lock().unwrap().iter().map(|(c, m)| (*c, *m)).collect())
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
    rows: Mutex<HashMap<(UserId, ChannelId), ReadProgress>>,
}

struct ReadProgress {
    state: ReadState,
    acknowledged_through: Option<MessageId>,
    baseline: u32,
    seen: BTreeSet<MessageId>,
    active: BTreeSet<MessageId>,
}

impl ReadProgress {
    fn new(user_id: UserId, channel_id: ChannelId) -> Self {
        Self {
            state: ReadState::unread(user_id, channel_id),
            acknowledged_through: None,
            baseline: 0,
            seen: BTreeSet::new(),
            active: BTreeSet::new(),
        }
    }

    fn advance(&mut self) -> Result<(), PortError> {
        self.state.version = self
            .state
            .version
            .checked_add(1)
            .filter(|version| *version <= i64::MAX as u64)
            .ok_or_else(|| PortError::new("read-state version exhausted"))?;
        Ok(())
    }

    fn count(&mut self) {
        self.state.mention_count = self
            .baseline
            .saturating_add(u32::try_from(self.active.len()).unwrap_or(u32::MAX));
    }
}

#[async_trait]
impl ReadStateRepository for InMemoryReadStates {
    async fn find(&self, user_id: UserId, channel_id: ChannelId) -> Result<Option<ReadState>, PortError> {
        Ok(self
            .rows
            .lock()
            .unwrap()
            .get(&(user_id, channel_id))
            .map(|row| row.state.clone()))
    }

    async fn record_mention(
        &self,
        user: UserId,
        channel: ChannelId,
        message: MessageId,
    ) -> Result<ReadState, PortError> {
        let mut rows = self.rows.lock().unwrap();
        let row = rows
            .entry((user, channel))
            .or_insert_with(|| ReadProgress::new(user, channel));
        if row.acknowledged_through.is_none_or(|last| message > last) && !row.seen.contains(&message) {
            row.advance()?;
            row.seen.insert(message);
            row.active.insert(message);
            row.count();
        }
        Ok(row.state.clone())
    }

    async fn acknowledge(&self, user: UserId, channel: ChannelId, message: MessageId) -> Result<ReadState, PortError> {
        let mut rows = self.rows.lock().unwrap();
        let row = rows
            .entry((user, channel))
            .or_insert_with(|| ReadProgress::new(user, channel));
        if row.state.last_read_id.is_none_or(|last| message > last) {
            row.advance()?;
            row.state.last_read_id = Some(message);
            row.acknowledged_through = Some(row.acknowledged_through.map_or(message, |last| last.max(message)));
            row.baseline = 0;
            row.active.retain(|id| *id > message);
            row.seen.retain(|id| Some(*id) > row.acknowledged_through);
            row.count();
        }
        Ok(row.state.clone())
    }

    async fn set_position(
        &self,
        user: UserId,
        channel: ChannelId,
        message: MessageId,
        mentions: u32,
    ) -> Result<ReadState, PortError> {
        let mut rows = self.rows.lock().unwrap();
        let row = rows
            .entry((user, channel))
            .or_insert_with(|| ReadProgress::new(user, channel));
        row.advance()?;
        row.state.last_read_id = Some(message);
        row.acknowledged_through = Some(row.acknowledged_through.map_or(message, |last| last.max(message)));
        row.baseline = mentions;
        row.active.clear();
        row.seen.retain(|id| Some(*id) > row.acknowledged_through);
        row.count();
        Ok(row.state.clone())
    }

    async fn for_user(&self, user_id: UserId) -> Result<Vec<ReadState>, PortError> {
        Ok(self
            .rows
            .lock()
            .unwrap()
            .values()
            .filter(|row| row.state.user_id == user_id)
            .map(|row| row.state.clone())
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
    /// Channels answered with empty permissions.
    pub denied: Mutex<std::collections::HashSet<ChannelId>>,
}

impl FakeAccess {
    pub fn deny_channel(&self, channel_id: ChannelId) {
        self.denied.lock().unwrap().insert(channel_id);
    }

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
    async fn access(&self, channel_id: ChannelId, _user_id: UserId) -> Result<ChannelAccess, AccessError> {
        let mut answer = self.access.lock().unwrap().clone();
        if let (true, Ok(access)) = (self.denied.lock().unwrap().contains(&channel_id), answer.as_mut()) {
            access.permissions = Permissions::new(0);
        }
        answer
    }

    async fn channel(&self, _channel_id: ChannelId) -> Result<ChannelAccess, AccessError> {
        self.channel.lock().unwrap().clone()
    }

    async fn refresh(&self, channel_id: ChannelId, user_id: UserId) -> Result<ChannelAccess, AccessError> {
        let fresh = self.fresh.lock().unwrap().clone();
        match fresh {
            Some(answer) => answer,
            None => self.access(channel_id, user_id).await,
        }
    }

    async fn record_thread_activity(&self, _channel_id: ChannelId, _message_id: MessageId) -> Result<(), PortError> {
        Ok(())
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

    async fn copy(&self, from: &UploadKey, to: &UploadKey) -> Result<bool, PortError> {
        let source = from.as_object_key();
        let stored = self.objects.lock().unwrap().get(&source).cloned();
        let Some(stored) = stored else { return Ok(false) };
        let target = to.as_object_key();
        self.objects.lock().unwrap().insert(target.clone(), stored);
        let body = self.bodies.lock().unwrap().get(&source).cloned();
        if let Some(body) = body {
            self.bodies.lock().unwrap().insert(target, body);
        }
        Ok(true)
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
    fn next_id(&self) -> Result<Snowflake, PortError> {
        Ok(self.generator.lock().unwrap().next_id(self.clock.now_unix_ms()))
    }
    fn is_ready(&self) -> bool {
        true
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

/// An in-memory search index with the semantics of the SQL one: every word of
/// the text must occur (case-insensitive), all filters combine with AND.
pub struct FakeSearchIndex {
    rows: Mutex<BTreeMap<(ChannelId, MessageId), IndexedMessage>>,
    state: Mutex<BackfillState>,
}

impl Default for FakeSearchIndex {
    fn default() -> Self {
        Self {
            rows: Mutex::default(),
            state: Mutex::new(BackfillState::Complete),
        }
    }
}

impl FakeSearchIndex {
    pub fn index_message(&self, message: &Message, guild_id: Option<GuildId>) {
        if let Some(entry) = IndexedMessage::from_message(message, guild_id) {
            self.rows
                .lock()
                .unwrap()
                .insert((entry.channel_id, entry.message_id), entry);
        }
    }

    pub fn len(&self) -> usize {
        self.rows.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn set_state(&self, state: BackfillState) {
        *self.state.lock().unwrap() = state;
    }
}

#[async_trait]
impl SearchIndex for FakeSearchIndex {
    async fn apply(&self, ops: &[IndexOp], _checkpoint: Option<&Checkpoint>) -> Result<(), PortError> {
        let mut rows = self.rows.lock().unwrap();
        for op in ops {
            match op {
                IndexOp::Upsert(entry) => {
                    let key = (entry.channel_id, entry.message_id);
                    if rows.get(&key).is_none_or(|old| old.edited_at_ms <= entry.edited_at_ms) {
                        rows.insert(key, entry.clone());
                    }
                }
                IndexOp::Delete {
                    channel_id,
                    message_ids,
                } => {
                    for id in message_ids {
                        rows.remove(&(*channel_id, *id));
                    }
                }
            }
        }
        Ok(())
    }

    async fn checkpoint(&self, _topic: &str, _partition: i32) -> Result<Option<i64>, PortError> {
        Ok(None)
    }

    async fn guild_channels(&self, guild_id: GuildId, only: &[ChannelId]) -> Result<Vec<ChannelId>, PortError> {
        let mut channels: Vec<ChannelId> = self
            .rows
            .lock()
            .unwrap()
            .values()
            .filter(|e| e.guild_id == Some(guild_id) && (only.is_empty() || only.contains(&e.channel_id)))
            .map(|e| e.channel_id)
            .collect();
        channels.sort();
        channels.dedup();
        Ok(channels)
    }

    async fn search(&self, request: &SearchRequest) -> Result<SearchPage, PortError> {
        let f = &request.filters;
        let words: Vec<String> = f
            .content
            .iter()
            .flat_map(|c| c.split_whitespace().map(str::to_lowercase).collect::<Vec<_>>())
            .collect();
        let mut hits: Vec<SearchHit> = self
            .rows
            .lock()
            .unwrap()
            .values()
            .filter(|e| request.channels.contains(&e.channel_id))
            .filter(|e| words.iter().all(|w| e.content.to_lowercase().contains(w.as_str())))
            .filter(|e| f.author_ids.is_empty() || f.author_ids.contains(&e.author_id))
            .filter(|e| f.mentions.iter().all(|m| e.mentions.contains(m)))
            .filter(|e| e.has.covers(f.has))
            .filter(|e| f.min_id.is_none_or(|min| e.message_id > min))
            .filter(|e| f.max_id.is_none_or(|max| e.message_id < max))
            .filter(|e| match &request.pinned {
                None => true,
                Some(PinnedFilter::Only(ids)) => ids.contains(&e.message_id),
                Some(PinnedFilter::Except(ids)) => !ids.contains(&e.message_id),
            })
            .map(|e| SearchHit {
                channel_id: e.channel_id,
                message_id: e.message_id,
            })
            .collect();
        hits.sort_by_key(|h| h.message_id);
        if f.sort_order == SortOrder::Desc {
            hits.reverse();
        }
        let total = hits.len() as u64;
        let hits = hits
            .into_iter()
            .skip(f.offset as usize)
            .take(f.limit as usize)
            .collect();
        Ok(SearchPage { total, hits })
    }

    async fn backfill_state(&self) -> Result<BackfillState, PortError> {
        Ok(*self.state.lock().unwrap())
    }

    async fn set_backfill_state(&self, state: BackfillState) -> Result<(), PortError> {
        *self.state.lock().unwrap() = state;
        Ok(())
    }
}

/// A link unfurler answering from a map and counting fetches.
#[derive(Default)]
pub struct FakeUnfurler {
    embeds: Mutex<HashMap<String, String>>,
    calls: AtomicUsize,
    edit: Mutex<Option<(Arc<InMemoryMessages>, Message)>>,
}

impl FakeUnfurler {
    pub fn set(&self, url: &str, embed: &str) {
        self.embeds.lock().unwrap().insert(url.to_owned(), embed.to_owned());
    }

    pub fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    /// Rewrites the message's content while the next fetch is in flight.
    pub fn edit_during_fetch(&self, messages: Arc<InMemoryMessages>, message: Message) {
        *self.edit.lock().unwrap() = Some((messages, message));
    }
}

#[async_trait]
impl LinkUnfurler for FakeUnfurler {
    async fn unfurl(&self, url: &str) -> Option<String> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let pending = self.edit.lock().unwrap().take();
        if let Some((messages, mut message)) = pending {
            message.content = "edited meanwhile".into();
            messages.update(&message).await.unwrap();
        }
        self.embeds.lock().unwrap().get(url).cloned()
    }
}

/// A media proxy upstream answering from a map of remote URL to bytes.
#[derive(Default)]
pub struct FakeMedia {
    files: Mutex<HashMap<String, (String, Vec<u8>)>>,
    pub opened: Mutex<Vec<String>>,
}

impl FakeMedia {
    pub fn put(&self, url: &str, content_type: &str, body: &[u8]) {
        self.files
            .lock()
            .unwrap()
            .insert(url.to_owned(), (content_type.to_owned(), body.to_vec()));
    }
}

#[async_trait]
impl RemoteMediaFetcher for FakeMedia {
    async fn open(&self, url: &str, _range: Option<&str>) -> Result<RemoteMedia, MediaError> {
        self.opened.lock().unwrap().push(url.to_owned());
        if url.contains("169.254.") {
            return Err(MediaError::Forbidden);
        }
        let Some((content_type, body)) = self.files.lock().unwrap().get(url).cloned() else {
            return Err(MediaError::Unavailable("not found".into()));
        };
        Ok(RemoteMedia {
            status: 200,
            content_type,
            content_length: Some(body.len() as u64),
            content_range: None,
            body: futures::stream::once(async move { Ok(bytes::Bytes::from(body)) }).boxed(),
        })
    }
}

/// All fakes, wired into a [`MessagingPorts`].
pub struct FakeNotificationRecipients {
    pub recipients: Mutex<Vec<NotificationRecipient>>,
    pub error: Mutex<Option<PortError>>,
    pub calls: Mutex<Vec<RecipientQuery>>,
    pub hidden: Mutex<BTreeSet<UserId>>,
    access: Arc<FakeAccess>,
}

#[async_trait]
impl NotificationRecipients for FakeNotificationRecipients {
    async fn page(&self, query: RecipientQuery) -> Result<NotificationRecipientPage, PortError> {
        self.calls.lock().unwrap().push(query);
        if let Some(error) = self.error.lock().unwrap().clone() {
            return Err(error);
        }
        let access = self.access.channel.lock().unwrap().clone().unwrap();
        let mut candidates: Vec<_> = self
            .recipients
            .lock()
            .unwrap()
            .iter()
            .filter(|recipient| query.after.is_none_or(|after| recipient.user_id > after))
            .cloned()
            .collect();
        candidates.sort_by_key(|recipient| recipient.user_id);
        let more = candidates.len() > usize::from(query.limit);
        candidates.truncate(usize::from(query.limit));
        let next_after = more.then(|| candidates.last().unwrap().user_id);
        candidates.retain(|recipient| {
            recipient.user_id != query.author_id && !self.hidden.lock().unwrap().contains(&recipient.user_id)
        });
        if access.delivery_mode != DeliveryMode::Standard {
            candidates.clear();
        }
        Ok(NotificationRecipientPage {
            guild_id: access.guild_id,
            delivery_mode: access.delivery_mode,
            recipients: candidates,
            next_after: if access.delivery_mode == DeliveryMode::Standard {
                next_after
            } else {
                None
            },
        })
    }
}

pub fn notification_recipient(user_id: UserId) -> NotificationRecipient {
    NotificationRecipient {
        user_id,
        role_ids: vec![],
        online: false,
        level: discordy_messages_domain::NotificationLevel::OnlyMentions,
        muted: false,
        suppress_everyone: false,
        suppress_roles: false,
    }
}

pub struct Harness {
    pub access: Arc<FakeAccess>,
    pub messages: Arc<InMemoryMessages>,
    pub reactions: Arc<InMemoryReactions>,
    pub pins: Arc<InMemoryPins>,
    pub read_states: Arc<InMemoryReadStates>,
    pub notification_recipients: Arc<FakeNotificationRecipients>,
    pub storage: Arc<FakeStorage>,
    pub events: Arc<RecordingEvents>,
    pub clock: Arc<FakeClock>,
    pub users: Arc<FakeUsers>,
    pub search: Arc<FakeSearchIndex>,
    pub unfurler: Arc<FakeUnfurler>,
    pub media: Arc<FakeMedia>,
    pub ports: MessagingPorts,
}

pub fn guild_access(bits: u64) -> ChannelAccess {
    ChannelAccess {
        permissions: Permissions::new(bits),
        guild_id: Some(GUILD),
        channel_type: 0,
        thread_locked: false,
        delivery_mode: DeliveryMode::Standard,
        recipient_ids: vec![],
    }
}

pub fn dm_access(mode: DeliveryMode) -> ChannelAccess {
    ChannelAccess {
        permissions: Permissions::new(MEMBER),
        guild_id: None,
        channel_type: 1,
        thread_locked: false,
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
            denied: Mutex::default(),
        });
        let messages = Arc::new(InMemoryMessages::default());
        let reactions = Arc::new(InMemoryReactions::default());
        let pins = Arc::new(InMemoryPins::default());
        let read_states = Arc::new(InMemoryReadStates::default());
        let notification_recipients = Arc::new(FakeNotificationRecipients {
            recipients: Mutex::new(vec![notification_recipient(ALICE), notification_recipient(BOB)]),
            error: Mutex::new(None),
            calls: Mutex::new(Vec::new()),
            hidden: Mutex::default(),
            access: access.clone(),
        });
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
        let search = Arc::new(FakeSearchIndex::default());
        let unfurler = Arc::new(FakeUnfurler::default());
        let ports = MessagingPorts {
            access: access.clone(),
            messages: messages.clone(),
            reactions: reactions.clone(),
            pins: pins.clone(),
            read_states: read_states.clone(),
            notification_recipients: notification_recipients.clone(),
            activity: Arc::new(InMemoryActivity::default()),
            storage: storage.clone(),
            events: events.clone(),
            ids,
            clock: clock.clone(),
            history: Arc::new(HistoryReader::new(messages.clone(), reactions.clone())),
            users: users.clone(),
            search: search.clone(),
            unfurler: unfurler.clone(),
        };
        Self {
            access,
            messages,
            reactions,
            pins,
            read_states,
            notification_recipients,
            storage,
            events,
            clock,
            users,
            search,
            unfurler,
            media: Arc::new(FakeMedia::default()),
            ports,
        }
    }

    /// Stores a message written by `author` at the current fake time.
    pub async fn seed(&self, author: UserId, content: &str) -> Message {
        let draft = MessageDraft {
            content: content.to_owned(),
            ..MessageDraft::default()
        };
        let message = Message::compose(self.ports.ids.next_id().unwrap(), CHANNEL, author, draft, false).unwrap();
        self.messages.insert(&message).await.unwrap();
        message
    }
}

impl Default for Harness {
    fn default() -> Self {
        Self::new()
    }
}

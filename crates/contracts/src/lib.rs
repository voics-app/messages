//! Wire contracts of the messages service.
//!
//! These mirror the pinned interfaces of the master plan (P3, P4, P6, P9)
//! exactly. They live in this one crate so it can be swapped for the
//! generated `discordy-contracts` package once that is published. All ids are
//! decimal strings; field names follow Discord API v10.

use serde::{Deserialize, Serialize};
use serde_json::Value;

mod notification_recipients;
pub use notification_recipients::{NotificationRecipientDto, NotificationRecipientPageDto};

#[rustfmt::skip]
#[allow(clippy::all)]
#[path = "scheduled/types.rs"]
pub mod scheduled;

/// The event-bus topic this service produces to (P6).
pub const MESSAGE_TOPIC: &str = "discordy.message.v1";
/// The guild topic this service only consumes (bus-only BAN_MESSAGE_PURGE).
pub const GUILD_TOPIC: &str = "discordy.guild.v1";
/// Envelope schema version (P6).
pub const ENVELOPE_VERSION: u32 = 1;

/// Dispatch event names published on [`MESSAGE_TOPIC`].
pub mod event_type {
    pub const MESSAGE_CREATE: &str = "MESSAGE_CREATE";
    pub const MESSAGE_UPDATE: &str = "MESSAGE_UPDATE";
    pub const MESSAGE_DELETE: &str = "MESSAGE_DELETE";
    pub const MESSAGE_DELETE_BULK: &str = "MESSAGE_DELETE_BULK";
    pub const MESSAGE_REACTION_ADD: &str = "MESSAGE_REACTION_ADD";
    pub const MESSAGE_REACTION_REMOVE: &str = "MESSAGE_REACTION_REMOVE";
    pub const MESSAGE_REACTION_REMOVE_ALL: &str = "MESSAGE_REACTION_REMOVE_ALL";
    pub const CHANNEL_PINS_UPDATE: &str = "CHANNEL_PINS_UPDATE";
    pub const MESSAGE_ACK: &str = "MESSAGE_ACK";
    pub const READ_STATE_UPDATE: &str = "READ_STATE_UPDATE";
    /// Consumed from [`super::GUILD_TOPIC`]: purge a banned user's recent messages.
    pub const BAN_MESSAGE_PURGE: &str = "BAN_MESSAGE_PURGE";
}

/// Discord-style error codes (P9).
pub mod error_code {
    pub const UNKNOWN_CHANNEL: u32 = 10003;
    pub const UNKNOWN_MESSAGE: u32 = 10008;
    pub const MAX_PINS: u32 = 30003;
    pub const UNAUTHORIZED: u32 = 40001;
    pub const MISSING_ACCESS: u32 = 50001;
    pub const MISSING_PERMISSIONS: u32 = 50013;
    pub const INVALID_FORM_BODY: u32 = 50035;
    pub const DELIVERY_MODE_CONFLICT: u32 = 50160;
    pub const SERVICE_UNAVAILABLE: u32 = 0;
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ErrorBody {
    pub code: u32,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub errors: Option<Value>,
}

// ---------------------------------------------------------------------------
// Public REST objects (P9)
// ---------------------------------------------------------------------------

/// A partial Discord User, used for mentions: clients resolve the rest from
/// their user cache.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserRef {
    pub id: String,
}

/// A message author. `id` is always present; the profile fields are filled
/// in from the api service and left out when it could not be reached.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthorDto {
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub username: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub discriminator: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub global_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub avatar: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub bot: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub system: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub public_flags: Option<u64>,
}

/// Request of the api service's `POST /internal/v1/users/lookup` (P4).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UsersLookupRequest {
    pub ids: Vec<String>,
}

/// One user in the lookup answer: the public fields only.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct LookedUpUser {
    pub id: String,
    pub username: String,
    #[serde(default)]
    pub global_name: Option<String>,
    #[serde(default)]
    pub avatar: Option<String>,
    #[serde(default)]
    pub bot: bool,
    #[serde(default)]
    pub system: bool,
    #[serde(default)]
    pub flags: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StickerItemDto {
    pub id: String,
    pub name: String,
    pub format_type: u8,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MessageDto {
    pub id: String,
    pub channel_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub guild_id: Option<String>,
    pub author: AuthorDto,
    pub content: String,
    pub timestamp: String,
    pub edited_timestamp: Option<String>,
    pub tts: bool,
    pub mention_everyone: bool,
    /// Extension: with `mention_everyone`, set when the message is `@here`
    /// (notify only members who are online at send time).
    #[serde(skip_serializing_if = "std::ops::Not::not", default)]
    pub mention_here: bool,
    pub mentions: Vec<UserRef>,
    pub mention_roles: Vec<String>,
    pub attachments: Vec<AttachmentDto>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sticker_items: Vec<StickerItemDto>,
    pub embeds: Vec<Value>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub reactions: Vec<ReactionDto>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nonce: Option<String>,
    pub pinned: bool,
    #[serde(rename = "type")]
    pub kind: u8,
    pub flags: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message_reference: Option<MessageReferenceDto>,
    /// Call messages (type 3): who took part and when the call ended.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub call: Option<CallDto>,
    pub components: Vec<Value>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub webhook_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub application_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub interaction_metadata: Option<InteractionMetadataDto>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub author_override: Option<AuthorOverrideDto>,
    /// Search results only: true for the match, absent for its context.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub hit: Option<bool>,
    /// Forwarded messages: the frozen copy of the original.
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub message_snapshots: Vec<MessageSnapshotDto>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MessageSnapshotDto {
    pub message: SnapshotMessageDto,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SnapshotMessageDto {
    pub content: String,
    pub timestamp: String,
    pub edited_timestamp: Option<String>,
    pub mentions: Vec<UserRef>,
    pub mention_roles: Vec<String>,
    pub attachments: Vec<AttachmentDto>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sticker_items: Vec<StickerItemDto>,
    pub embeds: Vec<Value>,
    #[serde(rename = "type")]
    pub kind: u8,
    pub flags: u32,
}

/// `GET /guilds/{id}/messages/search` and `GET /channels/{id}/messages/search`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MessageSearchResponse {
    pub total_results: u64,
    /// One group per hit, oldest message first; the hit has `hit: true`.
    pub messages: Vec<Vec<MessageDto>>,
    #[serde(skip_serializing_if = "std::ops::Not::not", default)]
    pub index_building: bool,
}

/// The interaction a message answers (Discord's `interaction_metadata`, id only).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InteractionMetadataDto {
    pub id: String,
}

/// Webhook display overrides applied to a message's author.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthorOverrideDto {
    pub username: Option<String>,
    pub avatar_url: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageReferenceDto {
    /// 1 for a forward; omitted for replies (DEFAULT).
    #[serde(rename = "type", skip_serializing_if = "Option::is_none", default)]
    pub kind: Option<u8>,
    /// Absent on thread created messages (type 18), whose `channel_id` is the thread.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub message_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub guild_id: Option<String>,
}

/// The `call` object of a type 3 message. `ended_timestamp` is null while the
/// call is running.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CallDto {
    pub participants: Vec<String>,
    #[serde(default)]
    pub ended_timestamp: Option<String>,
}

/// messages: `PATCH /internal/v1/channels/{channel_id}/messages/{message_id}/call`.
/// Records the end of a call; omitted fields stay unchanged.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpdateCallRequest {
    #[serde(default)]
    pub participants: Option<Vec<String>>,
    #[serde(default)]
    pub ended_timestamp: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttachmentDto {
    pub id: String,
    pub filename: String,
    pub size: u64,
    pub url: String,
    pub proxy_url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub width: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub height: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub description: Option<String>,
    /// `AttachmentFlags` bit set; explicit zero overrides a SPOILER_ filename.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub flags: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EmojiDto {
    pub id: Option<String>,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReactionDto {
    pub count: u32,
    pub me: bool,
    pub emoji: EmojiDto,
}

/// `POST /channels/{id}/messages`. `nonce` may be a string or an integer.
#[derive(Debug, Clone, Default, PartialEq, Deserialize, Serialize)]
pub struct CreateMessageRequest {
    #[serde(default)]
    pub content: Option<String>,
    #[serde(default)]
    pub tts: bool,
    #[serde(default)]
    pub nonce: Option<Value>,
    #[serde(default)]
    pub attachments: Option<Vec<AttachmentReferenceDto>>,
    #[serde(default)]
    pub sticker_ids: Option<Vec<String>>,
    #[serde(default)]
    pub message_reference: Option<MessageReferenceRequest>,
    #[serde(default)]
    pub embeds: Option<Vec<Value>>,
    #[serde(default)]
    pub components: Option<Vec<Value>>,
    /// Only SUPPRESS_NOTIFICATIONS (4096, `@silent`) is honoured; other bits
    /// are dropped.
    #[serde(default)]
    pub flags: Option<u32>,
    #[serde(default)]
    pub allowed_mentions: Option<AllowedMentionsDto>,
}

/// `allowed_mentions` of a create request. Only `replied_user` is honoured;
/// `parse`, `roles` and `users` are accepted and ignored.
#[derive(Debug, Clone, Default, PartialEq, Deserialize, Serialize)]
pub struct AllowedMentionsDto {
    #[serde(default)]
    pub parse: Option<Vec<String>>,
    #[serde(default)]
    pub roles: Option<Vec<String>>,
    #[serde(default)]
    pub users: Option<Vec<String>>,
    /// `false` replies without pinging the replied-to author (default true).
    #[serde(default)]
    pub replied_user: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct AttachmentReferenceDto {
    #[serde(default)]
    pub id: Option<Value>,
    #[serde(default)]
    pub filename: Option<String>,
    pub uploaded_filename: String,
    /// Alt text.
    #[serde(default)]
    pub description: Option<String>,
    /// `AttachmentFlags` bits; only IS_SPOILER (8) is kept.
    #[serde(default)]
    pub flags: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct MessageReferenceRequest {
    /// 0 reply (default), 1 forward.
    #[serde(rename = "type", default)]
    pub kind: Option<u8>,
    pub message_id: String,
    /// Required for forwards: the channel the message is forwarded from.
    #[serde(default)]
    pub channel_id: Option<String>,
    #[serde(default)]
    pub guild_id: Option<String>,
}

/// `PATCH /channels/{id}/messages/{mid}`.
#[derive(Debug, Clone, Default, PartialEq, Deserialize, Serialize)]
pub struct EditMessageRequest {
    #[serde(default)]
    pub content: Option<String>,
    #[serde(default)]
    pub embeds: Option<Vec<Value>>,
    /// Only SUPPRESS_EMBEDS (4) is honoured.
    #[serde(default)]
    pub flags: Option<u32>,
    #[serde(default)]
    pub attachments: Option<Vec<AttachmentMetadataEditDto>>,
}

/// Partial metadata changes for one existing attachment. Files cannot be replaced here.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AttachmentMetadataEditDto {
    pub id: String,
    /// Missing retains alt text; null clears it.
    #[serde(
        default,
        deserialize_with = "present_description",
        skip_serializing_if = "Option::is_none"
    )]
    pub description: Option<Option<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub flags: Option<u32>,
}

fn present_description<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<Option<Option<String>>, D::Error> {
    Option::<String>::deserialize(deserializer).map(Some)
}

/// `POST /channels/{id}/messages/bulk-delete`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct BulkDeleteRequest {
    pub messages: Vec<String>,
}

/// `POST /channels/{id}/attachments/refresh-urls`: object keys or earlier CDN
/// URLs of this channel's attachments, to be re-signed.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct RefreshUrlsRequest {
    pub attachment_urls: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RefreshUrlsResponse {
    pub refreshed_urls: Vec<RefreshedUrlDto>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RefreshedUrlDto {
    pub original: String,
    pub refreshed: String,
}

/// `POST /channels/{id}/attachments`.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct UploadRequestBody {
    pub files: Vec<UploadFileDto>,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct UploadFileDto {
    /// Client correlation id (string or integer), echoed back.
    #[serde(default)]
    pub id: Option<Value>,
    pub filename: String,
    pub file_size: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UploadSlotsResponse {
    pub attachments: Vec<UploadSlotDto>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UploadSlotDto {
    pub id: String,
    pub upload_url: String,
    pub upload_filename: String,
}

/// `POST /channels/{id}/messages/{mid}/ack` response (Discord returns a null token).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AckResponse {
    pub token: Option<String>,
}

// ---------------------------------------------------------------------------
// Internal HTTP (P3, P4)
// ---------------------------------------------------------------------------

/// api: `GET /internal/v1/channels/{channel_id}/access/{user_id}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChannelAccessResponse {
    pub permissions: String,
    pub guild_id: Option<String>,
    pub channel_type: u8,
    pub delivery_mode: String,
    #[serde(default)]
    pub recipient_ids: Vec<String>,
}

/// api: `GET /internal/v1/channels/{channel_id}/delivery`. The channel's guild
/// and delivery mode without a user, for webhook posts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChannelRoutingResponse {
    pub guild_id: Option<String>,
    pub channel_type: u8,
    pub delivery_mode: String,
    #[serde(default)]
    pub recipient_ids: Vec<String>,
}

/// api: `POST /internal/v1/channels/{channel_id}/messages`. Bot, webhook and
/// interaction messages. `author_id` is the bot user the message is posted as,
/// or the webhook id for webhook posts. `flags & 64` (ephemeral) messages go
/// only to `target_user_ids` and are never stored.
#[derive(Debug, Clone, Default, PartialEq, Deserialize, Serialize)]
pub struct InternalCreateMessageRequest {
    pub author_id: String,
    #[serde(default)]
    pub tts: bool,
    #[serde(default)]
    pub webhook_id: Option<String>,
    #[serde(default)]
    pub username: Option<String>,
    #[serde(default)]
    pub avatar_url: Option<String>,
    #[serde(default)]
    pub interaction_id: Option<String>,
    #[serde(default)]
    pub application_id: Option<String>,
    #[serde(default)]
    pub content: Option<String>,
    #[serde(default)]
    pub embeds: Option<Vec<Value>>,
    #[serde(default)]
    pub components: Option<Vec<Value>>,
    #[serde(default)]
    pub flags: Option<u32>,
    #[serde(default)]
    pub message_reference: Option<MessageReferenceRequest>,
    #[serde(default)]
    pub attachments: Option<Vec<AttachmentReferenceDto>>,
    /// Recipients of an ephemeral message (the invoking user).
    #[serde(default)]
    pub target_user_ids: Option<Vec<String>>,
    /// System message type: 1 RECIPIENT_ADD, 2 RECIPIENT_REMOVE, 3 CALL,
    /// 4 CHANNEL_NAME_CHANGE, 5 CHANNEL_ICON_CHANGE, 6 CHANNEL_PINNED_MESSAGE
    /// (`message_reference` is the pinned message), 7 GUILD_MEMBER_JOIN
    /// (`author_id` is the member who joined) and 18 THREAD_CREATED
    /// (`thread_id`, `content` is the thread name).
    #[serde(default, rename = "type")]
    pub message_type: Option<u8>,
    /// The user a type 1/2 system message is about.
    #[serde(default)]
    pub target_user_id: Option<String>,
    /// The call a type 3 message reports (`author_id` is the caller).
    #[serde(default)]
    pub call: Option<CallDto>,
    /// The thread a type 18 message announces.
    #[serde(default)]
    pub thread_id: Option<String>,
}

/// api: `POST /internal/v1/auth/introspect`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IntrospectRequest {
    pub token: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IntrospectResponse {
    pub user_id: String,
    pub kind: String,
    pub application_id: Option<String>,
    pub session_id: Option<String>,
}

/// messages: `GET /internal/v1/channels/{id}/last-message-id`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LastMessageIdResponse {
    pub last_message_id: Option<String>,
}

/// messages: `GET /internal/v1/admin/stats`. Counts only.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdminStatsResponse {
    pub messages_24h: u64,
    pub messages_by_mode: MessagesByMode,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessagesByMode {
    pub standard: u64,
    pub ephemeral_relayed: u64,
    pub p2p_signaled: u64,
}

/// messages: `GET /internal/v1/users/{user_id}/read-states` entry, in the
/// shape of Discord's READY `read_state` entries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadStateDto {
    pub id: String,
    pub last_message_id: Option<String>,
    pub mention_count: u32,
    pub read_state_version: String,
}

// ---------------------------------------------------------------------------
// Event bus (P6)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Envelope {
    pub id: String,
    #[serde(rename = "type")]
    pub kind: String,
    pub v: u32,
    pub occurred_at: String,
    pub guild_id: Option<String>,
    pub channel_id: Option<String>,
    pub target_user_ids: Option<Vec<String>>,
    pub data: Value,
}

/// Payload of BAN_MESSAGE_PURGE on the guild topic.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BanMessagePurgeData {
    pub guild_id: String,
    pub user_id: String,
    pub channel_ids: Vec<String>,
    /// Unix milliseconds: messages created at or after this are deleted.
    pub since_ms: u64,
    pub delete_message_seconds: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageDeleteData {
    pub id: String,
    pub channel_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub guild_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageDeleteBulkData {
    pub ids: Vec<String>,
    pub channel_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub guild_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageReactionData {
    pub user_id: String,
    pub channel_id: String,
    pub message_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub guild_id: Option<String>,
    pub emoji: EmojiDto,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageReactionRemoveAllData {
    pub channel_id: String,
    pub message_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub guild_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChannelPinsUpdateData {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub guild_id: Option<String>,
    pub channel_id: String,
    pub last_pin_timestamp: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageAckData {
    pub channel_id: String,
    pub message_id: String,
    pub mention_count: u32,
    pub read_state_version: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadStateUpdateData {
    pub channel_id: String,
    pub last_message_id: Option<String>,
    pub mention_count: u32,
    pub read_state_version: String,
}

/// Optional body of `POST /channels/{id}/messages/{mid}/ack`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, Serialize)]
pub struct AckRequest {
    /// Set the marker to exactly this message, even backwards (Mark Unread).
    #[serde(default)]
    pub manual: bool,
    /// Unread mentions to keep after a manual ack.
    #[serde(default)]
    pub mention_count: u32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn envelope_uses_type_field_name() {
        let envelope = Envelope {
            id: "id".into(),
            kind: event_type::MESSAGE_CREATE.into(),
            v: ENVELOPE_VERSION,
            occurred_at: "2026-10-16T00:00:00Z".into(),
            guild_id: None,
            channel_id: Some("1".into()),
            target_user_ids: Some(vec!["2".into()]),
            data: Value::Null,
        };
        let json = serde_json::to_value(&envelope).unwrap();
        assert_eq!(json["type"], "MESSAGE_CREATE");
        assert_eq!(json["v"], 1);
        assert!(json["guild_id"].is_null());
    }

    #[test]
    fn access_response_parses_the_pinned_shape() {
        let raw = r#"{"permissions":"3072","guild_id":null,"channel_type":1,
                      "delivery_mode":"standard","recipient_ids":["1","2"]}"#;
        let parsed: ChannelAccessResponse = serde_json::from_str(raw).unwrap();
        assert_eq!(parsed.permissions, "3072");
        assert_eq!(parsed.recipient_ids, vec!["1", "2"]);
    }
}

//! Wire contracts of the messages service.
//!
//! These mirror the pinned interfaces of the master plan (P3, P4, P6, P9)
//! exactly. They live in this one crate so it can be swapped for the
//! generated `discordy-contracts` package once that is published. All ids are
//! decimal strings; field names follow Discord API v10.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// The event-bus topic this service produces to (P6).
pub const MESSAGE_TOPIC: &str = "discordy.message.v1";
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
    pub mentions: Vec<UserRef>,
    pub mention_roles: Vec<String>,
    pub attachments: Vec<AttachmentDto>,
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
    pub components: Vec<Value>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub webhook_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub application_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub interaction_metadata: Option<InteractionMetadataDto>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub author_override: Option<AuthorOverrideDto>,
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
    pub message_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub guild_id: Option<String>,
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
    pub nonce: Option<Value>,
    #[serde(default)]
    pub attachments: Option<Vec<AttachmentReferenceDto>>,
    #[serde(default)]
    pub message_reference: Option<MessageReferenceRequest>,
    #[serde(default)]
    pub embeds: Option<Vec<Value>>,
    #[serde(default)]
    pub components: Option<Vec<Value>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct AttachmentReferenceDto {
    #[serde(default)]
    pub id: Option<Value>,
    #[serde(default)]
    pub filename: Option<String>,
    pub uploaded_filename: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct MessageReferenceRequest {
    pub message_id: String,
}

/// `PATCH /channels/{id}/messages/{mid}`.
#[derive(Debug, Clone, Default, PartialEq, Deserialize, Serialize)]
pub struct EditMessageRequest {
    #[serde(default)]
    pub content: Option<String>,
    #[serde(default)]
    pub embeds: Option<Vec<Value>>,
}

/// `POST /channels/{id}/messages/bulk-delete`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct BulkDeleteRequest {
    pub messages: Vec<String>,
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

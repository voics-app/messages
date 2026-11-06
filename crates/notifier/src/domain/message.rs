//! The part of a MESSAGE_CREATE event the notifier needs.

use contracts::MessageDto;

/// `MessageFlag.SUPPRESS_NOTIFICATIONS` (`@silent`).
const FLAG_SUPPRESS_NOTIFICATIONS: u32 = 1 << 12;
/// `MessageFlag.EPHEMERAL`: delivered to named users only and never stored.
const FLAG_EPHEMERAL: u32 = 1 << 6;
/// Plain messages and replies are the user-written kinds worth a push.
const KIND_DEFAULT: u8 = 0;
const KIND_REPLY: u8 = 19;
/// A system message that reports a DM call; `author_id` is the caller.
const KIND_CALL: u8 = 3;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CallInfo {
    pub participants: Vec<String>,
    pub ended: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IncomingMessage {
    pub id: String,
    pub channel_id: String,
    pub guild_id: Option<String>,
    pub author_id: String,
    pub author_name: String,
    pub content: String,
    pub attachment_count: usize,
    pub mentions: Vec<String>,
    pub mention_roles: Vec<String>,
    pub mention_everyone: bool,
    pub mention_here: bool,
    /// Who is in the call a type 3 message reports, and whether it is over already.
    pub call: Option<CallInfo>,
    flags: u32,
    kind: u8,
}

impl IncomingMessage {
    pub fn from_dto(dto: MessageDto) -> Self {
        let author_name = dto
            .author
            .global_name
            .filter(|name| !name.is_empty())
            .or(dto.author.username)
            .unwrap_or_else(|| "Someone".to_owned());
        Self {
            id: dto.id,
            channel_id: dto.channel_id,
            guild_id: dto.guild_id,
            author_id: dto.author.id,
            author_name,
            content: dto.content,
            attachment_count: dto.attachments.len(),
            mentions: dto.mentions.into_iter().map(|user| user.id).collect(),
            mention_roles: dto.mention_roles,
            mention_everyone: dto.mention_everyone,
            mention_here: dto.mention_here,
            call: dto.call.map(|call| CallInfo {
                participants: call.participants,
                ended: call.ended_timestamp.is_some(),
            }),
            flags: dto.flags,
            kind: dto.kind,
        }
    }

    /// Whether this message may produce a push at all: written by a user, not `@silent`, and not
    /// an ephemeral reply addressed to named users.
    pub fn is_pushable(&self) -> bool {
        matches!(self.kind, KIND_DEFAULT | KIND_REPLY)
            && self.flags & (FLAG_SUPPRESS_NOTIFICATIONS | FLAG_EPHEMERAL) == 0
    }

    /// A call that has just started in a DM: the system message of a call that is still running.
    /// It is the one event that rings a closed app (a PushKit push); it is never an alert.
    pub fn is_call_ring(&self) -> bool {
        self.kind == KIND_CALL
            && self.flags & FLAG_EPHEMERAL == 0
            && self.guild_id.is_none()
            && self.call.as_ref().is_some_and(|call| !call.ended)
    }

    pub fn is_direct(&self) -> bool {
        self.guild_id.is_none()
    }
}

#[cfg(test)]
pub(crate) mod fixtures {
    use super::*;
    use serde_json::json;

    pub fn dto(overrides: serde_json::Value) -> MessageDto {
        let mut base = json!({
            "id": "100", "channel_id": "10", "guild_id": "1",
            "author": {"id": "7", "username": "alice", "global_name": "Alice"},
            "content": "hello there", "timestamp": "2026-11-05T10:00:00Z",
            "edited_timestamp": null, "tts": false, "mention_everyone": false,
            "mentions": [], "mention_roles": [], "attachments": [], "embeds": [],
            "pinned": false, "type": 0, "flags": 0, "components": []
        });
        if let (Some(base), Some(extra)) = (base.as_object_mut(), overrides.as_object()) {
            for (key, value) in extra {
                base.insert(key.clone(), value.clone());
            }
        }
        serde_json::from_value(base).expect("a valid message dto")
    }

    pub fn message(overrides: serde_json::Value) -> IncomingMessage {
        IncomingMessage::from_dto(dto(overrides))
    }
}

#[cfg(test)]
mod tests {
    use super::fixtures::message;
    use serde_json::json;

    #[test]
    fn plain_messages_and_replies_are_pushable() {
        assert!(message(json!({})).is_pushable());
        assert!(message(json!({"type": 19})).is_pushable());
    }

    #[test]
    fn silent_ephemeral_and_system_messages_are_not() {
        assert!(!message(json!({"flags": 1 << 12})).is_pushable());
        assert!(!message(json!({"flags": 64})).is_pushable());
        assert!(!message(json!({"type": 7})).is_pushable());
        assert!(!message(json!({"type": 3})).is_pushable());
    }

    #[test]
    fn a_running_dm_call_rings_and_an_ended_one_or_a_server_message_does_not() {
        let running = json!({"type": 3, "guild_id": null, "call": {"participants": ["7"]}});
        assert!(message(running.clone()).is_call_ring());
        assert!(!message(running.clone()).is_pushable(), "a call is never an alert");
        let ended = json!({"type": 3, "guild_id": null, "call": {"participants": ["7"], "ended_timestamp": "2026-11-05T10:05:00Z"}});
        assert!(!message(ended).is_call_ring());
        assert!(!message(json!({"type": 3, "guild_id": "1", "call": {"participants": ["7"]}})).is_call_ring());
        assert!(!message(json!({"type": 3, "guild_id": null})).is_call_ring());
        assert!(!message(json!({"type": 0, "guild_id": null, "call": {"participants": ["7"]}})).is_call_ring());
        assert!(
            !message(json!({"type": 3, "guild_id": null, "flags": 64, "call": {"participants": ["7"]}})).is_call_ring()
        );
    }

    #[test]
    fn the_display_name_wins_over_the_username() {
        assert_eq!(message(json!({})).author_name, "Alice");
        let plain = json!({"author": {"id": "7", "username": "alice"}});
        assert_eq!(message(plain).author_name, "alice");
        assert_eq!(message(json!({"author": {"id": "7"}})).author_name, "Someone");
    }
}

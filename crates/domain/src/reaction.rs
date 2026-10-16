//! Reactions: one row per (message, emoji, user), summarised per viewer into
//! Discord's `{count, me, emoji}` shape.

use crate::error::DomainError;
use crate::{MessageId, Snowflake, UserId};

/// Maximum distinct emoji on one message (Discord parity).
pub const MAX_DISTINCT_EMOJI: usize = 20;

const MAX_EMOJI_BYTES: usize = 64;
const MAX_CUSTOM_NAME_CHARS: usize = 32;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Emoji {
    /// A Unicode emoji such as "👍".
    Unicode(String),
    /// A guild emoji, addressed as `name:id` in URLs.
    Custom { name: String, id: Snowflake },
}

impl Emoji {
    /// Parses the `{emoji}` path segment (already percent-decoded).
    pub fn parse(raw: &str) -> Result<Self, DomainError> {
        let invalid = || DomainError::invalid("emoji", "unknown emoji");
        if raw.is_empty() || raw.len() > MAX_EMOJI_BYTES || raw.chars().any(char::is_whitespace) {
            return Err(invalid());
        }
        match raw.split_once(':') {
            Some((name, id)) => {
                let valid_name = !name.is_empty()
                    && name.chars().count() <= MAX_CUSTOM_NAME_CHARS
                    && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
                if !valid_name {
                    return Err(invalid());
                }
                let id = id.parse().map_err(|_| invalid())?;
                Ok(Self::Custom {
                    name: name.to_owned(),
                    id,
                })
            }
            None if raw.is_ascii() => Err(invalid()),
            None => Ok(Self::Unicode(raw.to_owned())),
        }
    }

    /// The storage key; parsing it again yields the same emoji.
    pub fn key(&self) -> String {
        match self {
            Self::Unicode(value) => value.clone(),
            Self::Custom { name, id } => format!("{name}:{id}"),
        }
    }
}

/// One stored reaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReactionRow {
    pub message_id: MessageId,
    pub emoji: Emoji,
    pub user_id: UserId,
}

/// A reaction as one viewer sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReactionCount {
    pub emoji: Emoji,
    pub count: u32,
    pub me: bool,
}

/// Summarises one message's reaction rows for `viewer`, keeping the order in
/// which each emoji first appears.
pub fn summarize<'a>(rows: impl IntoIterator<Item = &'a ReactionRow>, viewer: UserId) -> Vec<ReactionCount> {
    let mut counts: Vec<ReactionCount> = Vec::new();
    for row in rows {
        let entry = match counts.iter_mut().position(|c| c.emoji == row.emoji) {
            Some(index) => &mut counts[index],
            None => {
                counts.push(ReactionCount {
                    emoji: row.emoji.clone(),
                    count: 0,
                    me: false,
                });
                counts.last_mut().expect("just pushed")
            }
        };
        entry.count += 1;
        entry.me |= row.user_id == viewer;
    }
    counts
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_unicode_emoji() {
        assert_eq!(Emoji::parse("👍").unwrap(), Emoji::Unicode("👍".into()));
        assert_eq!(Emoji::parse("1️⃣").unwrap().key(), "1️⃣");
    }

    #[test]
    fn parses_custom_emoji() {
        let emoji = Emoji::parse("party_blob:123").unwrap();
        assert_eq!(
            emoji,
            Emoji::Custom {
                name: "party_blob".into(),
                id: Snowflake::new(123)
            }
        );
        assert_eq!(Emoji::parse(&emoji.key()).unwrap(), emoji);
    }

    #[test]
    fn rejects_garbage() {
        for raw in ["", "hello", "name:", ":123", "bad name:1", "a:b", "👍 👍"] {
            assert!(Emoji::parse(raw).is_err(), "{raw} should be rejected");
        }
    }

    #[test]
    fn summarizes_counts_and_me_per_emoji() {
        let thumbs = Emoji::parse("👍").unwrap();
        let heart = Emoji::parse("❤️").unwrap();
        let row = |emoji: &Emoji, user| ReactionRow {
            message_id: Snowflake::new(1),
            emoji: emoji.clone(),
            user_id: Snowflake::new(user),
        };
        let rows = vec![row(&thumbs, 1), row(&heart, 2), row(&thumbs, 3)];
        let summary = summarize(&rows, Snowflake::new(3));
        assert_eq!(
            summary,
            vec![
                ReactionCount { emoji: thumbs, count: 2, me: true },
                ReactionCount { emoji: heart, count: 1, me: false },
            ]
        );
    }
}

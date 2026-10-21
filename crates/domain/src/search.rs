//! Message search: validated filters and the shape of an indexed message.
//!
//! The index never holds anything but Standard messages: Ephemeral and P2P
//! content is end-to-end encrypted and never reaches this service in plaintext,
//! and interaction replies flagged ephemeral are relays that are not stored.

use std::str::FromStr;

use crate::error::DomainError;
use crate::message_origin::FLAG_EPHEMERAL;
use crate::{ChannelId, GuildId, Message, MessageId, UserId};

/// Longest accepted search text, in characters.
pub const MAX_QUERY_CHARS: usize = 512;
/// Largest `offset` (Discord parity: 9975 + 25 = 10 000 results).
pub const MAX_OFFSET: u32 = 9975;
pub const DEFAULT_LIMIT: u32 = 25;
pub const MAX_LIMIT: u32 = 25;
/// Context messages on each side of a hit.
pub const MAX_CONTEXT: u32 = 2;
pub const DEFAULT_CONTEXT: u32 = 1;
pub const MAX_USER_FILTERS: usize = 10;
pub const MAX_CHANNEL_FILTERS: usize = 20;

/// An attachment kind of the `has` filter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HasFilter {
    Link,
    Embed,
    File,
    Image,
    Video,
    Sound,
}

impl HasFilter {
    pub const fn bit(self) -> u8 {
        match self {
            Self::Link => 1,
            Self::Embed => 1 << 1,
            Self::File => 1 << 2,
            Self::Image => 1 << 3,
            Self::Video => 1 << 4,
            Self::Sound => 1 << 5,
        }
    }
}

impl FromStr for HasFilter {
    type Err = DomainError;

    fn from_str(raw: &str) -> Result<Self, DomainError> {
        match raw {
            "link" => Ok(Self::Link),
            "embed" => Ok(Self::Embed),
            "file" => Ok(Self::File),
            "image" => Ok(Self::Image),
            "video" => Ok(Self::Video),
            "sound" => Ok(Self::Sound),
            _ => Err(DomainError::invalid(
                "has",
                "must be one of link, embed, file, image, video, sound",
            )),
        }
    }
}

/// The set of [`HasFilter`] kinds a message satisfies, as a bit mask.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct HasSet(u8);

impl HasSet {
    pub const fn from_bits(bits: u8) -> Self {
        Self(bits)
    }

    pub const fn bits(self) -> u8 {
        self.0
    }

    pub fn insert(&mut self, filter: HasFilter) {
        self.0 |= filter.bit();
    }

    pub const fn contains(self, filter: HasFilter) -> bool {
        self.0 & filter.bit() != 0
    }

    /// True when every kind of `required` is present.
    pub const fn covers(self, required: HasSet) -> bool {
        self.0 & required.0 == required.0
    }

    /// Classifies a message from its parts; attachments are
    /// `(content type, filename)` pairs.
    pub fn classify<'a>(
        content: &str,
        has_embeds: bool,
        attachments: impl IntoIterator<Item = (Option<&'a str>, &'a str)>,
    ) -> Self {
        let mut set = Self::default();
        let lower = content.to_ascii_lowercase();
        if lower.contains("http://") || lower.contains("https://") {
            set.insert(HasFilter::Link);
        }
        if has_embeds {
            set.insert(HasFilter::Embed);
        }
        for (content_type, filename) in attachments {
            set.insert(HasFilter::File);
            if let Some(kind) = media_kind(content_type, filename) {
                set.insert(kind);
            }
        }
        set
    }
}

fn media_kind(content_type: Option<&str>, filename: &str) -> Option<HasFilter> {
    if let Some(content_type) = content_type {
        let lower = content_type.to_ascii_lowercase();
        if lower.starts_with("image/") {
            return Some(HasFilter::Image);
        }
        if lower.starts_with("video/") {
            return Some(HasFilter::Video);
        }
        if lower.starts_with("audio/") {
            return Some(HasFilter::Sound);
        }
    }
    let extension = filename.rsplit_once('.')?.1.to_ascii_lowercase();
    match extension.as_str() {
        "png" | "jpg" | "jpeg" | "gif" | "webp" | "avif" | "bmp" => Some(HasFilter::Image),
        "mp4" | "webm" | "mov" | "mkv" | "avi" => Some(HasFilter::Video),
        "mp3" | "ogg" | "oga" | "wav" | "flac" | "m4a" | "opus" => Some(HasFilter::Sound),
        _ => None,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SortBy {
    #[default]
    Timestamp,
    Relevance,
}

impl FromStr for SortBy {
    type Err = DomainError;

    fn from_str(raw: &str) -> Result<Self, DomainError> {
        match raw {
            "timestamp" => Ok(Self::Timestamp),
            "relevance" => Ok(Self::Relevance),
            _ => Err(DomainError::invalid("sort_by", "must be timestamp or relevance")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SortOrder {
    Asc,
    #[default]
    Desc,
}

impl FromStr for SortOrder {
    type Err = DomainError;

    fn from_str(raw: &str) -> Result<Self, DomainError> {
        match raw {
            "asc" => Ok(Self::Asc),
            "desc" => Ok(Self::Desc),
            _ => Err(DomainError::invalid("sort_order", "must be asc or desc")),
        }
    }
}

/// A validated search request (everything but the scope and the caller).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchFilters {
    pub content: Option<String>,
    pub author_ids: Vec<UserId>,
    pub mentions: Vec<UserId>,
    pub has: HasSet,
    pub channel_ids: Vec<ChannelId>,
    /// Exclusive lower bound on the message id.
    pub min_id: Option<MessageId>,
    /// Exclusive upper bound on the message id.
    pub max_id: Option<MessageId>,
    pub pinned: Option<bool>,
    pub sort_by: SortBy,
    pub sort_order: SortOrder,
    pub offset: u32,
    pub limit: u32,
    pub context: u32,
}

impl Default for SearchFilters {
    fn default() -> Self {
        Self {
            content: None,
            author_ids: Vec::new(),
            mentions: Vec::new(),
            has: HasSet::default(),
            channel_ids: Vec::new(),
            min_id: None,
            max_id: None,
            pinned: None,
            sort_by: SortBy::Timestamp,
            sort_order: SortOrder::Desc,
            offset: 0,
            limit: DEFAULT_LIMIT,
            context: DEFAULT_CONTEXT,
        }
    }
}

impl SearchFilters {
    /// Normalizes and validates. Blank text is dropped; relevance ordering
    /// without text falls back to time order.
    pub fn validated(mut self) -> Result<Self, DomainError> {
        self.content = self
            .content
            .map(|text| text.trim().to_owned())
            .filter(|text| !text.is_empty());
        if self
            .content
            .as_ref()
            .is_some_and(|c| c.chars().count() > MAX_QUERY_CHARS)
        {
            return Err(DomainError::invalid("content", "must be 512 or fewer characters"));
        }
        if self.author_ids.len() > MAX_USER_FILTERS {
            return Err(DomainError::invalid("author_id", "at most 10 users"));
        }
        if self.mentions.len() > MAX_USER_FILTERS {
            return Err(DomainError::invalid("mentions", "at most 10 users"));
        }
        if self.channel_ids.len() > MAX_CHANNEL_FILTERS {
            return Err(DomainError::invalid("channel_id", "at most 20 channels"));
        }
        if self.offset > MAX_OFFSET {
            return Err(DomainError::invalid("offset", "must be 9975 or less"));
        }
        if !(1..=MAX_LIMIT).contains(&self.limit) {
            return Err(DomainError::invalid("limit", "must be between 1 and 25"));
        }
        if self.context > MAX_CONTEXT {
            return Err(DomainError::invalid("context", "must be between 0 and 2"));
        }
        if self.content.is_none() {
            self.sort_by = SortBy::Timestamp;
        }
        Ok(self)
    }
}

/// What the index stores about one message. The text is indexed but not
/// stored: results are hydrated from ScyllaDB, the source of truth.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexedMessage {
    pub channel_id: ChannelId,
    pub message_id: MessageId,
    pub guild_id: Option<GuildId>,
    pub author_id: UserId,
    pub content: String,
    pub mentions: Vec<UserId>,
    pub has: HasSet,
    /// Version for idempotent upserts: the edit time, 0 when never edited.
    pub edited_at_ms: u64,
}

impl IndexedMessage {
    /// The index entry of a stored message, or `None` when it must not be
    /// searchable (ephemeral relays).
    pub fn from_message(message: &Message, guild_id: Option<GuildId>) -> Option<Self> {
        if message.flags & FLAG_EPHEMERAL != 0 {
            return None;
        }
        Some(Self {
            channel_id: message.channel_id,
            message_id: message.id,
            guild_id,
            author_id: message.author_id,
            content: message.content.clone(),
            mentions: message.mentions.users.iter().copied().collect(),
            has: HasSet::classify(
                &message.content,
                !message.embeds.is_empty(),
                message
                    .attachments
                    .iter()
                    .map(|a| (a.content_type.as_deref(), a.filename.as_str())),
            ),
            edited_at_ms: message.edited_at_ms.unwrap_or(0),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::attachment::{Attachment, AttachmentFilename};
    use crate::{MessageDraft, Snowflake};

    fn message(draft: MessageDraft) -> Message {
        Message::compose(
            Snowflake::new(1 << 22),
            Snowflake::new(2),
            Snowflake::new(3),
            draft,
            false,
        )
        .unwrap()
    }

    fn attachment(name: &str, content_type: Option<&str>) -> Attachment {
        Attachment {
            id: Snowflake::new(9),
            filename: AttachmentFilename::parse(name).unwrap(),
            content_type: content_type.map(str::to_owned),
            size: 1,
            width: None,
            description: None,
            flags: 0,
            height: None,
        }
    }

    #[test]
    fn classifies_links_embeds_and_attachments() {
        let draft = MessageDraft {
            content: "see HTTPS://example.com".into(),
            embeds: vec!["{}".into()],
            attachments: vec![
                attachment("a.png", Some("image/png")),
                attachment("clip.bin", Some("video/mp4")),
                attachment("song.mp3", None),
                attachment("notes.txt", None),
            ],
            ..MessageDraft::default()
        };
        let indexed = IndexedMessage::from_message(&message(draft), Some(Snowflake::new(7))).unwrap();
        for kind in [
            HasFilter::Link,
            HasFilter::Embed,
            HasFilter::File,
            HasFilter::Image,
            HasFilter::Video,
            HasFilter::Sound,
        ] {
            assert!(indexed.has.contains(kind), "{kind:?}");
        }
        assert_eq!(indexed.guild_id, Some(Snowflake::new(7)));
    }

    #[test]
    fn plain_text_has_nothing() {
        let draft = MessageDraft {
            content: "hello".into(),
            ..MessageDraft::default()
        };
        let indexed = IndexedMessage::from_message(&message(draft), None).unwrap();
        assert_eq!(indexed.has, HasSet::default());
        assert_eq!(indexed.edited_at_ms, 0);
    }

    #[test]
    fn mentions_are_indexed() {
        let draft = MessageDraft {
            content: "hi <@42> <@43>".into(),
            ..MessageDraft::default()
        };
        let indexed = IndexedMessage::from_message(&message(draft), None).unwrap();
        assert_eq!(indexed.mentions, vec![Snowflake::new(42), Snowflake::new(43)]);
    }

    #[test]
    fn ephemeral_relays_are_never_indexed() {
        let draft = MessageDraft {
            content: "secret".into(),
            flags: FLAG_EPHEMERAL,
            ..MessageDraft::default()
        };
        assert!(IndexedMessage::from_message(&message(draft), None).is_none());
    }

    #[test]
    fn covers_requires_every_kind() {
        let mut have = HasSet::default();
        have.insert(HasFilter::Image);
        have.insert(HasFilter::File);
        let mut want = HasSet::default();
        want.insert(HasFilter::Image);
        assert!(have.covers(want));
        want.insert(HasFilter::Video);
        assert!(!have.covers(want));
    }

    #[test]
    fn filters_are_validated_and_normalized() {
        let ok = SearchFilters {
            content: Some("  hi  ".into()),
            ..SearchFilters::default()
        }
        .validated()
        .unwrap();
        assert_eq!(ok.content.as_deref(), Some("hi"));

        let blank = SearchFilters {
            content: Some("   ".into()),
            sort_by: SortBy::Relevance,
            ..SearchFilters::default()
        }
        .validated()
        .unwrap();
        assert_eq!(blank.content, None);
        assert_eq!(blank.sort_by, SortBy::Timestamp);

        let bad = [
            SearchFilters {
                content: Some("x".repeat(513)),
                ..SearchFilters::default()
            },
            SearchFilters {
                offset: 9976,
                ..SearchFilters::default()
            },
            SearchFilters {
                limit: 0,
                ..SearchFilters::default()
            },
            SearchFilters {
                limit: 26,
                ..SearchFilters::default()
            },
            SearchFilters {
                context: 3,
                ..SearchFilters::default()
            },
            SearchFilters {
                channel_ids: vec![Snowflake::new(1); 21],
                ..SearchFilters::default()
            },
        ];
        for filters in bad {
            assert!(matches!(filters.validated(), Err(DomainError::InvalidForm(_))));
        }
    }

    #[test]
    fn enum_parsers_reject_unknown_values() {
        assert_eq!("image".parse::<HasFilter>().unwrap(), HasFilter::Image);
        assert!("gif".parse::<HasFilter>().is_err());
        assert_eq!("relevance".parse::<SortBy>().unwrap(), SortBy::Relevance);
        assert!("size".parse::<SortBy>().is_err());
        assert_eq!("asc".parse::<SortOrder>().unwrap(), SortOrder::Asc);
        assert!("up".parse::<SortOrder>().is_err());
    }
}

//! Domain model of the Voics messages service.
//!
//! This crate is pure: no IO, no framework, no async. Everything here can be
//! reasoned about (and tested) in isolation.

pub mod access;
pub mod attachment;
pub mod bucket;
pub mod delivery_mode;
pub mod error;
pub mod forward;
pub mod link;
pub mod mention_policy;
pub mod mentions;
pub mod message;
pub mod message_origin;
pub mod paging;
pub mod permissions;
pub mod reaction;
pub mod read_state;
pub mod scheduled;
pub mod search;
pub mod snowflake;

pub use access::{Audience, ChannelAccess};
pub use attachment::{ATTACHMENT_FLAG_SPOILER, Attachment, AttachmentFilename, UploadKey};
pub use bucket::Bucket;
pub use delivery_mode::DeliveryMode;
pub use error::{DomainError, FieldError};
pub use forward::{ForwardOrigin, MessageSnapshot};
pub use link::{MAX_UNFURLED_LINKS, extract_links, is_player_iframe_host};
pub use mention_policy::{NotificationLevel, NotificationRecipient};
pub use mentions::Mentions;
pub use message::{Call, FORUM_MARKER_FLAG, Message, MessageDraft, MessageEdit, MessageKind};
pub use message_origin::{
    FLAG_CROSSPOSTED, FLAG_EPHEMERAL, FLAG_IS_CROSSPOST, FLAG_SUPPRESS_EMBEDS, FLAG_SUPPRESS_NOTIFICATIONS,
    InteractionLink, WebhookAuthor,
};
pub use paging::{HistoryCursor, PageLimit, Scan, ScanDirection};
pub use permissions::Permissions;
pub use reaction::{Emoji, ReactionCount, ReactionRow};
pub use read_state::ReadState;
pub use search::{HasFilter, HasSet, IndexedMessage, SearchFilters, SortBy, SortOrder};
pub use snowflake::{Snowflake, SnowflakeGenerator, WorkerId};

/// Channel identifiers are Snowflakes issued by the api service.
pub type ChannelId = Snowflake;
/// Message identifiers are Snowflakes issued by this service (workers 64–127).
pub type MessageId = Snowflake;
/// User identifiers are Snowflakes issued by the api service.
pub type UserId = Snowflake;
/// Guild identifiers are Snowflakes issued by the api service.
pub type GuildId = Snowflake;
/// Role identifiers are Snowflakes issued by the api service.
pub type RoleId = Snowflake;
/// Attachment identifiers are Snowflakes issued by this service.
pub type AttachmentId = Snowflake;

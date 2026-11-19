//! Attachments are uploaded straight to object storage with a presigned URL,
//! then referenced by their upload key when the message is created.

use crate::error::DomainError;
use crate::{AttachmentId, ChannelId, Snowflake};

/// Maximum upload size per file: 25 MiB.
pub const MAX_ATTACHMENT_BYTES: u64 = 25 * 1024 * 1024;
/// Maximum attachments per message (Discord parity).
pub const MAX_ATTACHMENTS_PER_MESSAGE: usize = 10;
/// Maximum files per presign request.
pub const MAX_UPLOADS_PER_REQUEST: usize = 10;

/// Longest alt text (Discord parity).
pub const MAX_DESCRIPTION_CHARS: usize = 1024;
/// Discord's IS_SPOILER attachment flag: blurred until the viewer reveals it.
pub const ATTACHMENT_FLAG_SPOILER: u32 = 1 << 3;
/// Filename prefix that marks a spoiler (Discord's convention).
pub const SPOILER_PREFIX: &str = "SPOILER_";

const MAX_FILENAME_CHARS: usize = 255;
const KEY_PREFIX: &str = "attachments";

/// Attachment metadata stored on a message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Attachment {
    pub id: AttachmentId,
    pub filename: AttachmentFilename,
    pub content_type: Option<String>,
    pub size: u64,
    pub width: Option<u32>,
    pub height: Option<u32>,
    /// Voice recording duration in milliseconds and base64 amplitude samples.
    pub duration_ms: Option<u32>,
    pub waveform: Option<String>,
    /// Alt text.
    pub description: Option<String>,
    /// `ATTACHMENT_FLAG_*` bits.
    pub flags: u32,
}

/// Changes to an existing attachment, never to its object identity or file bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttachmentMetadataEdit {
    pub id: AttachmentId,
    /// Missing retains alt text; present null clears it.
    pub description: Option<Option<String>>,
    pub flags: Option<u32>,
}

impl Attachment {
    pub fn is_spoiler(&self) -> bool {
        self.flags & ATTACHMENT_FLAG_SPOILER != 0
    }

    pub fn key(&self, channel_id: ChannelId) -> UploadKey {
        UploadKey::new(channel_id, self.id, self.filename.clone())
    }
}

/// A filename safe to embed in an object key and a URL path segment:
/// no path separators, no control characters, at most 255 characters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttachmentFilename(String);

impl AttachmentFilename {
    pub fn parse(raw: &str) -> Result<Self, DomainError> {
        let name: String = raw
            .trim()
            .chars()
            .map(|c| {
                if c.is_alphanumeric() || "._-".contains(c) {
                    c
                } else {
                    '_'
                }
            })
            .take(MAX_FILENAME_CHARS)
            .collect();
        if name.is_empty() || name.chars().all(|c| c == '.') {
            return Err(DomainError::invalid("filename", "must not be empty"));
        }
        Ok(Self(name))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// The object key of an uploaded attachment:
/// `attachments/{channel_id}/{attachment_id}/{filename}`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UploadKey {
    pub channel_id: ChannelId,
    pub attachment_id: AttachmentId,
    pub filename: AttachmentFilename,
}

impl UploadKey {
    pub fn new(channel_id: ChannelId, attachment_id: AttachmentId, filename: AttachmentFilename) -> Self {
        Self {
            channel_id,
            attachment_id,
            filename,
        }
    }

    /// Parses a key returned by the presign step. Keys from other channels
    /// are rejected so a message can only reference uploads to its channel.
    pub fn parse_for_channel(raw: &str, channel_id: ChannelId) -> Result<Self, DomainError> {
        let invalid = || DomainError::invalid("attachments", "unknown uploaded_filename");
        let mut parts = raw.splitn(4, '/');
        let (Some(prefix), Some(channel), Some(id), Some(filename)) =
            (parts.next(), parts.next(), parts.next(), parts.next())
        else {
            return Err(invalid());
        };
        if prefix != KEY_PREFIX || channel != channel_id.to_string() {
            return Err(invalid());
        }
        let attachment_id: Snowflake = id.parse().map_err(|_| invalid())?;
        let filename = AttachmentFilename::parse(filename)?;
        if filename.as_str() != filename_segment(raw) {
            return Err(invalid());
        }
        Ok(Self::new(channel_id, attachment_id, filename))
    }

    pub fn as_object_key(&self) -> String {
        format!(
            "{KEY_PREFIX}/{}/{}/{}",
            self.channel_id,
            self.attachment_id,
            self.filename.as_str()
        )
    }
}

fn filename_segment(raw: &str) -> &str {
    raw.rsplit('/').next().unwrap_or_default()
}

/// The flags stored for an upload: only the spoiler bit is client-settable,
/// and a `SPOILER_` filename prefix implies it.
pub fn upload_flags(filename: &AttachmentFilename, requested: u32) -> u32 {
    let spoiler = requested & ATTACHMENT_FLAG_SPOILER != 0 || filename.as_str().starts_with(SPOILER_PREFIX);
    if spoiler { ATTACHMENT_FLAG_SPOILER } else { 0 }
}

/// Trims alt text; empty text means none.
pub fn validate_description(raw: Option<&str>) -> Result<Option<String>, DomainError> {
    let Some(text) = raw.map(str::trim).filter(|t| !t.is_empty()) else {
        return Ok(None);
    };
    if text.chars().count() > MAX_DESCRIPTION_CHARS {
        return Err(DomainError::invalid("description", "must be 1024 or fewer characters"));
    }
    Ok(Some(text.to_owned()))
}

/// Validates a requested upload size.
pub fn validate_upload_size(size: u64) -> Result<(), DomainError> {
    if size == 0 || size > MAX_ATTACHMENT_BYTES {
        Err(DomainError::invalid("file_size", "must be between 1 byte and 25 MiB"))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const CHANNEL: Snowflake = Snowflake::new(42);

    #[test]
    fn filenames_are_sanitised() {
        assert_eq!(
            AttachmentFilename::parse("cat pic.png").unwrap().as_str(),
            "cat_pic.png"
        );
        assert_eq!(
            AttachmentFilename::parse("../../etc/passwd").unwrap().as_str(),
            ".._.._etc_passwd"
        );
        assert!(AttachmentFilename::parse("   ").is_err());
        assert!(AttachmentFilename::parse("..").is_err());
    }

    #[test]
    fn filenames_are_truncated_to_255_chars() {
        let long = "a".repeat(400);
        assert_eq!(AttachmentFilename::parse(&long).unwrap().as_str().len(), 255);
    }

    #[test]
    fn upload_keys_round_trip() {
        let key = UploadKey::new(CHANNEL, Snowflake::new(7), AttachmentFilename::parse("a.png").unwrap());
        assert_eq!(key.as_object_key(), "attachments/42/7/a.png");
        assert_eq!(
            UploadKey::parse_for_channel(&key.as_object_key(), CHANNEL).unwrap(),
            key
        );
    }

    #[test]
    fn upload_keys_from_other_channels_are_rejected() {
        assert!(UploadKey::parse_for_channel("attachments/43/7/a.png", CHANNEL).is_err());
        assert!(UploadKey::parse_for_channel("avatars/42/7/a.png", CHANNEL).is_err());
        assert!(UploadKey::parse_for_channel("attachments/42/x/a.png", CHANNEL).is_err());
        assert!(UploadKey::parse_for_channel("attachments/42/7/a b.png", CHANNEL).is_err());
        assert!(UploadKey::parse_for_channel("attachments/42/7/x/a.png", CHANNEL).is_err());
    }

    #[test]
    fn spoiler_comes_from_the_flag_or_the_filename_prefix() {
        let plain = AttachmentFilename::parse("a.png").unwrap();
        let prefixed = AttachmentFilename::parse("SPOILER_a.png").unwrap();
        assert_eq!(upload_flags(&plain, 0), 0);
        assert_eq!(upload_flags(&plain, ATTACHMENT_FLAG_SPOILER), ATTACHMENT_FLAG_SPOILER);
        assert_eq!(upload_flags(&prefixed, 0), ATTACHMENT_FLAG_SPOILER);
        assert_eq!(upload_flags(&plain, 0xFFFF_FFF7), 0, "unknown bits are dropped");
    }

    #[test]
    fn descriptions_are_trimmed_and_bounded() {
        assert_eq!(validate_description(None).unwrap(), None);
        assert_eq!(validate_description(Some("  ")).unwrap(), None);
        assert_eq!(validate_description(Some(" a cat ")).unwrap().as_deref(), Some("a cat"));
        assert!(validate_description(Some(&"é".repeat(1024))).is_ok());
        assert!(validate_description(Some(&"é".repeat(1025))).is_err());
    }

    #[test]
    fn upload_size_is_bounded() {
        assert!(validate_upload_size(0).is_err());
        assert!(validate_upload_size(1).is_ok());
        assert!(validate_upload_size(MAX_ATTACHMENT_BYTES).is_ok());
        assert!(validate_upload_size(MAX_ATTACHMENT_BYTES + 1).is_err());
    }
}

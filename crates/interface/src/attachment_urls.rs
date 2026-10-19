//! Public CDN URLs of stored attachments.

use discordy_messages_domain::{Attachment, ChannelId};

/// Builds `{CDN_PUBLIC_URL}/{bucket}/{object key}`, the path-style address the
/// ingress forwards to MinIO.
#[derive(Debug, Clone)]
pub struct AttachmentUrls {
    base: String,
}

impl AttachmentUrls {
    pub fn new(cdn_public_url: &str, bucket: &str) -> Self {
        Self {
            base: format!("{}/{bucket}", cdn_public_url.trim_end_matches('/')),
        }
    }

    pub fn url(&self, channel_id: ChannelId, attachment: &Attachment) -> String {
        format!("{}/{}", self.base, attachment.key(channel_id).as_object_key())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use discordy_messages_domain::{AttachmentFilename, Snowflake};

    #[test]
    fn urls_are_path_style_under_the_cdn() {
        let urls = AttachmentUrls::new("http://localhost:8000/cdn/", "discordy-attachments");
        let attachment = Attachment {
            id: Snowflake::new(9),
            filename: AttachmentFilename::parse("cat.png").unwrap(),
            content_type: None,
            size: 1,
            width: None,
            height: None,
        };
        assert_eq!(
            urls.url(Snowflake::new(5), &attachment),
            "http://localhost:8000/cdn/discordy-attachments/attachments/5/9/cat.png"
        );
    }
}

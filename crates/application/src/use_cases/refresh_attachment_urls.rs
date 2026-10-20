use std::sync::Arc;

use discordy_messages_domain::{ChannelId, DomainError, UploadKey, UserId};

use super::MessagingPorts;
use super::support::checked_access;
use crate::error::AppResult;
use crate::ports::ChannelAccessProvider;

/// Most references accepted in one refresh call.
pub const MAX_REFRESH_REFERENCES: usize = 50;

pub struct RefreshAttachmentUrls {
    access: Arc<dyn ChannelAccessProvider>,
}

impl RefreshAttachmentUrls {
    pub fn new(ports: &MessagingPorts) -> Self {
        Self {
            access: ports.access.clone(),
        }
    }

    /// Authorizes the caller for `channel_id` and resolves each reference (an
    /// object key such as `attachments/{channel}/{id}/{file}`, or any CDN URL
    /// ending in that path, signed or not) to its key. Keys of another channel
    /// are refused, so view access to this channel never signs a foreign file.
    /// The interface layer signs the returned keys.
    pub async fn execute(
        &self,
        channel_id: ChannelId,
        user: UserId,
        references: Vec<String>,
    ) -> AppResult<Vec<(String, UploadKey)>> {
        checked_access(&*self.access, channel_id, user, |a| a.ensure_can_read_history()).await?;
        if references.is_empty() || references.len() > MAX_REFRESH_REFERENCES {
            return Err(DomainError::invalid("attachment_urls", "must contain between 1 and 50 entries").into());
        }
        references
            .into_iter()
            .map(|original| {
                let key = parse_reference(&original, channel_id)?;
                Ok((original, key))
            })
            .collect()
    }
}

fn parse_reference(raw: &str, channel_id: ChannelId) -> Result<UploadKey, DomainError> {
    let path = raw.split(['?', '#']).next().unwrap_or_default();
    let segments: Vec<&str> = path.rsplit('/').take(4).collect();
    let [file, id, channel, prefix] = segments[..] else {
        return Err(DomainError::invalid("attachment_urls", "not an attachment reference"));
    };
    UploadKey::parse_for_channel(&format!("{prefix}/{channel}/{id}/{}", percent_decode(file)), channel_id)
}

fn percent_decode(segment: &str) -> String {
    let bytes = segment.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        let decoded = (bytes[i] == b'%' && i + 2 < bytes.len())
            .then(|| segment.get(i + 1..i + 3).and_then(|h| u8::from_str_radix(h, 16).ok()))
            .flatten();
        match decoded {
            Some(byte) => {
                out.push(byte);
                i += 3;
            }
            None => {
                out.push(bytes[i]);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::AppError;
    use crate::fakes::*;
    use discordy_messages_domain::DeliveryMode;

    #[tokio::test]
    async fn keys_and_signed_urls_resolve_to_the_same_key() {
        let h = Harness::new();
        let use_case = RefreshAttachmentUrls::new(&h.ports);
        let key = format!("attachments/{CHANNEL}/77/cat.png");
        let url = format!("https://cdn.test/cdn/attachments/{CHANNEL}/77/cat.png?ex=1&is=0&hm=00");
        let resolved = use_case.execute(CHANNEL, ALICE, vec![key.clone(), url]).await.unwrap();
        assert_eq!(resolved.len(), 2);
        assert_eq!(resolved[0].1, resolved[1].1);
        assert_eq!(resolved[0].1.as_object_key(), key);
    }

    #[tokio::test]
    async fn non_ascii_filenames_survive_percent_encoding() {
        let h = Harness::new();
        let use_case = RefreshAttachmentUrls::new(&h.ports);
        let url = format!("http://x/attachments/{CHANNEL}/77/caf%C3%A9.png?ex=1");
        let resolved = use_case.execute(CHANNEL, ALICE, vec![url]).await.unwrap();
        assert_eq!(resolved[0].1.filename.as_str(), "café.png");
    }

    #[tokio::test]
    async fn another_channels_keys_and_junk_are_refused() {
        let h = Harness::new();
        let use_case = RefreshAttachmentUrls::new(&h.ports);
        for bad in ["attachments/999/77/cat.png", "cat.png", "x/y/z", ""] {
            let result = use_case.execute(CHANNEL, ALICE, vec![bad.into()]).await;
            assert!(
                matches!(result, Err(AppError::Domain(DomainError::InvalidForm(_)))),
                "{bad}"
            );
        }
        assert!(use_case.execute(CHANNEL, ALICE, vec![]).await.is_err());
    }

    #[tokio::test]
    async fn viewers_without_access_cannot_refresh() {
        let h = Harness::new();
        h.access.set_permissions(0);
        let key = format!("attachments/{CHANNEL}/77/cat.png");
        assert!(
            RefreshAttachmentUrls::new(&h.ports)
                .execute(CHANNEL, ALICE, vec![key])
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn ephemeral_channels_can_refresh_too() {
        let h = Harness::new();
        h.access.set(Ok(dm_access(DeliveryMode::Ephemeral)));
        let key = format!("attachments/{CHANNEL}/77/blob.bin");
        assert!(
            RefreshAttachmentUrls::new(&h.ports)
                .execute(CHANNEL, ALICE, vec![key])
                .await
                .is_ok()
        );
    }
}

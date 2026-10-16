use std::sync::Arc;

use discordy_messages_domain::attachment::{MAX_UPLOADS_PER_REQUEST, validate_upload_size};
use discordy_messages_domain::{AttachmentFilename, ChannelId, DomainError, UploadKey, UserId};

use super::MessagingPorts;
use crate::error::AppResult;
use crate::ports::{AttachmentStorage, ChannelAccessProvider, IdGenerator};
use crate::views::UploadSlot;

/// One file the client wants to upload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UploadRequest {
    pub client_id: String,
    pub filename: String,
    pub file_size: u64,
}

pub struct CreateUploadSlots {
    access: Arc<dyn ChannelAccessProvider>,
    storage: Arc<dyn AttachmentStorage>,
    ids: Arc<dyn IdGenerator>,
}

impl CreateUploadSlots {
    pub fn new(ports: &MessagingPorts) -> Self {
        Self {
            access: ports.access.clone(),
            storage: ports.storage.clone(),
            ids: ports.ids.clone(),
        }
    }

    /// Issues presigned upload URLs. The returned `upload_filename` is then
    /// referenced when the message is created.
    pub async fn execute(&self, channel_id: ChannelId, user: UserId, files: Vec<UploadRequest>) -> AppResult<Vec<UploadSlot>> {
        self.access.access(channel_id, user).await?.ensure_can_send(true)?;
        if files.is_empty() || files.len() > MAX_UPLOADS_PER_REQUEST {
            return Err(DomainError::invalid("files", "must contain between 1 and 10 files").into());
        }
        let mut slots = Vec::with_capacity(files.len());
        for file in files {
            validate_upload_size(file.file_size)?;
            let filename = AttachmentFilename::parse(&file.filename)?;
            let key = UploadKey::new(channel_id, self.ids.next_id(), filename);
            slots.push(UploadSlot {
                client_id: file.client_id,
                upload_url: self.storage.presign_upload(&key),
                upload_filename: key.as_object_key(),
            });
        }
        Ok(slots)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::AppError;
    use crate::fakes::*;
    use discordy_messages_domain::{DeliveryMode, Permissions};

    fn request(name: &str, size: u64) -> UploadRequest {
        UploadRequest {
            client_id: "0".into(),
            filename: name.into(),
            file_size: size,
        }
    }

    #[tokio::test]
    async fn issues_presigned_urls_scoped_to_the_channel() {
        let h = Harness::new();
        let slots = CreateUploadSlots::new(&h.ports)
            .execute(CHANNEL, ALICE, vec![request("cat pic.png", 100)])
            .await
            .unwrap();
        assert_eq!(slots.len(), 1);
        assert!(slots[0].upload_filename.starts_with(&format!("attachments/{CHANNEL}/")));
        assert!(slots[0].upload_filename.ends_with("/cat_pic.png"));
        assert!(slots[0].upload_url.contains(&slots[0].upload_filename));
    }

    #[tokio::test]
    async fn requires_attach_files_and_a_standard_channel() {
        let h = Harness::new();
        h.access
            .set_permissions(Permissions::VIEW_CHANNEL | Permissions::SEND_MESSAGES);
        let use_case = CreateUploadSlots::new(&h.ports);
        assert_eq!(
            use_case.execute(CHANNEL, ALICE, vec![request("a", 1)]).await,
            Err(AppError::Domain(DomainError::MissingPermissions))
        );
        h.access.set(Ok(dm_access(DeliveryMode::P2p)));
        assert_eq!(
            use_case.execute(CHANNEL, ALICE, vec![request("a", 1)]).await,
            Err(AppError::Domain(DomainError::DeliveryModeConflict(DeliveryMode::P2p)))
        );
    }

    #[tokio::test]
    async fn validates_file_count_and_size() {
        let h = Harness::new();
        let use_case = CreateUploadSlots::new(&h.ports);
        assert!(use_case.execute(CHANNEL, ALICE, vec![]).await.is_err());
        assert!(use_case.execute(CHANNEL, ALICE, vec![request("a", 1); 11]).await.is_err());
        assert!(use_case.execute(CHANNEL, ALICE, vec![request("a", 0)]).await.is_err());
    }
}

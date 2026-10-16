use async_trait::async_trait;
use discordy_messages_domain::UploadKey;

use crate::error::PortError;

/// What object storage knows about an uploaded file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredObject {
    pub size: u64,
    pub content_type: Option<String>,
}

#[async_trait]
pub trait AttachmentStorage: Send + Sync {
    /// A time-limited URL the client PUTs the file to.
    fn presign_upload(&self, key: &UploadKey) -> String;

    /// `None` when nothing was uploaded under `key`.
    async fn stat(&self, key: &UploadKey) -> Result<Option<StoredObject>, PortError>;

    async fn delete(&self, key: &UploadKey) -> Result<(), PortError>;
}

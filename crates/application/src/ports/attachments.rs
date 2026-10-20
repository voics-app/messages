use async_trait::async_trait;
use bytes::Bytes;
use discordy_messages_domain::UploadKey;
use futures::stream::BoxStream;

use crate::error::PortError;

/// What object storage knows about an uploaded file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredObject {
    pub size: u64,
    pub content_type: Option<String>,
}

/// An object opened for download. `status` is the storage answer (200, or 206
/// for a range request); the body streams without buffering.
pub struct ObjectRead {
    pub status: u16,
    pub content_type: Option<String>,
    pub content_length: Option<u64>,
    pub content_range: Option<String>,
    pub etag: Option<String>,
    pub body: BoxStream<'static, Result<Bytes, PortError>>,
}

#[async_trait]
pub trait AttachmentStorage: Send + Sync {
    /// A time-limited URL the client PUTs the file to.
    fn presign_upload(&self, key: &UploadKey) -> String;

    /// `None` when nothing was uploaded under `key`.
    async fn stat(&self, key: &UploadKey) -> Result<Option<StoredObject>, PortError>;

    /// Opens the object for download; `range` is a raw HTTP `Range` header
    /// value. `None` when nothing is stored under `key`.
    async fn open(&self, key: &UploadKey, range: Option<&str>) -> Result<Option<ObjectRead>, PortError>;

    async fn delete(&self, key: &UploadKey) -> Result<(), PortError>;
}

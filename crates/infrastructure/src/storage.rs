//! MinIO (S3 API) attachment storage using sans-IO request signing.
//!
//! Upload URLs are presigned against the public CDN base URL, so clients PUT
//! straight to object storage through the CDN host. Server-side calls (HEAD,
//! DELETE) are signed against the internal MinIO endpoint.

use std::time::Duration;

use async_trait::async_trait;
use discordy_messages_application::PortError;
use discordy_messages_application::ports::{AttachmentStorage, StoredObject};
use discordy_messages_domain::UploadKey;
use reqwest::StatusCode;
use rusty_s3::{Bucket, Credentials, S3Action, UrlStyle};
use url::Url;

/// How long a presigned upload URL stays valid.
const UPLOAD_URL_TTL: Duration = Duration::from_secs(600);
/// Server-side request signatures only need to outlive the request.
const SERVER_URL_TTL: Duration = Duration::from_secs(60);
const REGION: &str = "us-east-1";

fn port_error(error: impl std::fmt::Display) -> PortError {
    PortError::new(format!("storage: {error}"))
}

pub struct S3Settings {
    pub internal_endpoint: String,
    pub public_endpoint: String,
    pub bucket: String,
    pub access_key: String,
    pub secret_key: String,
}

pub struct S3AttachmentStorage {
    internal: Bucket,
    public: Bucket,
    credentials: Credentials,
    http: reqwest::Client,
}

impl S3AttachmentStorage {
    pub fn new(settings: S3Settings, http: reqwest::Client) -> Result<Self, PortError> {
        let bucket = |endpoint: &str| -> Result<Bucket, PortError> {
            let url: Url = endpoint.parse().map_err(port_error)?;
            Bucket::new(url, UrlStyle::Path, settings.bucket.clone(), REGION).map_err(port_error)
        };
        Ok(Self {
            internal: bucket(&settings.internal_endpoint)?,
            public: bucket(&settings.public_endpoint)?,
            credentials: Credentials::new(settings.access_key, settings.secret_key),
            http,
        })
    }

    /// Creates the bucket when it does not exist yet (local development).
    pub async fn ensure_bucket(&self) -> Result<(), PortError> {
        let head = self.internal.head_bucket(Some(&self.credentials)).sign(SERVER_URL_TTL);
        let status = self.http.head(head).send().await.map_err(port_error)?.status();
        if status.is_success() {
            return Ok(());
        }
        let create = self.internal.create_bucket(&self.credentials).sign(SERVER_URL_TTL);
        let response = self.http.put(create).send().await.map_err(port_error)?;
        if response.status().is_success() || response.status() == StatusCode::CONFLICT {
            Ok(())
        } else {
            Err(port_error(format!("create bucket returned {}", response.status())))
        }
    }
}

#[async_trait]
impl AttachmentStorage for S3AttachmentStorage {
    fn presign_upload(&self, key: &UploadKey) -> String {
        self.public
            .put_object(Some(&self.credentials), &key.as_object_key())
            .sign(UPLOAD_URL_TTL)
            .to_string()
    }

    async fn stat(&self, key: &UploadKey) -> Result<Option<StoredObject>, PortError> {
        let url = self
            .internal
            .head_object(Some(&self.credentials), &key.as_object_key())
            .sign(SERVER_URL_TTL);
        let response = self.http.head(url).send().await.map_err(port_error)?;
        match response.status() {
            StatusCode::NOT_FOUND => Ok(None),
            status if status.is_success() => {
                let headers = response.headers();
                let size = headers
                    .get(reqwest::header::CONTENT_LENGTH)
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(0);
                let content_type = headers
                    .get(reqwest::header::CONTENT_TYPE)
                    .and_then(|v| v.to_str().ok())
                    .map(str::to_owned);
                Ok(Some(StoredObject { size, content_type }))
            }
            status => Err(port_error(format!("HEAD object returned {status}"))),
        }
    }

    async fn delete(&self, key: &UploadKey) -> Result<(), PortError> {
        let url = self
            .internal
            .delete_object(Some(&self.credentials), &key.as_object_key())
            .sign(SERVER_URL_TTL);
        let status = self.http.delete(url).send().await.map_err(port_error)?.status();
        if status.is_success() || status == StatusCode::NOT_FOUND {
            Ok(())
        } else {
            Err(port_error(format!("DELETE object returned {status}")))
        }
    }
}

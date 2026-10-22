//! MinIO (S3 API) attachment storage using sans-IO request signing.
//!
//! Upload URLs are presigned against the public CDN base URL, so clients PUT
//! straight to object storage through the CDN host. Server-side calls (HEAD,
//! DELETE) are signed against the internal MinIO endpoint.

use std::time::Duration;

use async_trait::async_trait;
use discordy_messages_application::PortError;
use discordy_messages_application::ports::{AttachmentStorage, ObjectRead, StoredObject};
use discordy_messages_domain::UploadKey;
use futures::StreamExt;
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
    /// Path of the public CDN base URL (for example `/cdn`). The ingress strips
    /// it before the request reaches MinIO, so it must stay outside the
    /// signed path.
    public_path_prefix: String,
    credentials: Credentials,
    http: reqwest::Client,
}

impl S3AttachmentStorage {
    pub fn new(settings: S3Settings, http: reqwest::Client) -> Result<Self, PortError> {
        let bucket = |endpoint: &str| -> Result<Bucket, PortError> {
            let url: Url = endpoint.parse().map_err(port_error)?;
            Bucket::new(url, UrlStyle::Path, settings.bucket.clone(), REGION).map_err(port_error)
        };
        let public_url: Url = settings.public_endpoint.parse().map_err(port_error)?;
        let public_path_prefix = public_url.path().trim_end_matches('/').to_owned();
        let mut public_origin = public_url;
        public_origin.set_path("/");
        Ok(Self {
            internal: bucket(&settings.internal_endpoint)?,
            public: bucket(public_origin.as_str())?,
            public_path_prefix,
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
        // Signed against the origin alone; the CDN path prefix is added to the
        // URL afterwards because the ingress removes it before MinIO verifies
        // the signature.
        let mut url = self
            .public
            .put_object(Some(&self.credentials), &key.as_object_key())
            .sign(UPLOAD_URL_TTL);
        let path = format!("{}{}", self.public_path_prefix, url.path());
        url.set_path(&path);
        url.to_string()
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

    async fn open(&self, key: &UploadKey, range: Option<&str>) -> Result<Option<ObjectRead>, PortError> {
        let url = self
            .internal
            .get_object(Some(&self.credentials), &key.as_object_key())
            .sign(SERVER_URL_TTL);
        let mut request = self.http.get(url);
        if let Some(range) = range {
            request = request.header(reqwest::header::RANGE, range);
        }
        let response = request.send().await.map_err(port_error)?;
        let status = response.status();
        if status == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !matches!(
            status,
            StatusCode::OK | StatusCode::PARTIAL_CONTENT | StatusCode::RANGE_NOT_SATISFIABLE
        ) {
            return Err(port_error(format!("GET object returned {status}")));
        }
        let header = |name| {
            response
                .headers()
                .get(name)
                .and_then(|v| v.to_str().ok())
                .map(str::to_owned)
        };
        Ok(Some(ObjectRead {
            status: status.as_u16(),
            content_type: header(reqwest::header::CONTENT_TYPE),
            content_length: header(reqwest::header::CONTENT_LENGTH).and_then(|v| v.parse().ok()),
            content_range: header(reqwest::header::CONTENT_RANGE),
            etag: header(reqwest::header::ETAG),
            body: response.bytes_stream().map(|chunk| chunk.map_err(port_error)).boxed(),
        }))
    }

    async fn copy(&self, from: &UploadKey, to: &UploadKey) -> Result<bool, PortError> {
        // Attachments are at most 25 MiB, so the object is buffered between the
        // GET and the PUT instead of relying on server-side copy headers.
        let Some(object) = self.open(from, None).await? else {
            return Ok(false);
        };
        let mut body = Vec::new();
        let mut stream = object.body;
        while let Some(chunk) = stream.next().await {
            body.extend_from_slice(&chunk?);
        }
        let url = self
            .internal
            .put_object(Some(&self.credentials), &to.as_object_key())
            .sign(SERVER_URL_TTL);
        let mut request = self.http.put(url).body(body);
        if let Some(content_type) = object.content_type {
            request = request.header(reqwest::header::CONTENT_TYPE, content_type);
        }
        let status = request.send().await.map_err(port_error)?.status();
        if status.is_success() {
            Ok(true)
        } else {
            Err(port_error(format!("PUT object returned {status}")))
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

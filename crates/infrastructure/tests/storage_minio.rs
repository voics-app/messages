//! The attachment store against a real MinIO: presigned upload, stat, delete.

mod support;

use discordy_messages_application::ports::AttachmentStorage;
use discordy_messages_domain::{AttachmentFilename, Snowflake, UploadKey};
use discordy_messages_infrastructure::storage::{S3AttachmentStorage, S3Settings};
use futures::StreamExt;

#[tokio::test]
async fn presigned_uploads_can_be_stat_ed_and_deleted() {
    let minio = support::start_minio().await;
    let storage = S3AttachmentStorage::new(
        S3Settings {
            internal_endpoint: minio.endpoint.clone(),
            // The public URL carries a path prefix the ingress strips; here the
            // test plays the ingress by removing it before calling MinIO.
            public_endpoint: format!("{}/cdn", minio.endpoint),
            bucket: "attachments-test".into(),
            access_key: support::MINIO_USER.into(),
            secret_key: support::MINIO_PASSWORD.into(),
        },
        reqwest::Client::new(),
    )
    .unwrap();
    storage.ensure_bucket().await.unwrap();
    storage
        .ensure_bucket()
        .await
        .expect("creating the bucket twice is fine");

    let key = UploadKey::new(
        Snowflake::new(1),
        Snowflake::new(2),
        AttachmentFilename::parse("cat.txt").unwrap(),
    );
    assert_eq!(storage.stat(&key).await.unwrap(), None);

    let url = storage.presign_upload(&key);
    assert!(url.contains("/cdn/attachments-test/attachments/1/2/cat.txt"), "{url}");
    let upstream = url.replacen("/cdn/", "/", 1);
    let response = reqwest::Client::new()
        .put(&upstream)
        .header("content-type", "text/plain")
        .body("meow")
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success(), "upload failed: {}", response.status());

    let stored = storage.stat(&key).await.unwrap().expect("uploaded object");
    assert_eq!(stored.size, 4);
    assert_eq!(stored.content_type.as_deref(), Some("text/plain"));

    let mut object = storage.open(&key, None).await.unwrap().expect("stored object opens");
    assert_eq!((object.status, object.content_length), (200, Some(4)));
    let mut bytes = Vec::new();
    while let Some(chunk) = object.body.next().await {
        bytes.extend_from_slice(&chunk.unwrap());
    }
    assert_eq!(bytes, b"meow");
    let ranged = storage.open(&key, Some("bytes=2-")).await.unwrap().unwrap();
    assert_eq!(ranged.status, 206);
    assert_eq!(ranged.content_range.as_deref(), Some("bytes 2-3/4"));

    storage.delete(&key).await.unwrap();
    assert_eq!(storage.stat(&key).await.unwrap(), None);
    assert!(storage.open(&key, None).await.unwrap().is_none());
    storage.delete(&key).await.expect("deleting a missing object is fine");
}

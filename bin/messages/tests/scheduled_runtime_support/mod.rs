use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;
use discordy_messages_application::{
    fakes::{ALICE, BOB},
    ports::{AuthError, Authenticator, Principal},
};
use serde_json::{Value, json};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

pub struct Auth;
#[async_trait]
impl Authenticator for Auth {
    async fn authenticate(&self, header: &str) -> Result<Principal, AuthError> {
        let user_id = match header {
            "Bearer alice" => ALICE,
            "Bearer bob" => BOB,
            _ => return Err(AuthError::Invalid),
        };
        Ok(Principal { user_id, is_bot: false })
    }
}

pub struct Server(pub tokio::task::JoinHandle<()>);
impl Drop for Server {
    fn drop(&mut self) {
        self.0.abort();
    }
}

pub fn config() -> super::config::Config {
    let vars = HashMap::from([
        ("INTERNAL_SERVICE_TOKEN", "fixture-internal"),
        ("JWT_PUBLIC_KEY", "fixture-key"),
        ("SNOWFLAKE_WORKER_ID", "64"),
        ("KAFKA_BROKERS", "fixture:9092"),
        ("SCYLLA_NODES", "fixture:9042"),
        ("API_INTERNAL_URL", "http://fixture:8080"),
        ("MINIO_ENDPOINT", "http://fixture:9000"),
        ("MINIO_ACCESS_KEY", "fixture"),
        ("MINIO_SECRET_KEY", "fixture"),
        ("CDN_PUBLIC_URL", "http://cdn.test"),
        (
            "ATTACHMENT_SIGNING_KEY",
            "fixture-signing-key-at-least-thirty-two-characters",
        ),
    ]);
    super::config::Config::from_lookup(|key| vars.get(key).map(|value| (*value).into())).unwrap()
}

pub fn request(now: u64, sequence: u64, content: &str) -> Value {
    let bits = sequence * 64 + ALICE.value() % 64;
    let instant = OffsetDateTime::from_unix_timestamp_nanos(i128::from(now + 60_000) * 1_000_000)
        .unwrap()
        .format(&Rfc3339)
        .unwrap();
    json!({"request_id":format!("00000000-0000-4000-8000-{bits:012x}"),"scheduled_at":instant,"message":{"content":content}})
}

pub async fn wait_for(client: &reqwest::Client, url: &str, status: &str) -> Value {
    tokio::time::timeout(Duration::from_secs(40), async {
        loop {
            let response = client.get(url).bearer_auth("alice").send().await.unwrap();
            assert_eq!(response.status(), 200);
            let body: Value = response.json().await.unwrap();
            if body["status"] == status {
                return body;
            }
            assert!(
                matches!(body["status"].as_str(), Some("pending" | "dispatching")),
                "unexpected schedule outcome: {body}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("real scheduler did not complete within bounded acceptance window")
}

//! Loopback-only fixture for the cross-repository announcement bridge test.
//! This is a test example, not a deployable service.
use async_trait::async_trait;
use axum::{Json, Router, extract::State, routing::post};
use discordy_messages_application::PortError;
use discordy_messages_application::fakes::{Harness, MEMBER, guild_access};
use discordy_messages_application::ports::{
    AccessError, AuthError, Authenticator, ChannelAccessProvider, MessageRepository, Principal,
};
use discordy_messages_domain::{ChannelAccess, ChannelId, Message, MessageDraft, MessageId, Snowflake, UserId};
use discordy_messages_interface::{AppState, AttachmentUrls, InternalToken, metrics::Metrics, router};
use serde::Deserialize;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::Duration;

struct NoAuth;
#[async_trait]
impl Authenticator for NoAuth {
    async fn authenticate(&self, _: &str) -> Result<Principal, AuthError> {
        Err(AuthError::Invalid)
    }
}

struct FixtureAccess(Arc<AtomicU64>);
#[async_trait]
impl ChannelAccessProvider for FixtureAccess {
    async fn access(&self, channel: ChannelId, _: UserId) -> Result<ChannelAccess, AccessError> {
        self.channel(channel).await
    }
    async fn channel(&self, channel: ChannelId) -> Result<ChannelAccess, AccessError> {
        let mut access = guild_access(MEMBER);
        access.channel_type = if channel.value() == self.0.load(Ordering::SeqCst) {
            5
        } else {
            0
        };
        Ok(access)
    }
    async fn record_thread_activity(&self, _: ChannelId, _: MessageId) -> Result<(), PortError> {
        Ok(())
    }
}

struct Fixture {
    harness: Harness,
    source: Arc<AtomicU64>,
}
#[derive(Deserialize)]
struct Seed {
    channel_id: String,
    message_id: String,
    author_id: String,
    content: String,
}
async fn seed(State(fixture): State<Arc<Fixture>>, Json(body): Json<Seed>) {
    let channel: Snowflake = body.channel_id.parse().unwrap();
    let id: Snowflake = body.message_id.parse().unwrap();
    let author: Snowflake = body.author_id.parse().unwrap();
    fixture.source.store(channel.value(), Ordering::SeqCst);
    let message = Message::compose(
        id,
        channel,
        author,
        MessageDraft {
            content: body.content,
            ..MessageDraft::default()
        },
        false,
    )
    .unwrap();
    fixture.harness.messages.insert(&message).await.unwrap();
}

#[tokio::main]
async fn main() {
    let mut harness = Harness::new();
    let source = Arc::new(AtomicU64::new(0));
    harness.ports.access = Arc::new(FixtureAccess(source.clone()));
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let metrics = Metrics::new(recorder.handle());
    let app = router(AppState::new(
        &harness.ports,
        Arc::new(NoAuth),
        InternalToken::new("test-internal-token"),
        AttachmentUrls::new(
            "http://fixture",
            "fixture-signing-key-0123456789abcdef",
            Duration::from_secs(3600),
            harness.clock.clone(),
        ),
        harness.media.clone(),
        vec![],
        metrics,
    ));
    let fixture = Arc::new(Fixture { harness, source });
    let app = app.merge(Router::new().route("/fixture/seed", post(seed)).with_state(fixture));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    println!("{}", listener.local_addr().unwrap());
    axum::serve(listener, app).await.unwrap();
}

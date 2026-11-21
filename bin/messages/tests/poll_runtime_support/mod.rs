use async_trait::async_trait;
use discordy_messages_application::{
    PortError,
    fakes::{ALICE, BOB, Harness},
    ports::{AuthError, Authenticator, EventSink, Principal},
};
use discordy_messages_interface::{
    AppState, AttachmentUrls, EnvelopePublisher, InternalToken, ReadinessCheck, metrics::Metrics, router,
};
use serde_json::{Value, json};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::time::Duration;

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
#[derive(Default)]
pub struct Sink {
    pub events: Mutex<Vec<Value>>,
    pub fail_final: AtomicBool,
    pub final_attempts: AtomicUsize,
}
#[async_trait]
impl EventSink for Sink {
    async fn send(&self, _: &str, _: &str, payload: Vec<u8>) -> Result<(), PortError> {
        let event: Value = serde_json::from_slice(&payload).unwrap();
        if event["data"]["poll"]["results"]["is_finalized"] == true {
            self.final_attempts.fetch_add(1, Ordering::SeqCst);
            if self.fail_final.load(Ordering::SeqCst) {
                return Err(PortError::new("injected final transport failure"));
            }
        }
        self.events.lock().unwrap().push(event);
        Ok(())
    }
}
pub fn wire_events(h: &mut Harness, sink: Arc<Sink>) {
    h.ports.events = Arc::new(EnvelopePublisher::new(sink, h.clock.clone(), urls(h), h.users.clone()));
}
fn urls(h: &Harness) -> AttachmentUrls {
    AttachmentUrls::new(
        "http://cdn.test",
        "fixture-signing-key-at-least-thirty-two-characters",
        Duration::from_secs(60),
        h.clock.clone(),
    )
}
pub struct Server {
    pub base: String,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Server {
    pub async fn new(h: &Harness, probes: Vec<Arc<dyn ReadinessCheck>>) -> Self {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let state = AppState::new(
            &h.ports,
            Arc::new(Auth),
            InternalToken::new("fixture-internal"),
            urls(h),
            h.media.clone(),
            probes,
            Metrics::new(recorder.handle()),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            axum::serve(listener, router(state)).await.unwrap();
        });
        Self { base, task }
    }
}
pub fn body() -> Value {
    json!({"poll":{"question":{"text":"Route?"},"answers":[{"poll_media":{"text":"Forest"}},{"poll_media":{"text":"River","emoji":{"name":"🌊"}}}],"duration":1}})
}
pub async fn request(
    client: &reqwest::Client,
    method: reqwest::Method,
    url: &str,
    actor: &str,
    body: Option<&Value>,
    status: u16,
) -> Value {
    let mut request = client.request(method, url).bearer_auth(actor);
    if let Some(body) = body {
        request = request.json(body);
    }
    let response = request.send().await.unwrap();
    let actual = response.status();
    let bytes = response.bytes().await.unwrap();
    let value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap()
    };
    assert_eq!(actual.as_u16(), status, "{value}");
    value
}

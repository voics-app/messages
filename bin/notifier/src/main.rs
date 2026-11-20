//! Composition root of the notifier: configuration, wiring, probes and shutdown.

mod config;

use std::error::Error;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use notifier::application::{DispatchConfig, Dispatcher, EventDeliveryWorker, EventHandler, HandlerConfig, PushSender};
use notifier::infrastructure::api_client::ApiClient;
use notifier::infrastructure::apns::{ApnsConfig, ApnsSender};
use notifier::infrastructure::fcm::{FcmConfig, FcmSender};
use notifier::infrastructure::kafka::MessageTopic;
use notifier::infrastructure::routing::RoutingSender;
use tracing_subscriber::EnvFilter;

use config::{ApnsEnvironment, Config};

#[derive(Clone)]
struct ProbeState {
    topic: Arc<MessageTopic>,
    metrics: PrometheusHandle,
}

async fn live() -> StatusCode {
    StatusCode::OK
}

async fn ready(State(state): State<ProbeState>) -> (StatusCode, String) {
    match state.topic.ping().await {
        Ok(()) => (StatusCode::OK, "ok".to_owned()),
        Err(error) => (StatusCode::SERVICE_UNAVAILABLE, error.to_string()),
    }
}

async fn scrape(State(state): State<ProbeState>) -> String {
    state.metrics.render()
}

fn now_unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX))
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        if let Ok(mut terminate) = signal(SignalKind::terminate()) {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = terminate.recv() => {}
            }
            return;
        }
    }
    let _ = tokio::signal::ctrl_c().await;
}

async fn run(config: Config) -> Result<(), Box<dyn Error>> {
    let metrics = PrometheusBuilder::new().install_recorder()?;
    // Each provider is optional: one without credentials only skips its own tokens.
    let apns: Option<Arc<dyn PushSender>> = match &config.apns {
        Some(credentials) => {
            let mut apns = ApnsConfig::new(
                credentials.key_pem.clone(),
                credentials.key_id.clone(),
                credentials.team_id.clone(),
                config.apns_topic.clone(),
            );
            apns.environment_override = config.apns_environment.map(|environment| match environment {
                ApnsEnvironment::Production => notifier::application::Environment::Production,
                ApnsEnvironment::Sandbox => notifier::application::Environment::Sandbox,
            });
            Some(Arc::new(ApnsSender::new(apns)?))
        }
        None => {
            tracing::warn!("APNs is not configured: iOS tokens are skipped");
            None
        }
    };
    let fcm: Option<Arc<dyn PushSender>> = match &config.fcm_service_account {
        Some(json) => Some(Arc::new(FcmSender::new(FcmConfig::new(json.clone()))?)),
        None => {
            tracing::warn!("FCM is not configured: Android tokens are skipped");
            None
        }
    };
    let sender = Arc::new(RoutingSender::new(apns, fcm));
    let api = Arc::new(ApiClient::new(
        &config.api_internal_url,
        config.internal_service_token.clone(),
    )?);
    tokio::spawn(EventDeliveryWorker::new(api.clone(), sender.clone(), api.clone()).run());
    let dispatcher = Dispatcher::spawn(
        DispatchConfig {
            queue_capacity: config.queue_capacity,
            max_in_flight: config.max_in_flight,
            max_attempts: config.max_attempts,
            base_backoff: config.base_backoff,
            max_backoff: config.max_backoff,
        },
        sender,
        api.clone(),
    );
    let handler = Arc::new(EventHandler::new(
        api,
        dispatcher,
        HandlerConfig {
            max_event_age: config.max_event_age,
            dedupe_capacity: config.dedupe_capacity,
        },
        Arc::new(now_unix_ms),
    ));
    let topic = Arc::new(MessageTopic::connect(config.kafka_brokers.clone()).await?);
    topic.spawn(handler).await?;

    let app = Router::new()
        .route("/healthz", get(live))
        .route("/readyz", get(ready))
        .route("/metrics", get(scrape))
        .with_state(ProbeState { topic, metrics });
    let listener = tokio::net::TcpListener::bind(config.bind_addr).await?;
    tracing::info!(addr = %config.bind_addr, "notifier listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

#[tokio::main]
async fn main() {
    let config = match Config::from_env() {
        Ok(config) => config,
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(2);
        }
    };
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(EnvFilter::new(&config.log_level))
        .init();
    if let Err(error) = run(config).await {
        tracing::error!(%error, "notifier stopped");
        std::process::exit(1);
    }
}

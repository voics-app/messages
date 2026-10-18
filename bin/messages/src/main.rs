//! Composition root of the messages service: reads configuration, builds the
//! adapters, wires them into the use cases and serves the HTTP interface.

mod config;
mod probes;

use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use config::{Config, Environment};
use discordy_messages_application::use_cases::MessagingPorts;
use discordy_messages_application::{HistoryReader, PortError};
use discordy_messages_infrastructure::api::{HttpChannelAccess, HttpUserDirectory, InternalApi, TokenAuthenticator};
use discordy_messages_infrastructure::kafka::KafkaEventSink;
use discordy_messages_infrastructure::scylla::{
    self, ScyllaActivityRepository, ScyllaMessageRepository, ScyllaPinRepository, ScyllaReactionRepository,
    ScyllaReadStateRepository,
};
use discordy_messages_infrastructure::storage::{S3AttachmentStorage, S3Settings};
use discordy_messages_infrastructure::system::{SnowflakeIds, SystemClock};
use discordy_messages_interface::cors::{self, InvalidOrigin};
use discordy_messages_interface::{AppState, AttachmentUrls, EnvelopePublisher, InternalToken, ReadinessCheck, router};
use probes::{KafkaProbe, ScyllaProbe};
use tracing_subscriber::EnvFilter;

const HTTP_TIMEOUT: Duration = Duration::from_secs(5);

#[tokio::main]
async fn main() -> ExitCode {
    let config = match Config::from_env() {
        Ok(config) => config,
        Err(error) => {
            eprintln!("{error}");
            return ExitCode::from(2);
        }
    };
    init_tracing(&config);
    match run(config).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            tracing::error!(%error, "messages service stopped");
            ExitCode::FAILURE
        }
    }
}

fn init_tracing(config: &Config) {
    let filter = EnvFilter::try_new(&config.log_level).unwrap_or_else(|_| EnvFilter::new("info"));
    let builder = tracing_subscriber::fmt().with_env_filter(filter);
    match config.environment {
        Environment::Production => builder.json().init(),
        Environment::Local => builder.init(),
    }
}

#[derive(Debug, thiserror::Error)]
enum StartupError {
    #[error(transparent)]
    Port(#[from] PortError),
    #[error("http client: {0}")]
    HttpClient(#[from] reqwest::Error),
    #[error("listener: {0}")]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Cors(#[from] InvalidOrigin),
}

async fn run(config: Config) -> Result<(), StartupError> {
    let http = reqwest::Client::builder().timeout(HTTP_TIMEOUT).build()?;
    let session = scylla::connect(&config.scylla_nodes, &config.scylla_keyspace).await?;
    let messages = Arc::new(ScyllaMessageRepository::new(session.clone()).await?);
    let reactions = Arc::new(ScyllaReactionRepository::new(session.clone()).await?);
    let kafka = Arc::new(KafkaEventSink::connect(config.kafka_brokers.clone()).await?);

    let storage = S3AttachmentStorage::new(
        S3Settings {
            internal_endpoint: config.minio_endpoint.clone(),
            public_endpoint: config.cdn_public_url.clone(),
            bucket: config.attachments_bucket.clone(),
            access_key: config.minio_access_key.clone(),
            secret_key: config.minio_secret_key.clone(),
        },
        http.clone(),
    )?;
    if config.environment == Environment::Local {
        storage.ensure_bucket().await?;
    }

    let api = InternalApi::new(http, &config.api_internal_url, config.internal_service_token.clone());
    let authenticator = Arc::new(TokenAuthenticator::new(&config.jwt_public_key, api.clone())?);
    let clock = Arc::new(SystemClock);
    let urls = AttachmentUrls::new(&config.cdn_public_url, &config.attachments_bucket);
    let users = Arc::new(HttpUserDirectory::new(api.clone()));
    let ports = MessagingPorts {
        access: Arc::new(HttpChannelAccess::new(api)),
        messages: messages.clone(),
        reactions: reactions.clone(),
        pins: Arc::new(ScyllaPinRepository::new(session.clone()).await?),
        read_states: Arc::new(ScyllaReadStateRepository::new(session.clone()).await?),
        activity: Arc::new(ScyllaActivityRepository::new(session.clone()).await?),
        storage: Arc::new(storage),
        events: Arc::new(EnvelopePublisher::new(
            kafka.clone(),
            clock.clone(),
            urls.clone(),
            users.clone(),
        )),
        ids: Arc::new(SnowflakeIds::new(config.worker)),
        clock,
        history: Arc::new(HistoryReader::new(messages, reactions)),
        users,
    };
    let probes: Vec<Arc<dyn ReadinessCheck>> = vec![Arc::new(ScyllaProbe(session)), Arc::new(KafkaProbe(kafka))];
    let state = AppState::new(
        &ports,
        authenticator,
        InternalToken::new(&config.internal_service_token),
        urls,
        probes,
    );

    let cors = cors::layer(&config.cors_allowed_origins)?;
    let listener = tokio::net::TcpListener::bind(config.bind_addr).await?;
    tracing::info!(addr = %config.bind_addr, worker = config.worker.value(), "messages service listening");
    axum::serve(listener, router(state).layer(cors))
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    tracing::info!("messages service stopped gracefully");
    Ok(())
}

async fn shutdown_signal() {
    let interrupt = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        () = interrupt => {},
        () = terminate => {},
    }
}

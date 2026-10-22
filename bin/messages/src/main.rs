//! Composition root of the messages service: reads configuration, builds the
//! adapters, wires them into the use cases and serves the HTTP interface.

mod config;
mod probes;
mod telemetry;

use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use config::{Config, Environment};
use discordy_messages_application::ports::{BackfillState, SearchIndex};
use discordy_messages_application::use_cases::{MessagingPorts, PurgeAuthorMessages};
use discordy_messages_application::{HistoryReader, PortError};
use discordy_messages_infrastructure::api::{HttpChannelAccess, HttpUserDirectory, InternalApi, TokenAuthenticator};
use discordy_messages_infrastructure::ban_purge;
use discordy_messages_infrastructure::kafka::KafkaEventSink;
use discordy_messages_infrastructure::scylla::{
    self, ScyllaActivityRepository, ScyllaMessageRepository, ScyllaPinRepository, ScyllaReactionRepository,
    ScyllaReadStateRepository,
};
use discordy_messages_infrastructure::search::{self, DisabledSearchIndex, PgSearchIndex};
use discordy_messages_infrastructure::storage::{S3AttachmentStorage, S3Settings};
use discordy_messages_infrastructure::system::{SnowflakeIds, SystemClock};
use discordy_messages_infrastructure::unfurl::{HttpLinkUnfurler, HttpMediaFetcher, SafeHttp, SsrfPolicy};
use discordy_messages_interface::cors::{self, InvalidOrigin};
use discordy_messages_interface::{AppState, AttachmentUrls, EnvelopePublisher, InternalToken, ReadinessCheck, router};
use probes::{KafkaProbe, ScyllaProbe};
use tracing_subscriber::filter::filter_fn;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer};

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
    let metrics = match telemetry::install_recorder() {
        Ok(handle) => discordy_messages_interface::metrics::Metrics::new(handle),
        Err(error) => {
            eprintln!("metrics recorder: {error}");
            return ExitCode::FAILURE;
        }
    };
    match run(config, metrics).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            tracing::error!(%error, "messages service stopped");
            ExitCode::FAILURE
        }
    }
}

fn init_tracing(config: &Config) {
    let filter = EnvFilter::try_new(&config.log_level).unwrap_or_else(|_| EnvFilter::new("info"));
    let logs = match config.environment {
        Environment::Production => tracing_subscriber::fmt::layer().json().boxed(),
        Environment::Local => tracing_subscriber::fmt::layer().boxed(),
    }
    .with_filter(filter);
    let db = telemetry::PostgresLatency.with_filter(filter_fn(|meta| meta.target() == "sqlx::query"));
    tracing_subscriber::registry().with(logs).with(db).init();
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

async fn run(config: Config, metrics: discordy_messages_interface::metrics::Metrics) -> Result<(), StartupError> {
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
    let urls = AttachmentUrls::new(
        &config.cdn_public_url,
        &config.attachment_signing_key,
        config.attachment_url_ttl,
        clock.clone(),
    );
    let users = Arc::new(HttpUserDirectory::new(api.clone()));
    let (search_index, pg_index): (Arc<dyn SearchIndex>, Option<Arc<PgSearchIndex>>) = match &config.search_database_url
    {
        Some(url) => {
            let pg = Arc::new(PgSearchIndex::new(search::connect(url)?));
            (pg.clone(), Some(pg))
        }
        None => {
            tracing::warn!("SEARCH_DATABASE_URL is not set: message search is disabled");
            (Arc::new(DisabledSearchIndex), None)
        }
    };
    // One guarded client for everything that reaches third-party sites.
    let outbound = SafeHttp::new(SsrfPolicy::strict())?;
    let media = Arc::new(HttpMediaFetcher::new(outbound.clone()));
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
        search: search_index.clone(),
        unfurler: Arc::new(HttpLinkUnfurler::new(outbound)),
    };
    let probes: Vec<Arc<dyn ReadinessCheck>> = vec![Arc::new(ScyllaProbe(session)), Arc::new(KafkaProbe(kafka))];
    let state = AppState::new(
        &ports,
        authenticator,
        InternalToken::new(&config.internal_service_token),
        urls,
        media,
        probes,
        metrics,
    );

    if let Some(pg) = pg_index {
        tokio::spawn(run_search(
            pg,
            search_index,
            state.clone(),
            config.kafka_brokers.clone(),
            config.search_backfill_on_start,
        ));
    }

    // Ban "delete message history": the api asks for the purge through the guild topic.
    tokio::spawn(run_ban_purge(
        Arc::new(PurgeAuthorMessages::new(&ports)),
        ports.search.clone(),
        config.kafka_brokers.clone(),
    ));

    let cors = cors::layer(&config.cors_allowed_origins)?;
    let listener = tokio::net::TcpListener::bind(config.bind_addr).await?;
    tracing::info!(addr = %config.bind_addr, worker = config.worker.value(), "messages service listening");
    axum::serve(listener, router(state).layer(cors))
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    tracing::info!("messages service stopped gracefully");
    Ok(())
}

/// Brings the search index up in the background so a slow or absent
/// PostgreSQL never delays or breaks messaging: migrate, start the Redpanda
/// consumer, then rebuild from history when the index is not complete yet.
async fn run_search(
    pg: Arc<PgSearchIndex>,
    index: Arc<dyn SearchIndex>,
    state: AppState,
    brokers: Vec<String>,
    backfill_on_start: bool,
) {
    const RETRY: Duration = Duration::from_secs(5);
    while let Err(error) = pg.migrate().await {
        tracing::warn!(%error, "search database not ready, retrying");
        tokio::time::sleep(RETRY).await;
    }
    while let Err(error) = search::consumer::spawn(brokers.clone(), index.clone()).await {
        tracing::warn!(%error, "search consumer could not start, retrying");
        tokio::time::sleep(RETRY).await;
    }
    tracing::info!("search index ready");
    if !backfill_on_start {
        return;
    }
    match index.backfill_state().await {
        Ok(BackfillState::Complete) => {}
        Ok(_) => {
            if let Err(error) = state.use_cases.reindex.execute().await {
                tracing::error!(%error, "initial search reindex failed; POST /internal/v1/search/reindex to retry");
            }
        }
        Err(error) => tracing::warn!(%error, "could not read the search backfill state"),
    }
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

/// Starts the BAN_MESSAGE_PURGE consumer, retrying until the bus is reachable.
async fn run_ban_purge(purger: Arc<PurgeAuthorMessages>, offsets: Arc<dyn SearchIndex>, brokers: Vec<String>) {
    while let Err(error) = ban_purge::spawn(brokers.clone(), purger.clone(), offsets.clone()).await {
        tracing::warn!(%error, "ban purge consumer could not start, retrying");
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    }
}

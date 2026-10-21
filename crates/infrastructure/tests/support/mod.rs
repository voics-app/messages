//! Shared testcontainers helpers. Containers are named `messages-it-*` and
//! use random host ports; they are removed when the handle is dropped.

#![allow(dead_code)]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

pub const SCYLLA_IMAGE: (&str, &str) = ("scylladb/scylla", "2025.1");
pub const REDPANDA_IMAGE: (&str, &str) = ("docker.redpanda.com/redpandadata/redpanda", "v25.1.1");
pub const MINIO_IMAGE: (&str, &str) = ("pgsty/minio", "RELEASE.2026-08-04T00-00-00Z");

static NEXT: AtomicU32 = AtomicU32::new(0);

pub fn container_name(kind: &str) -> String {
    format!(
        "messages-it-{kind}-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}

pub struct Scylla {
    pub container: ContainerAsync<GenericImage>,
    pub session: Arc<Session>,
}

/// ScyllaDB aborts at boot without SSE4.2 and PCLMULQDQ; virtual CPUs such as
/// QEMU's default model lack them. Non-Linux hosts are assumed capable.
pub fn cpu_supports_scylla() -> bool {
    match std::fs::read_to_string("/proc/cpuinfo") {
        Ok(info) => info.contains("pclmulqdq") && info.contains("sse4_2"),
        Err(_) => true,
    }
}

/// Starts a single-node ScyllaDB in developer mode and returns a migrated
/// session bound to `keyspace`.
pub async fn start_scylla(keyspace: &str) -> Scylla {
    let container = GenericImage::new(SCYLLA_IMAGE.0, SCYLLA_IMAGE.1)
        .with_exposed_port(9042.tcp())
        .with_wait_for(WaitFor::message_on_either_std("Starting listening for CQL clients"))
        .with_cmd([
            "--smp",
            "1",
            "--developer-mode",
            "1",
            "--overprovisioned",
            "1",
            "--memory",
            "750M",
        ])
        .with_mapped_port(0, 9042.tcp())
        .with_container_name(container_name("scylla"))
        .with_startup_timeout(Duration::from_secs(240))
        .start()
        .await
        .expect("start scylla");
    let port = container.get_host_port_ipv4(9042.tcp()).await.expect("scylla port");
    let internal_ip = container.get_bridge_ip_address().await.expect("bridge ip");
    let local: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    // The node advertises its bridge address; the driver pairs it with either
    // the CQL port or the contact point's port, so translate both.
    let translator: HashMap<SocketAddr, SocketAddr> = HashMap::from([
        (SocketAddr::new(internal_ip, 9042), local),
        (SocketAddr::new(internal_ip, port), local),
    ]);
    let builder = SessionBuilder::new()
        .known_node_addr(local)
        .address_translator(Arc::new(translator));
    let session = discordy_messages_infrastructure::scylla::open(builder, keyspace)
        .await
        .expect("connect and migrate");
    Scylla { container, session }
}

pub const MINIO_USER: &str = "messages-it";
pub const MINIO_PASSWORD: &str = "messages-it-secret";

pub struct Minio {
    pub container: ContainerAsync<GenericImage>,
    pub endpoint: String,
}

/// Starts MinIO on a random host port.
pub async fn start_minio() -> Minio {
    let container = GenericImage::new(MINIO_IMAGE.0, MINIO_IMAGE.1)
        .with_exposed_port(9000.tcp())
        .with_wait_for(WaitFor::message_on_either_std("API:"))
        .with_env_var("MINIO_ROOT_USER", MINIO_USER)
        .with_env_var("MINIO_ROOT_PASSWORD", MINIO_PASSWORD)
        .with_cmd(["server", "/data"])
        .with_container_name(container_name("minio"))
        .with_startup_timeout(Duration::from_secs(300))
        .start()
        .await
        .expect("start minio");
    let port = container.get_host_port_ipv4(9000.tcp()).await.expect("minio port");
    Minio {
        container,
        endpoint: format!("http://127.0.0.1:{port}"),
    }
}

pub struct Redpanda {
    pub container: ContainerAsync<GenericImage>,
    pub broker: String,
}

/// Starts Redpanda advertising the host port it is published on, so clients
/// that follow broker metadata stay reachable from the test process.
pub async fn start_redpanda() -> Redpanda {
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .and_then(|listener| listener.local_addr())
        .expect("free port")
        .port();
    let container = GenericImage::new(REDPANDA_IMAGE.0, REDPANDA_IMAGE.1)
        .with_exposed_port(9092.tcp())
        .with_wait_for(WaitFor::message_on_either_std("Successfully started Redpanda"))
        .with_cmd([
            "redpanda".to_owned(),
            "start".to_owned(),
            "--smp=1".to_owned(),
            "--memory=512M".to_owned(),
            "--overprovisioned".to_owned(),
            "--mode=dev-container".to_owned(),
            "--kafka-addr=0.0.0.0:9092".to_owned(),
            format!("--advertise-kafka-addr=127.0.0.1:{port}"),
        ])
        .with_mapped_port(port, 9092.tcp())
        .with_container_name(container_name("redpanda"))
        .with_startup_timeout(Duration::from_secs(300))
        .start()
        .await
        .expect("start redpanda");
    Redpanda {
        container,
        broker: format!("127.0.0.1:{port}"),
    }
}

pub const POSTGRES_IMAGE: (&str, &str) = ("postgres", "17-alpine");

pub struct Postgres {
    pub container: ContainerAsync<GenericImage>,
    pub url: String,
}

/// Starts PostgreSQL on a random host port.
pub async fn start_postgres() -> Postgres {
    let container = GenericImage::new(POSTGRES_IMAGE.0, POSTGRES_IMAGE.1)
        .with_exposed_port(5432.tcp())
        .with_wait_for(WaitFor::message_on_either_std(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_PASSWORD", "messages-it")
        .with_env_var("POSTGRES_DB", "search")
        .with_container_name(container_name("postgres"))
        .with_startup_timeout(Duration::from_secs(120))
        .start()
        .await
        .expect("start postgres");
    // The image restarts the server once after initialising; wait for that second start.
    tokio::time::sleep(Duration::from_secs(4)).await;
    let port = container.get_host_port_ipv4(5432.tcp()).await.expect("postgres port");
    Postgres {
        container,
        url: format!("postgres://postgres:messages-it@127.0.0.1:{port}/search"),
    }
}

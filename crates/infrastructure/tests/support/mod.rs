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
pub const MINIO_IMAGE: (&str, &str) = ("pgsty/minio", "latest");

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

//! Readiness probes for the adapters the service cannot work without.

use std::sync::Arc;

use async_trait::async_trait;
use discordy_messages_application::PortError;
use discordy_messages_infrastructure::kafka::KafkaEventSink;
use discordy_messages_infrastructure::scylla;
use discordy_messages_interface::ReadinessCheck;

pub struct ScyllaProbe(pub Arc<scylla::Session>);

#[async_trait]
impl ReadinessCheck for ScyllaProbe {
    fn name(&self) -> &'static str {
        "scylla"
    }

    async fn check(&self) -> Result<(), PortError> {
        scylla::ping(&self.0).await
    }
}

pub struct KafkaProbe(pub Arc<KafkaEventSink>);

#[async_trait]
impl ReadinessCheck for KafkaProbe {
    fn name(&self) -> &'static str {
        "redpanda"
    }

    async fn check(&self) -> Result<(), PortError> {
        self.0.ping().await
    }
}

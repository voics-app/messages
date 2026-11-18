//! Readiness probes for the adapters the service cannot work without.

use std::sync::Arc;

use async_trait::async_trait;
use discordy_messages_application::PortError;
use discordy_messages_infrastructure::kafka::KafkaEventSink;
use discordy_messages_infrastructure::scylla;
use discordy_messages_interface::ReadinessCheck;

pub struct ScyllaProbe(pub Arc<scylla::Session>);

pub struct ReadStateProbe(pub Arc<scylla::ScyllaReadStateRepository>);

pub struct ScheduledProbe {
    pub repository: Arc<scylla::ScyllaScheduledMessages>,
    pub running: Arc<std::sync::atomic::AtomicBool>,
}

#[async_trait]
impl ReadinessCheck for ScheduledProbe {
    fn name(&self) -> &'static str {
        "scheduled_messages"
    }
    async fn check(&self) -> Result<(), PortError> {
        if !self.running.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(PortError::new("scheduled worker is not running"));
        }
        self.repository.check_ready().await
    }
}

#[async_trait]
impl ReadinessCheck for ReadStateProbe {
    fn name(&self) -> &'static str {
        "read_states"
    }

    async fn check(&self) -> Result<(), PortError> {
        self.0.check_ready().await
    }
}

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

pub struct IdProbe(pub Arc<dyn discordy_messages_application::ports::IdGenerator>);
#[async_trait]
impl ReadinessCheck for IdProbe {
    fn name(&self) -> &'static str {
        "ids"
    }
    async fn check(&self) -> Result<(), PortError> {
        if self.0.is_ready() {
            Ok(())
        } else {
            Err(PortError::new("ID reservation unavailable"))
        }
    }
}

#[async_trait]
impl ReadinessCheck for KafkaProbe {
    fn name(&self) -> &'static str {
        "redpanda"
    }

    async fn check(&self) -> Result<(), PortError> {
        self.0.ping().await
    }
}

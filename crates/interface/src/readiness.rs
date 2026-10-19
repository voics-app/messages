//! Dependency probes behind `/readyz`.

use async_trait::async_trait;
use discordy_messages_application::PortError;

#[async_trait]
pub trait ReadinessCheck: Send + Sync {
    fn name(&self) -> &'static str;

    async fn check(&self) -> Result<(), PortError>;
}

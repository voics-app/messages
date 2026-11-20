use crate::PortError;
use discordy_messages_domain::Snowflake;

pub trait IdGenerator: Send + Sync {
    fn next_id(&self) -> Result<Snowflake, PortError>;
    fn is_ready(&self) -> bool;
}

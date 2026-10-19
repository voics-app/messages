use discordy_messages_domain::Snowflake;

pub trait IdGenerator: Send + Sync {
    fn next_id(&self) -> Snowflake;
}

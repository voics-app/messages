use super::MessagingPorts;
use crate::{AppResult, ports::MessageRepository};
use discordy_messages_domain::GuildId;
use std::sync::Arc;

pub struct GetGuildMessageActivity {
    messages: Arc<dyn MessageRepository>,
}

impl GetGuildMessageActivity {
    pub fn new(ports: &MessagingPorts) -> Self {
        Self {
            messages: ports.messages.clone(),
        }
    }
    pub async fn execute(&self, guild_id: GuildId, utc_day: i64) -> AppResult<(u64, Option<u64>)> {
        let count = self.messages.guild_message_count(guild_id, utc_day).await?;
        let initialized = self.messages.guild_activity_source_initialized_ms().await?;
        Ok((count, initialized))
    }
}

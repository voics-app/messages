use super::ScyllaPollRepository;
use discordy_messages_application::{
    PortError,
    ports::{PollKey, PollRepository},
};
use discordy_messages_domain::Snowflake;

impl ScyllaPollRepository {
    /// Bounded read-only probes exercise the serial metadata table and due index.
    pub async fn check_ready(&self) -> Result<(), PortError> {
        self.read(
            PollKey {
                channel_id: Snowflake::new(1),
                message_id: Snowflake::new(1),
            },
            0,
        )
        .await
        .map_err(PortError::new)?;
        self.due_page(0, None, 1).await.map_err(PortError::new)?;
        Ok(())
    }
}

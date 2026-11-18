//! Mirrors contracts notification-recipient(-page).json, introduced by contracts#44.

use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NotificationRecipientDto {
    pub user_id: String,
    pub role_ids: Vec<String>,
    pub online: bool,
    pub level: u8,
    pub muted: bool,
    pub suppress_everyone: bool,
    pub suppress_roles: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NotificationRecipientPageDto {
    pub channel_id: String,
    #[serde(deserialize_with = "Option::deserialize")]
    pub guild_id: Option<String>,
    pub delivery_mode: String,
    pub evaluated_at: String,
    pub recipients: Vec<NotificationRecipientDto>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub next_after: Option<String>,
}

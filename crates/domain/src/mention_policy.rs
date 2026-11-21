use crate::{Message, RoleId, UserId};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NotificationLevel {
    AllMessages,
    OnlyMentions,
    NoMessages,
}

/// Current permission-filtered policy, independent of device-token ownership.
#[derive(Clone, Debug)]
pub struct NotificationRecipient {
    pub user_id: UserId,
    pub role_ids: Vec<RoleId>,
    pub online: bool,
    pub level: NotificationLevel,
    pub muted: bool,
    pub suppress_everyone: bool,
    pub suppress_roles: bool,
}

impl NotificationRecipient {
    /// AllMessages permits alerts; it does not turn plain guild posts into mentions.
    /// Presence only qualifies @here and never establishes mobile foreground state.
    pub fn counts_mention(&self, message: &Message, is_direct: bool) -> bool {
        if self.user_id == message.author_id || self.muted || self.level == NotificationLevel::NoMessages {
            return false;
        }
        is_direct
            || message.mentions.users.contains(&self.user_id)
            || (!self.suppress_roles && self.role_ids.iter().any(|role| message.mentions.roles.contains(role)))
            || (!self.suppress_everyone && message.mentions.everyone && (!message.mentions.here || self.online))
    }
}

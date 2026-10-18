//! Author hydration: message authors travel as full user objects.

use std::collections::{HashMap, HashSet};

use contracts::AuthorDto;
use discordy_messages_application::MessageView;
use discordy_messages_application::ports::{UserDirectory, UserProfile};
use discordy_messages_domain::{Message, UserId};

/// Profiles resolved for one response or event.
pub type Authors = HashMap<UserId, UserProfile>;

/// Resolves the authors of `messages` with a single batch lookup. Ids that
/// cannot be resolved are absent from the result.
pub async fn resolve<'a>(users: &dyn UserDirectory, messages: impl IntoIterator<Item = &'a Message>) -> Authors {
    let ids: Vec<UserId> = messages
        .into_iter()
        .map(|message| message.author_id)
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    if ids.is_empty() {
        return Authors::new();
    }
    users.lookup(&ids).await
}

pub async fn resolve_views(users: &dyn UserDirectory, views: &[MessageView]) -> Authors {
    resolve(users, views.iter().map(|view| &view.message)).await
}

/// The author object of `message`. Without a profile it degrades to `{id}`.
/// A webhook message keeps the username stored with it.
pub fn author_dto(message: &Message, authors: &Authors) -> AuthorDto {
    let mut author = AuthorDto {
        id: message.author_id.to_string(),
        username: None,
        discriminator: None,
        global_name: None,
        avatar: None,
        bot: None,
        system: None,
        public_flags: None,
    };
    if let Some(profile) = authors.get(&message.author_id) {
        author.username = Some(profile.username.clone());
        author.discriminator = Some("0".to_owned());
        author.global_name = profile.global_name.clone();
        author.avatar = profile.avatar.clone();
        author.bot = Some(profile.bot);
        author.system = Some(profile.system);
        author.public_flags = Some(profile.flags);
    }
    if let Some(webhook) = &message.webhook {
        if let Some(name) = &webhook.username {
            author.username = Some(name.clone());
            author.global_name = None;
            author.discriminator = Some("0".to_owned());
        }
        author.bot = Some(true);
    }
    author
}

#[cfg(test)]
mod tests {
    use super::*;
    use discordy_messages_domain::{MessageDraft, Snowflake, WebhookAuthor};

    fn message(webhook: Option<WebhookAuthor>) -> Message {
        let draft = MessageDraft {
            content: "hi".into(),
            ..MessageDraft::default()
        };
        let mut message = Message::compose(
            Snowflake::new(1 << 22),
            Snowflake::new(5),
            Snowflake::new(6),
            draft,
            false,
        )
        .unwrap();
        message.webhook = webhook;
        message
    }

    fn profile() -> UserProfile {
        UserProfile {
            id: Snowflake::new(6),
            username: "alice".into(),
            global_name: Some("Alice".into()),
            avatar: Some("abc".into()),
            bot: false,
            system: false,
            flags: 0,
        }
    }

    #[test]
    fn known_authors_carry_their_public_profile() {
        let authors = Authors::from([(Snowflake::new(6), profile())]);
        let author = author_dto(&message(None), &authors);
        assert_eq!(author.id, "6");
        assert_eq!(author.username.as_deref(), Some("alice"));
        assert_eq!(author.global_name.as_deref(), Some("Alice"));
        assert_eq!(author.avatar.as_deref(), Some("abc"));
        assert_eq!(author.bot, Some(false));
    }

    #[test]
    fn unknown_authors_degrade_to_the_id() {
        let author = author_dto(&message(None), &Authors::new());
        assert_eq!(author.id, "6");
        assert_eq!(author.username, None);
        let json = serde_json::to_value(&author).unwrap();
        assert_eq!(json, serde_json::json!({"id": "6"}));
    }

    #[test]
    fn webhook_messages_keep_their_stored_username() {
        let webhook = WebhookAuthor {
            webhook_id: Snowflake::new(9),
            username: Some("Deploy bot".into()),
            avatar_url: None,
        };
        let authors = Authors::from([(Snowflake::new(6), profile())]);
        let author = author_dto(&message(Some(webhook)), &authors);
        assert_eq!(author.username.as_deref(), Some("Deploy bot"));
        assert_eq!(author.global_name, None);
        assert_eq!(author.bot, Some(true));
    }
}

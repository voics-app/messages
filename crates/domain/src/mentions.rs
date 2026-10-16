//! Mention extraction from message content, using Discord's markup:
//! `<@id>` / `<@!id>` for users, `<@&id>` for roles, `@everyone` / `@here`.

use std::collections::BTreeSet;

use crate::{RoleId, Snowflake, UserId};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Mentions {
    pub users: BTreeSet<UserId>,
    pub roles: BTreeSet<RoleId>,
    pub everyone: bool,
}

impl Mentions {
    /// Extracts mentions. `@everyone`/`@here` only count when the author is
    /// allowed to use them.
    pub fn parse(content: &str, may_mention_everyone: bool) -> Self {
        let mut mentions = Self {
            everyone: may_mention_everyone
                && (content.contains("@everyone") || content.contains("@here")),
            ..Self::default()
        };
        let mut rest = content;
        while let Some(start) = rest.find("<@") {
            rest = &rest[start + 2..];
            let Some(end) = rest.find('>') else { break };
            mentions.record(&rest[..end]);
            rest = &rest[end + 1..];
        }
        mentions
    }

    fn record(&mut self, tag: &str) {
        let (is_role, digits) = match tag.as_bytes().first() {
            Some(b'&') => (true, &tag[1..]),
            Some(b'!') => (false, &tag[1..]),
            _ => (false, tag),
        };
        if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
            return;
        }
        let Ok(value) = digits.parse::<u64>() else { return };
        let id = Snowflake::new(value);
        if is_role {
            self.roles.insert(id);
        } else {
            self.users.insert(id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ids(values: &[u64]) -> BTreeSet<Snowflake> {
        values.iter().copied().map(Snowflake::new).collect()
    }

    #[test]
    fn extracts_user_and_nickname_mentions() {
        let m = Mentions::parse("hi <@123> and <@!456>", false);
        assert_eq!(m.users, ids(&[123, 456]));
        assert!(m.roles.is_empty());
    }

    #[test]
    fn extracts_role_mentions() {
        let m = Mentions::parse("ping <@&77>", false);
        assert_eq!(m.roles, ids(&[77]));
        assert!(m.users.is_empty());
    }

    #[test]
    fn ignores_malformed_tags() {
        let m = Mentions::parse("<@> <@abc> <@12 <@&> trailing <@99", false);
        assert!(m.users.is_empty());
        assert!(m.roles.is_empty());
    }

    #[test]
    fn deduplicates_repeated_mentions() {
        let m = Mentions::parse("<@1> <@1> <@!1>", false);
        assert_eq!(m.users, ids(&[1]));
    }

    #[test]
    fn everyone_requires_permission() {
        assert!(!Mentions::parse("@everyone look", false).everyone);
        assert!(Mentions::parse("@everyone look", true).everyone);
        assert!(Mentions::parse("@here look", true).everyone);
        assert!(!Mentions::parse("hello", true).everyone);
    }
}

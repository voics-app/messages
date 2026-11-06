//! Who gets a push: the clients' notification rules (`incomingAlert`, `shouldAlert`) applied to
//! one recipient. The api service resolves the level and the mute for the channel (channel
//! override, category, guild, server default); this module only combines them with the message.

use super::message::IncomingMessage;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Level {
    AllMessages,
    OnlyMentions,
    NoMessages,
}

impl Level {
    /// The wire code of `message_notifications`: 0 all, 1 mentions, 2 nothing.
    pub fn from_code(code: u8) -> Self {
        match code {
            0 => Level::AllMessages,
            1 => Level::OnlyMentions,
            _ => Level::NoMessages,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Recipient {
    pub user_id: String,
    pub role_ids: Vec<String>,
    /// The user holds a live gateway session. It says nothing about the phone: a desktop can be
    /// open, or the socket of a backgrounded app can linger, so it never holds back a message push.
    /// The app drops the banner itself while it is in the foreground.
    pub online: bool,
    pub level: Level,
    pub muted: bool,
    pub suppress_everyone: bool,
    pub suppress_roles: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Skip {
    Muted,
    LevelNothing,
    NotMentioned,
}

impl Skip {
    pub const fn label(self) -> &'static str {
        match self {
            Skip::Muted => "muted",
            Skip::LevelNothing => "level_nothing",
            Skip::NotMentioned => "not_mentioned",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    Notify,
    Skip(Skip),
}

/// DMs, direct mentions, a mention of one of the recipient's roles and `@everyone` count as
/// mentions unless the recipient suppressed them. `@here` only pings members who are online when
/// it is sent, so a push never treats it as a mention.
pub fn is_mentioned(message: &IncomingMessage, recipient: &Recipient) -> bool {
    if message.is_direct() || message.mentions.contains(&recipient.user_id) {
        return true;
    }
    if !recipient.suppress_roles
        && message
            .mention_roles
            .iter()
            .any(|role| recipient.role_ids.contains(role))
    {
        return true;
    }
    message.mention_everyone && !message.mention_here && !recipient.suppress_everyone
}

pub fn decide(message: &IncomingMessage, recipient: &Recipient) -> Verdict {
    if recipient.muted {
        return Verdict::Skip(Skip::Muted);
    }
    match recipient.level {
        Level::NoMessages => Verdict::Skip(Skip::LevelNothing),
        Level::OnlyMentions if !is_mentioned(message, recipient) => Verdict::Skip(Skip::NotMentioned),
        _ => Verdict::Notify,
    }
}

#[cfg(test)]
mod tests {
    use super::super::message::fixtures::message;
    use super::*;
    use serde_json::json;

    fn recipient() -> Recipient {
        Recipient {
            user_id: "42".into(),
            role_ids: vec!["900".into()],
            online: false,
            level: Level::AllMessages,
            muted: false,
            suppress_everyone: false,
            suppress_roles: false,
        }
    }

    fn only_mentions() -> Recipient {
        Recipient {
            level: Level::OnlyMentions,
            ..recipient()
        }
    }

    #[test]
    fn level_all_notifies_every_message() {
        assert_eq!(decide(&message(json!({})), &recipient()), Verdict::Notify);
    }

    #[test]
    fn a_live_session_does_not_hold_back_a_push() {
        let online = Recipient {
            online: true,
            ..recipient()
        };
        let ping = message(json!({"mentions": [{"id": "42"}]}));
        assert_eq!(decide(&ping, &online), Verdict::Notify);
    }

    #[test]
    fn a_mute_silences_even_a_direct_mention() {
        let muted = Recipient {
            muted: true,
            ..recipient()
        };
        let ping = message(json!({"mentions": [{"id": "42"}]}));
        assert_eq!(decide(&ping, &muted), Verdict::Skip(Skip::Muted));
    }

    #[test]
    fn level_nothing_never_notifies() {
        let none = Recipient {
            level: Level::NoMessages,
            ..recipient()
        };
        let ping = message(json!({"mentions": [{"id": "42"}]}));
        assert_eq!(decide(&ping, &none), Verdict::Skip(Skip::LevelNothing));
    }

    #[test]
    fn only_mentions_needs_a_mention() {
        assert_eq!(
            decide(&message(json!({})), &only_mentions()),
            Verdict::Skip(Skip::NotMentioned)
        );
        let direct = message(json!({"mentions": [{"id": "42"}]}));
        assert_eq!(decide(&direct, &only_mentions()), Verdict::Notify);
        let other = message(json!({"mentions": [{"id": "43"}]}));
        assert_eq!(decide(&other, &only_mentions()), Verdict::Skip(Skip::NotMentioned));
    }

    #[test]
    fn role_and_everyone_mentions_respect_the_suppress_settings() {
        let role = message(json!({"mention_roles": ["900"]}));
        assert_eq!(decide(&role, &only_mentions()), Verdict::Notify);
        let no_roles = Recipient {
            suppress_roles: true,
            ..only_mentions()
        };
        assert_eq!(decide(&role, &no_roles), Verdict::Skip(Skip::NotMentioned));
        let unrelated = message(json!({"mention_roles": ["901"]}));
        assert_eq!(decide(&unrelated, &only_mentions()), Verdict::Skip(Skip::NotMentioned));

        let everyone = message(json!({"mention_everyone": true}));
        assert_eq!(decide(&everyone, &only_mentions()), Verdict::Notify);
        let no_everyone = Recipient {
            suppress_everyone: true,
            ..only_mentions()
        };
        assert_eq!(decide(&everyone, &no_everyone), Verdict::Skip(Skip::NotMentioned));
    }

    #[test]
    fn at_here_does_not_reach_a_member_who_is_offline() {
        let here = message(json!({"mention_everyone": true, "mention_here": true}));
        assert_eq!(decide(&here, &only_mentions()), Verdict::Skip(Skip::NotMentioned));
        assert_eq!(decide(&here, &recipient()), Verdict::Notify, "level all still notifies");
    }

    #[test]
    fn every_direct_message_counts_as_a_mention() {
        let dm = message(json!({"guild_id": null}));
        assert_eq!(decide(&dm, &only_mentions()), Verdict::Notify);
    }

    #[test]
    fn level_codes_follow_the_wire_values() {
        assert_eq!(Level::from_code(0), Level::AllMessages);
        assert_eq!(Level::from_code(1), Level::OnlyMentions);
        assert_eq!(Level::from_code(2), Level::NoMessages);
        assert_eq!(Level::from_code(9), Level::NoMessages);
    }
}

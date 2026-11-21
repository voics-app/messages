//! Immutable poll content on a normal message row; vote data lives separately.
use discordy_messages_domain::poll::{PollDefinition, PollMedia};
use serde::Deserialize;
use serde_json::json;

pub(super) fn encode(poll: &PollDefinition) -> String {
    json!({
        "question": poll.question,
        "answers": poll.answers.iter().map(|answer| json!({"text":answer.text,"emoji":answer.emoji})).collect::<Vec<_>>(),
        "expires_at_ms":poll.expires_at_ms,
        "duration_hours":poll.duration_hours,
        "allow_multiselect":poll.allow_multiselect,
    }).to_string()
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredPoll {
    question: String,
    answers: Vec<StoredAnswer>,
    expires_at_ms: i64,
    duration_hours: u16,
    allow_multiselect: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredAnswer {
    text: String,
    emoji: Option<String>,
}

pub(super) fn decode(raw: &str) -> Option<PollDefinition> {
    let value: StoredPoll = serde_json::from_str(raw).ok()?;
    let poll = PollDefinition {
        question: value.question,
        answers: value
            .answers
            .into_iter()
            .map(|answer| PollMedia {
                text: answer.text,
                emoji: answer.emoji,
            })
            .collect(),
        expires_at_ms: value.expires_at_ms,
        duration_hours: value.duration_hours,
        allow_multiselect: value.allow_multiselect,
    };
    poll.validate().ok()?;
    Some(poll)
}

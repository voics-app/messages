use crate::DomainError;

use super::PollError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PollMedia {
    pub text: String,
    pub emoji: Option<String>,
}

impl PollMedia {
    pub fn text(value: impl Into<String>) -> Self {
        Self {
            text: value.into(),
            emoji: None,
        }
    }

    fn validate(&self) -> Result<(), PollError> {
        text(&self.text, "poll.answers.poll_media.text", 55)?;
        if let Some(emoji) = &self.emoji
            && (emoji.is_empty()
                || emoji.is_ascii()
                || emoji.chars().count() > 64
                || emoji.chars().any(char::is_whitespace))
        {
            return Err(DomainError::invalid("poll.answers.poll_media.emoji", "must be a Unicode emoji").into());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PollDraft {
    pub question: String,
    pub answers: Vec<PollMedia>,
    pub duration_hours: u16,
    pub allow_multiselect: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PollDefinition {
    pub question: String,
    /// The one-based position is the immutable answer ID.
    pub answers: Vec<PollMedia>,
    pub expires_at_ms: i64,
    pub duration_hours: u16,
    pub allow_multiselect: bool,
}

fn text(value: &str, field: &str, maximum: usize) -> Result<(), PollError> {
    if value.trim().is_empty() || value.chars().count() > maximum {
        Err(DomainError::invalid(
            field,
            format!("must contain 1 to {maximum} characters, not only whitespace"),
        )
        .into())
    } else {
        Ok(())
    }
}

impl PollDefinition {
    /// Also used when loading persisted definitions before exposing them.
    pub fn validate(&self) -> Result<(), DomainError> {
        if !(1..=253_402_300_799_999).contains(&self.expires_at_ms) {
            return Err(DomainError::invalid(
                "poll.expiry",
                "must be a supported RFC3339 instant",
            ));
        }
        match Self::new(self.draft(), 0) {
            Ok(_) => Ok(()),
            Err(PollError::Invalid(error)) => Err(error),
            Err(_) => Err(DomainError::invalid("poll", "invalid persisted definition")),
        }
    }

    pub fn new(draft: PollDraft, now_ms: i64) -> Result<Self, PollError> {
        text(&draft.question, "poll.question.text", 300)?;
        if !(2..=10).contains(&draft.answers.len()) {
            return Err(DomainError::invalid("poll.answers", "must contain 2 to 10 answers").into());
        }
        if !(1..=768).contains(&draft.duration_hours) {
            return Err(DomainError::invalid("poll.duration", "must be 1 to 768 whole hours").into());
        }
        for answer in &draft.answers {
            answer.validate()?;
        }
        let expires_at_ms = now_ms
            .checked_add(i64::from(draft.duration_hours) * 3_600_000)
            .ok_or(PollError::Exhausted)?;
        if now_ms < 0 || expires_at_ms > 253_402_300_799_999 {
            return Err(DomainError::invalid("poll.expiry", "must be a supported RFC3339 instant").into());
        }
        Ok(Self {
            question: draft.question,
            answers: draft.answers,
            expires_at_ms,
            duration_hours: draft.duration_hours,
            allow_multiselect: draft.allow_multiselect,
        })
    }

    pub fn draft(&self) -> PollDraft {
        PollDraft {
            question: self.question.clone(),
            answers: self.answers.clone(),
            duration_hours: self.duration_hours,
            allow_multiselect: self.allow_multiselect,
        }
    }
}

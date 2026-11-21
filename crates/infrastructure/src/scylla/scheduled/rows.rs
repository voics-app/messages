//! Versioned private storage format; domain types have no serializer dependency.
use discordy_messages_application::{PortError, ports::scheduled::ScheduleKey};
use discordy_messages_domain::Snowflake;
use discordy_messages_domain::scheduled::{
    MAX_SCHEDULE_REVISION, ScheduledDraft, ScheduledFailure, ScheduledMessage, ScheduledStatus,
};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredDraft {
    content: String,
    tts: bool,
    reply_to: Option<u64>,
    replied_user: bool,
    silent: bool,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredJob {
    format: u8,
    id: String,
    author_id: u64,
    channel_id: u64,
    original_fingerprint: String,
    revision: u32,
    scheduled_at_ms: u64,
    created_at_ms: u64,
    updated_at_ms: u64,
    status: String,
    draft: Option<StoredDraft>,
    message_id: Option<u64>,
    attempt_id: Option<String>,
    failure: Option<String>,
    next_attempt_at_ms: u64,
    dependency_failures: u8,
}

fn status(value: ScheduledStatus) -> &'static str {
    match value {
        ScheduledStatus::Preparing => "preparing",
        ScheduledStatus::Pending => "pending",
        ScheduledStatus::Dispatching => "dispatching",
        ScheduledStatus::Sent => "sent",
        ScheduledStatus::Failed => "failed",
        ScheduledStatus::Cancelled => "cancelled",
        ScheduledStatus::DeliveryUnknown => "delivery_unknown",
    }
}

fn failure(value: ScheduledFailure) -> &'static str {
    match value {
        ScheduledFailure::AccessDenied => "access_denied",
        ScheduledFailure::ChannelMissing => "channel_missing",
        ScheduledFailure::DeliveryModeChanged => "delivery_mode_changed",
        ScheduledFailure::InvalidMessage => "invalid_message",
        ScheduledFailure::DependencyUnavailable => "dependency_unavailable",
        ScheduledFailure::DeliveryUnknown => "delivery_unknown",
    }
}

fn invalid() -> PortError {
    PortError::new("stored schedule violates its invariants")
}

fn parse_status(value: &str) -> Result<ScheduledStatus, PortError> {
    [
        ScheduledStatus::Preparing,
        ScheduledStatus::Pending,
        ScheduledStatus::Dispatching,
        ScheduledStatus::Sent,
        ScheduledStatus::Failed,
        ScheduledStatus::Cancelled,
        ScheduledStatus::DeliveryUnknown,
    ]
    .into_iter()
    .find(|item| status(*item) == value)
    .ok_or_else(invalid)
}

fn parse_failure(value: &str) -> Result<ScheduledFailure, PortError> {
    [
        ScheduledFailure::AccessDenied,
        ScheduledFailure::ChannelMissing,
        ScheduledFailure::DeliveryModeChanged,
        ScheduledFailure::InvalidMessage,
        ScheduledFailure::DependencyUnavailable,
        ScheduledFailure::DeliveryUnknown,
    ]
    .into_iter()
    .find(|item| failure(*item) == value)
    .ok_or_else(invalid)
}

pub(super) fn checked(value: u64) -> Result<i64, PortError> {
    i64::try_from(value).map_err(|_| invalid())
}

fn validate(job: &ScheduledMessage) -> Result<(), PortError> {
    checked(job.author_id.value())?;
    checked(job.channel_id.value())?;
    for time in [
        job.scheduled_at_ms,
        job.created_at_ms,
        job.updated_at_ms,
        job.next_attempt_at_ms,
    ] {
        checked(time)?;
    }
    if let Some(message) = job.message_id {
        checked(message.value())?;
    }
    if job.revision > MAX_SCHEDULE_REVISION
        || job.original_fingerprint.len() != 64
        || !job
            .original_fingerprint
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(invalid());
    }
    let draft = job.draft.is_some();
    let attempt = job.attempt_id.is_some();
    let message = job.message_id.is_some();
    let valid = match job.status {
        ScheduledStatus::Preparing => job.revision == 0 && draft && !attempt && !message && job.failure.is_none(),
        ScheduledStatus::Pending => job.revision >= 1 && draft && !attempt && !message && job.failure.is_none(),
        ScheduledStatus::Dispatching => job.revision >= 2 && draft && attempt && message && job.failure.is_none(),
        ScheduledStatus::Sent => job.revision >= 3 && !draft && attempt && message && job.failure.is_none(),
        ScheduledStatus::Cancelled => job.revision >= 2 && !draft && !attempt && !message && job.failure.is_none(),
        ScheduledStatus::Failed => {
            job.revision >= 1
                && !draft
                && !message
                && job
                    .failure
                    .is_some_and(|reason| reason != ScheduledFailure::DeliveryUnknown)
        }
        ScheduledStatus::DeliveryUnknown => {
            job.revision >= 3 && !draft && attempt && message && job.failure == Some(ScheduledFailure::DeliveryUnknown)
        }
    };
    if !valid {
        return Err(invalid());
    }
    if let Some(draft) = &job.draft {
        draft.validate().map_err(|_| invalid())?;
        if let Some(reply) = draft.reply_to {
            checked(reply.value())?;
        }
    }
    Ok(())
}

pub(super) fn encode(job: &ScheduledMessage) -> Result<String, PortError> {
    validate(job)?;
    let stored = StoredJob {
        format: 1,
        id: job.id.to_string(),
        author_id: job.author_id.value(),
        channel_id: job.channel_id.value(),
        original_fingerprint: job.original_fingerprint.clone(),
        revision: job.revision,
        scheduled_at_ms: job.scheduled_at_ms,
        created_at_ms: job.created_at_ms,
        updated_at_ms: job.updated_at_ms,
        status: status(job.status).into(),
        draft: job.draft.as_ref().map(|draft| StoredDraft {
            content: draft.content.clone(),
            tts: draft.tts,
            reply_to: draft.reply_to.map(|id| id.value()),
            replied_user: draft.replied_user,
            silent: draft.silent,
        }),
        message_id: job.message_id.map(|id| id.value()),
        attempt_id: job.attempt_id.map(|id| id.to_string()),
        failure: job.failure.map(|reason| failure(reason).into()),
        next_attempt_at_ms: job.next_attempt_at_ms,
        dependency_failures: job.dependency_failures,
    };
    serde_json::to_string(&stored).map_err(|_| invalid())
}

pub(super) fn decode(body: &str, key: ScheduleKey) -> Result<ScheduledMessage, PortError> {
    let stored: StoredJob = serde_json::from_str(body).map_err(|_| invalid())?;
    if stored.format != 1 {
        return Err(invalid());
    }
    let job = ScheduledMessage {
        id: stored.id.parse().map_err(|_| invalid())?,
        author_id: Snowflake::new(stored.author_id),
        channel_id: Snowflake::new(stored.channel_id),
        original_fingerprint: stored.original_fingerprint,
        revision: stored.revision,
        scheduled_at_ms: stored.scheduled_at_ms,
        created_at_ms: stored.created_at_ms,
        updated_at_ms: stored.updated_at_ms,
        status: parse_status(&stored.status)?,
        draft: stored.draft.map(|draft| ScheduledDraft {
            content: draft.content,
            tts: draft.tts,
            reply_to: draft.reply_to.map(Snowflake::new),
            replied_user: draft.replied_user,
            silent: draft.silent,
        }),
        message_id: stored.message_id.map(Snowflake::new),
        attempt_id: stored
            .attempt_id
            .map(|id| id.parse().map_err(|_| invalid()))
            .transpose()?,
        failure: stored.failure.map(|reason| parse_failure(&reason)).transpose()?,
        next_attempt_at_ms: stored.next_attempt_at_ms,
        dependency_failures: stored.dependency_failures,
    };
    if ScheduleKey::from(&job) != key {
        return Err(invalid());
    }
    validate(&job)?;
    Ok(job)
}

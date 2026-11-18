//! Canonical generated DTOs plus constraints that typify intentionally cannot enforce.
use contracts::scheduled as wire;
use discordy_messages_application::scheduled::{CreateSchedule, EditSchedule, ScheduleAddress};
use discordy_messages_domain::{ChannelId, UserId, scheduled::*};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

use crate::{error::ApiError, extract::parse_id};

fn decode<T: DeserializeOwned>(value: Value) -> Result<T, ApiError> {
    serde_json::from_value(value).map_err(|error| ApiError::invalid("body", &error.to_string()))
}

pub(super) fn id(raw: &str) -> Result<ScheduleId, ApiError> {
    raw.parse()
        .map_err(|_| ApiError::invalid("request_id", "must be a lowercase UUIDv4"))
}

pub(super) fn revision(value: u64) -> Result<u32, ApiError> {
    if !(1..=u64::from(MAX_SCHEDULE_REVISION)).contains(&value) {
        return Err(ApiError::invalid(
            "expected_revision",
            "must be an integer in 1..2147483647",
        ));
    }
    Ok(value as u32)
}

fn timestamp(value: chrono::DateTime<chrono::Utc>) -> Result<u64, ApiError> {
    // Round upward to storage milliseconds: fractional precision must never cause an early send.
    let millis = value
        .timestamp_millis()
        .checked_add(i64::from(!value.timestamp_subsec_nanos().is_multiple_of(1_000_000)))
        .and_then(|value| u64::try_from(value).ok());
    millis.ok_or_else(|| ApiError::invalid("scheduled_at", "timestamp is outside the supported range"))
}

fn draft(value: wire::ScheduledMessageDraft) -> Result<ScheduledDraft, ApiError> {
    let reply_to = value
        .reply_to
        .as_ref()
        .map(|value| parse_id("message.reply_to", value))
        .transpose()?;
    if reply_to.is_some_and(|id| id.value() > i64::MAX as u64) {
        return Err(ApiError::invalid(
            "message.reply_to",
            "message ID is outside the supported range",
        ));
    }
    Ok(ScheduledDraft {
        content: value.content.into(),
        tts: value.tts,
        reply_to,
        replied_user: value.replied_user,
        silent: value.silent,
    })
}

pub(super) fn create(value: Value, author_id: UserId, channel_id: ChannelId) -> Result<CreateSchedule, ApiError> {
    let request_id = id(value
        .get("request_id")
        .and_then(Value::as_str)
        .ok_or_else(|| ApiError::invalid("request_id", "required UUIDv4"))?)?;
    let body: wire::CreateScheduledMessageRequest = decode(value)?;
    Ok(CreateSchedule {
        id: request_id,
        author_id,
        channel_id,
        scheduled_at_ms: timestamp(body.scheduled_at)?,
        draft: draft(body.message)?,
    })
}

pub(super) fn edit(value: Value, address: ScheduleAddress) -> Result<EditSchedule, ApiError> {
    for name in ["message", "scheduled_at"] {
        if value.get(name).is_some_and(Value::is_null) {
            return Err(ApiError::invalid(name, "replacement cannot be null"));
        }
    }
    let body: wire::EditScheduledMessageRequest = decode(value)?;
    Ok(EditSchedule {
        address,
        expected_revision: revision(body.expected_revision.0.get())?,
        scheduled_at_ms: body.scheduled_at.map(timestamp).transpose()?,
        draft: body.message.map(draft).transpose()?,
    })
}

fn utc(millis: u64) -> Result<String, ApiError> {
    OffsetDateTime::from_unix_timestamp_nanos(i128::from(millis) * 1_000_000)
        .map_err(|_| ApiError::Unavailable("stored scheduled timestamp is invalid".into()))?
        .format(&Rfc3339)
        .map_err(|_| ApiError::Unavailable("scheduled timestamp formatting failed".into()))
}

pub(super) fn resource(job: ScheduledMessage) -> Result<wire::ScheduledMessage, ApiError> {
    let status = match job.status {
        ScheduledStatus::Preparing => return Err(ApiError::Unavailable("schedule acceptance is incomplete".into())),
        ScheduledStatus::Pending => "pending",
        ScheduledStatus::Dispatching => "dispatching",
        ScheduledStatus::Sent => "sent",
        ScheduledStatus::Failed => "failed",
        ScheduledStatus::Cancelled => "cancelled",
        ScheduledStatus::DeliveryUnknown => "delivery_unknown",
    };
    let failure = job.failure.map(|failure| match failure {
        ScheduledFailure::AccessDenied => "access_denied",
        ScheduledFailure::ChannelMissing => "channel_missing",
        ScheduledFailure::DeliveryModeChanged => "delivery_mode_changed",
        ScheduledFailure::InvalidMessage => "invalid_message",
        ScheduledFailure::DependencyUnavailable => "dependency_unavailable",
        ScheduledFailure::DeliveryUnknown => "delivery_unknown",
    });
    let message = job.draft.map(|draft| json!({"content":draft.content,"tts":draft.tts,"reply_to":draft.reply_to.map(|id|id.to_string()),"replied_user":draft.replied_user,"silent":draft.silent}));
    serde_json::from_value(json!({"id":job.id.to_string(),"channel_id":job.channel_id.to_string(),"author_id":job.author_id.to_string(),
        "revision":job.revision,"status":status,"scheduled_at":utc(job.scheduled_at_ms)?,"created_at":utc(job.created_at_ms)?,"updated_at":utc(job.updated_at_ms)?,
        "message":message,"message_id":job.message_id.map(|id|id.to_string()),"failure":failure}))
        .map_err(|_| ApiError::Unavailable("stored scheduled resource is invalid".into()))
}

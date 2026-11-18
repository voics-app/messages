use async_trait::async_trait;
use discordy_messages_application::ports::scheduled::*;
use discordy_messages_application::{AppResult, PortError};
use discordy_messages_domain::scheduled::{
    AUTHOR_SCHEDULE_LIMIT, CHANNEL_SCHEDULE_LIMIT, ScheduleChange, ScheduledMessage, ScheduledStatus, conflict,
};
use discordy_messages_domain::{DomainError, UserId};
use uuid::Uuid;

use super::super::{fetch_all, port_error};
use super::{
    MAX_CAS_ATTEMPTS, Metadata, ScyllaScheduledMessages, conflict_backoff, exhausted, rows, statements::applied,
};

#[async_trait]
impl ScheduleJournal for ScyllaScheduledMessages {
    async fn find(&self, key: ScheduleKey) -> Result<Option<ScheduledMessage>, PortError> {
        let found: Vec<(String,)> = fetch_all(
            &self.session,
            &self.statements.find,
            (rows::checked(key.author_id.value())?, key.id.to_string()),
        )
        .await?;
        found
            .into_iter()
            .next()
            .map(|row| rows::decode(&row.0, key))
            .transpose()
    }

    async fn reserve(&self, job: ScheduledMessage) -> AppResult<ScheduleReservation> {
        rows::encode(&job)?;
        if job.status != ScheduledStatus::Preparing {
            return Err(PortError::new("only a preparing job can reserve quota").into());
        }
        let _admission = self.admission(job.author_id).lock().await;
        for attempt in 0..MAX_CAS_ATTEMPTS {
            let metadata = self.ensure_metadata(job.author_id).await?;
            if let Some(existing) = self.find((&job).into()).await? {
                return replay(existing, &job);
            }
            let count = self.channel_count(job.author_id, job.channel_id).await?;
            if metadata.active >= AUTHOR_SCHEDULE_LIMIT as i32 {
                return Err(quota("request_id", "author has 100 active schedules"));
            }
            if count >= CHANNEL_SCHEDULE_LIMIT as i32 {
                return Err(quota("channel_id", "channel has 25 active schedules"));
            }
            if self.commit(&metadata, count + 1, &job, metadata.active + 1).await? {
                return Ok(ScheduleReservation { job, created: true });
            }
            conflict_backoff(attempt).await;
        }
        Err(exhausted().into())
    }

    async fn change(&self, key: ScheduleKey, change: ScheduleChange, now: u64) -> AppResult<ScheduledMessage> {
        let _admission = self.admission(key.author_id).lock().await;
        for attempt in 0..MAX_CAS_ATTEMPTS {
            let metadata = self.ensure_metadata(key.author_id).await?;
            let mut job = self.find(key).await?.ok_or(DomainError::UnknownSchedule)?;
            let occupied = job.occupies_quota();
            if !job.transition(change.clone(), now)? {
                return Ok(job);
            }
            let count = self.channel_count(job.author_id, job.channel_id).await?;
            let released = i32::from(occupied && !job.occupies_quota());
            if metadata.active < released || count < released {
                return Err(PortError::new("scheduled quota underflow").into());
            }
            if self
                .commit(&metadata, count - released, &job, metadata.active - released)
                .await?
            {
                return Ok(job);
            }
            conflict_backoff(attempt).await;
        }
        Err(exhausted().into())
    }

    async fn active(&self, author: UserId) -> Result<Vec<ScheduledMessage>, PortError> {
        let found: Vec<(String, String)> = fetch_all(
            &self.session,
            &self.statements.active,
            (rows::checked(author.value())?,),
        )
        .await?;
        if found.len() > AUTHOR_SCHEDULE_LIMIT as usize {
            return Err(PortError::new("scheduled active discovery exceeds quota"));
        }
        found.into_iter().map(|row| decode_row(author, row)).collect()
    }

    async fn list(&self, query: ScheduleListQuery) -> Result<SchedulePage, PortError> {
        if !(1..=SCHEDULE_PAGE_SIZE).contains(&query.limit) {
            return Err(PortError::new("scheduled list limit must be 1..100"));
        }
        let after = query.after.map(|id| id.to_string()).unwrap_or_default();
        let found: Vec<(String, String)> = fetch_all(
            &self.session,
            &self.statements.list,
            (rows::checked(query.author_id.value())?, after, i32::from(query.limit)),
        )
        .await?;
        let mut items = found
            .into_iter()
            .map(|row| decode_row(query.author_id, row))
            .collect::<Result<Vec<_>, _>>()?;
        let next_after =
            (items.len() == usize::from(query.limit)).then(|| items.last().expect("full page is nonempty").id);
        items.retain(|job| job.channel_id == query.channel_id && job.visible_in_list(query.now_ms));
        Ok(SchedulePage { items, next_after })
    }
}

fn decode_row(author: UserId, (id, body): (String, String)) -> Result<ScheduledMessage, PortError> {
    rows::decode(
        &body,
        ScheduleKey {
            author_id: author,
            id: id.parse().map_err(PortError::new)?,
        },
    )
}

fn quota(field: &str, reason: &str) -> discordy_messages_application::AppError {
    metrics::counter!("scheduled_quota_refusals_total").increment(1);
    conflict(field, reason).into()
}

fn replay(existing: ScheduledMessage, requested: &ScheduledMessage) -> AppResult<ScheduleReservation> {
    if existing.channel_id != requested.channel_id || existing.original_fingerprint != requested.original_fingerprint {
        return Err(conflict(
            "request_id",
            "request key already identifies a different original request",
        )
        .into());
    }
    Ok(ScheduleReservation {
        job: existing,
        created: false,
    })
}

impl ScyllaScheduledMessages {
    async fn commit(
        &self,
        before: &Metadata,
        channel_count: i32,
        job: &ScheduledMessage,
        author_count: i32,
    ) -> Result<bool, PortError> {
        let author = rows::checked(job.author_id.value())?;
        let id = job.id.to_string();
        let body = rows::encode(job)?;
        let metadata = (Uuid::new_v4(), author_count, author, before.revision);
        let record = (author, &id, &body);
        let channel = (author, job.channel_id.to_string(), channel_count);
        let result = if job.occupies_quota() {
            self.session
                .batch(&self.statements.keep_active, (metadata, record, channel, record))
                .await
        } else {
            self.session
                .batch(
                    &self.statements.release_active,
                    (metadata, record, channel, (author, &id)),
                )
                .await
        };
        // Transport errors are ambiguous: only an explicit not-applied result may enter the CAS retry loop.
        applied(result.map_err(port_error)?)
    }
}

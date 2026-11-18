//! Real single-cluster Scylla tests; independent repository objects must contend through LWT.
mod support;

use std::sync::Arc;

use discordy_messages_application::AppError;
use discordy_messages_application::ports::scheduled::*;
use discordy_messages_domain::Snowflake;
use discordy_messages_domain::scheduled::*;
use discordy_messages_infrastructure::scylla::ScyllaScheduledMessages;
use futures::{StreamExt, TryStreamExt};

const NOW: u64 = 1_800_000_000_000;
const AUTHOR: Snowflake = Snowflake::new(100);

fn job(sequence: u64, channel: u64) -> ScheduledMessage {
    ScheduledMessage::prepare(
        ScheduleRequest {
            id: format!("00000000-0000-4000-8000-{sequence:012x}").parse().unwrap(),
            author_id: AUTHOR,
            channel_id: Snowflake::new(channel),
            scheduled_at_ms: NOW + MIN_SCHEDULE_DELAY_MS,
            draft: ScheduledDraft {
                content: format!("private scheduled text {sequence}"),
                tts: false,
                reply_to: None,
                replied_user: true,
                silent: false,
            },
            original_fingerprint: format!("{sequence:064x}"),
        },
        NOW,
    )
    .unwrap()
}

#[tokio::test]
async fn independent_repositories_enforce_both_quotas_and_original_request_identity() {
    let metrics = metrics_exporter_prometheus::PrometheusBuilder::new()
        .install_recorder()
        .unwrap();
    let database = support::start_scylla("schedule_quota").await;
    let a = Arc::new(
        ScyllaScheduledMessages::open(database.session.clone(), None)
            .await
            .unwrap(),
    );
    let b = Arc::new(
        ScyllaScheduledMessages::open(database.session.clone(), None)
            .await
            .unwrap(),
    );
    let started = std::time::Instant::now();
    let results = futures::stream::iter(1..=40)
        .map(|sequence| {
            let repo = if sequence % 2 == 0 { a.clone() } else { b.clone() };
            async move { repo.reserve(job(sequence, 200)).await }
        })
        .buffer_unordered(12)
        .collect::<Vec<_>>()
        .await;
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 25);
    assert!(
        results
            .iter()
            .filter_map(|result| result.as_ref().err())
            .all(|error| matches!(
                error,
                AppError::Domain(discordy_messages_domain::DomainError::ScheduleConflict(_))
            ))
    );
    let selected = results.into_iter().find_map(Result::ok).unwrap().job;
    assert!(!b.reserve(selected.clone()).await.unwrap().created);
    let mut conflicting = selected.clone();
    conflicting.original_fingerprint = "f".repeat(64);
    assert!(matches!(
        a.reserve(conflicting).await,
        Err(AppError::Domain(
            discordy_messages_domain::DomainError::ScheduleConflict(_)
        ))
    ));
    let mut other_channel = selected.clone();
    other_channel.channel_id = Snowflake::new(201);
    assert!(a.reserve(other_channel).await.is_err());

    let results = futures::stream::iter(100..200)
        .map(|sequence| {
            let repo = if sequence % 2 == 0 { a.clone() } else { b.clone() };
            async move { repo.reserve(job(sequence, sequence)).await }
        })
        .buffer_unordered(12)
        .collect::<Vec<_>>()
        .await;
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 75);
    assert!(
        results
            .iter()
            .filter_map(|result| result.as_ref().err())
            .all(|error| matches!(
                error,
                AppError::Domain(discordy_messages_domain::DomainError::ScheduleConflict(_))
            ))
    );
    assert_eq!(b.active(AUTHOR).await.unwrap().len(), 100);
    let page = b
        .list(ScheduleListQuery {
            author_id: AUTHOR,
            channel_id: Snowflake::new(200),
            after: None,
            limit: 50,
            now_ms: NOW,
        })
        .await
        .unwrap();
    assert!(
        page.items.is_empty(),
        "preparing entries are private implementation state"
    );
    assert!(
        page.next_after.is_some(),
        "a filtered empty page still advances its raw journal cursor"
    );

    let key = (&selected).into();
    a.change(key, ScheduleChange::Activate, NOW).await.unwrap();
    a.change(key, ScheduleChange::Cancel { expected_revision: 1 }, NOW)
        .await
        .unwrap();
    b.change(key, ScheduleChange::Cancel { expected_revision: 1 }, NOW)
        .await
        .unwrap();
    let replay = a.reserve(selected).await.unwrap();
    assert_eq!(replay.job.status, ScheduledStatus::Cancelled);
    assert!(!replay.created);
    a.reserve(job(999, 200)).await.unwrap();
    assert!(b.reserve(job(1000, 200)).await.is_err());
    assert_eq!(a.active(AUTHOR).await.unwrap().len(), 100);
    let rendered = metrics.render();
    let counter = |name: &str| {
        rendered
            .lines()
            .find_map(|line| line.strip_prefix(&format!("{name} ")))
            .map_or(0.0, |value| value.parse::<f64>().unwrap())
    };
    assert!(counter("scheduled_cas_conflicts_total") > 0.0);
    assert_eq!(counter("scheduled_cas_exhausted_total"), 0.0);
    println!(
        "scheduled_quota repositories=2 in_flight=12 reservations=140 author_limit=100 channel_limit=25 elapsed_ms={} conflicts={} exhausted=0",
        started.elapsed().as_millis(),
        counter("scheduled_cas_conflicts_total")
    );
}

#[tokio::test]
async fn claim_cancel_recovery_and_content_erasure_are_atomic_across_repositories() {
    let database = support::start_scylla("schedule_races").await;
    let a = ScyllaScheduledMessages::open(database.session.clone(), None)
        .await
        .unwrap();
    let b = ScyllaScheduledMessages::open(database.session.clone(), None)
        .await
        .unwrap();
    let original = job(1, 200);
    let key = (&original).into();
    a.reserve(original.clone()).await.unwrap();
    a.stage(DueSchedule {
        wake_at_ms: original.scheduled_at_ms,
        key,
        revision: 1,
    })
    .await
    .unwrap();
    a.change(key, ScheduleChange::Activate, NOW).await.unwrap();
    let attempt = job(2, 200).id;
    let claim = ScheduleChange::Claim {
        expected_revision: 1,
        attempt_id: attempt,
        message_id: Snowflake::new(500),
    };
    let (claimed, cancelled) = tokio::join!(
        a.change(key, claim.clone(), NOW + MIN_SCHEDULE_DELAY_MS),
        b.change(
            key,
            ScheduleChange::Cancel { expected_revision: 1 },
            NOW + MIN_SCHEDULE_DELAY_MS
        )
    );
    assert_ne!(claimed.is_ok(), cancelled.is_ok());
    let current = b.find(key).await.unwrap().unwrap();
    if current.status == ScheduledStatus::Dispatching {
        a.change(
            key,
            ScheduleChange::Recover,
            NOW + MIN_SCHEDULE_DELAY_MS + PREPARATION_TIMEOUT_MS,
        )
        .await
        .unwrap();
        let unknown = b.find(key).await.unwrap().unwrap();
        assert_eq!(unknown.status, ScheduledStatus::DeliveryUnknown);
        assert!(unknown.draft.is_none());
        assert!(
            b.change(key, claim, NOW + MIN_SCHEDULE_DELAY_MS + PREPARATION_TIMEOUT_MS)
                .await
                .is_err()
        );
        a.change(
            key,
            ScheduleChange::Finish {
                attempt_id: attempt,
                outcome: DispatchOutcome::Sent,
            },
            NOW + MIN_SCHEDULE_DELAY_MS + PREPARATION_TIMEOUT_MS,
        )
        .await
        .unwrap();
        assert_eq!(b.find(key).await.unwrap().unwrap().status, ScheduledStatus::Sent);
    }
    assert!(a.active(AUTHOR).await.unwrap().is_empty());
    let body = database
        .session
        .query_unpaged(
            "SELECT body FROM schedule_races_scheduled.scheduled_journal WHERE author_id=100 AND kind=1 AND entry=?",
            (original.id.to_string(),),
        )
        .await
        .unwrap()
        .into_rows_result()
        .unwrap()
        .single_row::<(String,)>()
        .unwrap()
        .0;
    assert!(!body.contains("private scheduled text"));
    assert!(!b.reserve(original).await.unwrap().created);

    let abandoned = job(3, 200);
    let abandoned_key = (&abandoned).into();
    a.reserve(abandoned).await.unwrap();
    b.change(abandoned_key, ScheduleChange::Recover, NOW + PREPARATION_TIMEOUT_MS)
        .await
        .unwrap();
    assert!(
        a.change(abandoned_key, ScheduleChange::Activate, NOW + PREPARATION_TIMEOUT_MS)
            .await
            .is_err()
    );
    assert!(a.active(AUTHOR).await.unwrap().is_empty());

    // Exercise unknown recovery and late certification regardless of which racer won above.
    let dispatching = job(4, 200);
    let dispatch_key = (&dispatching).into();
    a.reserve(dispatching).await.unwrap();
    a.change(dispatch_key, ScheduleChange::Activate, NOW).await.unwrap();
    let attempt = job(5, 200).id;
    a.change(
        dispatch_key,
        ScheduleChange::Claim {
            expected_revision: 1,
            attempt_id: attempt,
            message_id: Snowflake::new(501),
        },
        NOW + MIN_SCHEDULE_DELAY_MS,
    )
    .await
    .unwrap();
    b.change(
        dispatch_key,
        ScheduleChange::Recover,
        NOW + MIN_SCHEDULE_DELAY_MS + PREPARATION_TIMEOUT_MS,
    )
    .await
    .unwrap();
    assert_eq!(
        a.find(dispatch_key).await.unwrap().unwrap().status,
        ScheduledStatus::DeliveryUnknown
    );
    assert!(a.active(AUTHOR).await.unwrap().is_empty());
    assert!(
        b.change(
            dispatch_key,
            ScheduleChange::Finish {
                attempt_id: job(6, 200).id,
                outcome: DispatchOutcome::Sent
            },
            NOW + 500_000
        )
        .await
        .is_err()
    );
    a.change(
        dispatch_key,
        ScheduleChange::Finish {
            attempt_id: attempt,
            outcome: DispatchOutcome::Sent,
        },
        NOW + 500_000,
    )
    .await
    .unwrap();
    assert_eq!(
        b.find(dispatch_key).await.unwrap().unwrap().status,
        ScheduledStatus::Sent
    );
    assert!(b.active(AUTHOR).await.unwrap().is_empty());
}

#[tokio::test]
async fn durable_due_cursors_bound_pages_and_revisit_entries_inserted_behind_them() {
    let database = support::start_scylla("schedule_index").await;
    let a = ScyllaScheduledMessages::open(database.session.clone(), Some("schedule_index_custom"))
        .await
        .unwrap();
    let b = ScyllaScheduledMessages::open(database.session.clone(), Some("schedule_index_custom"))
        .await
        .unwrap();
    let entries = (1..=225)
        .map(|sequence| {
            let item = job(sequence * 64, 200);
            DueSchedule {
                wake_at_ms: NOW + sequence,
                key: (&item).into(),
                revision: 1,
            }
        })
        .collect::<Vec<_>>();
    let shard = entries[0].shard();
    assert!(entries.iter().all(|entry| entry.shard() == shard));
    futures::stream::iter(entries.clone())
        .map(|entry| a.stage(entry))
        .buffer_unordered(8)
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let first = a.page(shard, None, NOW + 300).await.unwrap();
    assert_eq!(first.len(), 100);
    a.save_cursor(shard, first.last().copied()).await.unwrap();
    let cursor = b.cursor(shard).await.unwrap();
    let second = b.page(shard, cursor, NOW + 300).await.unwrap();
    assert_eq!(second.len(), 100);
    let third = b.page(shard, second.last().copied(), NOW + 300).await.unwrap();
    assert_eq!(third.len(), 25);
    let behind = DueSchedule {
        wake_at_ms: NOW,
        ..entries[0]
    };
    a.stage(behind).await.unwrap();
    b.save_cursor(shard, None).await.unwrap();
    assert_eq!(a.cursor(shard).await.unwrap(), None);
    assert_eq!(a.page(shard, None, NOW + 300).await.unwrap()[0], behind);
    a.remove(behind).await.unwrap();
    assert_eq!(b.page(shard, None, NOW + 300).await.unwrap()[0], entries[0]);
    a.check_ready().await.unwrap();
    assert_eq!(database.session.get_keyspace().unwrap().as_str(), "schedule_index");
    assert!(
        ScyllaScheduledMessages::open(database.session.clone(), Some("schedule_index"))
            .await
            .is_err()
    );
    assert!(
        ScyllaScheduledMessages::open(database.session.clone(), Some("bad; DROP TABLE x"))
            .await
            .is_err()
    );
}

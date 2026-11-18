use super::observability::committed;
use discordy_messages_domain::{Snowflake, scheduled::*};

#[test]
fn recovery_counts_unknown_and_claim_latency_without_counting_a_replay() {
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let mut job = ScheduledMessage::prepare(
        ScheduleRequest {
            id: "00000000-0000-4000-8000-000000000001".parse().unwrap(),
            author_id: Snowflake::new(1),
            channel_id: Snowflake::new(2),
            scheduled_at_ms: 60_000,
            draft: ScheduledDraft {
                content: "fixture".into(),
                tts: false,
                reply_to: None,
                replied_user: true,
                silent: false,
            },
            original_fingerprint: "a".repeat(64),
        },
        0,
    )
    .unwrap();
    job.transition(ScheduleChange::Activate, 0).unwrap();
    job.transition(
        ScheduleChange::Claim {
            expected_revision: 1,
            attempt_id: job.id,
            message_id: Snowflake::new(3),
        },
        60_000,
    )
    .unwrap();
    job.transition(ScheduleChange::Recover, 180_000).unwrap();
    metrics::with_local_recorder(&recorder, || {
        committed((ScheduledStatus::Dispatching, 60_000), &job, 180_000);
        committed((ScheduledStatus::DeliveryUnknown, 180_000), &job, 181_000);
    });
    let snapshot = recorder.handle().render();
    assert!(snapshot.contains("scheduled_delivery_unknown_total 1"), "{snapshot}");
    assert!(
        snapshot.contains("scheduled_claim_to_outcome_seconds_sum{outcome=\"delivery_unknown\"} 120"),
        "{snapshot}"
    );
    assert!(
        snapshot.contains("scheduled_claim_to_outcome_seconds_count{outcome=\"delivery_unknown\"} 1"),
        "{snapshot}"
    );
    assert!(!snapshot.contains("author_id") && !snapshot.contains("channel_id") && !snapshot.contains("schedule_id"));
}

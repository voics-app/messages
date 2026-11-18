use discordy_messages_domain::Snowflake;
use discordy_messages_domain::scheduled::{
    DispatchOutcome, MAX_SCHEDULE_REVISION, MIN_SCHEDULE_DELAY_MS, PREPARATION_TIMEOUT_MS, ScheduleChange, ScheduleId,
    ScheduleRequest, ScheduledDraft, ScheduledFailure, ScheduledMessage, ScheduledStatus,
};

const NOW: u64 = 1_795_000_000_000;

fn identifier(n: u8) -> ScheduleId {
    format!("00000000-0000-4000-8000-{n:012x}").parse().unwrap()
}

fn draft(content: &str) -> ScheduledDraft {
    ScheduledDraft {
        content: content.into(),
        tts: false,
        reply_to: None,
        replied_user: true,
        silent: false,
    }
}

fn preparing() -> ScheduledMessage {
    ScheduledMessage::prepare(
        ScheduleRequest {
            id: identifier(1),
            author_id: Snowflake::new(10),
            channel_id: Snowflake::new(20),
            scheduled_at_ms: NOW + MIN_SCHEDULE_DELAY_MS,
            draft: draft("later"),
            original_fingerprint: "fingerprint".into(),
        },
        NOW,
    )
    .unwrap()
}

fn pending() -> ScheduledMessage {
    let mut job = preparing();
    job.transition(ScheduleChange::Activate, NOW).unwrap();
    job
}

#[test]
fn accepts_only_lowercase_uuid_v4_with_rfc_variant() {
    assert_eq!(identifier(1).to_string(), "00000000-0000-4000-8000-000000000001");
    for invalid in [
        "00000000-0000-7000-8000-000000000001",
        "00000000-0000-4000-0000-000000000001",
        "00000000-0000-4000-8000-00000000000A",
        "00000000000040008000000000000001",
    ] {
        assert!(invalid.parse::<ScheduleId>().is_err(), "{invalid}");
    }
}

#[test]
fn scheduling_validates_text_and_server_time_without_trimming_user_content() {
    for invalid in ["".to_owned(), " \n\t".into(), "界".repeat(2001)] {
        assert!(draft(&invalid).validate().is_err());
    }
    assert!(draft(&"界".repeat(2000)).validate().is_ok());
    assert_eq!(preparing().draft.unwrap().content, "later");
    for due in [NOW, NOW + MIN_SCHEDULE_DELAY_MS - 1, NOW + 30 * 86_400_000 + 1] {
        assert!(ScheduledMessage::validate_time(due, NOW).is_err());
    }
    assert!(ScheduledMessage::validate_time(NOW + MIN_SCHEDULE_DELAY_MS, NOW).is_ok());
    assert!(ScheduledMessage::validate_time(NOW + 30 * 86_400_000, NOW).is_ok());
}

#[test]
fn an_abandoned_reservation_cannot_be_activated_by_a_paused_creator() {
    let mut job = preparing();
    assert!(job.occupies_quota());
    assert!(
        !job.transition(ScheduleChange::Recover, NOW + PREPARATION_TIMEOUT_MS - 1)
            .unwrap()
    );
    assert!(
        job.transition(ScheduleChange::Recover, NOW + PREPARATION_TIMEOUT_MS)
            .unwrap()
    );
    assert_eq!(job.status, ScheduledStatus::Failed);
    assert!(!job.occupies_quota());
    assert!(job.draft.is_none());
    assert!(
        job.transition(ScheduleChange::Activate, NOW + PREPARATION_TIMEOUT_MS)
            .is_err()
    );
}

#[test]
fn accepted_cancellation_fences_claim_and_repeated_cancel_is_idempotent() {
    let mut job = pending();
    job.transition(ScheduleChange::Cancel { expected_revision: 1 }, NOW)
        .unwrap();
    assert_eq!(job.status, ScheduledStatus::Cancelled);
    assert!(
        !job.transition(ScheduleChange::Cancel { expected_revision: 1 }, NOW + 1)
            .unwrap()
    );
    assert!(
        job.transition(
            ScheduleChange::Claim {
                expected_revision: 1,
                attempt_id: identifier(2),
                message_id: Snowflake::new(30)
            },
            NOW + MIN_SCHEDULE_DELAY_MS
        )
        .is_err()
    );
    assert_eq!(job.revision, 2);
    assert!(job.draft.is_none());
}

#[test]
fn a_claim_never_sends_early_and_fences_edits_or_another_claim() {
    let mut job = pending();
    let claim = ScheduleChange::Claim {
        expected_revision: 1,
        attempt_id: identifier(2),
        message_id: Snowflake::new(30),
    };
    assert!(job.transition(claim.clone(), NOW).is_err());
    job.transition(claim.clone(), NOW + MIN_SCHEDULE_DELAY_MS).unwrap();
    assert!(job.transition(claim, NOW + MIN_SCHEDULE_DELAY_MS).is_err());
    assert!(
        job.transition(
            ScheduleChange::Edit {
                expected_revision: 2,
                scheduled_at_ms: Some(NOW + 120_000),
                draft: draft("changed")
            },
            NOW + MIN_SCHEDULE_DELAY_MS
        )
        .is_err()
    );
    assert_eq!(job.draft.unwrap().content, "later");
}

#[test]
fn unknown_releases_once_and_only_its_original_attempt_can_certify_sent() {
    let mut job = pending();
    job.transition(
        ScheduleChange::Claim {
            expected_revision: 1,
            attempt_id: identifier(2),
            message_id: Snowflake::new(30),
        },
        NOW + MIN_SCHEDULE_DELAY_MS,
    )
    .unwrap();
    job.transition(
        ScheduleChange::Recover,
        NOW + MIN_SCHEDULE_DELAY_MS + PREPARATION_TIMEOUT_MS,
    )
    .unwrap();
    assert_eq!(job.status, ScheduledStatus::DeliveryUnknown);
    assert!(!job.occupies_quota());
    assert!(job.draft.is_none());
    assert!(!job.transition(ScheduleChange::Recover, NOW + 999_999).unwrap());
    assert!(
        job.transition(
            ScheduleChange::Finish {
                attempt_id: identifier(3),
                outcome: DispatchOutcome::Sent
            },
            NOW + 999_999
        )
        .is_err()
    );
    job.transition(
        ScheduleChange::Finish {
            attempt_id: identifier(2),
            outcome: DispatchOutcome::Sent,
        },
        NOW + 999_999,
    )
    .unwrap();
    assert_eq!(job.status, ScheduledStatus::Sent);
    assert_eq!(job.message_id, Some(Snowflake::new(30)));
    assert_eq!(job.failure, None);
    assert!(!job.occupies_quota());
}

#[test]
fn known_preinsert_failure_clears_the_selected_id_and_draft() {
    let mut job = pending();
    job.transition(
        ScheduleChange::Claim {
            expected_revision: 1,
            attempt_id: identifier(2),
            message_id: Snowflake::new(30),
        },
        NOW + MIN_SCHEDULE_DELAY_MS,
    )
    .unwrap();
    job.transition(
        ScheduleChange::Finish {
            attempt_id: identifier(2),
            outcome: DispatchOutcome::Failed(ScheduledFailure::AccessDenied),
        },
        NOW + 70_000,
    )
    .unwrap();
    assert_eq!(job.status, ScheduledStatus::Failed);
    assert_eq!(job.message_id, None);
    assert_eq!(job.draft, None);
    assert_eq!(job.failure, Some(ScheduledFailure::AccessDenied));
}

#[test]
fn revision_exhaustion_fails_without_mutating_the_job() {
    let mut job = pending();
    job.revision = MAX_SCHEDULE_REVISION;
    let before = job.clone();
    assert!(
        job.transition(
            ScheduleChange::Cancel {
                expected_revision: MAX_SCHEDULE_REVISION
            },
            NOW
        )
        .is_err()
    );
    assert_eq!(job, before);
}

#[test]
fn edit_changes_current_draft_but_not_original_replay_fingerprint() {
    let mut job = pending();
    job.transition(
        ScheduleChange::Edit {
            expected_revision: 1,
            scheduled_at_ms: Some(NOW + 120_000),
            draft: draft("edited"),
        },
        NOW,
    )
    .unwrap();
    assert_eq!(job.revision, 2);
    assert_eq!(job.original_fingerprint, "fingerprint");
    assert_eq!(job.created_at_ms, NOW);
    assert_eq!(job.draft.unwrap().content, "edited");
}

#[test]
fn a_pending_overdue_draft_can_change_without_rescheduling_into_the_past() {
    let mut job = pending();
    let later = NOW + MIN_SCHEDULE_DELAY_MS + 1;
    job.transition(
        ScheduleChange::Edit {
            expected_revision: 1,
            scheduled_at_ms: None,
            draft: draft("edited while delayed"),
        },
        later,
    )
    .unwrap();
    assert_eq!(job.scheduled_at_ms, NOW + MIN_SCHEDULE_DELAY_MS);
    assert!(
        job.transition(
            ScheduleChange::Edit {
                expected_revision: 2,
                scheduled_at_ms: Some(job.scheduled_at_ms),
                draft: draft("again")
            },
            later
        )
        .is_err()
    );
}

#[test]
fn dependency_backoff_is_bounded_and_stops_at_twenty_four_hours_overdue() {
    let mut job = pending();
    let now = NOW + MIN_SCHEDULE_DELAY_MS;
    for _ in 0..20 {
        job.transition(ScheduleChange::Defer { expected_revision: 1 }, now)
            .unwrap();
        assert!((1_000..=60_000).contains(&(job.next_attempt_at_ms - now)));
        assert_eq!(job.revision, 1);
    }
    job.transition(ScheduleChange::Defer { expected_revision: 1 }, now + 86_400_000)
        .unwrap();
    assert_eq!(job.status, ScheduledStatus::Failed);
    assert_eq!(job.failure, Some(ScheduledFailure::DependencyUnavailable));
}

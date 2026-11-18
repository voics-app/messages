use super::fakes::{ClaimGate, MemorySchedules, SequenceAttempts};
use super::{CreateSchedule, EditSchedule, ScheduleAddress, ScheduledDispatcher, ScheduledMessages, ScheduledPorts};
use crate::fakes::{ALICE, BOB, CHANNEL, Harness};
use crate::{
    AppError,
    ports::{Clock, MessageRepository, scheduled::*},
    use_cases::CreateMessage,
};
use discordy_messages_domain::scheduled::{
    MIN_SCHEDULE_DELAY_MS, PREPARATION_TIMEOUT_MS, ScheduledDraft, ScheduledStatus,
};
use discordy_messages_domain::{DomainError, Snowflake};
use std::sync::{Arc, atomic::Ordering};

#[path = "policy_tests.rs"]
mod policy_tests;

struct Fixture {
    harness: Harness,
    memory: Arc<MemorySchedules>,
    service: Arc<ScheduledMessages>,
    dispatcher: ScheduledDispatcher,
}

impl Fixture {
    fn new() -> Self {
        let harness = Harness::new();
        let memory = Arc::new(MemorySchedules::default());
        let ports = ScheduledPorts {
            journal: memory.clone(),
            index: memory.clone(),
            delivery: Arc::new(CreateMessage::new(&harness.ports)),
            attempts: Arc::new(SequenceAttempts::default()),
            clock: harness.clock.clone(),
        };
        Self {
            service: Arc::new(ScheduledMessages::new(ports.clone())),
            dispatcher: ScheduledDispatcher::new(ports),
            harness,
            memory,
        }
    }

    fn command(&self) -> CreateSchedule {
        CreateSchedule {
            id: "d71cf4c0-2a63-49b5-85de-3c3cbe407820".parse().unwrap(),
            author_id: ALICE,
            channel_id: CHANNEL,
            scheduled_at_ms: self.harness.clock.now_unix_ms() + MIN_SCHEDULE_DELAY_MS,
            draft: ScheduledDraft {
                content: "later".into(),
                tts: false,
                reply_to: None,
                replied_user: true,
                silent: false,
            },
        }
    }
}

fn address(command: &CreateSchedule) -> ScheduleAddress {
    ScheduleAddress {
        key: ScheduleKey {
            author_id: command.author_id,
            id: command.id,
        },
        channel_id: command.channel_id,
    }
}

#[tokio::test]
async fn lost_create_response_replays_the_current_resource_after_time_and_access_change() {
    let f = Fixture::new();
    let command = f.command();
    assert!(f.service.create(command.clone()).await.unwrap().created);
    f.service.cancel(address(&command), 1).await.unwrap();
    f.harness.clock.advance_ms(31 * 86_400_000);
    f.harness.access.set_permissions(0);
    let replay = f.service.create(command.clone()).await.unwrap();
    assert!(!replay.created);
    assert_eq!(replay.job.status, ScheduledStatus::Cancelled);
    assert_eq!(f.memory.jobs.lock().unwrap().len(), 1);
    let mut conflict = command;
    conflict.draft.content = "different".into();
    assert!(matches!(
        f.service.create(conflict).await,
        Err(AppError::Domain(DomainError::ScheduleConflict(_)))
    ));
}

#[tokio::test]
async fn an_unindexed_reservation_is_never_returned_as_accepted_and_can_resume_with_the_same_key() {
    let f = Fixture::new();
    let command = f.command();
    f.memory.fail_next_stage.store(true, Ordering::SeqCst);
    assert!(matches!(
        f.service.create(command.clone()).await,
        Err(AppError::Unavailable(_))
    ));
    assert_eq!(
        f.memory.find(address(&command).key).await.unwrap().unwrap().status,
        ScheduledStatus::Preparing
    );
    let accepted = f.service.create(command.clone()).await.unwrap();
    assert!(accepted.created);
    assert_eq!(accepted.job.status, ScheduledStatus::Pending);
    assert!(
        f.memory
            .due
            .lock()
            .unwrap()
            .contains(&DueSchedule::pending(&accepted.job))
    );
}

#[tokio::test]
async fn a_stale_prepare_replay_is_failed_without_recreating_or_leaking_quota() {
    let f = Fixture::new();
    let command = f.command();
    f.memory.fail_next_stage.store(true, Ordering::SeqCst);
    assert!(f.service.create(command.clone()).await.is_err());
    f.harness.clock.advance_ms(PREPARATION_TIMEOUT_MS);
    let result = f.service.create(command).await.unwrap();
    assert_eq!(result.job.status, ScheduledStatus::Failed);
    assert!(!result.created);
    assert!(f.memory.active(ALICE).await.unwrap().is_empty());
}

#[tokio::test]
async fn failed_edit_index_write_preserves_the_previous_discoverable_pending_draft() {
    let f = Fixture::new();
    let command = f.command();
    let before = f.service.create(command.clone()).await.unwrap().job;
    f.memory.fail_next_stage.store(true, Ordering::SeqCst);
    let edit = EditSchedule {
        address: address(&command),
        expected_revision: 1,
        scheduled_at_ms: Some(command.scheduled_at_ms + 60_000),
        draft: None,
    };
    assert!(f.service.edit(edit).await.is_err());
    assert_eq!(f.service.get(address(&command)).await.unwrap(), before);
    assert!(f.memory.due.lock().unwrap().contains(&DueSchedule::pending(&before)));
}

#[tokio::test]
async fn two_dispatchers_cannot_create_two_messages() {
    let f = Fixture::new();
    let command = f.command();
    let job = f.service.create(command.clone()).await.unwrap().job;
    f.harness.clock.advance_ms(MIN_SCHEDULE_DELAY_MS);
    let key = (&job).into();
    let (a, b) = tokio::join!(f.dispatcher.execute(key), f.dispatcher.execute(key));
    a.unwrap();
    b.unwrap();
    let sent = f.service.get(address(&command)).await.unwrap();
    assert_eq!(sent.status, ScheduledStatus::Sent);
    assert!(
        f.harness
            .messages
            .find(CHANNEL, sent.message_id.unwrap())
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(f.harness.events.events.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn a_pause_before_claim_cannot_reuse_revoked_send_permission() {
    let f = Fixture::new();
    let command = f.command();
    f.service.create(command.clone()).await.unwrap();
    f.harness.clock.advance_ms(MIN_SCHEDULE_DELAY_MS);
    let gate = Arc::new(ClaimGate::default());
    *f.memory.claim_gate.lock().unwrap() = Some(gate.clone());
    let key = address(&command).key;
    let dispatch = tokio::spawn(async move { f.dispatcher.execute(key).await });
    gate.entered.notified().await;
    f.harness.access.set_permissions(0);
    gate.release.notify_one();
    dispatch.await.unwrap().unwrap();
    assert_eq!(
        f.service.get(address(&command)).await.unwrap().status,
        ScheduledStatus::Failed
    );
    assert_eq!(f.harness.messages.last_message_id(CHANNEL).await.unwrap(), None);
}

#[tokio::test]
async fn a_lost_claim_result_never_starts_insertion_and_recovers_to_unknown() {
    let f = Fixture::new();
    let command = f.command();
    f.service.create(command.clone()).await.unwrap();
    f.harness.clock.advance_ms(MIN_SCHEDULE_DELAY_MS);
    f.memory.fail_after_claim.store(true, Ordering::SeqCst);
    assert!(f.dispatcher.execute(address(&command).key).await.is_err());
    assert_eq!(f.harness.messages.last_message_id(CHANNEL).await.unwrap(), None);
    f.harness.clock.advance_ms(PREPARATION_TIMEOUT_MS);
    f.dispatcher.execute(address(&command).key).await.unwrap();
    assert_eq!(
        f.service.get(address(&command)).await.unwrap().status,
        ScheduledStatus::DeliveryUnknown
    );
    assert_eq!(f.harness.messages.last_message_id(CHANNEL).await.unwrap(), None);
}

#[tokio::test]
async fn own_list_and_cancel_survive_membership_loss_but_other_authors_or_channels_cannot_read() {
    let f = Fixture::new();
    let command = f.command();
    f.service.create(command.clone()).await.unwrap();
    f.harness.access.set_permissions(0);
    assert_eq!(
        f.service
            .list(ScheduleListQuery {
                author_id: ALICE,
                channel_id: CHANNEL,
                after: None,
                limit: 50,
                now_ms: 0
            })
            .await
            .unwrap()
            .items
            .len(),
        1
    );
    assert!(
        f.service
            .get(ScheduleAddress {
                key: ScheduleKey {
                    author_id: BOB,
                    id: command.id
                },
                channel_id: CHANNEL
            })
            .await
            .is_err()
    );
    assert!(
        f.service
            .get(ScheduleAddress {
                key: address(&command).key,
                channel_id: Snowflake::new(999)
            })
            .await
            .is_err()
    );
    assert_eq!(
        f.service.cancel(address(&command), 1).await.unwrap().status,
        ScheduledStatus::Cancelled
    );
}

#[tokio::test]
async fn resuming_unaccepted_preparation_requires_current_permission() {
    let f = Fixture::new();
    let command = f.command();
    f.memory.fail_next_stage.store(true, Ordering::SeqCst);
    assert!(f.service.create(command.clone()).await.is_err());
    f.harness.access.set_permissions(0);
    assert!(matches!(
        f.service.create(command.clone()).await,
        Err(AppError::Domain(DomainError::MissingAccess))
    ));
    assert_eq!(
        f.memory.find(address(&command).key).await.unwrap().unwrap().status,
        ScheduledStatus::Preparing
    );
}

#[tokio::test]
async fn editing_a_terminal_resource_is_a_conflict_even_without_a_retained_draft() {
    let f = Fixture::new();
    let command = f.command();
    f.service.create(command.clone()).await.unwrap();
    f.service.cancel(address(&command), 1).await.unwrap();
    let result = f
        .service
        .edit(EditSchedule {
            address: address(&command),
            expected_revision: 2,
            scheduled_at_ms: Some(command.scheduled_at_ms + 60_000),
            draft: None,
        })
        .await;
    assert!(matches!(
        result,
        Err(AppError::Domain(DomainError::ScheduleConflict(_)))
    ));
}

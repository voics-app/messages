use super::*;
use crate::{
    PortError,
    fakes::{MEMBER, dm_access},
    ports::AccessError,
};
use discordy_messages_domain::{DeliveryMode, FLAG_SUPPRESS_NOTIFICATIONS, Permissions, scheduled::ScheduledFailure};

#[tokio::test]
async fn queries_do_not_reconcile_abandoned_reservations() {
    let f = Fixture::new();
    let command = f.command();
    f.memory.fail_next_stage.store(true, Ordering::SeqCst);
    assert!(f.service.create(command.clone()).await.is_err());
    f.harness.clock.advance_ms(PREPARATION_TIMEOUT_MS);

    assert!(f.service.get(address(&command)).await.is_err());
    f.service
        .list(ScheduleListQuery {
            author_id: ALICE,
            channel_id: CHANNEL,
            after: None,
            limit: 50,
            now_ms: 0,
        })
        .await
        .unwrap();
    let untouched = f.memory.find(address(&command).key).await.unwrap().unwrap();
    assert_eq!(untouched.status, ScheduledStatus::Preparing);
    assert!(untouched.draft.is_some());
    assert_eq!(f.memory.active(ALICE).await.unwrap().len(), 1);
}

#[tokio::test]
async fn explicit_reconciliation_recovers_an_unindexed_reservation_without_send_permission() {
    let f = Fixture::new();
    let command = f.command();
    f.memory.fail_next_stage.store(true, Ordering::SeqCst);
    assert!(f.service.create(command.clone()).await.is_err());
    f.harness.clock.advance_ms(PREPARATION_TIMEOUT_MS);
    f.harness.access.set_permissions(0);
    f.service.reconcile_author(ALICE).await.unwrap();
    let page = f
        .service
        .list(ScheduleListQuery {
            author_id: ALICE,
            channel_id: CHANNEL,
            after: None,
            limit: 50,
            now_ms: 0,
        })
        .await
        .unwrap();
    assert_eq!(
        page.items.len(),
        1,
        "explicit author reconciliation must expose an expired unindexed preparation"
    );
    assert_eq!(page.items[0].status, ScheduledStatus::Failed);
    assert!(page.items[0].draft.is_none());
    assert!(f.memory.active(ALICE).await.unwrap().is_empty());
    assert_eq!(
        f.service.get(address(&command)).await.unwrap().status,
        ScheduledStatus::Failed
    );
}

#[tokio::test]
async fn dispatch_preserves_tts_reply_ping_and_silent_options() {
    for ping in [false, true] {
        let f = Fixture::new();
        f.harness
            .access
            .set_permissions(MEMBER | Permissions::SEND_TTS_MESSAGES);
        let target = f.harness.seed(BOB, "reply target").await;
        let mut command = f.command();
        command.draft.reply_to = Some(target.id);
        command.draft.replied_user = ping;
        command.draft.silent = true;
        command.draft.tts = true;
        f.service.create(command.clone()).await.unwrap();
        f.harness.clock.advance_ms(MIN_SCHEDULE_DELAY_MS);
        f.dispatcher.execute(address(&command).key).await.unwrap();
        let job = f.service.get(address(&command)).await.unwrap();
        let stored = f
            .harness
            .messages
            .find(CHANNEL, job.message_id.unwrap())
            .await
            .unwrap()
            .unwrap();
        assert!(stored.tts);
        assert_eq!(stored.reply_to, Some(target.id));
        assert_eq!(stored.flags & FLAG_SUPPRESS_NOTIFICATIONS, FLAG_SUPPRESS_NOTIFICATIONS);
        assert_eq!(stored.mentions.users.contains(&BOB), ping);
    }
}

#[tokio::test]
async fn mode_channel_tts_and_reply_are_rechecked_before_and_after_claim() {
    for after_claim in [false, true] {
        for cause in ["mode", "channel", "tts", "reply"] {
            let f = Fixture::new();
            f.harness
                .access
                .set_permissions(MEMBER | Permissions::SEND_TTS_MESSAGES);
            let target = f.harness.seed(BOB, "reply target").await;
            let mut command = f.command();
            command.draft.tts = true;
            command.draft.reply_to = Some(target.id);
            f.service.create(command.clone()).await.unwrap();
            f.harness.clock.advance_ms(MIN_SCHEDULE_DELAY_MS);
            let gate = Arc::new(ClaimGate::default());
            let key = address(&command).key;
            if after_claim {
                *f.memory.after_claim_gate.lock().unwrap() = Some(gate.clone());
            }
            let dispatch = if after_claim {
                let task = tokio::spawn(async move { f.dispatcher.execute(key).await });
                gate.entered.notified().await;
                Some(task)
            } else {
                None
            };
            let expected = match cause {
                "mode" => {
                    f.harness.access.set(Ok(dm_access(DeliveryMode::P2p)));
                    ScheduledFailure::DeliveryModeChanged
                }
                "channel" => {
                    f.harness.access.set(Err(AccessError::UnknownChannel));
                    ScheduledFailure::ChannelMissing
                }
                "tts" => {
                    f.harness.access.set_permissions(MEMBER);
                    ScheduledFailure::AccessDenied
                }
                "reply" => {
                    f.harness.messages.delete(CHANNEL, &[target.id]).await.unwrap();
                    ScheduledFailure::InvalidMessage
                }
                _ => unreachable!(),
            };
            if let Some(dispatch) = dispatch {
                gate.release.notify_one();
                dispatch.await.unwrap().unwrap();
            } else {
                // A separate dispatcher uses the identical ports without moving the fixture above.
                ScheduledDispatcher::new(ScheduledPorts {
                    journal: f.memory.clone(),
                    index: f.memory.clone(),
                    delivery: Arc::new(CreateMessage::new(&f.harness.ports)),
                    attempts: Arc::new(SequenceAttempts::default()),
                    clock: f.harness.clock.clone(),
                })
                .execute(key)
                .await
                .unwrap();
            }
            let job = f.service.get(address(&command)).await.unwrap();
            assert_eq!(job.status, ScheduledStatus::Failed, "{cause} after_claim={after_claim}");
            assert_eq!(job.failure, Some(expected));
            assert!(job.draft.is_none() && job.message_id.is_none());
            assert!(f.harness.events.events.lock().unwrap().is_empty());
            assert!(
                f.harness
                    .messages
                    .last_message_id(CHANNEL)
                    .await
                    .unwrap()
                    .is_none_or(|id| id == target.id)
            );
        }
    }
}

#[tokio::test]
async fn preclaim_dependency_backoff_remains_pending_then_sends_after_recovery() {
    let f = Fixture::new();
    let command = f.command();
    f.service.create(command.clone()).await.unwrap();
    f.harness.clock.advance_ms(MIN_SCHEDULE_DELAY_MS);
    f.harness
        .access
        .set(Err(AccessError::Unavailable(PortError::new("fixture unavailable"))));
    f.dispatcher.execute(address(&command).key).await.unwrap();
    let waiting = f.service.get(address(&command)).await.unwrap();
    assert_eq!(waiting.status, ScheduledStatus::Pending);
    assert_eq!(waiting.revision, 1);
    assert!(waiting.next_attempt_at_ms > f.harness.clock.now_unix_ms());
    assert!(f.harness.messages.last_message_id(CHANNEL).await.unwrap().is_none());
    f.harness.access.set(Ok(dm_access(DeliveryMode::Standard)));
    f.dispatcher.execute(address(&command).key).await.unwrap();
    assert!(f.harness.messages.last_message_id(CHANNEL).await.unwrap().is_none());
    f.harness.clock.advance_ms(60_000);
    f.dispatcher.execute(address(&command).key).await.unwrap();
    assert_eq!(
        f.service.get(address(&command)).await.unwrap().status,
        ScheduledStatus::Sent
    );
}

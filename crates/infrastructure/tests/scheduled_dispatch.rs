//! Real journal and message storage, with faults injected only after actual durable operations.
#[path = "scheduled_dispatch/faults.rs"]
mod faults;
#[path = "scheduled_dispatch/fixture.rs"]
mod fixture;
#[path = "scheduled_dispatch/load.rs"]
mod load;
mod support;

use std::sync::{Arc, atomic::Ordering};

use discordy_messages_application::{
    ports::MessageRepository,
    scheduled::{ScheduledDispatcher, ScheduledMessages, ScheduledWorker},
};
use discordy_messages_domain::scheduled::{MIN_SCHEDULE_DELAY_MS, PREPARATION_TIMEOUT_MS, ScheduledStatus};
use fixture::{Fixture, address};

#[tokio::test]
async fn independent_workers_insert_once_and_lost_success_never_resends_after_delete_or_restart() {
    let f = Fixture::new("scheduled_delivery").await;
    let service = ScheduledMessages::new(f.first.clone());
    let command = f.command(1);
    service.create(command.clone()).await.unwrap();
    f.harness.clock.advance_ms(MIN_SCHEDULE_DELAY_MS);
    let mut first = ScheduledWorker::new(f.first.clone());
    let mut second = ScheduledWorker::new(f.second.clone());
    let (a, b) = tokio::join!(first.scan(), second.scan());
    a.unwrap();
    b.unwrap();
    tokio::join!(first.drain(), second.drain());
    let sent = service.get(address(&command)).await.unwrap();
    assert_eq!(sent.status, ScheduledStatus::Sent);
    assert_eq!(f.messages.calls.load(Ordering::SeqCst), 1);
    assert!(
        f.messages
            .find(command.channel_id, sent.message_id.unwrap())
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(f.harness.events.events.lock().unwrap().len(), 1);
    let sent_id = sent.message_id.unwrap();
    let day = (sent_id.unix_millis() / 86_400_000) as i64;
    let result = f
        ._database
        .session
        .query_unpaged(
            "SELECT count(*) FROM guild_message_activity WHERE guild_id = ? AND utc_day = ?",
            (discordy_messages_application::fakes::GUILD.value() as i64, day),
        )
        .await
        .unwrap();
    let rows = result.into_rows_result().unwrap();
    let (count,): (i64,) = rows.single_row().unwrap();
    assert_eq!(count, 1, "scheduled insert writes the same durable marker");

    let uncertain = f.command(2);
    service.create(uncertain.clone()).await.unwrap();
    f.messages.lose_result.store(true, Ordering::SeqCst);
    f.harness.clock.advance_ms(MIN_SCHEDULE_DELAY_MS);
    let mut worker = ScheduledWorker::new(f.second.clone());
    worker.scan().await.unwrap();
    worker.drain().await;
    let unknown = service.get(address(&uncertain)).await.unwrap();
    assert_eq!(unknown.status, ScheduledStatus::DeliveryUnknown);
    let stored = unknown.message_id.unwrap();
    assert!(f.messages.find(uncertain.channel_id, stored).await.unwrap().is_some());
    assert!(unknown.draft.is_none());
    assert_eq!(f.messages.calls.load(Ordering::SeqCst), 2);
    f.messages.delete(uncertain.channel_id, &[stored]).await.unwrap();
    f.harness.clock.advance_ms(31 * 86_400_000);
    let replay = service.create(uncertain.clone()).await.unwrap();
    assert!(!replay.created);
    assert_eq!(replay.job.status, ScheduledStatus::DeliveryUnknown);
    let mut restarted = ScheduledWorker::new(f.first.clone());
    restarted.scan().await.unwrap();
    restarted.drain().await;
    assert_eq!(f.messages.calls.load(Ordering::SeqCst), 2);
    assert!(f.messages.find(uncertain.channel_id, stored).await.unwrap().is_none());
    assert_eq!(f.harness.events.events.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn post_claim_access_rejects_revocation_and_a_lost_claim_result_authorizes_no_insert() {
    let mut f = Fixture::new("scheduled_claim").await;
    let paused = Arc::new(faults::PausedJournal::new(f.first.journal.clone(), true));
    f.first.journal = paused.clone();
    let service = ScheduledMessages::new(f.first.clone());
    let command = f.command(1);
    service.create(command.clone()).await.unwrap();
    f.harness.clock.advance_ms(MIN_SCHEDULE_DELAY_MS);
    let dispatcher = ScheduledDispatcher::new(f.first.clone());
    let key = address(&command).key;
    let dispatch = tokio::spawn(async move { dispatcher.execute(key).await });
    paused.entered.notified().await;
    f.harness.access.set_permissions(0);
    paused.release.notify_one();
    dispatch.await.unwrap().unwrap();
    assert_eq!(
        service.get(address(&command)).await.unwrap().status,
        ScheduledStatus::Failed
    );
    assert_eq!(f.messages.calls.load(Ordering::SeqCst), 0);

    f.harness
        .access
        .set_permissions(discordy_messages_application::fakes::MEMBER);
    let uncertain = f.command(2);
    service.create(uncertain.clone()).await.unwrap();
    f.harness.clock.advance_ms(MIN_SCHEDULE_DELAY_MS);
    paused.lose_claim_result.store(true, Ordering::SeqCst);
    assert!(
        ScheduledDispatcher::new(f.first.clone())
            .execute(address(&uncertain).key)
            .await
            .is_err()
    );
    assert_eq!(f.messages.calls.load(Ordering::SeqCst), 0);
    f.harness.clock.advance_ms(PREPARATION_TIMEOUT_MS);
    let mut restarted = ScheduledWorker::new(f.second.clone());
    restarted.scan().await.unwrap();
    restarted.drain().await;
    assert_eq!(
        service.get(address(&uncertain)).await.unwrap().status,
        ScheduledStatus::DeliveryUnknown
    );
    assert_eq!(f.messages.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn aborted_preparation_fences_a_creator_paused_after_indexing_before_activation() {
    let mut f = Fixture::new("scheduled_activation").await;
    let paused = Arc::new(faults::PausedJournal::new(f.first.journal.clone(), false));
    f.first.journal = paused.clone();
    let service = Arc::new(ScheduledMessages::new(f.first.clone()));
    let command = f.command(1);
    let creator = {
        let service = service.clone();
        let command = command.clone();
        tokio::spawn(async move { service.create(command).await })
    };
    paused.entered.notified().await;
    f.harness.clock.advance_ms(PREPARATION_TIMEOUT_MS);
    let mut recovering = ScheduledWorker::new(f.second.clone());
    recovering.scan().await.unwrap();
    recovering.drain().await;
    paused.release.notify_one();
    let result = creator.await.unwrap().unwrap();
    assert!(!result.created);
    assert_eq!(result.job.status, ScheduledStatus::Failed);
    assert!(result.job.draft.is_none());
    assert!(f.second.journal.active(command.author_id).await.unwrap().is_empty());
    assert_eq!(f.messages.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn explicit_reconciliation_recovers_a_real_reservation_that_never_reached_the_due_index() {
    let mut f = Fixture::new("scheduled_unindexed").await;
    f.first.index = Arc::new(faults::UnavailableIndex(f.first.index.clone()));
    let service = ScheduledMessages::new(f.first.clone());
    let command = f.command(1);
    assert!(service.create(command.clone()).await.is_err());
    assert_eq!(f.second.journal.active(command.author_id).await.unwrap().len(), 1);
    f.harness.clock.advance_ms(PREPARATION_TIMEOUT_MS);
    f.harness.access.set_permissions(0);
    let reopened = ScheduledMessages::new(f.second.clone());
    reopened.reconcile_author(command.author_id).await.unwrap();
    let failed = reopened.get(address(&command)).await.unwrap();
    assert_eq!(failed.status, ScheduledStatus::Failed);
    assert!(failed.draft.is_none());
    assert!(f.second.journal.active(command.author_id).await.unwrap().is_empty());
    let replay = reopened.create(command).await.unwrap();
    assert!(!replay.created);
    assert_eq!(replay.job.status, ScheduledStatus::Failed);
    assert_eq!(f.messages.calls.load(Ordering::SeqCst), 0);
}

use super::{PollWorker, Polls};
use crate::PortError;
use crate::fakes::{ALICE, BOB, CHANNEL, Harness, InMemoryPolls, START_MS, dm_access};
use crate::ports::{EventPublisher, MessageEvent, MessageRepository, PollKey, PollRepository};
use async_trait::async_trait;
use discordy_messages_domain::poll::{PollDefinition, PollDraft, PollMedia, PollStatus};
use discordy_messages_domain::{DeliveryMode, Message, MessageDraft, Snowflake};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tokio::sync::Semaphore;

fn setup() -> (Harness, Arc<InMemoryPolls>) {
    let mut h = Harness::new();
    let repository = Arc::new(InMemoryPolls::new(h.clock.clone()));
    h.ports.polls = Some(repository.clone());
    (h, repository)
}
fn worker(h: &Harness) -> PollWorker {
    PollWorker::new(Arc::new(Polls::new(&h.ports).unwrap()), h.clock.clone())
}
async fn seed(h: &Harness, repository: &InMemoryPolls, sequence: u64, shard: u8) -> PollKey {
    let definition = PollDefinition::new(
        PollDraft {
            question: "Choose".into(),
            answers: vec![PollMedia::text("One"), PollMedia::text("Two")],
            duration_hours: 1,
            allow_multiselect: false,
        },
        START_MS as i64,
    )
    .unwrap();
    let key = PollKey {
        channel_id: CHANNEL,
        message_id: Snowflake::new((1 << 22) + sequence * 64 + u64::from(shard)),
    };
    repository.initialize(key, &definition).await.unwrap();
    let message = Message::compose(
        key.message_id,
        key.channel_id,
        ALICE,
        MessageDraft {
            poll: Some(definition),
            ..Default::default()
        },
        false,
    )
    .unwrap();
    h.messages.insert(&message).await.unwrap();
    key
}
async fn complete(worker: &mut PollWorker) {
    while worker.in_flight() > 0 {
        worker.next_completion().await.unwrap().unwrap();
    }
    assert_eq!(worker.queued(), 0);
}

#[tokio::test]
async fn all_64_shards_are_visited_in_16_scans_and_the_clock_controls_expiry() {
    let (h, repository) = setup();
    let mut keys = Vec::new();
    for shard in 0..64 {
        keys.push(seed(&h, &repository, 1, shard).await);
    }
    let mut worker = worker(&h);
    for _ in 0..16 {
        assert_eq!(worker.scan().await.unwrap().examined, 0);
    }
    h.clock.advance_ms(3_600_000);
    for scan in 0..16 {
        let report = worker.scan().await.unwrap();
        assert_eq!((report.pages, report.examined), (4, 4));
        complete(&mut worker).await;
        assert_eq!(h.events.kinds().len(), (scan + 1) * 4);
    }
    for key in keys {
        assert_eq!(
            repository.snapshot(key, BOB).await.unwrap().unwrap().aggregate.status,
            PollStatus::Finalized
        );
    }
    for _ in 0..16 {
        assert_eq!(worker.scan().await.unwrap().examined, 0);
    }
    assert_eq!(h.events.kinds().len(), 64);
}

#[tokio::test]
async fn full_shard_pages_resume_after_100_and_restart_rediscovers_remaining_entries() {
    let (h, repository) = setup();
    for sequence in 1..=105 {
        seed(&h, &repository, sequence, 0).await;
    }
    h.clock.advance_ms(3_600_000);
    let mut first = worker(&h);
    assert_eq!(first.scan().await.unwrap().examined, 100);
    complete(&mut first).await;
    assert_eq!(h.events.kinds().len(), 100);
    drop(first);
    let mut restarted = worker(&h);
    assert_eq!(restarted.scan().await.unwrap().examined, 5);
    complete(&mut restarted).await;
    assert_eq!(h.events.kinds().len(), 105);
    assert!(repository.due_page(0, None, 100).await.unwrap().is_empty());
}

struct BlockedEvents {
    entered: AtomicUsize,
    release: Semaphore,
}
#[async_trait]
impl EventPublisher for BlockedEvents {
    async fn publish(&self, _: MessageEvent) -> Result<(), PortError> {
        self.entered.fetch_add(1, Ordering::SeqCst);
        self.release.acquire().await.unwrap().forget();
        Ok(())
    }
}
async fn wait_entered(events: &BlockedEvents, count: usize) {
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while events.entered.load(Ordering::SeqCst) < count {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn blocked_completion_keeps_four_active_and_400_total_without_skipping_unqueued_work() {
    let (mut h, repository) = setup();
    for shard in 0..6 {
        for sequence in 1..=100 {
            seed(&h, &repository, sequence, shard).await;
        }
    }
    h.clock.advance_ms(3_600_000);
    let events = Arc::new(BlockedEvents {
        entered: AtomicUsize::new(0),
        release: Semaphore::new(0),
    });
    h.ports.events = events.clone();
    let mut first = worker(&h);
    assert_eq!(first.scan().await.unwrap().examined, 400);
    wait_entered(&events, 4).await;
    assert_eq!((first.in_flight(), first.queued()), (4, 396));
    assert_eq!(first.scan().await.unwrap().examined, 2);
    assert_eq!((first.in_flight(), first.queued()), (4, 396));
    events.release.add_permits(1);
    first.next_completion().await.unwrap().unwrap();
    wait_entered(&events, 5).await;
    assert_eq!((first.in_flight(), first.queued()), (4, 395));
    first.stop();
    events.release.add_permits(4);
    first.drain().await;
    assert_eq!((first.in_flight(), first.queued()), (0, 0));
    assert!(first.scan().await.is_err());
    assert_eq!(events.entered.load(Ordering::SeqCst), 5);
    h.ports.events = h.events.clone();
    let mut restarted = worker(&h);
    for _ in 0..32 {
        restarted.scan().await.unwrap();
        complete(&mut restarted).await;
    }
    assert_eq!(h.events.kinds().len(), 595);
    for shard in 0..6 {
        assert!(repository.due_page(shard, None, 100).await.unwrap().is_empty());
    }
}

#[tokio::test]
async fn private_or_missing_channel_and_unaccepted_orphan_finalize_without_content_publication() {
    for scenario in 0..3 {
        let (h, repository) = setup();
        let key = seed(&h, &repository, 1, 0).await;
        if scenario == 2 {
            h.messages.delete(CHANNEL, &[key.message_id]).await.unwrap();
        }
        *h.access.channel.lock().unwrap() = if scenario == 1 {
            Err(crate::ports::AccessError::UnknownChannel)
        } else {
            Ok(dm_access(DeliveryMode::P2p))
        };
        h.clock.advance_ms(3_600_000);
        let mut worker = worker(&h);
        worker.scan().await.unwrap();
        complete(&mut worker).await;
        assert_eq!(
            repository.snapshot(key, BOB).await.unwrap().unwrap().aggregate.status,
            PollStatus::Finalized
        );
        assert!(h.events.kinds().is_empty());
        if scenario == 2 {
            assert!(h.messages.find(CHANNEL, key.message_id).await.unwrap().is_none());
        }
    }
}

#[tokio::test]
async fn discovery_failure_keeps_durable_work_for_a_later_rotation() {
    let (h, repository) = setup();
    let key = seed(&h, &repository, 1, 0).await;
    h.clock.advance_ms(3_600_000);
    let mut worker = worker(&h);
    *repository.failure.lock().unwrap() = Some(PortError::new("offline"));
    assert!(worker.scan().await.is_err());
    *repository.failure.lock().unwrap() = None;
    for _ in 0..16 {
        worker.scan().await.unwrap();
        complete(&mut worker).await;
    }
    assert_eq!(
        repository.snapshot(key, BOB).await.unwrap().unwrap().aggregate.status,
        PollStatus::Finalized
    );
    assert_eq!(h.events.kinds().len(), 1);
}

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use async_trait::async_trait;
use discordy_messages_domain::Snowflake;
use discordy_messages_domain::scheduled::*;

use super::fakes::{MemorySchedules, SequenceAttempts};
use super::{ScheduledPorts, ScheduledWorker};
use crate::{
    AppResult,
    fakes::{Harness, START_MS},
    ports::scheduled::*,
    use_cases::CreateMessage,
};

struct Fixture {
    harness: Harness,
    memory: Arc<MemorySchedules>,
    ports: ScheduledPorts,
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
        Self { harness, memory, ports }
    }

    async fn seed(&self, sequence: u64, status: ScheduledStatus) -> ScheduledMessage {
        self.seed_in_shard(sequence, status, 0).await
    }

    async fn seed_in_shard(&self, sequence: u64, status: ScheduledStatus, shard: u8) -> ScheduledMessage {
        let author = 100 + sequence / 25;
        let bits = sequence * 64 + ((author % 64) ^ u64::from(shard));
        let mut job = ScheduledMessage::prepare(
            ScheduleRequest {
                id: format!("00000000-0000-4000-8000-{bits:012x}").parse().unwrap(),
                author_id: Snowflake::new(author),
                channel_id: Snowflake::new(200),
                scheduled_at_ms: START_MS + MIN_SCHEDULE_DELAY_MS,
                draft: ScheduledDraft {
                    content: "scheduled".into(),
                    tts: false,
                    reply_to: None,
                    replied_user: true,
                    silent: false,
                },
                original_fingerprint: "a".repeat(64),
            },
            START_MS,
        )
        .unwrap();
        self.memory.reserve(job.clone()).await.unwrap();
        self.memory
            .stage(DueSchedule {
                wake_at_ms: job.scheduled_at_ms,
                key: (&job).into(),
                revision: 1,
            })
            .await
            .unwrap();
        if status == ScheduledStatus::Pending {
            job = self
                .memory
                .change((&job).into(), ScheduleChange::Activate, START_MS)
                .await
                .unwrap();
        }
        assert_eq!(DueSchedule::pending(&job).shard(), shard);
        job
    }
}

struct GatedDelivery {
    inner: CreateMessage,
    release: tokio::sync::Semaphore,
    current: AtomicUsize,
    maximum: AtomicUsize,
}

#[async_trait]
impl ScheduledDelivery for GatedDelivery {
    async fn validate(&self, draft: &ScheduledAuthorDraft) -> AppResult<()> {
        self.inner.validate(draft).await
    }
    async fn prepare(&self, draft: &ScheduledAuthorDraft) -> AppResult<PreparedScheduledMessage> {
        self.inner.prepare(draft).await
    }
    async fn insert(&self, message: &mut PreparedScheduledMessage) -> DispatchOutcome {
        let current = self.current.fetch_add(1, Ordering::SeqCst) + 1;
        self.maximum.fetch_max(current, Ordering::SeqCst);
        self.release.acquire().await.unwrap().forget();
        let result = self.inner.insert(message).await;
        self.current.fetch_sub(1, Ordering::SeqCst);
        result
    }
    async fn publish(&self, message: &PreparedScheduledMessage) {
        self.inner.publish(message).await;
    }
}

#[tokio::test]
async fn worker_bounds_active_dispatches_and_does_not_duplicate_queued_keys() {
    let mut f = Fixture::new();
    for sequence in 1..=12 {
        f.seed(sequence, ScheduledStatus::Pending).await;
    }
    f.harness.clock.advance_ms(MIN_SCHEDULE_DELAY_MS);
    let delivery = Arc::new(GatedDelivery {
        inner: CreateMessage::new(&f.harness.ports),
        release: tokio::sync::Semaphore::new(0),
        current: AtomicUsize::new(0),
        maximum: AtomicUsize::new(0),
    });
    f.ports.delivery = delivery.clone();
    let mut worker = ScheduledWorker::new(f.ports);
    let report = worker.scan().await.unwrap();
    assert_eq!(report.pages, 4);
    assert_eq!(report.examined, 12);
    assert_eq!((worker.in_flight(), worker.queued()), (4, 8));
    for _ in 0..100 {
        tokio::task::yield_now().await;
        if delivery.current.load(Ordering::SeqCst) == 4 {
            break;
        }
    }
    assert_eq!(delivery.current.load(Ordering::SeqCst), 4);
    for _ in 0..20 {
        let report = worker.scan().await.unwrap();
        assert!(report.pages <= 4 && report.examined <= 400);
    }
    assert_eq!((worker.in_flight(), worker.queued()), (4, 8));
    delivery.release.add_permits(12);
    worker.drain().await;
    assert_eq!(delivery.maximum.load(Ordering::SeqCst), 4);
    assert!(
        f.memory
            .jobs
            .lock()
            .unwrap()
            .values()
            .all(|job| job.status == ScheduledStatus::Sent)
    );
    assert_eq!(f.harness.events.events.lock().unwrap().len(), 12);
}

#[tokio::test]
async fn recovery_observations_include_unknown_and_the_age_of_the_abandoned_claim() {
    let f = Fixture::new();
    let pending = f.seed(1, ScheduledStatus::Pending).await;
    f.harness.clock.advance_ms(MIN_SCHEDULE_DELAY_MS);
    f.memory
        .change(
            (&pending).into(),
            ScheduleChange::Claim {
                expected_revision: 1,
                attempt_id: "00000000-0000-4000-8000-000000000001".parse().unwrap(),
                message_id: Snowflake::new(1000),
            },
            START_MS + MIN_SCHEDULE_DELAY_MS,
        )
        .await
        .unwrap();
    f.harness.clock.advance_ms(PREPARATION_TIMEOUT_MS + 1000);
    let report = ScheduledWorker::new(f.ports).scan().await.unwrap();
    assert_eq!(report.recovered, 1);
    assert_eq!(
        report.states[6], 1,
        "a newly recovered unknown must be visible to alerts"
    );
    assert_eq!(report.shard_claim_ages[0], (0, [0, PREPARATION_TIMEOUT_MS + 1000]));
}

#[tokio::test]
async fn restart_continues_beyond_a_page_and_wrap_revisits_a_new_earlier_entry() {
    let f = Fixture::new();
    for sequence in 1..=225 {
        f.seed(sequence, ScheduledStatus::Pending).await;
    }
    f.harness.clock.advance_ms(MIN_SCHEDULE_DELAY_MS);
    f.harness.access.set_permissions(0);
    for expected in [100, 100, 25] {
        let mut worker = ScheduledWorker::new(f.ports.clone());
        let report = worker.scan().await.unwrap();
        assert_eq!(report.examined, expected);
        worker.drain().await;
    }
    assert!(
        f.memory
            .jobs
            .lock()
            .unwrap()
            .values()
            .all(|job| job.status == ScheduledStatus::Failed)
    );
    let early = f.seed(0, ScheduledStatus::Pending).await;
    let mut restarted = ScheduledWorker::new(f.ports.clone());
    restarted.scan().await.unwrap();
    restarted.drain().await;
    assert_eq!(
        f.memory.find((&early).into()).await.unwrap().unwrap().status,
        ScheduledStatus::Failed
    );
}

#[tokio::test]
async fn preparing_and_not_yet_committed_revision_entries_are_retained() {
    let f = Fixture::new();
    let preparing = f.seed(1, ScheduledStatus::Preparing).await;
    let pending = f.seed(2, ScheduledStatus::Pending).await;
    let future = DueSchedule {
        wake_at_ms: pending.scheduled_at_ms - 1,
        key: (&pending).into(),
        revision: pending.revision + 1,
    };
    f.memory.stage(future).await.unwrap();
    f.harness.clock.advance_ms(MIN_SCHEDULE_DELAY_MS);
    let mut worker = ScheduledWorker::new(f.ports.clone());
    worker.scan().await.unwrap();
    assert!(f.memory.due.lock().unwrap().contains(&DueSchedule {
        wake_at_ms: preparing.scheduled_at_ms,
        key: (&preparing).into(),
        revision: 1
    }));
    assert!(f.memory.due.lock().unwrap().contains(&future));
    worker.drain().await;
    f.harness.clock.advance_ms(PREPARATION_TIMEOUT_MS);
    let mut restarted = ScheduledWorker::new(f.ports.clone());
    restarted.scan().await.unwrap();
    assert_eq!(
        f.memory.find((&preparing).into()).await.unwrap().unwrap().status,
        ScheduledStatus::Failed
    );
    assert!(
        !f.memory
            .due
            .lock()
            .unwrap()
            .iter()
            .any(|entry| entry.key.id == preparing.id)
    );
}

#[tokio::test]
async fn stopping_a_worker_does_not_start_its_unclaimed_queue() {
    let f = Fixture::new();
    for sequence in 1..=12 {
        f.seed(sequence, ScheduledStatus::Pending).await;
    }
    f.harness.clock.advance_ms(MIN_SCHEDULE_DELAY_MS);
    let mut worker = ScheduledWorker::new(f.ports.clone());
    worker.scan().await.unwrap();
    worker.stop();
    worker.drain().await;
    assert_eq!(
        f.memory
            .jobs
            .lock()
            .unwrap()
            .values()
            .filter(|job| job.status == ScheduledStatus::Pending)
            .count(),
        8
    );
}

#[tokio::test]
async fn backoff_cannot_extend_the_twenty_four_hour_failure_deadline() {
    let f = Fixture::new();
    let job = f.seed(1, ScheduledStatus::Pending).await;
    f.harness.clock.advance_ms(MIN_SCHEDULE_DELAY_MS + MAX_OVERDUE_MS - 100);
    f.memory
        .change(
            (&job).into(),
            ScheduleChange::Defer { expected_revision: 1 },
            START_MS + MIN_SCHEDULE_DELAY_MS + MAX_OVERDUE_MS - 100,
        )
        .await
        .unwrap();
    f.harness.clock.advance_ms(100);
    let mut worker = ScheduledWorker::new(f.ports.clone());
    worker.scan().await.unwrap();
    worker.drain().await;
    assert_eq!(
        f.memory.find((&job).into()).await.unwrap().unwrap().status,
        ScheduledStatus::Failed
    );
    assert!(f.harness.events.events.lock().unwrap().is_empty());
}

#[tokio::test]
async fn full_buffer_never_advances_a_cursor_past_an_unretained_candidate() {
    let mut f = Fixture::new();
    for sequence in 1..=404 {
        f.seed_in_shard(sequence, ScheduledStatus::Pending, (sequence % 4) as u8)
            .await;
    }
    f.harness.clock.advance_ms(MIN_SCHEDULE_DELAY_MS);
    let delivery = Arc::new(GatedDelivery {
        inner: CreateMessage::new(&f.harness.ports),
        release: tokio::sync::Semaphore::new(0),
        current: AtomicUsize::new(0),
        maximum: AtomicUsize::new(0),
    });
    f.ports.delivery = delivery.clone();
    let mut worker = ScheduledWorker::new(f.ports.clone());
    let report = worker.scan().await.unwrap();
    assert_eq!(
        (report.pages, report.examined, worker.in_flight(), worker.queued()),
        (4, 400, 4, 396)
    );
    let before = f.memory.cursors.lock().unwrap().clone();
    for _ in 0..16 {
        let report = worker.scan().await.unwrap();
        assert!(report.examined <= 400);
    }
    assert_eq!(*f.memory.cursors.lock().unwrap(), before);
    assert_eq!((worker.in_flight(), worker.queued()), (4, 396));
    delivery.release.add_permits(404);
    worker.drain().await;
    for _ in 0..16 {
        worker.scan().await.unwrap();
    }
    worker.drain().await;
    assert!(
        f.memory
            .jobs
            .lock()
            .unwrap()
            .values()
            .all(|job| job.status == ScheduledStatus::Sent)
    );
    assert_eq!(f.harness.events.events.lock().unwrap().len(), 404);
}

//! Bounded delivery to the push provider (ADR 0016).
//!
//! `submit` never waits: a job goes into a bounded queue, or is dropped and counted when the
//! queue is full, so a burst can never stall the event consumer or grow memory without limit. A
//! pump takes jobs out of the queue only while one of `max_in_flight` slots is free (the
//! provider's cap on pending requests), and a job keeps its slot across retries. Failed sends are
//! retried with exponential backoff up to `max_attempts`; a token the provider rejects is removed.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{Semaphore, mpsc};

use super::ports::{DeviceToken, PushSender, SendError, TokenRegistry};
use crate::domain::OutboundPush;

/// How long a call ring (a PushKit push) may be retried: the phone rings for 30 seconds.
const VOIP_RETRY_WINDOW: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Copy)]
pub struct DispatchConfig {
    /// Jobs that may wait for a free slot; beyond this, new jobs are dropped.
    pub queue_capacity: usize,
    /// Sends (and retry waits) in progress at once.
    pub max_in_flight: usize,
    pub max_attempts: u32,
    pub base_backoff: Duration,
    pub max_backoff: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Submitted {
    Queued,
    Dropped,
}

struct Job {
    token: DeviceToken,
    push: OutboundPush,
}

#[derive(Clone)]
pub struct Dispatcher {
    queue: mpsc::Sender<Job>,
}

impl Dispatcher {
    /// Starts the pump on the current tokio runtime.
    pub fn spawn(config: DispatchConfig, sender: Arc<dyn PushSender>, registry: Arc<dyn TokenRegistry>) -> Self {
        let (queue, jobs) = mpsc::channel(config.queue_capacity.max(1));
        tokio::spawn(pump(config, jobs, sender, registry));
        Self { queue }
    }

    pub fn submit(&self, token: DeviceToken, push: OutboundPush) -> Submitted {
        let provider = token.platform.label();
        match self.queue.try_send(Job { token, push }) {
            Ok(()) => {
                metrics::gauge!("notifier_queue_depth").set((self.queue.max_capacity() - self.queue.capacity()) as f64);
                Submitted::Queued
            }
            Err(_) => {
                metrics::counter!("notifier_pushes_total", "provider" => provider, "outcome" => "dropped").increment(1);
                Submitted::Dropped
            }
        }
    }
}

async fn pump(
    config: DispatchConfig,
    mut jobs: mpsc::Receiver<Job>,
    sender: Arc<dyn PushSender>,
    registry: Arc<dyn TokenRegistry>,
) {
    let slots = Arc::new(Semaphore::new(config.max_in_flight.max(1)));
    while let Some(job) = jobs.recv().await {
        let Ok(slot) = Arc::clone(&slots).acquire_owned().await else {
            return;
        };
        metrics::gauge!("notifier_queue_depth").set(jobs.len() as f64);
        let sender = Arc::clone(&sender);
        let registry = Arc::clone(&registry);
        tokio::spawn(async move {
            deliver(config, job, sender.as_ref(), registry.as_ref()).await;
            drop(slot);
        });
    }
}

fn outcome(provider: &'static str, label: &'static str) {
    metrics::counter!("notifier_pushes_total", "provider" => provider, "outcome" => label).increment(1);
}

async fn deliver(config: DispatchConfig, job: Job, sender: &dyn PushSender, registry: &dyn TokenRegistry) {
    let provider = job.token.platform.label();
    let started = tokio::time::Instant::now();
    let mut attempt = 1;
    loop {
        match sender.send(&job.token, &job.push).await {
            Ok(()) => {
                metrics::histogram!("notifier_push_duration_seconds", "provider" => provider)
                    .record(started.elapsed().as_secs_f64());
                outcome(provider, "sent");
                return;
            }
            Err(SendError::InvalidToken(reason)) => {
                tracing::debug!(provider, %reason, "provider rejected a token, removing it");
                outcome(provider, "invalid_token");
                if let Err(error) = registry.forget(&job.token).await {
                    tracing::warn!(provider, %error, "could not remove a rejected token");
                }
                return;
            }
            Err(SendError::Failed(reason)) => {
                tracing::warn!(provider, %reason, "push failed");
                outcome(provider, "failed");
                return;
            }
            Err(SendError::Retry { reason, after }) => {
                // A call ring that has been retried for longer than the phone rings is of no use.
                let ring_over = matches!(job.push, OutboundPush::Voip(_)) && started.elapsed() > VOIP_RETRY_WINDOW;
                if attempt >= config.max_attempts || ring_over {
                    tracing::warn!(provider, %reason, attempt, "push failed, giving up");
                    outcome(provider, "failed");
                    return;
                }
                metrics::counter!("notifier_push_retries_total", "provider" => provider).increment(1);
                let backoff = config.base_backoff.saturating_mul(2u32.saturating_pow(attempt - 1));
                tokio::time::sleep(after.unwrap_or(backoff).min(config.max_backoff)).await;
                attempt += 1;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use tokio::sync::Notify;

    use super::*;
    use crate::application::ports::{Environment, Platform, PortError};
    use crate::domain::AlertContent;

    #[derive(Default)]
    struct FakeSender {
        script: Mutex<VecDeque<Result<(), SendError>>>,
        gate: Option<Arc<Notify>>,
        work: Duration,
        in_flight: AtomicUsize,
        max_in_flight: AtomicUsize,
        attempts: AtomicUsize,
    }

    #[async_trait]
    impl PushSender for FakeSender {
        async fn send(&self, _token: &DeviceToken, _push: &OutboundPush) -> Result<(), SendError> {
            self.attempts.fetch_add(1, Ordering::SeqCst);
            let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_in_flight.fetch_max(now, Ordering::SeqCst);
            if let Some(gate) = &self.gate {
                gate.notified().await;
            }
            tokio::time::sleep(self.work).await;
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
            self.script.lock().unwrap().pop_front().unwrap_or(Ok(()))
        }
    }

    #[derive(Default)]
    struct FakeRegistry {
        forgotten: Mutex<Vec<String>>,
    }

    #[async_trait]
    impl TokenRegistry for FakeRegistry {
        async fn forget(&self, token: &DeviceToken) -> Result<(), PortError> {
            self.forgotten.lock().unwrap().push(token.token.clone());
            Ok(())
        }
    }

    fn config() -> DispatchConfig {
        DispatchConfig {
            queue_capacity: 1000,
            max_in_flight: 5,
            max_attempts: 3,
            base_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(1),
        }
    }

    fn token(name: &str) -> DeviceToken {
        DeviceToken {
            platform: Platform::Apns,
            token: name.into(),
            environment: Environment::Sandbox,
            previews: true,
            only_mentions: false,
        }
    }

    fn push() -> OutboundPush {
        OutboundPush::Alert(AlertContent {
            title: "t".into(),
            body: "b".into(),
            thread_id: "1".into(),
            data: Vec::new(),
        })
    }

    async fn settle() {
        for _ in 0..50 {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    #[tokio::test(start_paused = true)]
    async fn in_flight_sends_never_exceed_the_cap() {
        let sender = Arc::new(FakeSender {
            work: Duration::from_millis(50),
            ..FakeSender::default()
        });
        let dispatcher = Dispatcher::spawn(config(), sender.clone(), Arc::new(FakeRegistry::default()));
        for n in 0..200 {
            assert_eq!(dispatcher.submit(token(&format!("t{n}")), push()), Submitted::Queued);
        }
        settle().await;
        assert_eq!(sender.attempts.load(Ordering::SeqCst), 200, "every job was sent");
        assert_eq!(
            sender.max_in_flight.load(Ordering::SeqCst),
            5,
            "the cap was reached and not passed"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_full_queue_drops_new_jobs_instead_of_waiting() {
        let gate = Arc::new(Notify::new());
        let sender = Arc::new(FakeSender {
            gate: Some(gate.clone()),
            ..FakeSender::default()
        });
        let queue = DispatchConfig {
            queue_capacity: 2,
            max_in_flight: 1,
            ..config()
        };
        let dispatcher = Dispatcher::spawn(queue, sender.clone(), Arc::new(FakeRegistry::default()));
        let outcomes: Vec<Submitted> = (0..10)
            .map(|n| dispatcher.submit(token(&format!("t{n}")), push()))
            .collect();
        assert_eq!(outcomes.iter().filter(|o| **o == Submitted::Queued).count(), 2);
        assert_eq!(outcomes.iter().filter(|o| **o == Submitted::Dropped).count(), 8);
        for _ in 0..10 {
            gate.notify_one();
            settle().await;
        }
        assert_eq!(
            sender.attempts.load(Ordering::SeqCst),
            2,
            "only the queued jobs were sent"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_rejected_token_is_removed_and_not_retried() {
        let sender = Arc::new(FakeSender::default());
        sender
            .script
            .lock()
            .unwrap()
            .push_back(Err(SendError::InvalidToken("Unregistered".into())));
        let registry = Arc::new(FakeRegistry::default());
        let dispatcher = Dispatcher::spawn(config(), sender.clone(), registry.clone());
        dispatcher.submit(token("dead"), push());
        settle().await;
        assert_eq!(sender.attempts.load(Ordering::SeqCst), 1);
        assert_eq!(*registry.forgotten.lock().unwrap(), vec!["dead".to_owned()]);
    }

    #[tokio::test(start_paused = true)]
    async fn transient_failures_are_retried_with_backoff_until_they_work() {
        let sender = Arc::new(FakeSender::default());
        for _ in 0..2 {
            sender.script.lock().unwrap().push_back(Err(SendError::Retry {
                reason: "503".into(),
                after: None,
            }));
        }
        let registry = Arc::new(FakeRegistry::default());
        let dispatcher = Dispatcher::spawn(config(), sender.clone(), registry.clone());
        dispatcher.submit(token("flaky"), push());
        settle().await;
        assert_eq!(sender.attempts.load(Ordering::SeqCst), 3);
        assert!(registry.forgotten.lock().unwrap().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn retries_stop_at_the_attempt_limit_without_removing_the_token() {
        let sender = Arc::new(FakeSender::default());
        for _ in 0..10 {
            sender.script.lock().unwrap().push_back(Err(SendError::Retry {
                reason: "503".into(),
                after: None,
            }));
        }
        let registry = Arc::new(FakeRegistry::default());
        let dispatcher = Dispatcher::spawn(config(), sender.clone(), registry.clone());
        dispatcher.submit(token("down"), push());
        settle().await;
        assert_eq!(sender.attempts.load(Ordering::SeqCst), 3, "max_attempts");
        assert!(registry.forgotten.lock().unwrap().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn a_call_ring_stops_retrying_once_the_phone_would_have_stopped_ringing() {
        let slow = DispatchConfig {
            max_attempts: 50,
            max_backoff: Duration::from_secs(5),
            ..config()
        };
        let run = |push: OutboundPush| async {
            let sender = Arc::new(FakeSender::default());
            for _ in 0..50 {
                sender.script.lock().unwrap().push_back(Err(SendError::Retry {
                    reason: "503".into(),
                    after: Some(Duration::from_secs(3)),
                }));
            }
            let dispatcher = Dispatcher::spawn(slow, sender.clone(), Arc::new(FakeRegistry::default()));
            dispatcher.submit(token("t"), push);
            tokio::time::sleep(Duration::from_secs(300)).await;
            sender.attempts.load(Ordering::SeqCst)
        };
        assert_eq!(
            run(OutboundPush::Voip(serde_json::json!({"type": "call"}))).await,
            5,
            "attempts at 0, 3, 6, 9 and 12 seconds"
        );
        assert_eq!(run(push()).await, 50, "an alert keeps retrying up to max_attempts");
    }

    #[tokio::test(start_paused = true)]
    async fn a_permanent_failure_is_not_retried() {
        let sender = Arc::new(FakeSender::default());
        sender
            .script
            .lock()
            .unwrap()
            .push_back(Err(SendError::Failed("bad key".into())));
        let dispatcher = Dispatcher::spawn(config(), sender.clone(), Arc::new(FakeRegistry::default()));
        dispatcher.submit(token("x"), push());
        settle().await;
        assert_eq!(sender.attempts.load(Ordering::SeqCst), 1);
    }
}

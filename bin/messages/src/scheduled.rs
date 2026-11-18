//! Optional scheduler lifecycle. The default path creates no schema, task or acceptance service.
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, Instant};

use discordy_messages_application::{
    PortError,
    scheduled::{ScheduledMessages, ScheduledPorts, ScheduledWorker},
    use_cases::{CreateMessage, MessagingPorts},
};
use discordy_messages_infrastructure::{
    scylla::{ScyllaMessageRepository, ScyllaScheduledMessages, Session},
    system::RandomScheduleAttempts,
};
use tokio::sync::watch;

use crate::config::Config;

pub struct Scheduler {
    pub service: Arc<ScheduledMessages>,
    pub repository: Arc<ScyllaScheduledMessages>,
    pub running: Arc<AtomicBool>,
    shutdown: watch::Sender<bool>,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl Scheduler {
    pub async fn start(
        config: &Config,
        session: Arc<Session>,
        ports: &MessagingPorts,
    ) -> Result<Option<Self>, PortError> {
        if !config.scheduled_messages_enabled {
            return Ok(None);
        }
        let repository = Arc::new(
            ScyllaScheduledMessages::open(session.clone(), config.scylla_scheduled_message_keyspace.as_deref()).await?,
        );
        let mut messaging = ports.clone();
        messaging.messages = Arc::new(ScyllaMessageRepository::for_scheduled_delivery(session).await?);
        let scheduled = ScheduledPorts {
            journal: repository.clone(),
            index: repository.clone(),
            delivery: Arc::new(CreateMessage::new(&messaging)),
            attempts: Arc::new(RandomScheduleAttempts),
            clock: ports.clock.clone(),
        };
        let service = Arc::new(ScheduledMessages::new(scheduled.clone()));
        let running = Arc::new(AtomicBool::new(true));
        let (shutdown, receiver) = watch::channel(false);
        let alive = running.clone();
        let task = tokio::spawn(async move {
            let _guard = Running(alive);
            run(ScheduledWorker::new(scheduled), receiver).await;
        });
        Ok(Some(Self {
            service,
            repository,
            running,
            shutdown,
            task: Some(task),
        }))
    }

    pub async fn shutdown(mut self) {
        let _ = self.shutdown.send(true);
        if let Some(task) = self.task.take()
            && let Err(error) = task.await
        {
            tracing::warn!(%error, "scheduled worker stopped unexpectedly");
        }
    }
}

impl Drop for Scheduler {
    fn drop(&mut self) {
        let _ = self.shutdown.send(true);
    }
}
struct Running(Arc<AtomicBool>);
impl Drop for Running {
    fn drop(&mut self) {
        self.0.store(false, Ordering::SeqCst);
    }
}

async fn run(mut worker: ScheduledWorker, mut shutdown: watch::Receiver<bool>) {
    let mut ticks = tokio::time::interval(Duration::from_secs(1));
    ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut due_ages = [0_u64; 64];
    let mut claim_ages = [[0_u64; 2]; 64];
    let mut passes = [Instant::now(); 64];
    loop {
        let busy = worker.in_flight() > 0;
        tokio::select! {
            biased;
            changed = shutdown.changed() => { if changed.is_err() || *shutdown.borrow() { break; } }
            outcome = worker.next_completion(), if busy => {
                if let Some(Err(error)) = outcome {
                    tracing::warn!(%error, "scheduled dispatch could not finish");
                    metrics::counter!("scheduled_dispatch_errors_total").increment(1);
                }
            }
            _ = ticks.tick() => {
                match tokio::time::timeout(Duration::from_secs(5), worker.scan()).await {
                    Ok(Ok(report)) => observe(report, &mut due_ages, &mut claim_ages, &mut passes),
                    Ok(Err(error)) => { tracing::warn!(%error, "scheduled discovery failed; cursors will be revisited"); metrics::counter!("scheduled_scan_errors_total").increment(1); }
                    Err(_) => { tracing::warn!("scheduled discovery timed out; cursors will be revisited"); metrics::counter!("scheduled_scan_errors_total").increment(1); }
                }
            }
        }
        metrics::gauge!("scheduled_in_flight").set(worker.in_flight() as f64);
        metrics::gauge!("scheduled_buffered").set(worker.queued() as f64);
    }
    worker.stop();
    if tokio::time::timeout(Duration::from_secs(30), worker.drain())
        .await
        .is_err()
    {
        tracing::warn!("scheduled shutdown timed out; unfinished claims require unknown recovery");
    }
}

fn observe(
    report: discordy_messages_application::scheduled::ScheduleScan,
    due_ages: &mut [u64; 64],
    claim_ages: &mut [[u64; 2]; 64],
    passes: &mut [Instant; 64],
) {
    for (shard, age) in report.shard_due_ages {
        due_ages[usize::from(shard)] = age;
    }
    for (shard, ages) in report.shard_claim_ages {
        claim_ages[usize::from(shard)] = ages;
    }
    for (index, status) in ["preparing", "dispatching"].into_iter().enumerate() {
        metrics::gauge!("scheduled_observed_oldest_claim_seconds", "status" => status)
            .set(claim_ages.iter().map(|ages| ages[index]).max().unwrap_or(0) as f64 / 1000.0);
    }
    if let Ok(now) = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        metrics::gauge!("scheduled_last_successful_scan_unixtime_seconds").set(now.as_secs_f64());
    }
    metrics::gauge!("scheduled_observed_oldest_due_seconds")
        .set(due_ages.iter().copied().max().unwrap_or(0) as f64 / 1000.0);
    metrics::counter!("scheduled_scanned_candidates_total").increment(report.examined as u64);
    metrics::counter!("scheduled_recovered_total").increment(report.recovered as u64);
    for (status, count) in [
        "preparing",
        "pending",
        "dispatching",
        "sent",
        "failed",
        "cancelled",
        "delivery_unknown",
    ]
    .into_iter()
    .zip(report.states)
    {
        metrics::counter!("scheduled_observed_state_total", "status" => status).increment(count);
    }
    for shard in report.wrapped_shards {
        let started = &mut passes[usize::from(shard)];
        metrics::histogram!("scheduled_shard_pass_seconds").record(started.elapsed().as_secs_f64());
        *started = Instant::now();
    }
}

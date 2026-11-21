//! Existing polls keep expiring when new creation is disabled.
use discordy_messages_application::polls::{PollScan, PollWorker, Polls};
use discordy_messages_application::{PortError, use_cases::MessagingPorts};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, Instant};
use tokio::sync::watch;

pub struct PollExpiry {
    pub running: Arc<AtomicBool>,
    shutdown: watch::Sender<bool>,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl PollExpiry {
    pub fn start(ports: &MessagingPorts) -> Result<Self, PortError> {
        let service = Arc::new(Polls::new(ports).ok_or_else(|| PortError::new("poll storage was not composed"))?);
        let running = Arc::new(AtomicBool::new(true));
        let (shutdown, receiver) = watch::channel(false);
        let alive = running.clone();
        let clock = ports.clock.clone();
        let task = tokio::spawn(async move {
            let _guard = Running(alive);
            run(PollWorker::new(service, clock), receiver).await;
        });
        Ok(Self {
            running,
            shutdown,
            task: Some(task),
        })
    }
    pub async fn shutdown(mut self) {
        let _ = self.shutdown.send(true);
        if let Some(task) = self.task.take()
            && let Err(error) = task.await
        {
            tracing::warn!(%error, "poll worker stopped unexpectedly");
        }
    }
}
impl Drop for PollExpiry {
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

async fn run(mut worker: PollWorker, mut shutdown: watch::Receiver<bool>) {
    let mut ticks = tokio::time::interval(Duration::from_secs(1));
    ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut ages = [0_u64; 64];
    let mut passes = [Instant::now(); 64];
    loop {
        let busy = worker.in_flight() > 0;
        tokio::select! {
            biased;
            changed = shutdown.changed() => { if changed.is_err() || *shutdown.borrow() { break; } }
            outcome = worker.next_completion(), if busy => {
                match outcome {
                    Some(Ok(true)) => metrics::counter!("poll_finalized_total").increment(1),
                    Some(Err(error)) => { tracing::warn!(%error, "poll expiry did not complete"); metrics::counter!("poll_expiry_errors_total").increment(1); }
                    _ => {}
                }
            }
            _ = ticks.tick() => {
                match tokio::time::timeout(Duration::from_secs(5), worker.scan()).await {
                    Ok(Ok(report)) => observe(report, &mut ages, &mut passes),
                    Ok(Err(error)) => { tracing::warn!(%error, "poll discovery failed; durable entries will be revisited"); metrics::counter!("poll_scan_errors_total").increment(1); }
                    Err(_) => { tracing::warn!("poll discovery timed out"); metrics::counter!("poll_scan_errors_total").increment(1); }
                }
            }
        }
        metrics::gauge!("poll_expiry_in_flight").set(worker.in_flight() as f64);
        metrics::gauge!("poll_expiry_buffered").set(worker.queued() as f64);
    }
    worker.stop();
    if tokio::time::timeout(Duration::from_secs(15), worker.drain())
        .await
        .is_err()
    {
        tracing::warn!("poll expiry drain timed out; durable entries will be rediscovered");
    }
}

fn observe(report: PollScan, ages: &mut [u64; 64], passes: &mut [Instant; 64]) {
    for (shard, age) in report.shard_due_ages_ms {
        ages[usize::from(shard)] = age;
    }
    metrics::gauge!("poll_observed_oldest_due_seconds").set(ages.iter().copied().max().unwrap_or(0) as f64 / 1000.0);
    metrics::counter!("poll_scanned_candidates_total").increment(report.examined as u64);
    if let Ok(now) = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        metrics::gauge!("poll_last_successful_scan_unixtime_seconds").set(now.as_secs_f64());
    }
    for shard in report.wrapped_shards {
        let started = &mut passes[usize::from(shard)];
        metrics::histogram!("poll_shard_pass_seconds").record(started.elapsed().as_secs_f64());
        *started = Instant::now();
    }
}

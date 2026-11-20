use std::{
    sync::atomic::Ordering,
    time::{Duration, Instant},
};

use discordy_messages_application::{
    ports::scheduled::{DueSchedule, ScheduleListQuery},
    scheduled::{ScheduledMessages, ScheduledWorker},
};
use discordy_messages_domain::{Snowflake, scheduled::ScheduledStatus};

use super::fixture::Fixture;

#[tokio::test]
async fn paced_hot_shard_load_bounds_work_and_completes_a_real_persisted_scan_pass() {
    let f = Fixture::new("scheduled_paced_load").await;
    let service = ScheduledMessages::new(f.first.clone());
    let accepting = Instant::now();
    for sequence in 0..225 {
        // Nine authors each fill the real 25-per-channel quota; all due entries hash to shard zero.
        let author = Snowflake::new(100 + sequence / 25);
        let mut command = f.command(sequence);
        command.author_id = author;
        command.id = format!("00000000-0000-4000-8000-{:012x}", sequence * 64 + author.value() % 64)
            .parse()
            .unwrap();
        let accepted = service.create(command).await.unwrap();
        assert_eq!(DueSchedule::pending(&accepted.job).shard(), 0);
    }
    let acceptance_time = accepting.elapsed();
    f.harness.clock.advance_ms(60_000);
    let started = Instant::now();
    let mut previous_ms = 0;
    let mut worker = ScheduledWorker::new(f.second.clone());
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut pages = Vec::new();
    let mut maximum_age = 0;
    let mut maximum_in_flight = 0;
    let mut maximum_buffered = 0;
    tokio::time::timeout(Duration::from_secs(180), async {
        loop {
            interval.tick().await;
            let elapsed_ms = started.elapsed().as_millis() as u64;
            f.harness.clock.advance_ms(elapsed_ms.saturating_sub(previous_ms));
            previous_ms = elapsed_ms;
            let report = worker.scan().await.unwrap();
            assert!(report.pages <= 4 && report.examined <= 400);
            maximum_age = maximum_age.max(report.oldest_due_age_ms);
            maximum_in_flight = maximum_in_flight.max(worker.in_flight());
            maximum_buffered = maximum_buffered.max(worker.queued());
            assert!(worker.in_flight() <= 4 && worker.in_flight() + worker.queued() <= 400);
            if report.examined > 0 {
                pages.push(report.examined);
            }
            let wrapped = report.wrapped_shards.contains(&0);
            worker.drain().await;
            if wrapped {
                break;
            }
        }
    })
    .await
    .expect("bounded hot-shard acceptance load did not complete its pass");
    assert_eq!(pages, vec![100, 100, 25]);
    assert_eq!(f.messages.calls.load(Ordering::SeqCst), 225);
    assert_eq!(f.harness.events.events.lock().unwrap().len(), 225);
    for author in 100..109 {
        assert!(f.first.journal.active(Snowflake::new(author)).await.unwrap().is_empty());
        let page = f
            .first
            .journal
            .list(ScheduleListQuery {
                author_id: Snowflake::new(author),
                channel_id: f.command(0).channel_id,
                after: None,
                limit: 100,
                now_ms: 0,
            })
            .await
            .unwrap();
        assert_eq!(page.items.len(), 25);
        assert!(
            page.items
                .iter()
                .all(|job| job.status == ScheduledStatus::Sent && job.draft.is_none())
        );
    }
    println!(
        "scheduled real Scylla hot-shard load: 225 accepted in {acceptance_time:?}; paced pass {:?}; page counts {pages:?}; max observed due age {maximum_age}ms; max in-flight {maximum_in_flight}; max waiting {maximum_buffered}; 225 writes/events; zero active quota. Local acceptance measurement, not production SLO.",
        started.elapsed()
    );
}

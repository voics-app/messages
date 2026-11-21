//! One disposable real Scylla fixture; independent poll IDs isolate scenarios.
mod support;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use discordy_messages_application::ports::{Clock, PollDueEntry, PollKey, PollRepository, PollStoreError, VoteCommand};
use discordy_messages_domain::Snowflake;
use discordy_messages_domain::poll::{PollDefinition, PollDraft, PollError, PollMedia, PollStatus};
use discordy_messages_infrastructure::scylla::ScyllaPollRepository;
use futures::{StreamExt, TryStreamExt};

struct TestClock(AtomicU64);
impl Clock for TestClock {
    fn now_unix_ms(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}

fn id(value: u64) -> Snowflake {
    Snowflake::new(value)
}
fn key(value: u64) -> PollKey {
    PollKey {
        channel_id: id(200),
        message_id: id(value),
    }
}

fn definition(multiple: bool) -> PollDefinition {
    PollDefinition::new(
        PollDraft {
            question: "Route".into(),
            answers: vec![
                PollMedia::text("Forest"),
                PollMedia::text("River"),
                PollMedia::text("Coast"),
            ],
            duration_hours: 1,
            allow_multiselect: multiple,
        },
        1_000,
    )
    .unwrap()
}

fn vote(key: PollKey, user: u64, answer: u8, revision: i64, selected: bool) -> VoteCommand {
    VoteCommand {
        key,
        user_id: id(user),
        answer_id: answer,
        selected,
        expected_vote_version: revision,
    }
}

async fn independent_replicas(repos: &[Arc<ScyllaPollRepository>]) {
    let key = key(1);
    repos[0].initialize(key, &definition(true)).await.unwrap();
    let started = std::time::Instant::now();
    let changes: Vec<_> = futures::stream::iter(0..512)
        .map(|call| {
            let repo = repos[call % repos.len()].clone();
            async move {
                let user = (call % 256 + 1) as u64;
                repo.vote(vote(key, user, (user % 3 + 1) as u8, 0, true)).await
            }
        })
        .buffer_unordered(16)
        .try_collect()
        .await
        .unwrap();
    assert_eq!(changes.iter().filter(|change| change.changed).count(), 256);
    let snapshot = repos[1].snapshot(key, id(1)).await.unwrap().unwrap();
    assert_eq!(snapshot.aggregate.counts.iter().sum::<u32>(), 256);
    assert_eq!(
        (snapshot.aggregate.version, snapshot.aggregate.participants),
        (256, 256)
    );
    assert_eq!(snapshot.voter.unwrap().revision, 1);
    assert!(repos[2].initialize(key, &definition(true)).await.is_err());
    assert_eq!(
        repos[3].snapshot(key, id(1)).await.unwrap().unwrap().aggregate,
        snapshot.aggregate
    );
    eprintln!(
        "poll contention:256 unique voters/512 calls/4 repositories/16 in flight; elapsed={:?}",
        started.elapsed()
    );
}

async fn stale_same_voter_and_single_choice(repos: &[Arc<ScyllaPollRepository>]) {
    let key = key(2);
    repos[0].initialize(key, &definition(false)).await.unwrap();
    let first = repos[0].vote(vote(key, 1, 1, 0, true)).await.unwrap();
    let (a, b) = tokio::join!(
        repos[1].vote(vote(key, 1, 2, 1, true)),
        repos[2].vote(vote(key, 1, 3, 1, true))
    );
    let winner = match (a, b) {
        (Ok(winner), Err(PollStoreError::Rule(PollError::Conflict)))
        | (Err(PollStoreError::Rule(PollError::Conflict)), Ok(winner)) => winner,
        other => panic!("exactly one changing choice must win: {other:?}"),
    };
    assert_eq!(winner.aggregate.counts.iter().sum::<u32>(), 1);
    let selected = winner.voter.answers()[0];
    let removed = repos[0].vote(vote(key, 1, selected, 2, false)).await.unwrap();
    assert_eq!(removed.voter.revision, 3);
    assert_eq!(removed.voter.mask, 0);
    assert_eq!(
        repos[3].vote(vote(key, 1, first.voter.answers()[0], 0, true)).await,
        Err(PollStoreError::Rule(PollError::Conflict))
    );
    let readded = repos[3].vote(vote(key, 1, 1, 3, true)).await.unwrap();
    assert_eq!(readded.voter.revision, 4);
    assert_eq!(readded.aggregate.participants, 1);
}

async fn closing_race(repos: &[Arc<ScyllaPollRepository>]) {
    let key = key(3);
    repos[0].initialize(key, &definition(false)).await.unwrap();
    let writers = futures::stream::iter(1..=32)
        .map(|user| {
            let repo = repos[user as usize % repos.len()].clone();
            async move { repo.vote(vote(key, user, 1, 0, true)).await }
        })
        .buffer_unordered(16)
        .collect::<Vec<_>>();
    let (writes, closed) = tokio::join!(writers, repos[0].close(key, true));
    let (closed, changed) = closed.unwrap().unwrap();
    assert!(changed);
    let mut committed = 0;
    for write in writes {
        match write {
            Ok(change) => {
                assert!(change.changed);
                committed += 1;
            }
            Err(PollStoreError::Rule(PollError::Closed)) => {}
            other => panic!("unexpected vote outcome: {other:?}"),
        }
    }
    assert_eq!(closed.status, PollStatus::Finalized);
    assert_eq!(closed.counts, vec![committed, 0, 0]);
    assert_eq!(closed.version, i64::from(committed) + 1);
    assert_eq!(
        repos[1].close(key, true).await.unwrap().unwrap(),
        (closed.clone(), false)
    );
    assert_eq!(repos[2].snapshot(key, id(1)).await.unwrap().unwrap().aggregate, closed);
}

async fn deleting_race(repos: &[Arc<ScyllaPollRepository>], database: &support::Scylla) {
    let key = key(4);
    repos[0].initialize(key, &definition(true)).await.unwrap();
    repos[0].vote(vote(key, 1, 1, 0, true)).await.unwrap();
    let writers = futures::stream::iter(2..=32)
        .map(|user| {
            let repo = repos[user as usize % repos.len()].clone();
            async move { repo.vote(vote(key, user, 2, 0, true)).await }
        })
        .buffer_unordered(16)
        .collect::<Vec<_>>();
    let (writes, deleted) = tokio::join!(writers, repos[0].delete(key));
    deleted.unwrap();
    for write in writes {
        assert!(write.is_ok() || matches!(write, Err(PollStoreError::Rule(PollError::Deleted))));
    }
    let snapshot = repos[1].snapshot(key, id(1)).await.unwrap().unwrap();
    assert_eq!(snapshot.aggregate.status, PollStatus::Deleted);
    assert_eq!(snapshot.aggregate.counts, vec![0; 3]);
    assert!(snapshot.voter.is_none());
    let rows = database
        .session
        .query_unpaged(
            "SELECT count(*) FROM poll_acceptance_polls.poll_state WHERE channel_id=200 AND message_id=4",
            (),
        )
        .await
        .unwrap()
        .into_rows_result()
        .unwrap()
        .single_row::<(i64,)>()
        .unwrap()
        .0;
    assert_eq!(rows, 1, "only the content-free deleted fence remains");
    assert_eq!(
        repos[3].vote(vote(key, 1, 3, 1, true)).await,
        Err(PollStoreError::Rule(PollError::Deleted))
    );
    repos[3].delete(key).await.unwrap();
}

async fn bounded_expiry_index_and_restart(
    repos: &[Arc<ScyllaPollRepository>],
    database: &support::Scylla,
    clock: Arc<TestClock>,
) {
    let first_key = key(1_000);
    let shard = PollDueEntry {
        key: first_key,
        expires_at_ms: 3_601_000,
    }
    .shard();
    for index in 0..105 {
        repos[0]
            .initialize(key(1_000 + index * 64), &definition(false))
            .await
            .unwrap();
    }
    assert!(repos[0].due_page(shard, None, 100).await.unwrap().is_empty());
    clock.0.store(3_601_000, Ordering::SeqCst);
    let restarted = ScyllaPollRepository::new(database.session.clone(), clock, None)
        .await
        .unwrap();
    let page = restarted.due_page(shard, None, 100).await.unwrap();
    assert_eq!(page.len(), 100);
    let tail = restarted.due_page(shard, page.last().copied(), 100).await.unwrap();
    assert_eq!(tail.len(), 5);
    assert!(
        restarted
            .due_page(shard, tail.last().copied(), 100)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        restarted.vote(vote(first_key, 99, 1, 0, true)).await,
        Err(PollStoreError::Rule(PollError::Closed))
    );
    assert!(restarted.close(first_key, false).await.unwrap().unwrap().1);
    assert_eq!(restarted.due_page(shard, None, 100).await.unwrap()[0].key, key(1_064));
    assert_eq!(
        restarted
            .snapshot(first_key, id(99))
            .await
            .unwrap()
            .unwrap()
            .aggregate
            .status,
        PollStatus::Finalized
    );
    eprintln!(
        "poll expiry:105 same-shard entries survive restart; bounded pages100/5, deadline rejects new votes, finalized entry removed"
    );
}

#[tokio::test]
async fn actual_scylla_votes_close_delete_and_restart_preserve_atomic_state() {
    let database = support::start_scylla("poll_acceptance").await;
    let clock = Arc::new(TestClock(AtomicU64::new(2_000)));
    let mut repos = Vec::new();
    for _ in 0..4 {
        repos.push(Arc::new(
            ScyllaPollRepository::new(database.session.clone(), clock.clone(), None)
                .await
                .unwrap(),
        ));
    }
    independent_replicas(&repos).await;
    stale_same_voter_and_single_choice(&repos).await;
    closing_race(&repos).await;
    deleting_race(&repos, &database).await;
    bounded_expiry_index_and_restart(&repos, &database, clock.clone()).await;
    let ttl = database.session.query_unpaged("SELECT ttl(version) FROM poll_acceptance_polls.poll_state WHERE channel_id=200 AND message_id=1 AND row_id=0",()).await.unwrap().into_rows_result().unwrap().single_row::<(Option<i32>,)>().unwrap().0;
    assert_eq!(
        ttl, None,
        "accepted poll metadata never expires with the reservation TTL"
    );
    database
        .session
        .query_unpaged("DROP TABLE poll_acceptance_polls.poll_due", ())
        .await
        .unwrap();
    assert!(repos[0].initialize(key(999_999), &definition(false)).await.is_err());
    let ttl = database.session.query_unpaged("SELECT ttl(version) FROM poll_acceptance_polls.poll_state WHERE channel_id=200 AND message_id=999999 AND row_id=0",()).await.unwrap().into_rows_result().unwrap().single_row::<(Option<i32>,)>().unwrap().0;
    assert!(
        ttl.is_some_and(|ttl| (1..=86_400).contains(&ttl)),
        "unaccepted unindexed reservation must expire without an unbounded scan"
    );
    assert!(
        ScyllaPollRepository::new(database.session.clone(), clock, Some("poll_acceptance"))
            .await
            .is_err()
    );
}

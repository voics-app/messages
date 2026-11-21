//! Real Scylla proof for concurrent replicas, ACK/replay and legacy migration.

mod support;

use std::sync::Arc;

use discordy_messages_application::ports::ReadStateRepository;
use discordy_messages_domain::Snowflake;
use discordy_messages_infrastructure::scylla::ScyllaReadStateRepository;
use futures::{StreamExt, TryStreamExt};

const USER: Snowflake = Snowflake::new(100);
const CHANNEL: Snowflake = Snowflake::new(200);
fn id(value: u64) -> Snowflake {
    Snowflake::new(value)
}

#[tokio::test]
async fn committed_mutations_have_monotonic_versions_without_replay_increments() {
    let database = support::start_scylla("reads_versions").await;
    let repo = ScyllaReadStateRepository::new(database.session.clone()).await.unwrap();
    repo.record_mention(USER, CHANNEL, id(20)).await.unwrap();
    repo.record_mention(USER, CHANNEL, id(10)).await.unwrap();
    repo.record_mention(USER, CHANNEL, id(20)).await.unwrap();
    repo.acknowledge(USER, CHANNEL, id(10)).await.unwrap();
    repo.acknowledge(USER, CHANNEL, id(9)).await.unwrap();
    let row = database.session.query_unpaged(
        "SELECT read_state_version,mention_count FROM reads_versions_reads.read_state_accounting WHERE user_id=100 AND channel_id=200 AND kind=0 AND generation=00000000-0000-0000-0000-000000000000 AND message_id=0", ()
    ).await.unwrap().into_rows_result().unwrap().single_row::<(i64,i64)>().unwrap();
    assert_eq!(row, (3, 1));
}

#[tokio::test]
async fn version_migration_preserves_old_metadata_and_exhaustion_rejects_mutations() {
    let database = support::start_scylla("reads_upgrade").await;
    let session = &database.session;
    session.query_unpaged("CREATE KEYSPACE reads_upgrade_reads WITH replication={'class':'NetworkTopologyStrategy','replication_factor':1} AND tablets={'enabled':false}", ()).await.unwrap();
    let script = include_str!("../../../migrations/0008_read_accounting.cql")
        .lines()
        .filter(|line| !line.trim_start().starts_with("--"))
        .collect::<Vec<_>>()
        .join("\n")
        .replace("{{keyspace}}", "reads_upgrade_reads");
    for statement in script.split(';').map(str::trim).filter(|line| !line.is_empty()) {
        session.query_unpaged(statement, ()).await.unwrap();
    }
    session
        .query_unpaged("INSERT INTO read_states (user_id,channel_id) VALUES (100,200)", ())
        .await
        .unwrap();
    session.query_unpaged("INSERT INTO reads_upgrade_reads.read_state_accounting (user_id,channel_id,kind,generation,message_id,revision,active_generation,last_read_id,acknowledged_through,baseline,mention_count) VALUES (100,200,0,00000000-0000-0000-0000-000000000000,0,?,?,10,10,7,7)", (uuid::Uuid::new_v4(), uuid::Uuid::new_v4())).await.unwrap();
    let repo = ScyllaReadStateRepository::new(session.clone()).await.unwrap();
    let old = repo.for_user(USER).await.unwrap().remove(0);
    assert_eq!((old.version, old.mention_count, old.last_read_id), (0, 7, Some(id(10))));
    let first = repo.record_mention(USER, CHANNEL, id(20)).await.unwrap();
    assert_eq!((first.version, first.mention_count), (1, 8));
    assert_eq!(repo.record_mention(USER, CHANNEL, id(20)).await.unwrap(), first);
    let manual = repo.set_position(USER, CHANNEL, id(5), 2).await.unwrap();
    assert_eq!(manual.version, 2);
    session.query_unpaged("UPDATE reads_upgrade_reads.read_state_accounting SET read_state_version=9223372036854775807 WHERE user_id=100 AND channel_id=200 AND kind=0 AND generation=00000000-0000-0000-0000-000000000000 AND message_id=0", ()).await.unwrap();
    assert!(
        repo.record_mention(USER, CHANNEL, id(21))
            .await
            .unwrap_err()
            .to_string()
            .contains("version exhausted")
    );
    assert!(repo.acknowledge(USER, CHANNEL, id(20)).await.is_err());
    let exhausted = repo.find(USER, CHANNEL).await.unwrap().unwrap();
    assert_eq!(
        (exhausted.version, exhausted.mention_count, exhausted.last_read_id),
        (i64::MAX as u64, 2, Some(id(5)))
    );
    assert_eq!(repo.record_mention(USER, CHANNEL, id(20)).await.unwrap(), exhausted);
}

#[tokio::test]
async fn concurrent_repositories_preserve_counts_and_idempotent_replays() {
    assert!(support::cpu_supports_scylla());
    let database = support::start_scylla("reads_concurrent").await;
    let first = Arc::new(ScyllaReadStateRepository::new(database.session.clone()).await.unwrap());
    let second = Arc::new(ScyllaReadStateRepository::new(database.session.clone()).await.unwrap());
    futures::stream::iter(1..=64_u64)
        .map(|message| {
            let repo = if message % 2 == 0 {
                first.clone()
            } else {
                second.clone()
            };
            async move {
                repo.record_mention(USER, CHANNEL, id(message)).await?;
                // Repeating an operation whose response was lost must not count twice.
                repo.record_mention(USER, CHANNEL, id(message)).await?;
                Ok::<_, discordy_messages_application::PortError>(())
            }
        })
        .buffer_unordered(16)
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(first.find(USER, CHANNEL).await.unwrap().unwrap().mention_count, 64);
    assert_eq!(first.find(USER, CHANNEL).await.unwrap().unwrap().version, 64);
    assert_eq!(second.for_user(USER).await.unwrap()[0].mention_count, 64);
    let state = second.acknowledge(USER, CHANNEL, id(32)).await.unwrap();
    assert_eq!(state.mention_count, 32);
    assert_eq!(state.version, 65);
    assert_eq!(first.acknowledge(USER, CHANNEL, id(31)).await.unwrap(), state);
    assert_eq!(first.acknowledge(USER, CHANNEL, id(32)).await.unwrap(), state);
    // Replays stay ignored even though ACK transactionally removed old ledger rows.
    for message in 1..=64 {
        first.record_mention(USER, CHANNEL, id(message)).await.unwrap();
    }
    assert_eq!(second.find(USER, CHANNEL).await.unwrap(), Some(state));
    let count = database
        .session
        .query_unpaged(
            "SELECT count(*) FROM reads_concurrent_reads.read_state_accounting WHERE user_id=100 AND channel_id=200",
            (),
        )
        .await
        .unwrap()
        .into_rows_result()
        .unwrap()
        .single_row::<(i64,)>()
        .unwrap()
        .0;
    assert_eq!(count, 65, "metadata plus 32 unread ledger/contribution pairs");
}

#[tokio::test]
async fn racing_ack_and_new_mentions_keep_only_the_newer_messages() {
    let database = support::start_scylla("reads_ack").await;
    let repo = Arc::new(ScyllaReadStateRepository::new(database.session.clone()).await.unwrap());
    repo.record_mention(USER, CHANNEL, id(1)).await.unwrap();
    let writer = async {
        futures::stream::iter(2..=80_u64)
            .map(|message| {
                let repo = repo.clone();
                async move { repo.record_mention(USER, CHANNEL, id(message)).await }
            })
            .buffer_unordered(12)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
    };
    let ack = async {
        for _ in 0..8 {
            repo.acknowledge(USER, CHANNEL, id(40)).await.unwrap();
        }
    };
    tokio::join!(writer, ack);
    let state = repo.find(USER, CHANNEL).await.unwrap().unwrap();
    assert_eq!(state.last_read_id, Some(id(40)));
    assert_eq!(state.mention_count, 40);
    assert_eq!(repo.acknowledge(USER, CHANNEL, id(100)).await.unwrap().mention_count, 0);
    repo.record_mention(USER, CHANNEL, id(101)).await.unwrap();
    repo.set_position(USER, CHANNEL, id(50), 3).await.unwrap();
    assert_eq!(
        repo.record_mention(USER, CHANNEL, id(101)).await.unwrap().mention_count,
        3,
        "manual unread retains delivery deduplication from the old generation"
    );
    assert_eq!(
        repo.record_mention(USER, CHANNEL, id(99)).await.unwrap().mention_count,
        3
    );
    assert_eq!(
        repo.record_mention(USER, CHANNEL, id(102)).await.unwrap().mention_count,
        4
    );
    let state = repo.acknowledge(USER, CHANNEL, id(51)).await.unwrap();
    assert_eq!(
        state.mention_count, 1,
        "advancing ACK clears the manual baseline, not newer unique deliveries"
    );
    assert_eq!(
        repo.record_mention(USER, CHANNEL, id(101)).await.unwrap().mention_count,
        1
    );
    repo.set_position(USER, CHANNEL, id(200), 5).await.unwrap();
    repo.set_position(USER, CHANNEL, id(190), 2).await.unwrap();
    assert_eq!(
        repo.record_mention(USER, CHANNEL, id(199)).await.unwrap().mention_count,
        2
    );
    assert_eq!(
        repo.record_mention(USER, CHANNEL, id(201)).await.unwrap().mention_count,
        3
    );
}

#[tokio::test]
async fn legacy_baselines_initialize_once_and_ready_uses_authoritative_state() {
    let database = support::start_scylla("reads_legacy").await;
    database
        .session
        .query_unpaged(
            "INSERT INTO read_states (user_id,channel_id,last_read_id,mention_count) VALUES (100,200,10,7)",
            (),
        )
        .await
        .unwrap();
    let repo = Arc::new(ScyllaReadStateRepository::new(database.session.clone()).await.unwrap());
    let legacy = repo.find(USER, CHANNEL).await.unwrap().unwrap();
    assert_eq!(legacy.last_read_id, Some(id(10)));
    assert_eq!(legacy.mention_count, 7);
    futures::stream::iter(20..=27_u64)
        .map(|message| {
            let repo = repo.clone();
            async move { repo.record_mention(USER, CHANNEL, id(message)).await }
        })
        .buffer_unordered(8)
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(repo.find(USER, CHANNEL).await.unwrap().unwrap().mention_count, 15);
    let state = repo.acknowledge(USER, CHANNEL, id(23)).await.unwrap();
    assert_eq!(state.mention_count, 4);
    assert_eq!(repo.for_user(USER).await.unwrap(), vec![state]);
    let original = database
        .session
        .query_unpaged(
            "SELECT last_read_id,mention_count FROM read_states WHERE user_id=100 AND channel_id=200",
            (),
        )
        .await
        .unwrap()
        .into_rows_result()
        .unwrap()
        .single_row::<(i64, i32)>()
        .unwrap();
    assert_eq!(
        original,
        (10, 7),
        "the legacy baseline is retained for audited migration/rollback"
    );

    // Simulate interruption after discovery was written but before metadata existed.
    database
        .session
        .query_unpaged("INSERT INTO read_states (user_id,channel_id) VALUES (100,201)", ())
        .await
        .unwrap();
    assert_eq!(repo.find(USER, id(201)).await.unwrap().unwrap().mention_count, 0);
    repo.record_mention(USER, id(201), id(30)).await.unwrap();
    assert_eq!(repo.for_user(USER).await.unwrap().len(), 2);
    assert!(repo.record_mention(USER, id(202), id(u64::MAX)).await.is_err());
    assert!(repo.find(USER, id(202)).await.unwrap().is_none());
}

#[tokio::test]
async fn schema_bootstrap_preserves_the_main_keyspace_and_rejects_incompatible_companions() {
    let database = support::start_scylla("reads_schema").await;
    assert!(
        ScyllaReadStateRepository::open(database.session.clone(), Some("reads_schema"))
            .await
            .is_err()
    );
    assert!(
        ScyllaReadStateRepository::open(database.session.clone(), Some("bad; DROP KEYSPACE reads_schema"))
            .await
            .is_err()
    );
    database.session.query_unpaged("CREATE KEYSPACE reads_wrong_rf WITH replication={'class':'NetworkTopologyStrategy','replication_factor':2} AND tablets={'enabled':false}", ()).await.unwrap();
    assert!(
        ScyllaReadStateRepository::open(database.session.clone(), Some("reads_wrong_rf"))
            .await
            .err()
            .unwrap()
            .to_string()
            .contains("replication differs")
    );
    database.session.query_unpaged("CREATE KEYSPACE reads_tablets WITH replication={'class':'NetworkTopologyStrategy','replication_factor':1} AND tablets={'enabled':true}", ()).await.unwrap();
    assert!(
        ScyllaReadStateRepository::open(database.session.clone(), Some("reads_tablets"))
            .await
            .err()
            .unwrap()
            .to_string()
            .contains("LWT capability")
    );
    let repo = ScyllaReadStateRepository::open(database.session.clone(), Some("reads_custom"))
        .await
        .unwrap();
    repo.record_mention(USER, CHANNEL, id(1)).await.unwrap();
    assert_eq!(database.session.get_keyspace().unwrap().as_str(), "reads_schema");
    assert_eq!(repo.for_user(USER).await.unwrap()[0].mention_count, 1);
    repo.check_ready().await.unwrap();
    database
        .session
        .query_unpaged("DROP TABLE reads_custom.read_state_accounting", ())
        .await
        .unwrap();
    assert!(
        repo.check_ready().await.is_err(),
        "readiness covers the authoritative companion table"
    );
}

#[tokio::test]
async fn concurrent_load_preserves_every_delivery_and_bounds_live_rows_after_ack() {
    let metrics = metrics_exporter_prometheus::PrometheusBuilder::new()
        .install_recorder()
        .unwrap();
    let database = support::start_scylla("reads_load").await;
    let mut repositories = Vec::new();
    for _ in 0..4 {
        repositories.push(Arc::new(
            ScyllaReadStateRepository::new(database.session.clone()).await.unwrap(),
        ));
    }
    let started = std::time::Instant::now();
    futures::stream::iter(1..=1024_u64)
        .map(|message| {
            let repository = repositories[message as usize % repositories.len()].clone();
            async move {
                repository.record_mention(USER, CHANNEL, id(message)).await?;
                // Discard the first response and retry the same delivery.
                repository.record_mention(USER, CHANNEL, id(message)).await?;
                Ok::<_, discordy_messages_application::PortError>(())
            }
        })
        .buffer_unordered(16)
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let writes_elapsed = started.elapsed();
    assert_eq!(
        repositories[0].find(USER, CHANNEL).await.unwrap().unwrap().version,
        1024
    );
    assert_eq!(
        repositories[0]
            .find(USER, CHANNEL)
            .await
            .unwrap()
            .unwrap()
            .mention_count,
        1024
    );
    let rows = || async {
        database
            .session
            .query_unpaged(
                "SELECT count(*) FROM reads_load_reads.read_state_accounting WHERE user_id=100 AND channel_id=200",
                (),
            )
            .await
            .unwrap()
            .into_rows_result()
            .unwrap()
            .single_row::<(i64,)>()
            .unwrap()
            .0
    };
    assert_eq!(rows().await, 2049, "metadata plus exactly two rows per unique delivery");
    let started = std::time::Instant::now();
    assert_eq!(
        repositories[1]
            .acknowledge(USER, CHANNEL, id(512))
            .await
            .unwrap()
            .mention_count,
        512
    );
    let ack_elapsed = started.elapsed();
    assert_eq!(rows().await, 1025);
    repositories[2].set_position(USER, CHANNEL, id(500), 3).await.unwrap();
    assert_eq!(
        rows().await,
        513,
        "manual unread removes old contributions but retains the replay ledger"
    );
    repositories[3].acknowledge(USER, CHANNEL, id(1024)).await.unwrap();
    assert_eq!(rows().await, 1, "fully acknowledged history retains metadata only");
    let rendered = metrics.render();
    let counter = |name: &str| {
        rendered
            .lines()
            .find_map(|line| line.strip_prefix(&format!("{name} ")))
            .map_or(0.0, |value| value.parse::<f64>().unwrap())
    };
    assert!(
        counter("read_state_cas_conflicts_total") > 0.0,
        "independent repositories must contend in this load case"
    );
    assert_eq!(counter("read_state_cas_exhausted_total"), 0.0);
    println!(
        "read_accounting_load unique=1024 calls=2048 repositories=4 in_flight=16 writes_ms={} ack512_ms={} conflicts={} exhausted=0 live_rows=2049/1025/513/1",
        writes_elapsed.as_millis(),
        ack_elapsed.as_millis(),
        counter("read_state_cas_conflicts_total")
    );
}

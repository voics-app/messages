//! The PostgreSQL search index against a real server (Docker required).

mod support;

use discordy_messages_application::ports::{
    BackfillState, Checkpoint, IndexOp, PinnedFilter, SearchIndex, SearchPage, SearchRequest,
};
use discordy_messages_domain::{HasFilter, HasSet, IndexedMessage, SearchFilters, Snowflake, SortBy, SortOrder};
use discordy_messages_infrastructure::search::{PgSearchIndex, connect};

const GUILD: Snowflake = Snowflake::new(900);
const C1: Snowflake = Snowflake::new(1);
const C2: Snowflake = Snowflake::new(2);
const ALICE: Snowflake = Snowflake::new(10);
const BOB: Snowflake = Snowflake::new(11);

fn entry(channel: Snowflake, id: u64, author: Snowflake, content: &str) -> IndexedMessage {
    IndexedMessage {
        channel_id: channel,
        message_id: Snowflake::new(id),
        guild_id: Some(GUILD),
        author_id: author,
        content: content.to_owned(),
        mentions: vec![],
        has: HasSet::default(),
        edited_at_ms: 0,
    }
}

fn request(channels: &[Snowflake], filters: SearchFilters) -> SearchRequest {
    SearchRequest {
        channels: channels.to_vec(),
        filters,
        pinned: None,
    }
}

fn text(content: &str) -> SearchFilters {
    SearchFilters {
        content: Some(content.to_owned()),
        ..SearchFilters::default()
    }
}

fn ids(page: &SearchPage) -> Vec<u64> {
    page.hits.iter().map(|h| h.message_id.value()).collect()
}

#[tokio::test]
async fn indexes_searches_filters_and_forgets() {
    let pg = support::start_postgres().await;
    let index = PgSearchIndex::new(connect(&pg.url).unwrap());
    index.migrate().await.unwrap();
    index.migrate().await.unwrap();

    let mut with_link = entry(C1, 3, BOB, "deploy notes https://x.test");
    with_link.has.insert(HasFilter::Link);
    with_link.mentions = vec![ALICE];
    let mut other_guild = entry(C2, 6, ALICE, "deploy elsewhere");
    other_guild.guild_id = Some(Snowflake::new(901));
    index
        .apply(
            &[
                IndexOp::Upsert(entry(C1, 1, ALICE, "Deploying the new build")),
                IndexOp::Upsert(entry(C1, 2, ALICE, "lunch anyone")),
                IndexOp::Upsert(with_link),
                IndexOp::Upsert(entry(C2, 4, BOB, "the deploy failed")),
                IndexOp::Upsert(entry(C2, 5, ALICE, "Deploy! deploy!")),
                IndexOp::Upsert(other_guild),
            ],
            None,
        )
        .await
        .unwrap();

    // Word match, newest first, case-insensitive; exclusion syntax.
    let page = index.search(&request(&[C1, C2], text("DEPLOY"))).await.unwrap();
    assert_eq!((page.total, ids(&page)), (4, vec![6, 5, 4, 3]));
    let page = index.search(&request(&[C1, C2], text("deploy -failed"))).await.unwrap();
    assert_eq!(ids(&page), vec![6, 5, 3]);

    // Only the channels the caller may read are searched.
    let page = index.search(&request(&[C1], text("deploy"))).await.unwrap();
    assert_eq!(ids(&page), vec![3]);

    // Filters combine: author, mentions, has, id range, ordering, paging.
    let mut by_bob = text("deploy");
    by_bob.author_ids = vec![BOB];
    assert_eq!(
        ids(&index.search(&request(&[C1, C2], by_bob)).await.unwrap()),
        vec![4, 3]
    );
    let mentioning = SearchFilters {
        mentions: vec![ALICE],
        ..SearchFilters::default()
    };
    assert_eq!(ids(&index.search(&request(&[C1], mentioning)).await.unwrap()), vec![3]);
    let mut links = SearchFilters::default();
    links.has.insert(HasFilter::Link);
    assert_eq!(ids(&index.search(&request(&[C1], links)).await.unwrap()), vec![3]);
    let range = SearchFilters {
        min_id: Some(Snowflake::new(1)),
        max_id: Some(Snowflake::new(4)),
        sort_order: SortOrder::Asc,
        ..SearchFilters::default()
    };
    assert_eq!(
        ids(&index.search(&request(&[C1, C2], range)).await.unwrap()),
        vec![2, 3]
    );
    let paged = SearchFilters {
        limit: 2,
        offset: 1,
        ..SearchFilters::default()
    };
    let page = index.search(&request(&[C1, C2], paged)).await.unwrap();
    assert_eq!((page.total, ids(&page)), (6, vec![5, 4]));
    let relevance = SearchFilters {
        sort_by: SortBy::Relevance,
        ..text("deploy")
    };
    assert_eq!(
        index.search(&request(&[C1, C2], relevance)).await.unwrap().hits.len(),
        4
    );

    // Pins come from the caller as an id list.
    let mut pinned = request(&[C1, C2], SearchFilters::default());
    pinned.pinned = Some(PinnedFilter::Only(vec![Snowflake::new(2), Snowflake::new(4)]));
    assert_eq!(ids(&index.search(&pinned).await.unwrap()), vec![4, 2]);
    pinned.pinned = Some(PinnedFilter::Except(vec![Snowflake::new(2), Snowflake::new(4)]));
    assert_eq!(index.search(&pinned).await.unwrap().total, 4);

    // Guild channel discovery.
    assert_eq!(index.guild_channels(GUILD, &[]).await.unwrap(), vec![C1, C2]);
    assert_eq!(index.guild_channels(GUILD, &[C2]).await.unwrap(), vec![C2]);

    // Edits replace text; an older replay never wins; deletes remove.
    let mut edited = entry(C1, 2, ALICE, "dinner anyone");
    edited.edited_at_ms = 50;
    index.apply(&[IndexOp::Upsert(edited)], None).await.unwrap();
    index
        .apply(&[IndexOp::Upsert(entry(C1, 2, ALICE, "lunch anyone"))], None)
        .await
        .unwrap();
    assert_eq!(
        ids(&index.search(&request(&[C1], text("dinner"))).await.unwrap()),
        vec![2]
    );
    assert_eq!(index.search(&request(&[C1], text("lunch"))).await.unwrap().total, 0);
    index
        .apply(
            &[IndexOp::Delete {
                channel_id: C1,
                message_ids: vec![Snowflake::new(2), Snowflake::new(3)],
            }],
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        index
            .search(&request(&[C1], SearchFilters::default()))
            .await
            .unwrap()
            .total,
        1
    );

    // Offsets and backfill state persist.
    assert_eq!(index.checkpoint("t", 0).await.unwrap(), None);
    let checkpoint = Checkpoint {
        topic: "t".into(),
        partition: 0,
        next_offset: 42,
    };
    index.apply(&[], Some(&checkpoint)).await.unwrap();
    assert_eq!(index.checkpoint("t", 0).await.unwrap(), Some(42));
    assert_eq!(index.backfill_state().await.unwrap(), BackfillState::Idle);
    index.set_backfill_state(BackfillState::Complete).await.unwrap();
    assert_eq!(index.backfill_state().await.unwrap(), BackfillState::Complete);
}

#[tokio::test]
async fn stale_replica_batch_cannot_resurrect_deleted_message_or_rewind_offset() {
    let pg = support::start_postgres().await;
    let index = PgSearchIndex::new(connect(&pg.url).unwrap());
    index.migrate().await.unwrap();
    let checkpoint = |next_offset| Checkpoint {
        topic: "replica-test".into(),
        partition: 0,
        next_offset,
    };
    index
        .apply(&[IndexOp::Upsert(entry(C1, 1, ALICE, "secret"))], Some(&checkpoint(1)))
        .await
        .unwrap();
    index
        .apply(
            &[IndexOp::Delete {
                channel_id: C1,
                message_ids: vec![Snowflake::new(1)],
            }],
            Some(&checkpoint(2)),
        )
        .await
        .unwrap();
    // A second replica finishes an older fetch after the delete was committed.
    index
        .apply(&[IndexOp::Upsert(entry(C1, 1, ALICE, "secret"))], Some(&checkpoint(1)))
        .await
        .unwrap();
    assert_eq!(index.checkpoint("replica-test", 0).await.unwrap(), Some(2));
    assert_eq!(index.search(&request(&[C1], text("secret"))).await.unwrap().total, 0);
}

#[tokio::test]
async fn concurrent_replica_batches_leave_the_highest_offset_and_value() {
    let pg = support::start_postgres().await;
    let index = std::sync::Arc::new(PgSearchIndex::new(connect(&pg.url).unwrap()));
    index.migrate().await.unwrap();
    let mut tasks = Vec::new();
    for n in 1..=20 {
        let index = index.clone();
        tasks.push(tokio::spawn(async move {
            let checkpoint = Checkpoint {
                topic: "concurrent".into(),
                partition: 0,
                next_offset: n,
            };
            let content = if n == 20 { "latest" } else { "stale" };
            index
                .apply(&[IndexOp::Upsert(entry(C1, 1, ALICE, content))], Some(&checkpoint))
                .await
                .unwrap();
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    assert_eq!(index.checkpoint("concurrent", 0).await.unwrap(), Some(20));
    assert_eq!(index.search(&request(&[C1], text("latest"))).await.unwrap().total, 1);
    assert_eq!(index.search(&request(&[C1], text("stale"))).await.unwrap().total, 0);
}

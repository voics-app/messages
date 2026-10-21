//! ScyllaDB adapter integration tests against a real node (testcontainers).
//! One container serves every scenario to keep the suite fast.

mod support;

use std::sync::Arc;

use discordy_messages_application::HistoryReader;
use discordy_messages_application::ports::{
    ActivityRepository, BucketQuery, MessageRepository, Pin, PinRepository, ReactionRepository, ReadStateRepository,
};
use discordy_messages_domain::bucket::BUCKET_SIZE_MS;
use discordy_messages_domain::paging::plan;
use discordy_messages_domain::snowflake::DISCORDY_EPOCH_MS;
use discordy_messages_domain::{
    Attachment, AttachmentFilename, Bucket, Emoji, HistoryCursor, Message, MessageDraft, PageLimit, ReadState,
    ScanDirection, Snowflake,
};
use discordy_messages_infrastructure::scylla::{
    ScyllaActivityRepository, ScyllaMessageRepository, ScyllaPinRepository, ScyllaReactionRepository,
    ScyllaReadStateRepository, migrate,
};

const AUTHOR: Snowflake = Snowflake::new(42);

/// A message id `ms` milliseconds after the epoch, in bucket `ms / 10 days`.
fn id_at(ms: u64, seq: u64) -> Snowflake {
    Snowflake::new((ms << 22) | (64 << 12) | seq)
}

fn message(channel: Snowflake, id: Snowflake, content: &str) -> Message {
    let draft = MessageDraft {
        content: content.to_owned(),
        ..MessageDraft::default()
    };
    Message::compose(id, channel, AUTHOR, draft, false).unwrap()
}

#[tokio::test]
async fn scylla_adapters_against_a_real_node() {
    if !support::cpu_supports_scylla() {
        eprintln!("skipping: ScyllaDB needs SSE4.2 and PCLMULQDQ, which this CPU does not expose");
        return;
    }
    let scylla = support::start_scylla("discordy_it").await;
    let session = Arc::clone(&scylla.session);

    migrate(&session, "discordy_it")
        .await
        .expect("migrations are idempotent");

    let messages = Arc::new(ScyllaMessageRepository::new(Arc::clone(&session)).await.unwrap());
    let reactions = Arc::new(ScyllaReactionRepository::new(Arc::clone(&session)).await.unwrap());
    let pins = ScyllaPinRepository::new(Arc::clone(&session)).await.unwrap();
    let read_states = ScyllaReadStateRepository::new(Arc::clone(&session)).await.unwrap();
    let activity = ScyllaActivityRepository::new(Arc::clone(&session)).await.unwrap();

    message_crud(messages.as_ref()).await;
    bucketed_history(Arc::clone(&messages), Arc::clone(&reactions)).await;
    last_message_id_never_regresses(messages.as_ref()).await;
    reactions_round_trip(reactions.as_ref()).await;
    pins_round_trip(&pins).await;
    read_states_round_trip(&read_states).await;
    activity_counts(&activity).await;
}

async fn message_crud(repo: &ScyllaMessageRepository) {
    let channel = id_at(1, 0);
    let mut original = message(channel, id_at(BUCKET_SIZE_MS * 2, 1), "hello <@7>");
    original.attachments.push(Attachment {
        id: Snowflake::new(900),
        filename: AttachmentFilename::parse("cat.png").unwrap(),
        content_type: Some("image/png".into()),
        size: 512,
        width: None,
        height: None,
    });
    original.embeds.push(r#"{"title":"t"}"#.into());
    repo.insert(&original).await.unwrap();
    assert_eq!(repo.find(channel, original.id).await.unwrap(), Some(original.clone()));

    let mut edited = original.clone();
    edited.content = "edited".into();
    edited.edited_at_ms = Some(DISCORDY_EPOCH_MS + 5);
    repo.update(&edited).await.unwrap();
    repo.set_pinned(channel, original.id, true).await.unwrap();
    let stored = repo.find(channel, original.id).await.unwrap().unwrap();
    assert_eq!(stored.content, "edited");
    assert!(stored.pinned);
    assert_eq!(stored.attachments, original.attachments);

    repo.delete(channel, &[original.id]).await.unwrap();
    assert_eq!(repo.find(channel, original.id).await.unwrap(), None);
    // An edit racing the delete leaves a partial row, which must stay invisible.
    repo.update(&edited).await.unwrap();
    assert_eq!(repo.find(channel, original.id).await.unwrap(), None);
}

async fn bucketed_history(repo: Arc<ScyllaMessageRepository>, reactions: Arc<ScyllaReactionRepository>) {
    let channel = id_at(BUCKET_SIZE_MS * 10, 0);
    let mut ids = Vec::new();
    for b in 0..4u64 {
        for i in 0..3u64 {
            let id = id_at(BUCKET_SIZE_MS * (10 + b * 2) + i * 1000, i);
            repo.insert(&message(channel, id, &format!("b{b}-{i}"))).await.unwrap();
            ids.push(id);
        }
    }

    let newest_bucket = Bucket::of(*ids.last().unwrap());
    let top = repo
        .scan_bucket(BucketQuery {
            channel_id: channel,
            bucket: newest_bucket,
            direction: ScanDirection::Older,
            bound: None,
            limit: 2,
        })
        .await
        .unwrap();
    assert_eq!(
        top.iter().map(|m| m.content.as_str()).collect::<Vec<_>>(),
        ["b3-2", "b3-1"]
    );

    let newer = repo
        .scan_bucket(BucketQuery {
            channel_id: channel,
            bucket: Bucket::of(ids[0]),
            direction: ScanDirection::Newer,
            bound: Some(ids[0]),
            limit: 10,
        })
        .await
        .unwrap();
    assert_eq!(
        newer.iter().map(|m| m.content.as_str()).collect::<Vec<_>>(),
        ["b0-1", "b0-2"]
    );

    // Walk a page of 7 backwards through the real buckets (with empty
    // buckets in between) using the domain planner and the coalesced reader.
    let reader = HistoryReader::new(repo.clone(), reactions);
    let newest = reader.last_message_id(channel).await.unwrap();
    assert_eq!(newest, ids.last().copied());
    let limit = PageLimit::parse(Some(7)).unwrap();
    let mut page = Vec::new();
    for scan in plan(HistoryCursor::Latest, limit, channel, newest) {
        for bucket in scan.buckets {
            let remaining = scan.limit - page.len();
            if remaining == 0 {
                break;
            }
            let rows = reader
                .scan_bucket(BucketQuery {
                    channel_id: channel,
                    bucket,
                    direction: scan.direction,
                    bound: scan.bound,
                    limit: remaining,
                })
                .await
                .unwrap();
            page.extend(rows.iter().map(|m| m.content.clone()));
        }
    }
    assert_eq!(page, ["b3-2", "b3-1", "b3-0", "b2-2", "b2-1", "b2-0", "b1-2"]);
}

async fn last_message_id_never_regresses(repo: &ScyllaMessageRepository) {
    let channel = id_at(BUCKET_SIZE_MS * 19, 0);
    let newer = id_at(BUCKET_SIZE_MS * 20, 2);
    let older = id_at(BUCKET_SIZE_MS * 20, 1);
    repo.insert(&message(channel, newer, "newer")).await.unwrap();
    repo.insert(&message(channel, older, "older, arriving late"))
        .await
        .unwrap();
    assert_eq!(repo.last_message_id(channel).await.unwrap(), Some(newer));
    assert_eq!(repo.last_message_id(id_at(7, 7)).await.unwrap(), None);
}

async fn reactions_round_trip(repo: &ScyllaReactionRepository) {
    let channel = id_at(5, 0);
    let first = id_at(BUCKET_SIZE_MS + 1, 0);
    let second = id_at(BUCKET_SIZE_MS + 2, 0);
    let thumbs = Emoji::parse("👍").unwrap();
    let custom = Emoji::parse("blob:123").unwrap();
    repo.add(channel, first, &thumbs, Snowflake::new(1)).await.unwrap();
    repo.add(channel, first, &thumbs, Snowflake::new(1)).await.unwrap();
    repo.add(channel, first, &custom, Snowflake::new(2)).await.unwrap();
    repo.add(channel, second, &thumbs, Snowflake::new(2)).await.unwrap();

    assert_eq!(repo.for_message(channel, first).await.unwrap().len(), 2);
    let range = repo.for_range(channel, Bucket::of(first), first, second).await.unwrap();
    assert_eq!(range.len(), 3);

    repo.remove(channel, first, &thumbs, Snowflake::new(1)).await.unwrap();
    let remaining = repo.for_message(channel, first).await.unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].emoji, custom);

    repo.remove_all(channel, first).await.unwrap();
    assert!(repo.for_message(channel, first).await.unwrap().is_empty());
    assert_eq!(repo.for_message(channel, second).await.unwrap().len(), 1);
}

async fn pins_round_trip(repo: &ScyllaPinRepository) {
    let channel = id_at(6, 0);
    for (message, at) in [(1u64, 100u64), (2, 300), (3, 200)] {
        repo.pin(
            channel,
            Pin {
                message_id: Snowflake::new(message),
                pinned_at_ms: DISCORDY_EPOCH_MS + at,
            },
        )
        .await
        .unwrap();
    }
    let order: Vec<u64> = repo
        .list(channel)
        .await
        .unwrap()
        .iter()
        .map(|p| p.message_id.value())
        .collect();
    assert_eq!(order, [2, 3, 1]);
    repo.unpin(channel, Snowflake::new(2)).await.unwrap();
    assert_eq!(repo.list(channel).await.unwrap().len(), 2);
}

async fn read_states_round_trip(repo: &ScyllaReadStateRepository) {
    let user = Snowflake::new(77);
    let mut state = ReadState::unread(user, Snowflake::new(1));
    state.record_mention(Snowflake::new(10));
    repo.save(&state).await.unwrap();
    repo.save(&ReadState::unread(user, Snowflake::new(2))).await.unwrap();
    assert_eq!(repo.find(user, Snowflake::new(1)).await.unwrap(), Some(state.clone()));
    assert_eq!(repo.for_user(user).await.unwrap().len(), 2);
    assert_eq!(repo.find(Snowflake::new(78), Snowflake::new(1)).await.unwrap(), None);
}

async fn activity_counts(repo: &ScyllaActivityRepository) {
    let now_ms = DISCORDY_EPOCH_MS + 400 * 86_400_000;
    let recent = [now_ms - 1_000, now_ms - 3_600_000 * 5, now_ms - 3_600_000 * 23];
    for (i, ms) in recent.iter().enumerate() {
        repo.record_standard_message(id_at(ms - DISCORDY_EPOCH_MS, i as u64))
            .await
            .unwrap();
    }
    repo.record_standard_message(id_at(now_ms - DISCORDY_EPOCH_MS - 3_600_000 * 30, 0))
        .await
        .unwrap();
    let count = repo.count_standard_since(now_ms - 86_400_000).await.unwrap();
    assert_eq!(count, 3);
}

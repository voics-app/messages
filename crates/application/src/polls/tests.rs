use std::sync::Arc;
use std::sync::atomic::Ordering;

use discordy_messages_domain::poll::{PollDraft, PollError, PollMedia, PollStatus};
use discordy_messages_domain::{DeliveryMode, DomainError, Permissions};

use super::*;
use crate::AppError;
use crate::fakes::*;
use crate::ports::{AccessError, MessageRepository};
use crate::use_cases::{CreateMessage, CreateMessageCommand, DeleteMessage};

fn harness() -> (Harness, Arc<InMemoryPolls>) {
    let mut h = Harness::new();
    h.access.set_permissions(MEMBER | Permissions::SEND_POLLS);
    let polls = Arc::new(InMemoryPolls::new(h.clock.clone()));
    h.ports.polls = Some(polls.clone());
    h.ports.poll_creation_enabled = true;
    (h, polls)
}

fn create() -> CreateMessageCommand {
    CreateMessageCommand {
        channel_id: CHANNEL,
        author_id: ALICE,
        poll: Some(PollDraft {
            question: "Choose".into(),
            answers: vec![PollMedia::text("One"), PollMedia::text("Two")],
            duration_hours: 1,
            allow_multiselect: false,
        }),
        ..Default::default()
    }
}

fn human(user_id: discordy_messages_domain::UserId) -> Principal {
    Principal { user_id, is_bot: false }
}
fn key(message: &Message) -> PollKey {
    PollKey {
        channel_id: message.channel_id,
        message_id: message.id,
    }
}
fn choice(key: PollKey, answer: u8, revision: i64) -> VoteCommand {
    VoteCommand {
        key,
        user_id: BOB,
        answer_id: answer,
        selected: true,
        expected_vote_version: revision,
    }
}

#[tokio::test]
async fn poll_creation_checks_fresh_send_poll_and_tts_permissions_and_rejects_internal_bypass() {
    let (h, polls) = harness();
    *h.access.fresh.lock().unwrap() = Some(Ok(guild_access(MEMBER)));
    assert_eq!(
        CreateMessage::new(&h.ports).execute(create()).await,
        Err(AppError::Domain(DomainError::MissingPermissions))
    );
    *h.access.fresh.lock().unwrap() = Some(Err(AccessError::NotMember));
    assert_eq!(
        CreateMessage::new(&h.ports).execute(create()).await,
        Err(AppError::Domain(DomainError::MissingAccess))
    );
    *h.access.fresh.lock().unwrap() = Some(Ok(guild_access(MEMBER | Permissions::SEND_POLLS)));
    let mut tts = create();
    tts.tts = true;
    assert_eq!(
        CreateMessage::new(&h.ports).execute(tts).await,
        Err(AppError::Domain(DomainError::MissingPermissions))
    );
    assert!(CreateMessage::new(&h.ports).execute_internal(create()).await.is_err());
    assert_eq!(polls.mutations.load(Ordering::SeqCst), 0);
    assert!(h.messages.last_message_id(CHANNEL).await.unwrap().is_none());
}

#[tokio::test]
async fn private_modes_never_initialize_or_vote_even_when_cached_access_allows_it() {
    for mode in [DeliveryMode::Ephemeral, DeliveryMode::P2p] {
        let (h, polls) = harness();
        *h.access.fresh.lock().unwrap() = Some(Ok(dm_access(mode)));
        assert_eq!(
            CreateMessage::new(&h.ports).execute(create()).await,
            Err(AppError::Domain(DomainError::DeliveryModeConflict(mode)))
        );
        assert_eq!(polls.mutations.load(Ordering::SeqCst), 0);
    }
    let (h, polls) = harness();
    let message = CreateMessage::new(&h.ports).execute(create()).await.unwrap().message;
    let service = Polls::new(&h.ports).unwrap();
    *h.access.fresh.lock().unwrap() = Some(Ok(dm_access(DeliveryMode::P2p)));
    assert_eq!(
        service.vote(human(BOB), choice(key(&message), 1, 0)).await,
        Err(AppError::Domain(DomainError::DeliveryModeConflict(DeliveryMode::P2p)))
    );
    assert_eq!(polls.mutations.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn viewer_snapshot_is_read_only_and_votes_publish_complete_actor_results_once() {
    let (h, polls) = harness();
    let message = CreateMessage::new(&h.ports).execute(create()).await.unwrap().message;
    let service = Polls::new(&h.ports).unwrap();
    let before = service.get(key(&message), human(BOB)).await.unwrap();
    assert_eq!(before.state.aggregate.counts, vec![0, 0]);
    assert!(before.state.voter.is_none());
    assert_eq!(polls.mutations.load(Ordering::SeqCst), 1);
    service.vote(human(BOB), choice(key(&message), 1, 0)).await.unwrap();
    service.vote(human(BOB), choice(key(&message), 1, 0)).await.unwrap();
    service.vote(human(BOB), choice(key(&message), 2, 1)).await.unwrap();
    let changes: Vec<_> = h
        .events
        .kinds()
        .into_iter()
        .filter_map(|kind| match kind {
            MessageEventKind::PollVoteChanged {
                answer_id,
                selected,
                voter,
                results,
                ..
            } => Some((answer_id, selected, voter, results)),
            _ => None,
        })
        .collect();
    assert_eq!(changes.len(), 3);
    assert_eq!(changes[1].2, changes[2].2);
    assert_eq!(changes[1].3, changes[2].3);
    assert_eq!(changes[2].3.counts, vec![0, 1]);
    assert_eq!(changes[2].2.answers(), vec![2]);
    assert_eq!(
        service.vote(human(BOB), choice(key(&message), 1, 0)).await,
        Err(AppError::Poll(PollError::Conflict))
    );
}

#[tokio::test]
async fn bots_cannot_vote_moderators_cannot_expire_and_fresh_revocation_wins() {
    let (h, polls) = harness();
    let message = CreateMessage::new(&h.ports).execute(create()).await.unwrap().message;
    let service = Polls::new(&h.ports).unwrap();
    let bot = Principal {
        user_id: BOB,
        is_bot: true,
    };
    assert_eq!(
        service.vote(bot, choice(key(&message), 1, 0)).await,
        Err(AppError::Domain(DomainError::MissingPermissions))
    );
    assert_eq!(
        service.vote(human(ALICE), choice(key(&message), 1, 0)).await,
        Err(AppError::Domain(DomainError::MissingPermissions))
    );
    h.access.set_permissions(MEMBER | Permissions::MANAGE_MESSAGES);
    assert_eq!(
        service.expire(key(&message), human(BOB)).await,
        Err(AppError::Domain(DomainError::MissingPermissions))
    );
    *h.access.fresh.lock().unwrap() = Some(Err(AccessError::NotMember));
    assert_eq!(
        service.vote(human(BOB), choice(key(&message), 1, 0)).await,
        Err(AppError::Domain(DomainError::MissingAccess))
    );
    assert_eq!(polls.mutations.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn author_expiry_is_idempotent_and_overdue_vote_closes_without_get_side_effects() {
    let (h, polls) = harness();
    let message = CreateMessage::new(&h.ports).execute(create()).await.unwrap().message;
    let service = Polls::new(&h.ports).unwrap();
    let owner = Principal {
        user_id: ALICE,
        is_bot: true,
    };
    let ended = service.expire(key(&message), owner).await.unwrap();
    assert_eq!(ended.state.aggregate.status, PollStatus::Finalized);
    assert_eq!(service.expire(key(&message), owner).await.unwrap(), ended);
    let next = CreateMessage::new(&h.ports).execute(create()).await.unwrap().message;
    h.clock.advance_ms(3_600_000);
    let before = polls.mutations.load(Ordering::SeqCst);
    assert_eq!(
        service
            .get(key(&next), human(BOB))
            .await
            .unwrap()
            .state
            .aggregate
            .status,
        PollStatus::Open
    );
    assert_eq!(polls.mutations.load(Ordering::SeqCst), before);
    assert_eq!(
        service.vote(human(BOB), choice(key(&next), 1, 0)).await,
        Err(AppError::Poll(PollError::Closed))
    );
    assert_eq!(
        service
            .get(key(&next), human(BOB))
            .await
            .unwrap()
            .state
            .aggregate
            .status,
        PollStatus::Finalized
    );
}

#[tokio::test]
async fn failed_reservation_never_inserts_and_failed_delete_fence_preserves_message() {
    let (h, polls) = harness();
    *polls.failure.lock().unwrap() = Some(PortError::new("offline"));
    assert!(CreateMessage::new(&h.ports).execute(create()).await.is_err());
    assert!(h.messages.last_message_id(CHANNEL).await.unwrap().is_none());
    *polls.failure.lock().unwrap() = None;
    let message = CreateMessage::new(&h.ports).execute(create()).await.unwrap().message;
    *polls.failure.lock().unwrap() = Some(PortError::new("offline"));
    assert!(
        DeleteMessage::new(&h.ports)
            .execute(CHANNEL, message.id, ALICE)
            .await
            .is_err()
    );
    assert!(h.messages.find(CHANNEL, message.id).await.unwrap().is_some());
    *polls.failure.lock().unwrap() = None;
    DeleteMessage::new(&h.ports)
        .execute(CHANNEL, message.id, ALICE)
        .await
        .unwrap();
    assert!(h.messages.find(CHANNEL, message.id).await.unwrap().is_none());
    assert_eq!(
        polls
            .snapshot(key(&message), BOB)
            .await
            .unwrap()
            .unwrap()
            .aggregate
            .status,
        PollStatus::Deleted
    );
}

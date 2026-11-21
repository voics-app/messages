use discordy_messages_domain::poll::{
    MAX_PARTICIPANTS, PollAggregate, PollDefinition, PollDraft, PollError, PollMedia, PollStatus, PollVote,
};
use discordy_messages_domain::{
    ChannelAccess, DeliveryMode, DomainError, Message, MessageDraft, MessageEdit, MessageKind, Permissions, Snowflake,
};

#[test]
fn poll_permissions_follow_normal_thread_attachment_and_private_mode_rules() {
    let mut access = ChannelAccess {
        permissions: Permissions::new(Permissions::VIEW_CHANNEL | Permissions::SEND_MESSAGES),
        guild_id: Some(Snowflake::new(1)),
        channel_type: 0,
        delivery_mode: DeliveryMode::Standard,
        recipient_ids: vec![],
    };
    assert_eq!(
        access.ensure_can_create_poll(false),
        Err(DomainError::MissingPermissions)
    );
    access.permissions = Permissions::new(access.permissions.bits() | Permissions::SEND_POLLS);
    assert!(access.ensure_can_create_poll(false).is_ok());
    assert_eq!(
        access.ensure_can_create_poll(true),
        Err(DomainError::MissingPermissions)
    );
    access.channel_type = 11;
    assert_eq!(
        access.ensure_can_create_poll(false),
        Err(DomainError::MissingPermissions)
    );
    access.permissions =
        Permissions::new(Permissions::VIEW_CHANNEL | Permissions::SEND_POLLS | Permissions::SEND_MESSAGES_IN_THREADS);
    assert!(access.ensure_can_create_poll(false).is_ok());
    for mode in [DeliveryMode::Ephemeral, DeliveryMode::P2p] {
        access.delivery_mode = mode;
        access.permissions = Permissions::new(Permissions::ADMINISTRATOR);
        assert_eq!(
            access.ensure_can_create_poll(false),
            Err(DomainError::DeliveryModeConflict(mode))
        );
        assert_eq!(
            access.ensure_can_read_poll(),
            Err(DomainError::DeliveryModeConflict(mode))
        );
    }
}

#[test]
fn poll_only_message_survives_content_edits_but_not_system_or_ephemeral_creation() {
    let draft = MessageDraft {
        poll: Some(definition(false)),
        ..Default::default()
    };
    let mut message = Message::compose(
        Snowflake::new(3),
        Snowflake::new(2),
        Snowflake::new(1),
        draft.clone(),
        false,
    )
    .unwrap();
    message
        .apply_edit(
            Snowflake::new(1),
            MessageEdit {
                content: Some(String::new()),
                ..Default::default()
            },
            false,
            2_000,
        )
        .unwrap();
    assert_eq!(message.poll, Some(definition(false)));
    assert!(
        Message::compose(
            Snowflake::new(3),
            Snowflake::new(2),
            Snowflake::new(1),
            MessageDraft {
                system_kind: Some(MessageKind::Call),
                ..draft.clone()
            },
            false
        )
        .is_err()
    );
    assert!(
        Message::compose(
            Snowflake::new(3),
            Snowflake::new(2),
            Snowflake::new(1),
            MessageDraft {
                flags: discordy_messages_domain::FLAG_EPHEMERAL,
                ..draft
            },
            false
        )
        .is_err()
    );
}

fn definition(multiple: bool) -> PollDefinition {
    PollDefinition::new(
        PollDraft {
            question: "Which route?".into(),
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

#[test]
fn poll_only_content_and_unicode_scalar_limits_are_validated() {
    let mut draft = definition(false).draft();
    draft.question = "🌊".repeat(300);
    assert!(PollDefinition::new(draft.clone(), 0).is_ok());
    draft.question.push('x');
    assert!(matches!(PollDefinition::new(draft, 0), Err(PollError::Invalid(_))));
    for text in ["", "  \n "] {
        let mut draft = definition(false).draft();
        draft.answers[0].text = text.into();
        assert!(PollDefinition::new(draft, 0).is_err());
    }
}

#[test]
fn definition_rejects_bad_answer_counts_duration_and_emoji() {
    for count in [0, 1, 11] {
        let mut draft = definition(false).draft();
        draft.answers = vec![PollMedia::text("answer"); count];
        assert!(PollDefinition::new(draft, 0).is_err());
    }
    for duration in [0, 769] {
        let mut draft = definition(false).draft();
        draft.duration_hours = duration;
        assert!(PollDefinition::new(draft, 0).is_err());
    }
    let mut draft = definition(false).draft();
    draft.answers[0].emoji = Some("party:123".into());
    assert!(PollDefinition::new(draft, 0).is_err());
}

#[test]
fn a_duplicate_does_not_increment_but_an_old_add_cannot_restore_a_removed_vote() {
    let state = PollAggregate::new(&definition(false));
    let added = state.vote(None, 1, true, 0, 2_000).unwrap();
    assert_eq!(added.aggregate.counts, vec![1, 0, 0]);
    let replay = added.aggregate.vote(Some(&added.voter), 1, true, 0, 2_000).unwrap();
    assert!(!replay.changed);
    assert_eq!(replay.aggregate.version, 1);
    let removed = replay.aggregate.vote(Some(&replay.voter), 1, false, 1, 2_000).unwrap();
    assert_eq!(removed.voter.revision, 2);
    assert_eq!(removed.voter.mask, 0);
    assert_eq!(removed.aggregate.participants, 1);
    assert_eq!(
        removed.aggregate.vote(Some(&removed.voter), 1, true, 0, 2_000),
        Err(PollError::Conflict)
    );
    let selected_again = removed.aggregate.vote(Some(&removed.voter), 2, true, 2, 2_000).unwrap();
    assert_eq!(selected_again.voter.revision, 3);
    assert_eq!(selected_again.aggregate.counts, vec![0, 1, 0]);
}

#[test]
fn single_choice_replacement_is_one_transition_and_other_voters_do_not_conflict() {
    let first = PollAggregate::new(&definition(false))
        .vote(None, 1, true, 0, 2_000)
        .unwrap();
    let other = first.aggregate.vote(None, 1, true, 0, 2_000).unwrap();
    let changed = other.aggregate.vote(Some(&first.voter), 3, true, 1, 2_000).unwrap();
    assert_eq!(changed.aggregate.counts, vec![1, 0, 1]);
    assert_eq!(changed.aggregate.version, 3);
    assert_eq!(changed.voter.revision, 2);
    assert_eq!(changed.added(), vec![3]);
    assert_eq!(changed.removed(), vec![1]);
}

#[test]
fn multiple_choice_preserves_other_answers_and_rejects_unknown_ids() {
    let first = PollAggregate::new(&definition(true))
        .vote(None, 1, true, 0, 2_000)
        .unwrap();
    let second = first.aggregate.vote(Some(&first.voter), 3, true, 1, 2_000).unwrap();
    assert_eq!(second.voter.answers(), vec![1, 3]);
    assert_eq!(second.aggregate.counts, vec![1, 0, 1]);
    for answer in [0, 4, 255] {
        assert!(matches!(
            second.aggregate.vote(Some(&second.voter), answer, true, 2, 2_000),
            Err(PollError::Invalid(_))
        ));
    }
}

#[test]
fn closing_is_idempotent_and_preserves_the_winning_votes() {
    let first = PollAggregate::new(&definition(false))
        .vote(None, 2, true, 0, 2_000)
        .unwrap();
    assert!(first.aggregate.close(2_000, false).unwrap().is_none());
    let closed = first.aggregate.close(2_000, true).unwrap().unwrap();
    assert_eq!(closed.status, PollStatus::Finalized);
    assert_eq!(closed.counts, vec![0, 1, 0]);
    assert_eq!(closed.version, 2);
    assert!(closed.close(2_000, true).unwrap().is_none());
    assert_eq!(
        closed.vote(Some(&first.voter), 1, true, 1, 2_000),
        Err(PollError::Closed)
    );
    assert_eq!(
        first.aggregate.vote(None, 1, true, 0, 3_601_000),
        Err(PollError::Closed)
    );
    assert!(first.aggregate.close(3_601_000, false).unwrap().is_some());
}

#[test]
fn deleting_fences_late_votes_and_erases_counts_without_reopening() {
    let first = PollAggregate::new(&definition(false))
        .vote(None, 1, true, 0, 2_000)
        .unwrap();
    let deleted = first.aggregate.delete().unwrap().unwrap();
    assert_eq!(deleted.status, PollStatus::Deleted);
    assert_eq!(deleted.counts, vec![0; 3]);
    assert_eq!(
        deleted.vote(Some(&first.voter), 2, true, 1, 2_000),
        Err(PollError::Deleted)
    );
    assert!(deleted.delete().unwrap().is_none());
    assert_eq!(deleted.close(2_000, true), Err(PollError::Deleted));
}

#[test]
fn cap_does_not_lock_out_existing_participants_and_revisions_fail_closed() {
    let first = PollAggregate::new(&definition(false))
        .vote(None, 1, true, 0, 2_000)
        .unwrap();
    let mut capped = first.aggregate.clone();
    capped.participants = MAX_PARTICIPANTS;
    assert_eq!(capped.vote(None, 2, true, 0, 2_000), Err(PollError::Capacity));
    assert!(capped.vote(Some(&first.voter), 2, true, 1, 2_000).is_ok());
    capped.version = i64::MAX;
    assert_eq!(
        capped.vote(Some(&first.voter), 2, true, 1, 2_000),
        Err(PollError::Exhausted)
    );
    assert_eq!(capped.close(2_000, true), Err(PollError::Exhausted));
}

#[test]
fn deterministic_mixed_votes_match_a_full_recount_without_runtime_scanning() {
    for multiple in [false, true] {
        let mut state = PollAggregate::new(&definition(multiple));
        let mut voters = vec![None::<PollVote>; 64];
        let mut seed = 937_u64;
        for _ in 0..8_000 {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            let user = ((seed >> 32) % 64) as usize;
            let answer = ((seed >> 20) % 3 + 1) as u8;
            let select = seed >> 63 == 0;
            let revision = voters[user].as_ref().map_or(0, |v| v.revision);
            let next = state
                .vote(voters[user].as_ref(), answer, select, revision, 2_000)
                .unwrap();
            if next.changed {
                voters[user] = Some(next.voter);
            }
            state = next.aggregate;
            let expected: Vec<u32> = (1..=3)
                .map(|id| voters.iter().flatten().filter(|v| v.answers().contains(&id)).count() as u32)
                .collect();
            assert_eq!(state.counts, expected);
            assert_eq!(state.participants, voters.iter().flatten().count() as u32);
        }
    }
}

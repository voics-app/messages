use super::*;
use crate::fakes::*;
use crate::ports::{NotificationRecipient, ReadStateRepository};
use discordy_messages_domain::{DeliveryMode, NotificationLevel, Permissions, Snowflake};

fn command(content: &str) -> CreateMessageCommand {
    CreateMessageCommand {
        channel_id: CHANNEL,
        author_id: ALICE,
        content: content.into(),
        ..Default::default()
    }
}

async fn count(h: &Harness, user: UserId) -> u32 {
    h.read_states
        .find(user, CHANNEL)
        .await
        .unwrap()
        .map_or(0, |state| state.mention_count)
}

#[tokio::test]
async fn resolved_preferences_roles_and_presence_control_badges() {
    let base = notification_recipient(BOB);
    let role = Snowflake::new(90);
    let with_role = NotificationRecipient {
        role_ids: vec![role],
        ..base.clone()
    };
    let cases = [
        ("direct", format!("hello <@{BOB}>"), base.clone(), 1),
        (
            "level nothing",
            format!("hello <@{BOB}>"),
            NotificationRecipient {
                level: NotificationLevel::NoMessages,
                ..base.clone()
            },
            0,
        ),
        (
            "muted",
            format!("hello <@{BOB}>"),
            NotificationRecipient {
                muted: true,
                ..base.clone()
            },
            0,
        ),
        (
            "plain all",
            "hello".into(),
            NotificationRecipient {
                level: NotificationLevel::AllMessages,
                ..base.clone()
            },
            0,
        ),
        ("matching role", format!("hello <@&{role}>"), with_role.clone(), 1),
        ("other role", "hello <@&91>".into(), with_role.clone(), 0),
        (
            "suppressed role",
            format!("hello <@&{role}>"),
            NotificationRecipient {
                suppress_roles: true,
                ..with_role.clone()
            },
            0,
        ),
        ("everyone", "@everyone".into(), base.clone(), 1),
        (
            "suppressed everyone",
            "@everyone".into(),
            NotificationRecipient {
                suppress_everyone: true,
                ..base.clone()
            },
            0,
        ),
        ("here offline", "@here".into(), base.clone(), 0),
        (
            "here online",
            "@here".into(),
            NotificationRecipient {
                online: true,
                ..base.clone()
            },
            1,
        ),
        ("everyone wins", "@here @everyone".into(), base.clone(), 1),
        (
            "direct wins suppression",
            format!("<@{BOB}> @everyone <@&{role}>"),
            NotificationRecipient {
                suppress_roles: true,
                suppress_everyone: true,
                ..with_role.clone()
            },
            1,
        ),
        (
            "overlap counts once",
            format!("<@{BOB}> <@{BOB}> @everyone <@&{role}>"),
            with_role,
            1,
        ),
    ];
    for (name, content, recipient, expected) in cases {
        let h = Harness::new();
        h.access.set_permissions(MEMBER | Permissions::MENTION_EVERYONE);
        *h.notification_recipients.recipients.lock().unwrap() = vec![recipient];
        CreateMessage::new(&h.ports).execute(command(&content)).await.unwrap();
        assert_eq!(count(&h, BOB).await, expected, "{name}");
    }
}

#[tokio::test]
async fn plain_dms_count_without_a_token_and_respect_current_mutes() {
    let h = Harness::new();
    h.access.set(Ok(dm_access(DeliveryMode::Standard)));
    let create = CreateMessage::new(&h.ports);
    create.execute(command("first")).await.unwrap();
    assert_eq!(count(&h, BOB).await, 1);
    assert_eq!(count(&h, ALICE).await, 0);
    h.notification_recipients.recipients.lock().unwrap()[1].muted = true;
    create.execute(command("muted")).await.unwrap();
    assert_eq!(count(&h, BOB).await, 1);
    h.notification_recipients.recipients.lock().unwrap()[1].muted = false;
    create.execute(command("expired mute resolved by api")).await.unwrap();
    assert_eq!(count(&h, BOB).await, 2);
    assert_eq!(h.notification_recipients.calls.lock().unwrap().len(), 3);
}

#[tokio::test]
async fn permission_filtering_and_policy_failures_never_fall_back_to_raw_mentions() {
    let h = Harness::new();
    h.notification_recipients.hidden.lock().unwrap().insert(BOB);
    let create = CreateMessage::new(&h.ports);
    create.execute(command(&format!("hidden <@{BOB}>"))).await.unwrap();
    assert_eq!(count(&h, BOB).await, 0);
    h.notification_recipients.hidden.lock().unwrap().clear();
    *h.notification_recipients.error.lock().unwrap() = Some(crate::PortError::new("policy unavailable"));
    let result = create.execute(command(&format!("unavailable <@{BOB}>"))).await.unwrap();
    assert!(
        h.messages.find(CHANNEL, result.message.id).await.unwrap().is_some(),
        "the message was already stored"
    );
    assert_eq!(count(&h, BOB).await, 0);
    *h.notification_recipients.error.lock().unwrap() = None;
    create.execute(command(&format!("recovered <@{BOB}>"))).await.unwrap();
    assert_eq!(count(&h, BOB).await, 1);
}

#[tokio::test]
async fn direct_lookups_seek_the_exact_id_and_plain_guild_posts_do_not_scan() {
    let h = Harness::new();
    let next_user = Snowflake::new(12);
    *h.notification_recipients.recipients.lock().unwrap() = vec![notification_recipient(next_user)];
    let create = CreateMessage::new(&h.ports);
    create.execute(command("plain")).await.unwrap();
    assert!(h.notification_recipients.calls.lock().unwrap().is_empty());
    create
        .execute(command(&format!("<@{BOB}> <@{}>", u64::MAX)))
        .await
        .unwrap();
    assert_eq!(
        count(&h, next_user).await,
        0,
        "a following candidate is not the requested user"
    );
    let calls = h.notification_recipients.calls.lock().unwrap();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].after, Some(Snowflake::new(BOB.value() - 1)));
    assert_eq!(calls[0].limit, 1);
}

#[tokio::test]
async fn empty_filtered_pages_still_advance_to_later_role_recipients() {
    let h = Harness::new();
    let role = Snowflake::new(90);
    *h.notification_recipients.recipients.lock().unwrap() = (100..=300)
        .map(|user| NotificationRecipient {
            role_ids: vec![role],
            ..notification_recipient(Snowflake::new(user))
        })
        .collect();
    h.notification_recipients
        .hidden
        .lock()
        .unwrap()
        .extend((100..300).map(Snowflake::new));
    CreateMessage::new(&h.ports)
        .execute(command(&format!("<@&{role}>")))
        .await
        .unwrap();
    assert_eq!(count(&h, Snowflake::new(300)).await, 1);
    let calls = h.notification_recipients.calls.lock().unwrap();
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[1].after, Some(Snowflake::new(299)));
}

#[tokio::test]
async fn silent_system_ephemeral_and_private_policy_pages_cannot_create_badges() {
    let h = Harness::new();
    let create = CreateMessage::new(&h.ports);
    let mut silent = command(&format!("<@{BOB}>"));
    silent.flags = discordy_messages_domain::FLAG_SUPPRESS_NOTIFICATIONS;
    create.execute(silent).await.unwrap();
    let mut system = command("");
    system.system_kind = Some(MessageKind::GuildMemberJoin);
    create.execute_internal(system).await.unwrap();
    let mut ephemeral = command(&format!("<@{BOB}>"));
    ephemeral.flags = FLAG_EPHEMERAL;
    ephemeral.target_user_ids = vec![BOB];
    create.execute_internal(ephemeral).await.unwrap();
    assert!(h.notification_recipients.calls.lock().unwrap().is_empty());
    let mut private = guild_access(MEMBER);
    private.delivery_mode = DeliveryMode::Ephemeral;
    *h.access.channel.lock().unwrap() = Ok(private);
    create.execute(command(&format!("<@{BOB}>"))).await.unwrap();
    assert_eq!(count(&h, BOB).await, 0);
}

#[tokio::test]
async fn suppressing_reply_ping_preserves_explicit_mentions() {
    let h = Harness::new();
    let parent = h.seed(BOB, "parent").await;
    let create = CreateMessage::new(&h.ports);
    let mut reply = command("reply without ping");
    reply.reply_to = Some(parent.id);
    reply.suppress_reply_ping = true;
    create.execute(reply.clone()).await.unwrap();
    assert_eq!(count(&h, BOB).await, 0);
    reply.content = format!("reply with explicit <@{BOB}>");
    create.execute(reply).await.unwrap();
    assert_eq!(count(&h, BOB).await, 1);
}

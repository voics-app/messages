use super::*;
use discordy_messages_application::fakes::InMemoryPolls;
use discordy_messages_application::ports::EventSink;
use discordy_messages_interface::EnvelopePublisher;
use std::sync::Mutex;

#[derive(Default)]
struct Sink(Mutex<Vec<Value>>);

#[async_trait]
impl EventSink for Sink {
    async fn send(&self, _topic: &str, _key: &str, payload: Vec<u8>) -> Result<(), PortError> {
        self.0.lock().unwrap().push(serde_json::from_slice(&payload).unwrap());
        Ok(())
    }
}

fn harness() -> (Harness, Arc<InMemoryPolls>, Arc<Sink>) {
    let mut h = Harness::new();
    h.access.set_permissions(MEMBER | Permissions::SEND_POLLS);
    let polls = Arc::new(InMemoryPolls::new(h.clock.clone()));
    let sink = Arc::new(Sink::default());
    h.ports.polls = Some(polls.clone());
    h.ports.poll_creation_enabled = true;
    h.ports.events = Arc::new(EnvelopePublisher::new(
        sink.clone(),
        h.clock.clone(),
        AttachmentUrls::new("http://cdn", SIGNING_KEY, Duration::from_secs(3600), h.clock.clone()),
        h.users.clone(),
    ));
    (h, polls, sink)
}

fn body() -> Value {
    json!({"poll":{"question":{"text":"Route?"},"answers":[{"poll_media":{"text":"Forest"}},{"poll_media":{"text":"River","emoji":{"name":"🌊"}}}],"duration":1}})
}

async fn create_poll(app: &Router) -> Value {
    let (status, response) = call(
        app,
        Method::POST,
        &format!("/api/v1/channels/{CHANNEL}/messages"),
        Some(ALICE_TOKEN),
        Some(body()),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{response}");
    response
}

fn path(message: &Value) -> String {
    format!("/api/v1/channels/{CHANNEL}/polls/{}", message["id"].as_str().unwrap())
}

#[tokio::test]
async fn public_bots_need_send_poll_permission_and_cannot_vote() {
    let (h, _, _) = harness();
    let app = app(&h);
    *h.access.fresh.lock().unwrap() = Some(Ok(guild_access(MEMBER)));
    assert_eq!(
        call(&app, Method::POST, &messages_uri(), Some("Bot alice"), Some(body()))
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    *h.access.fresh.lock().unwrap() = Some(Ok(guild_access(MEMBER | Permissions::SEND_POLLS)));
    let (status, message) = call(&app, Method::POST, &messages_uri(), Some("Bot alice"), Some(body())).await;
    assert_eq!(status, StatusCode::OK, "{message}");
    let route = path(&message);
    assert_eq!(
        call(&app, Method::GET, &route, Some("Bot alice"), None).await.0,
        StatusCode::OK
    );
    assert_eq!(
        call(
            &app,
            Method::PUT,
            &format!("{route}/answers/1/@me"),
            Some("Bot alice"),
            Some(json!({"expected_vote_version":"0"}))
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(&app, Method::POST, &format!("{route}/expire"), Some("Bot alice"), None)
            .await
            .0,
        StatusCode::OK
    );
}

#[tokio::test]
async fn http_poll_only_create_vote_replay_replace_remove_and_expire_match_snapshots() {
    let (h, _polls, sink) = harness();
    let app = app(&h);
    let message = create_poll(&app).await;
    let route = path(&message);
    assert!(message["content"].as_str().unwrap().is_empty());
    assert!(message["poll"].get("results").is_none());
    let (status, empty) = call(&app, Method::GET, &route, Some("Bearer bob"), None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(empty["results"]["vote_version"], "0");
    assert_eq!(empty["results"]["answer_counts"][0]["me_voted"], false);
    let select = format!("{route}/answers/1/@me");
    let (status, voted) = call(
        &app,
        Method::PUT,
        &select,
        Some("Bearer bob"),
        Some(json!({"expected_vote_version":"0"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(voted["results"]["answer_counts"][0]["count"], 1);
    let (_, replay) = call(
        &app,
        Method::PUT,
        &select,
        Some("Bearer bob"),
        Some(json!({"expected_vote_version":"0"})),
    )
    .await;
    assert_eq!(replay, voted);
    let (_, replaced) = call(
        &app,
        Method::PUT,
        &format!("{route}/answers/2/@me"),
        Some("Bearer bob"),
        Some(json!({"expected_vote_version":"1"})),
    )
    .await;
    assert_eq!(replaced["results"]["answer_counts"][0]["count"], 0);
    assert_eq!(replaced["results"]["answer_counts"][1]["me_voted"], true);
    let (status, conflict) = call(
        &app,
        Method::PUT,
        &select,
        Some("Bearer bob"),
        Some(json!({"expected_vote_version":"0"})),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert!(conflict["errors"].get("expected_vote_version").is_some());
    let (status, removed) = call(
        &app,
        Method::DELETE,
        &format!("{route}/answers/2/@me?expected_vote_version=2"),
        Some("Bearer bob"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(removed["results"]["answer_counts"][1]["count"], 0);
    assert_eq!(removed["results"]["vote_version"], "3");
    let (status, ended) = call(&app, Method::POST, &format!("{route}/expire"), Some(ALICE_TOKEN), None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(ended["poll"]["results"]["is_finalized"], true);
    assert_eq!(
        call(
            &app,
            Method::PUT,
            &select,
            Some("Bearer bob"),
            Some(json!({"expected_vote_version":"3"}))
        )
        .await
        .0,
        StatusCode::CONFLICT
    );
    let events = sink.0.lock().unwrap().clone();
    let votes: Vec<_> = events
        .iter()
        .filter(|event| {
            event["type"]
                .as_str()
                .is_some_and(|name| name.starts_with("MESSAGE_POLL_VOTE_"))
        })
        .collect();
    assert_eq!(votes.len(), 4);
    assert_eq!(votes[1]["data"]["results"], votes[2]["data"]["results"]);
    assert!(votes[0]["data"]["results"].get("vote_version").is_none());
    assert!(
        votes[0]["data"]["results"]["answer_counts"][0]
            .get("me_voted")
            .is_none()
    );
    let final_event = events.iter().find(|event| event["type"] == "MESSAGE_UPDATE").unwrap();
    assert_eq!(final_event["data"]["poll"]["results"]["is_finalized"], true);
    if let Ok(capture) = std::env::var("POLL_HTTP_CAPTURE") {
        let mut fixtures = vec![
            json!({"schema":"Message","data":message}),
            json!({"schema":"PollSnapshot","data":empty}),
            json!({"schema":"PollSnapshot","data":voted}),
            json!({"schema":"PollSnapshot","data":replaced}),
            json!({"schema":"PollSnapshot","data":removed}),
            json!({"schema":"Message","data":ended}),
        ];
        for event in votes {
            fixtures.push(json!({"schema":if event["type"]=="MESSAGE_POLL_VOTE_ADD" {"MessagePollVoteAddEvent"} else {"MessagePollVoteRemoveEvent"},"data":event["data"]}));
        }
        fixtures.push(json!({"schema":"MessageEvent","data":final_event["data"]}));
        std::fs::write(capture, serde_json::to_vec_pretty(&fixtures).unwrap()).unwrap();
    }
}

#[tokio::test]
async fn http_poll_schema_invalid_inputs_are_rejected_before_persistence() {
    let (h, polls, _) = harness();
    let app = app(&h);
    let create = format!("/api/v1/channels/{CHANNEL}/messages");
    let mut invalid = vec![json!({"content":"normal","poll":null})];
    for replacement in [json!({"id":"7","name":"custom"}), json!({"name":"ascii"})] {
        let mut value = body();
        value["poll"]["answers"][0]["poll_media"]["emoji"] = replacement;
        invalid.push(value);
    }
    for (name, value) in [
        ("duration", json!(769)),
        ("layout_type", json!(2)),
        ("unknown", json!(true)),
    ] {
        let mut input = body();
        input["poll"][name] = value;
        invalid.push(input);
    }
    let mut one = body();
    one["poll"]["answers"] = json!([{"poll_media":{"text":"Only"}}]);
    invalid.push(one);
    for input in invalid {
        let (status, response) = call(&app, Method::POST, &create, Some(ALICE_TOKEN), Some(input)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{response}");
    }
    assert_eq!(polls.mutations.load(std::sync::atomic::Ordering::SeqCst), 0);
    let message = create_poll(&app).await;
    let route = path(&message);
    for value in [json!(0), json!("01"), json!("+1"), json!("9223372036854775808")] {
        assert_eq!(
            call(
                &app,
                Method::PUT,
                &format!("{route}/answers/1/@me"),
                Some("Bearer bob"),
                Some(json!({"expected_vote_version":value}))
            )
            .await
            .0,
            StatusCode::BAD_REQUEST
        );
    }
    assert_eq!(
        call(
            &app,
            Method::DELETE,
            &format!("{route}/answers/1/@me"),
            Some("Bearer bob"),
            None
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
}

#[tokio::test]
async fn http_poll_permissions_modes_and_internal_bypass_are_enforced() {
    let (h, polls, _) = harness();
    let app = app(&h);
    let create = format!("/api/v1/channels/{CHANNEL}/messages");
    *h.access.fresh.lock().unwrap() = Some(Ok(guild_access(MEMBER)));
    assert_eq!(
        call(&app, Method::POST, &create, Some(ALICE_TOKEN), Some(body()))
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    *h.access.fresh.lock().unwrap() = None;
    let message = create_poll(&app).await;
    let route = path(&message);
    *h.access.fresh.lock().unwrap() = Some(Err(AccessError::NotMember));
    assert_eq!(
        call(&app, Method::GET, &route, Some("Bearer bob"), None).await.0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(
            &app,
            Method::PUT,
            &format!("{route}/answers/1/@me"),
            Some("Bearer bob"),
            Some(json!({"expected_vote_version":"0"}))
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    for mode in [DeliveryMode::Ephemeral, DeliveryMode::P2p] {
        *h.access.fresh.lock().unwrap() = Some(Ok(dm_access(mode)));
        let (status, response) = call(&app, Method::POST, &create, Some(ALICE_TOKEN), Some(body())).await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(response["code"], 50160);
        assert_eq!(
            call(&app, Method::GET, &route, Some("Bearer bob"), None).await.0,
            StatusCode::CONFLICT
        );
    }
    for poll in [Value::Null, body()["poll"].clone()] {
        assert_eq!(
            post_internal(
                &app,
                &format!("/internal/v1/channels/{CHANNEL}/messages"),
                Some(INTERNAL),
                json!({"author_id":ALICE.to_string(),"content":"text","poll":poll})
            )
            .await
            .0,
            StatusCode::BAD_REQUEST
        );
    }
    assert_eq!(polls.mutations.load(std::sync::atomic::Ordering::SeqCst), 1);
}

#[tokio::test]
async fn http_poll_definition_is_immutable_history_keeps_it_and_deletion_fences_votes() {
    let (h, _, _) = harness();
    let app = app(&h);
    let message = create_poll(&app).await;
    let poll = path(&message);
    let route = format!(
        "/api/v1/channels/{CHANNEL}/messages/{}",
        message["id"].as_str().unwrap()
    );
    for value in [Value::Null, body()["poll"].clone()] {
        assert_eq!(
            call(
                &app,
                Method::PATCH,
                &route,
                Some(ALICE_TOKEN),
                Some(json!({"poll":value}))
            )
            .await
            .0,
            StatusCode::BAD_REQUEST
        );
    }
    let (status, edited) = call(
        &app,
        Method::PATCH,
        &route,
        Some(ALICE_TOKEN),
        Some(json!({"content":"new comment"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(edited["poll"], message["poll"]);
    let (_, history) = call(
        &app,
        Method::GET,
        &format!("/api/v1/channels/{CHANNEL}/messages"),
        Some(ALICE_TOKEN),
        None,
    )
    .await;
    assert_eq!(history[0]["poll"], message["poll"]);
    let forwarded = json!({"message_reference":{"type":1,"channel_id":CHANNEL.to_string(),"message_id":message["id"]}});
    assert_eq!(
        call(
            &app,
            Method::POST,
            &format!("/api/v1/channels/{CHANNEL}/messages"),
            Some(ALICE_TOKEN),
            Some(forwarded)
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        call(&app, Method::DELETE, &route, Some(ALICE_TOKEN), None).await.0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        call(&app, Method::GET, &poll, Some("Bearer bob"), None).await.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(
            &app,
            Method::PUT,
            &format!("{poll}/answers/1/@me"),
            Some("Bearer bob"),
            Some(json!({"expected_vote_version":"0"}))
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
}

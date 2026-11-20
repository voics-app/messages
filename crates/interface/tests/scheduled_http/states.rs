use super::*;
use discordy_messages_application::ports::scheduled::{ScheduleJournal, ScheduleKey};
use discordy_messages_domain::{
    MessageId,
    scheduled::{DispatchOutcome, ScheduleChange, ScheduledFailure},
};

#[tokio::test]
async fn every_visible_state_keeps_required_nulls_and_clears_terminal_content() {
    let mut responses = Vec::new();
    for status in [
        "pending",
        "dispatching",
        "sent",
        "failed",
        "cancelled",
        "delivery_unknown",
    ] {
        let f = Fixture::new(true);
        assert_eq!(
            call(&f.app, Method::POST, &base(), Some("Bearer alice"), Some(f.create()))
                .await
                .0,
            StatusCode::CREATED
        );
        let key = ScheduleKey {
            author_id: ALICE,
            id: REQUEST.parse().unwrap(),
        };
        f.h.clock.advance_ms(120_000);
        let now = f.h.clock.now_unix_ms();
        let attempt = "00000000-0000-4000-8000-000000000002".parse().unwrap();
        if matches!(status, "dispatching" | "sent" | "delivery_unknown") {
            f.memory
                .change(
                    key,
                    ScheduleChange::Claim {
                        expected_revision: 1,
                        attempt_id: attempt,
                        message_id: MessageId::new(123),
                    },
                    now,
                )
                .await
                .unwrap();
        }
        let change = match status {
            "sent" => Some(ScheduleChange::Finish {
                attempt_id: attempt,
                outcome: DispatchOutcome::Sent,
            }),
            "delivery_unknown" => Some(ScheduleChange::Finish {
                attempt_id: attempt,
                outcome: DispatchOutcome::Unknown,
            }),
            "failed" => Some(ScheduleChange::Reject {
                expected_revision: 1,
                reason: ScheduledFailure::AccessDenied,
            }),
            "cancelled" => Some(ScheduleChange::Cancel { expected_revision: 1 }),
            _ => None,
        };
        if let Some(change) = change {
            f.memory.change(key, change, now).await.unwrap();
        }
        let (code, body) = call(&f.app, Method::GET, &item(), Some("Bearer alice"), None).await;
        assert_eq!(code, StatusCode::OK, "{body}");
        assert_eq!(body["status"], status);
        for field in [
            "id",
            "channel_id",
            "author_id",
            "revision",
            "status",
            "scheduled_at",
            "created_at",
            "updated_at",
            "message",
            "message_id",
            "failure",
        ] {
            assert!(body.get(field).is_some(), "{status}: omitted {field}");
        }
        assert_eq!(body["message"].is_object(), matches!(status, "pending" | "dispatching"));
        assert_eq!(
            body["message_id"].is_string(),
            matches!(status, "dispatching" | "sent" | "delivery_unknown")
        );
        assert_eq!(
            body["failure"],
            match status {
                "failed" => json!("access_denied"),
                "delivery_unknown" => json!("delivery_unknown"),
                _ => Value::Null,
            }
        );
        let (_, list) = call(&f.app, Method::GET, &base(), Some("Bearer alice"), None).await;
        assert_eq!(list["items"], json!([body]));
        assert!(list.get("next_after").is_some_and(Value::is_null));
        responses.push(list);
    }
    // Optional local acceptance artifact; only synthetic data from this test is written.
    if let Ok(path) = std::env::var("SCHEDULED_HTTP_CAPTURE") {
        std::fs::write(path, serde_json::to_vec_pretty(&responses).unwrap()).unwrap();
    }
}

#[tokio::test]
async fn timestamps_normalize_offsets_and_round_up_without_an_early_instant() {
    let f = Fixture::new(true);
    let mut body = f.create();
    let utc = chrono::DateTime::parse_from_rfc3339(body["scheduled_at"].as_str().unwrap()).unwrap();
    let local = utc.with_timezone(&chrono::FixedOffset::east_opt(7200).unwrap());
    body["scheduled_at"] = json!(local.to_rfc3339());
    let (code, accepted) = call(&f.app, Method::POST, &base(), Some("Bearer alice"), Some(body)).await;
    assert_eq!(code, StatusCode::CREATED);
    assert_eq!(
        chrono::DateTime::parse_from_rfc3339(accepted["scheduled_at"].as_str().unwrap()).unwrap(),
        utc
    );
    let (code, replay) = call(&f.app, Method::POST, &base(), Some("Bearer alice"), Some(f.create())).await;
    assert_eq!(code, StatusCode::OK);
    assert_eq!(accepted, replay);

    let precise = utc + chrono::Duration::nanoseconds(1);
    let (code, edited) = call(
        &f.app,
        Method::PATCH,
        &item(),
        Some("Bearer alice"),
        Some(json!({"expected_revision":1,"scheduled_at":precise.to_rfc3339()})),
    )
    .await;
    assert_eq!(code, StatusCode::OK);
    let saved = chrono::DateTime::parse_from_rfc3339(edited["scheduled_at"].as_str().unwrap()).unwrap();
    assert!(saved >= precise);
    assert_eq!((saved - utc).num_milliseconds(), 1);
}

#[tokio::test]
async fn empty_filtered_pages_keep_a_cursor_and_exact_expired_records_remain_visible() {
    let f = Fixture::new(true);
    call(&f.app, Method::POST, &base(), Some("Bearer alice"), Some(f.create())).await;
    call(
        &f.app,
        Method::DELETE,
        &format!("{}?expected_revision=1", item()),
        Some("Bearer alice"),
        None,
    )
    .await;
    f.h.clock.advance_ms(8 * 86_400_000);
    let (_, page) = call(
        &f.app,
        Method::GET,
        &format!("{}?limit=1", base()),
        Some("Bearer alice"),
        None,
    )
    .await;
    assert_eq!(page["items"], json!([]));
    assert_eq!(page["next_after"], REQUEST);
    let (_, next) = call(
        &f.app,
        Method::GET,
        &format!("{}?limit=1&after={REQUEST}", base()),
        Some("Bearer alice"),
        None,
    )
    .await;
    assert_eq!(next, json!({"items":[],"next_after":null}));
    let (code, exact) = call(&f.app, Method::GET, &item(), Some("Bearer alice"), None).await;
    assert_eq!(code, StatusCode::OK);
    assert_eq!(exact["status"], "cancelled");
    assert!(exact["message"].is_null());
}

#[tokio::test]
async fn author_mutations_return_the_existing_rate_limit_headers() {
    let f = Fixture::new(true);
    let original = f.create();
    for _ in 0..120 {
        let (code, _) = call(
            &f.app,
            Method::POST,
            &base(),
            Some("Bearer alice"),
            Some(original.clone()),
        )
        .await;
        assert!(code.is_success());
    }
    let request = Request::builder()
        .method(Method::POST)
        .uri(base())
        .header("authorization", "Bearer alice")
        .header("content-type", "application/json")
        .body(Body::from(original.to_string()))
        .unwrap();
    let response = f.app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(response.headers()["retry-after"], "60");
}

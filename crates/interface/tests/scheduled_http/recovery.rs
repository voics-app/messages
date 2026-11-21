use super::*;
use discordy_messages_application::ports::scheduled::ScheduleJournal;
use discordy_messages_domain::scheduled::{PREPARATION_TIMEOUT_MS, ScheduledStatus};
use std::sync::atomic::Ordering;

#[tokio::test]
async fn author_inspection_explicitly_recovers_only_own_abandoned_work() {
    for detail in [false, true] {
        let f = Fixture::new(true);
        f.memory.fail_next_stage.store(true, Ordering::SeqCst);
        assert_eq!(
            call(&f.app, Method::POST, &base(), Some("Bearer alice"), Some(f.create()))
                .await
                .0,
            StatusCode::SERVICE_UNAVAILABLE
        );
        f.h.clock.advance_ms(PREPARATION_TIMEOUT_MS);
        f.h.access.set_permissions(0);

        let (code, _) = call(&f.app, Method::GET, &item(), Some("Bearer bob"), None).await;
        assert_eq!(code, StatusCode::NOT_FOUND);
        let (code, page) = call(&f.app, Method::GET, &base(), Some("Bearer bob"), None).await;
        assert_eq!(code, StatusCode::OK);
        assert_eq!(page["items"], json!([]));
        for malformed in [format!("{}?limit=0", base()), format!("{}/not-a-uuid", base())] {
            assert_eq!(
                call(&f.app, Method::GET, &malformed, Some("Bearer alice"), None)
                    .await
                    .0,
                StatusCode::BAD_REQUEST
            );
        }
        let active = f.memory.active(ALICE).await.unwrap();
        assert_eq!(
            active.len(),
            1,
            "other authors and malformed reads must not recover Alice's work"
        );
        assert_eq!(active[0].status, ScheduledStatus::Preparing);
        assert!(active[0].draft.is_some());

        let path = if detail { item() } else { base() };
        let (code, body) = call(&f.app, Method::GET, &path, Some("Bearer alice"), None).await;
        assert_eq!(code, StatusCode::OK, "{body}");
        let job = if detail { &body } else { &body["items"][0] };
        assert_eq!(job["status"], "failed");
        assert!(job["message"].is_null());
        assert!(f.memory.active(ALICE).await.unwrap().is_empty());
    }
}

//! Route table.

use axum::Router;
use std::time::Instant;

use axum::extract::{MatchedPath, Request, State};
use axum::middleware::{self, Next};
use axum::response::Response;
use axum::routing::{delete, get, patch, post, put};

use crate::routes::{attachments, download, external, health, internal, messages, pins, reactions, scheduled, search};
use crate::state::AppState;

pub fn router(state: AppState) -> Router {
    let public = Router::new()
        .route(
            "/channels/{channel_id}/messages/scheduled",
            get(scheduled::inspect_list).post(scheduled::create),
        )
        .route(
            "/channels/{channel_id}/messages/scheduled/{schedule_id}",
            get(scheduled::inspect).patch(scheduled::edit).delete(scheduled::cancel),
        )
        .route("/guilds/{guild_id}/messages/search", get(search::guild))
        .route("/channels/{channel_id}/messages/search", get(search::channel))
        .route(
            "/channels/{channel_id}/messages",
            get(messages::history).post(messages::create),
        )
        .route(
            "/channels/{channel_id}/messages/bulk-delete",
            post(messages::bulk_delete),
        )
        .route(
            "/channels/{channel_id}/messages/{message_id}",
            delete(messages::delete).patch(messages::edit),
        )
        .route(
            "/channels/{channel_id}/messages/{message_id}/ack",
            post(messages::acknowledge),
        )
        .route(
            "/channels/{channel_id}/messages/{message_id}/reactions",
            delete(reactions::remove_all),
        )
        .route(
            "/channels/{channel_id}/messages/{message_id}/reactions/{emoji}",
            get(reactions::list_users).delete(reactions::remove_emoji),
        )
        .route(
            "/channels/{channel_id}/messages/{message_id}/reactions/{emoji}/{user_id}",
            delete(reactions::remove_user),
        )
        .route(
            "/channels/{channel_id}/messages/{message_id}/reactions/{emoji}/@me",
            put(reactions::add).delete(reactions::remove_own),
        )
        .route("/channels/{channel_id}/pins", get(pins::list))
        .route(
            "/channels/{channel_id}/pins/{message_id}",
            put(pins::pin).delete(pins::unpin),
        )
        .route("/channels/{channel_id}/attachments", post(attachments::create_slots))
        .route(
            "/channels/{channel_id}/attachments/refresh-urls",
            post(attachments::refresh_urls),
        );

    let internal = Router::new()
        .route("/channels/{channel_id}/crossposts", post(internal::deliver_crosspost))
        .route("/channels/{channel_id}/messages", post(internal::create_message))
        .route("/channels/{channel_id}/forum-start", post(internal::create_forum_start))
        .route("/forum-summaries", post(internal::forum_summaries))
        .route(
            "/channels/{channel_id}/messages/{message_id}",
            get(internal::get_message),
        )
        .route(
            "/channels/{channel_id}/messages/{message_id}/call",
            patch(internal::update_call),
        )
        .route(
            "/channels/{channel_id}/messages/{message_id}/crossposted",
            post(internal::mark_crossposted),
        )
        .route("/channels/{channel_id}/last-message-id", get(internal::last_message_id))
        .route("/users/{user_id}/read-states", get(internal::read_states))
        .route("/admin/stats", get(internal::admin_stats))
        .route("/search/reindex", post(internal::reindex_search));

    Router::new()
        .nest("/api/v1", public)
        .nest("/internal/v1", internal)
        .route(
            "/attachments/{channel_id}/{attachment_id}/{filename}",
            get(download::download),
        )
        .route("/external/{token}", get(external::proxy))
        .route("/healthz", get(health::live))
        .route("/readyz", get(health::ready))
        .route("/metrics", get(health::metrics))
        .layer(middleware::from_fn_with_state(state.clone(), count_responses))
        .with_state(state)
}

async fn count_responses(State(state): State<AppState>, request: Request, next: Next) -> Response {
    let method = request.method().as_str().to_owned();
    // The matched template keeps label cardinality bounded; unrouted paths share one label.
    let route = request
        .extensions()
        .get::<MatchedPath>()
        .map_or_else(|| "unmatched".to_owned(), |p| p.as_str().to_owned());
    let started = Instant::now();
    let response = next.run(request).await;
    state
        .metrics
        .record(&method, &route, response.status().as_u16(), started.elapsed());
    response
}

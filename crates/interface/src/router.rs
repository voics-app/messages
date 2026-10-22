//! Route table.

use axum::Router;
use axum::extract::{Request, State};
use axum::middleware::{self, Next};
use axum::response::Response;
use axum::routing::{delete, get, post, put};

use crate::routes::{attachments, download, external, health, internal, messages, pins, reactions, search};
use crate::state::AppState;

pub fn router(state: AppState) -> Router {
    let public = Router::new()
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
        .route("/channels/{channel_id}/messages", post(internal::create_message))
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
    let response = next.run(request).await;
    state.metrics.record(response.status().as_u16());
    response
}

//! Interface layer of the Discordy messages service: the axum HTTP surface
//! (public REST and internal API) and the event-bus envelope encoder.

pub mod attachment_urls;
pub mod authors;
pub mod cors;
pub mod dto;
pub mod envelope;
pub mod error;
pub mod extract;
pub mod metrics;
pub mod readiness;
pub mod router;
pub mod routes;
pub mod state;

pub use attachment_urls::AttachmentUrls;
pub use envelope::EnvelopePublisher;
pub use readiness::ReadinessCheck;
pub use router::router;
pub use state::{AppState, InternalToken};

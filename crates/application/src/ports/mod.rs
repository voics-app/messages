//! Ports: the traits that use cases depend on. Adapters in the
//! infrastructure and interface crates implement them.

mod activity;
mod auth;
mod attachments;
mod channel_access;
mod clock;
mod events;
mod ids;
mod messages;
mod pins;
mod reactions;
mod read_states;

pub use activity::ActivityRepository;
pub use auth::{AuthError, Authenticator, Principal};
pub use attachments::{AttachmentStorage, StoredObject};
pub use channel_access::{AccessError, ChannelAccessProvider};
pub use clock::Clock;
pub use events::{EventPublisher, EventSink, MessageEvent, MessageEventKind};
pub use ids::IdGenerator;
pub use messages::{BucketQuery, MessageRepository};
pub use pins::{Pin, PinRepository};
pub use reactions::ReactionRepository;
pub use read_states::ReadStateRepository;

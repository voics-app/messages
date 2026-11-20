//! Ports: the traits that use cases depend on. Adapters in the
//! infrastructure and interface crates implement them.

mod activity;
mod attachments;
mod auth;
mod channel_access;
mod clock;
mod events;
mod ids;
mod messages;
mod notification_recipients;
mod pins;
mod reactions;
mod read_states;
pub mod scheduled;
mod search;
mod unfurl;
mod users;

pub use activity::ActivityRepository;
pub use attachments::{AttachmentStorage, ObjectRead, StoredObject};
pub use auth::{AuthError, Authenticator, Principal};
pub use channel_access::{AccessError, ChannelAccessProvider};
pub use clock::Clock;
pub use events::{EventPublisher, EventSink, MessageEvent, MessageEventKind};
pub use ids::IdGenerator;
pub use messages::{BucketQuery, MessageRepository};
pub use notification_recipients::{
    NotificationRecipient, NotificationRecipientPage, NotificationRecipients, RecipientQuery,
};
pub use pins::{Pin, PinRepository};
pub use reactions::ReactionRepository;
pub use read_states::ReadStateRepository;
pub use search::{BackfillState, Checkpoint, IndexOp, PinnedFilter, SearchHit, SearchIndex, SearchPage, SearchRequest};
pub use unfurl::{LinkUnfurler, MediaError, RemoteMedia, RemoteMediaFetcher};
pub use users::{UserDirectory, UserProfile};

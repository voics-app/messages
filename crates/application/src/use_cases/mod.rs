//! One use case per file. Each is constructed from the shared
//! [`MessagingPorts`] bundle and takes only the ports it needs.

use std::sync::Arc;

use crate::history_reader::HistoryReader;
use crate::ports::{
    ActivityRepository, AttachmentStorage, ChannelAccessProvider, Clock, EventPublisher, IdGenerator,
    MessageRepository, PinRepository, ReactionRepository, ReadStateRepository, SearchIndex, UserDirectory,
};

mod acknowledge_message;
mod add_reaction;
mod bulk_delete_messages;
mod create_message;
mod create_upload_slots;
mod delete_message;
mod edit_message;
mod get_last_message_id;
mod get_message_history;
mod get_message_stats;
mod list_pins;
mod list_read_states;
mod pin_message;
mod refresh_attachment_urls;
mod reindex_search;
mod remove_all_reactions;
mod remove_own_reaction;
mod search_messages;
mod support;
mod unpin_message;

pub use acknowledge_message::AcknowledgeMessage;
pub use add_reaction::AddReaction;
pub use bulk_delete_messages::BulkDeleteMessages;
pub use create_message::{AttachmentReference, CreateMessage, CreateMessageCommand};
pub use create_upload_slots::{CreateUploadSlots, UploadRequest};
pub use delete_message::DeleteMessage;
pub use edit_message::EditMessage;
pub use get_last_message_id::GetLastMessageId;
pub use get_message_history::GetMessageHistory;
pub use get_message_stats::GetMessageStats;
pub use list_pins::ListPins;
pub use list_read_states::ListReadStates;
pub use pin_message::PinMessage;
pub use refresh_attachment_urls::RefreshAttachmentUrls;
pub use reindex_search::{ReindexReport, ReindexSearch};
pub use remove_all_reactions::RemoveAllReactions;
pub use remove_own_reaction::RemoveOwnReaction;
pub use search_messages::{SearchMessages, SearchOutcome, SearchResultMessage, SearchScope};
pub use unpin_message::UnpinMessage;

/// Every port the use cases depend on, wired once by the composition root.
#[derive(Clone)]
pub struct MessagingPorts {
    pub access: Arc<dyn ChannelAccessProvider>,
    pub messages: Arc<dyn MessageRepository>,
    pub reactions: Arc<dyn ReactionRepository>,
    pub pins: Arc<dyn PinRepository>,
    pub read_states: Arc<dyn ReadStateRepository>,
    pub activity: Arc<dyn ActivityRepository>,
    pub storage: Arc<dyn AttachmentStorage>,
    pub events: Arc<dyn EventPublisher>,
    pub ids: Arc<dyn IdGenerator>,
    pub clock: Arc<dyn Clock>,
    pub history: Arc<HistoryReader>,
    pub users: Arc<dyn UserDirectory>,
    pub search: Arc<dyn SearchIndex>,
}

//! Shared request state: the use cases plus the cross-cutting collaborators.

use std::sync::Arc;

use discordy_messages_application::ports::{AttachmentStorage, Authenticator, UserDirectory};
use discordy_messages_application::use_cases::{
    AcknowledgeMessage, AddReaction, BulkDeleteMessages, CreateMessage, CreateUploadSlots, DeleteMessage, EditMessage,
    GetLastMessageId, GetMessageHistory, GetMessageStats, ListPins, ListReadStates, MessagingPorts, PinMessage,
    RefreshAttachmentUrls, ReindexSearch, RemoveAllReactions, RemoveOwnReaction, SearchMessages, UnpinMessage,
};
use subtle::ConstantTimeEq;

use crate::attachment_urls::AttachmentUrls;
use crate::metrics::Metrics;
use crate::readiness::ReadinessCheck;

/// The shared secret expected in `X-Internal-Token` on `/internal/*` calls.
#[derive(Clone)]
pub struct InternalToken(Arc<str>);

impl InternalToken {
    pub fn new(secret: &str) -> Self {
        Self(Arc::from(secret))
    }

    pub fn matches(&self, presented: &str) -> bool {
        self.0.as_bytes().ct_eq(presented.as_bytes()).into()
    }
}

pub struct UseCases {
    pub create_message: CreateMessage,
    pub edit_message: EditMessage,
    pub delete_message: DeleteMessage,
    pub bulk_delete: BulkDeleteMessages,
    pub history: GetMessageHistory,
    pub add_reaction: AddReaction,
    pub remove_own_reaction: RemoveOwnReaction,
    pub remove_all_reactions: RemoveAllReactions,
    pub pin: PinMessage,
    pub unpin: UnpinMessage,
    pub list_pins: ListPins,
    pub acknowledge: AcknowledgeMessage,
    pub upload_slots: CreateUploadSlots,
    pub refresh_urls: RefreshAttachmentUrls,
    pub last_message_id: GetLastMessageId,
    pub read_states: ListReadStates,
    pub stats: GetMessageStats,
    pub search: SearchMessages,
    pub reindex: ReindexSearch,
}

impl UseCases {
    pub fn new(ports: &MessagingPorts) -> Self {
        Self {
            create_message: CreateMessage::new(ports),
            edit_message: EditMessage::new(ports),
            delete_message: DeleteMessage::new(ports),
            bulk_delete: BulkDeleteMessages::new(ports),
            history: GetMessageHistory::new(ports),
            add_reaction: AddReaction::new(ports),
            remove_own_reaction: RemoveOwnReaction::new(ports),
            remove_all_reactions: RemoveAllReactions::new(ports),
            pin: PinMessage::new(ports),
            unpin: UnpinMessage::new(ports),
            list_pins: ListPins::new(ports),
            acknowledge: AcknowledgeMessage::new(ports),
            upload_slots: CreateUploadSlots::new(ports),
            refresh_urls: RefreshAttachmentUrls::new(ports),
            last_message_id: GetLastMessageId::new(ports),
            read_states: ListReadStates::new(ports),
            stats: GetMessageStats::new(ports),
            search: SearchMessages::new(ports),
            reindex: ReindexSearch::new(ports),
        }
    }
}

pub struct Shared {
    pub use_cases: UseCases,
    pub authenticator: Arc<dyn Authenticator>,
    pub internal_token: InternalToken,
    pub urls: AttachmentUrls,
    pub storage: Arc<dyn AttachmentStorage>,
    pub users: Arc<dyn UserDirectory>,
    pub readiness: Vec<Arc<dyn ReadinessCheck>>,
    pub metrics: Metrics,
}

#[derive(Clone)]
pub struct AppState(Arc<Shared>);

impl AppState {
    pub fn new(
        ports: &MessagingPorts,
        authenticator: Arc<dyn Authenticator>,
        internal_token: InternalToken,
        urls: AttachmentUrls,
        readiness: Vec<Arc<dyn ReadinessCheck>>,
    ) -> Self {
        Self(Arc::new(Shared {
            use_cases: UseCases::new(ports),
            authenticator,
            internal_token,
            urls,
            storage: ports.storage.clone(),
            users: ports.users.clone(),
            readiness,
            metrics: Metrics::default(),
        }))
    }
}

impl std::ops::Deref for AppState {
    type Target = Shared;

    fn deref(&self) -> &Shared {
        &self.0
    }
}

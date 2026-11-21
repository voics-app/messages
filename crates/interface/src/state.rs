//! Shared request state: the use cases plus the cross-cutting collaborators.

use std::sync::Arc;

use discordy_messages_application::ports::{AttachmentStorage, Authenticator, RemoteMediaFetcher, UserDirectory};
use discordy_messages_application::use_cases::{
    AcknowledgeMessage, AddReaction, BulkDeleteMessages, CountMessages, CreateMessage, CreateUploadSlots,
    DeleteMessage, EditMessage, GetGuildMessageActivity, GetLastMessageId, GetMessage, GetMessageHistory,
    GetMessageStats, ListPins, ListReactionUsers, ListReadStates, MarkCrossposted, MessagingPorts, PinMessage,
    RefreshAttachmentUrls, ReindexSearch, RemoveAllReactions, RemoveOwnReaction, RemoveReactions, SearchMessages,
    UnpinMessage, UpdateCall,
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
    pub guild_message_activity: GetGuildMessageActivity,
    pub create_message: CreateMessage,
    pub edit_message: EditMessage,
    pub delete_message: DeleteMessage,
    pub bulk_delete: BulkDeleteMessages,
    pub history: GetMessageHistory,
    pub add_reaction: AddReaction,
    pub remove_own_reaction: RemoveOwnReaction,
    pub remove_all_reactions: RemoveAllReactions,
    pub list_reaction_users: ListReactionUsers,
    pub remove_reactions: RemoveReactions,
    pub pin: PinMessage,
    pub unpin: UnpinMessage,
    pub update_call: UpdateCall,
    pub mark_crossposted: MarkCrossposted,
    pub list_pins: ListPins,
    pub acknowledge: AcknowledgeMessage,
    pub upload_slots: CreateUploadSlots,
    pub refresh_urls: RefreshAttachmentUrls,
    pub last_message_id: GetLastMessageId,
    pub message_counts: CountMessages,
    pub get_message: GetMessage,
    pub read_states: ListReadStates,
    pub stats: GetMessageStats,
    pub search: SearchMessages,
    pub reindex: ReindexSearch,
}

impl UseCases {
    pub fn new(ports: &MessagingPorts) -> Self {
        Self {
            guild_message_activity: GetGuildMessageActivity::new(ports),
            create_message: CreateMessage::new(ports),
            edit_message: EditMessage::new(ports),
            delete_message: DeleteMessage::new(ports),
            bulk_delete: BulkDeleteMessages::new(ports),
            history: GetMessageHistory::new(ports),
            add_reaction: AddReaction::new(ports),
            remove_own_reaction: RemoveOwnReaction::new(ports),
            remove_all_reactions: RemoveAllReactions::new(ports),
            list_reaction_users: ListReactionUsers::new(ports),
            remove_reactions: RemoveReactions::new(ports),
            pin: PinMessage::new(ports),
            unpin: UnpinMessage::new(ports),
            update_call: UpdateCall::new(ports),
            mark_crossposted: MarkCrossposted::new(ports),
            list_pins: ListPins::new(ports),
            acknowledge: AcknowledgeMessage::new(ports),
            upload_slots: CreateUploadSlots::new(ports),
            refresh_urls: RefreshAttachmentUrls::new(ports),
            last_message_id: GetLastMessageId::new(ports),
            message_counts: CountMessages::new(ports),
            get_message: GetMessage::new(ports),
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
    pub media: Arc<dyn RemoteMediaFetcher>,
    pub storage: Arc<dyn AttachmentStorage>,
    pub users: Arc<dyn UserDirectory>,
    pub readiness: Vec<Arc<dyn ReadinessCheck>>,
    pub metrics: Metrics,
}

#[derive(Clone)]
pub struct AppState {
    shared: Arc<Shared>,
    pub scheduled: Option<Arc<discordy_messages_application::scheduled::ScheduledMessages>>,
    pub gif_media_hosts: Arc<[String]>,
}

impl AppState {
    pub fn new(
        ports: &MessagingPorts,
        authenticator: Arc<dyn Authenticator>,
        internal_token: InternalToken,
        urls: AttachmentUrls,
        media: Arc<dyn RemoteMediaFetcher>,
        readiness: Vec<Arc<dyn ReadinessCheck>>,
        metrics: Metrics,
    ) -> Self {
        Self {
            shared: Arc::new(Shared {
                use_cases: UseCases::new(ports),
                authenticator,
                internal_token,
                urls,
                media,
                storage: ports.storage.clone(),
                users: ports.users.clone(),
                readiness,
                metrics,
            }),
            scheduled: None,
            gif_media_hosts: Arc::from([]),
        }
    }

    pub fn with_gif_media_hosts(mut self, hosts: Vec<String>) -> Self {
        self.gif_media_hosts = hosts
            .into_iter()
            .map(|h| h.trim().to_ascii_lowercase())
            .filter(|h| !h.is_empty())
            .collect();
        self
    }

    pub fn with_scheduled(
        mut self,
        scheduled: Arc<discordy_messages_application::scheduled::ScheduledMessages>,
    ) -> Self {
        self.scheduled = Some(scheduled);
        self
    }
}

impl std::ops::Deref for AppState {
    type Target = Shared;

    fn deref(&self) -> &Shared {
        &self.shared
    }
}

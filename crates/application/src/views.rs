//! Read models returned by use cases.

use discordy_messages_domain::{Message, ReactionCount};

/// A message as one viewer sees it: reactions carry that viewer's `me` flag.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageView {
    pub message: Message,
    pub reactions: Vec<ReactionCount>,
}

impl MessageView {
    pub fn without_reactions(message: Message) -> Self {
        Self {
            message,
            reactions: Vec::new(),
        }
    }
}

/// An issued upload slot (`POST /channels/{id}/attachments`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UploadSlot {
    /// The client's correlation id for this file, echoed back.
    pub client_id: String,
    pub upload_url: String,
    pub upload_filename: String,
}

/// Admin stats (pinned interface P4). Counts only.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MessageStats {
    pub messages_24h: u64,
    pub standard: u64,
    pub ephemeral_relayed: u64,
    pub p2p_signaled: u64,
}

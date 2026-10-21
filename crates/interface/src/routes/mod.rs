//! One module per resource; handlers stay thin and delegate to use cases.

pub mod attachments;
pub mod download;
pub mod external;
pub mod health;
pub mod internal;
pub mod messages;
pub mod pins;
pub mod reactions;
pub mod search;

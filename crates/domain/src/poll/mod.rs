//! Immutable poll definitions and pure, atomic vote/finalization transitions.
//! Persistence must compare the observed aggregate revision before applying a
//! transition; this module never relies on process-local locking.

mod aggregate;
mod definition;

pub use aggregate::{MAX_PARTICIPANTS, PollAggregate, PollStatus, PollTransition, PollVote};
pub use definition::{PollDefinition, PollDraft, PollMedia};

use crate::DomainError;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PollError {
    #[error(transparent)]
    Invalid(#[from] DomainError),
    #[error("your vote changed; refresh the poll before changing it again")]
    Conflict,
    #[error("poll voting has ended")]
    Closed,
    #[error("poll was deleted")]
    Deleted,
    #[error("poll participant limit reached")]
    Capacity,
    #[error("poll revision exhausted")]
    Exhausted,
    #[error("inconsistent stored poll state")]
    Corrupt,
}

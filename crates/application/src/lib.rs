//! Application layer of the Voics messages service: use cases (one per
//! file) orchestrating the domain through ports.

pub mod coalescer;
pub mod error;
pub mod history_reader;
pub mod ports;
pub mod rate_limit;
pub mod use_cases;
pub mod views;

/// In-memory ports for tests of this crate and of the layers above it.
#[cfg(any(test, feature = "test-support"))]
pub mod fakes;

pub use coalescer::Coalescer;
pub use error::{AppError, AppResult, PortError};
pub use history_reader::HistoryReader;
pub use rate_limit::RateLimiter;
pub use views::{MessageStats, MessageView, UploadSlot};

//! The Voics notifier: reads new messages from the event bus, decides who gets a push with the
//! same rules the clients use, and sends it through APNs.
//!
//! Layers, dependencies pointing inward: `domain` (rules and payload text, no IO), `application`
//! (ports, the event handler and the bounded dispatcher), `infrastructure` (APNs, the api
//! service, Redpanda).

pub mod application;
pub mod domain;
pub mod infrastructure;

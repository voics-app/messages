//! Prometheus metrics: `http_requests_total` and `http_request_duration_seconds`, labelled by
//! method, route template and status class (docs/services/catalog.md). The recorder is owned by
//! the binary; this module records into whichever recorder is installed and renders its handle.

use std::time::Duration;

use discordy_messages_application::ports::MessageEventKind;
use metrics_exporter_prometheus::PrometheusHandle;

/// Histogram buckets (seconds) shared by request and database latency.
pub const LATENCY_BUCKETS: [f64; 12] = [0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 10.0];

/// `2xx`-style class for a status code.
pub fn status_class(status: u16) -> &'static str {
    match status {
        100..=199 => "1xx",
        200..=299 => "2xx",
        300..=399 => "3xx",
        400..=499 => "4xx",
        _ => "5xx",
    }
}

#[derive(Debug, Clone)]
pub struct Metrics {
    handle: PrometheusHandle,
}

impl Metrics {
    pub fn new(handle: PrometheusHandle) -> Self {
        Self { handle }
    }

    /// Records one finished request. `route` is the matched template, never the raw path.
    pub fn record(&self, method: &str, route: &str, status: u16, elapsed: Duration) {
        let status = status_class(status);
        metrics::counter!(
            "http_requests_total",
            "method" => method.to_owned(),
            "route" => route.to_owned(),
            "status" => status
        )
        .increment(1);
        metrics::histogram!(
            "http_request_duration_seconds",
            "method" => method.to_owned(),
            "route" => route.to_owned(),
            "status" => status
        )
        .record(elapsed.as_secs_f64());
    }

    pub fn render(&self) -> String {
        self.handle.render()
    }
}

/// Counts the message lifecycle events that were published: `messages_created_total`,
/// `messages_edited_total` and `messages_deleted_total` (a bulk delete adds one per message).
pub fn record_message_event(kind: &MessageEventKind) {
    match kind {
        MessageEventKind::Created { .. } => metrics::counter!("messages_created_total").increment(1),
        MessageEventKind::Updated { .. } => metrics::counter!("messages_edited_total").increment(1),
        MessageEventKind::Deleted { .. } => metrics::counter!("messages_deleted_total").increment(1),
        MessageEventKind::DeletedBulk { message_ids } => {
            metrics::counter!("messages_deleted_total").increment(message_ids.len() as u64);
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use discordy_messages_domain::Snowflake;
    use metrics_exporter_prometheus::PrometheusBuilder;

    #[test]
    fn counts_lifecycle_events() {
        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let _guard = metrics::set_default_local_recorder(&recorder);
        record_message_event(&MessageEventKind::Deleted {
            message_id: Snowflake::new(1),
        });
        record_message_event(&MessageEventKind::DeletedBulk {
            message_ids: vec![Snowflake::new(2), Snowflake::new(3)],
        });
        record_message_event(&MessageEventKind::Acknowledged {
            message_id: Snowflake::new(1),
            mention_count: 0,
            version: 1,
        });
        let text = handle.render();
        assert!(text.contains("messages_deleted_total 3"), "{text}");
        assert!(!text.contains("messages_created_total"), "{text}");
    }

    #[test]
    fn classifies_status() {
        assert_eq!(status_class(200), "2xx");
        assert_eq!(status_class(404), "4xx");
        assert_eq!(status_class(500), "5xx");
    }
}

//! Minimal Prometheus counters, rendered in the text exposition format.

use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Default)]
pub struct Metrics {
    informational: AtomicU64,
    success: AtomicU64,
    redirection: AtomicU64,
    client_error: AtomicU64,
    server_error: AtomicU64,
}

impl Metrics {
    pub fn record(&self, status: u16) {
        let counter = match status {
            100..=199 => &self.informational,
            200..=299 => &self.success,
            300..=399 => &self.redirection,
            400..=499 => &self.client_error,
            _ => &self.server_error,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    pub fn render(&self) -> String {
        let mut out = String::from(
            "# HELP messages_http_requests_total HTTP responses by status class.\n\
             # TYPE messages_http_requests_total counter\n",
        );
        for (class, counter) in [
            ("1xx", &self.informational),
            ("2xx", &self.success),
            ("3xx", &self.redirection),
            ("4xx", &self.client_error),
            ("5xx", &self.server_error),
        ] {
            let _ = writeln!(
                out,
                "messages_http_requests_total{{status=\"{class}\"}} {}",
                counter.load(Ordering::Relaxed)
            );
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_by_status_class() {
        let metrics = Metrics::default();
        metrics.record(200);
        metrics.record(204);
        metrics.record(404);
        let text = metrics.render();
        assert!(text.contains("messages_http_requests_total{status=\"2xx\"} 2"));
        assert!(text.contains("messages_http_requests_total{status=\"4xx\"} 1"));
        assert!(text.contains("messages_http_requests_total{status=\"5xx\"} 0"));
    }
}

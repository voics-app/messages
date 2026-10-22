//! Prometheus metrics: `http_requests_total` and `http_request_duration_seconds`, labelled by
//! method, route template and status class (docs/services/catalog.md). The recorder is owned by
//! the binary; this module records into whichever recorder is installed and renders its handle.

use std::time::Duration;

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_status() {
        assert_eq!(status_class(200), "2xx");
        assert_eq!(status_class(404), "4xx");
        assert_eq!(status_class(500), "5xx");
    }
}

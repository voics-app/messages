//! Metrics recorder and the sqlx query-latency layer.

use metrics_exporter_prometheus::{BuildError, PrometheusBuilder, PrometheusHandle};
use tracing_subscriber::Layer;

use discordy_messages_interface::metrics::LATENCY_BUCKETS;

/// Installs the global Prometheus recorder; the handle renders `/metrics`.
pub fn install_recorder() -> Result<PrometheusHandle, BuildError> {
    PrometheusBuilder::new()
        .set_buckets(&LATENCY_BUCKETS)?
        .install_recorder()
}

/// Records sqlx's per-statement `elapsed_secs` events (target `sqlx::query`) as
/// `db_query_duration_seconds{db="postgres"}`. Scylla is timed where statements run.
pub struct PostgresLatency;

impl<S: tracing::Subscriber> Layer<S> for PostgresLatency {
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: tracing_subscriber::layer::Context<'_, S>) {
        let mut visitor = QueryVisitor::default();
        event.record(&mut visitor);
        if let Some(seconds) = visitor.elapsed_secs {
            metrics::histogram!(
                "db_query_duration_seconds",
                "db" => "postgres",
                "operation" => sql_operation(visitor.summary.as_deref().unwrap_or(""))
            )
            .record(seconds);
        }
    }
}

#[derive(Default)]
struct QueryVisitor {
    elapsed_secs: Option<f64>,
    summary: Option<String>,
}

impl tracing::field::Visit for QueryVisitor {
    fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
        if field.name() == "elapsed_secs" {
            self.elapsed_secs = Some(value);
        }
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "summary" {
            self.summary = Some(value.to_owned());
        }
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "summary" {
            self.summary = Some(format!("{value:?}"));
        }
    }
}

/// Low-cardinality label: the leading SQL verb.
fn sql_operation(summary: &str) -> &'static str {
    let verb = summary
        .trim_start_matches(|c: char| !c.is_ascii_alphabetic())
        .split(|c: char| !c.is_ascii_alphabetic())
        .next()
        .unwrap_or("")
        .to_ascii_lowercase();
    match verb.as_str() {
        "select" | "with" => "select",
        "insert" => "insert",
        "update" => "update",
        "delete" => "delete",
        _ => "other",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tracing_subscriber::layer::SubscriberExt;

    #[test]
    fn operation_is_the_sql_verb() {
        assert_eq!(sql_operation("SELECT id FROM t"), "select");
        assert_eq!(sql_operation("\"insert into x\""), "insert");
        assert_eq!(sql_operation("DELETE FROM t"), "delete");
        assert_eq!(sql_operation("BEGIN"), "other");
    }

    #[test]
    fn layer_records_elapsed_into_histogram() {
        let recorder = PrometheusBuilder::new()
            .set_buckets(&LATENCY_BUCKETS)
            .unwrap()
            .build_recorder();
        let handle = recorder.handle();
        let _guard = metrics::set_default_local_recorder(&recorder);
        let subscriber = tracing_subscriber::registry().with(PostgresLatency);
        tracing::subscriber::with_default(subscriber, || {
            tracing::info!(target: "sqlx::query", summary = "SELECT 1", elapsed_secs = 0.004_f64);
        });
        let text = handle.render();
        assert!(
            text.contains("db_query_duration_seconds_count{db=\"postgres\",operation=\"select\"} 1"),
            "{text}"
        );
    }
}

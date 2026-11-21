//! Report acknowledged journal transitions, including recovery outside the dispatching process.
use discordy_messages_domain::scheduled::{ScheduledMessage, ScheduledStatus};

pub(super) fn committed(previous: (ScheduledStatus, u64), job: &ScheduledMessage, now: u64) {
    let (from, updated_at) = previous;
    if from == job.status {
        return;
    }
    metrics::counter!("scheduled_state_transitions_total", "from" => name(from), "to" => name(job.status)).increment(1);
    match job.status {
        ScheduledStatus::Sent => metrics::counter!("scheduled_sent_total").increment(1),
        ScheduledStatus::Failed => metrics::counter!("scheduled_failed_total").increment(1),
        ScheduledStatus::DeliveryUnknown => metrics::counter!("scheduled_delivery_unknown_total").increment(1),
        _ => {}
    }
    if from == ScheduledStatus::Dispatching {
        metrics::histogram!("scheduled_claim_to_outcome_seconds", "outcome" => name(job.status))
            .record(now.saturating_sub(updated_at) as f64 / 1000.0);
    }
}

fn name(status: ScheduledStatus) -> &'static str {
    match status {
        ScheduledStatus::Preparing => "preparing",
        ScheduledStatus::Pending => "pending",
        ScheduledStatus::Dispatching => "dispatching",
        ScheduledStatus::Sent => "sent",
        ScheduledStatus::Failed => "failed",
        ScheduledStatus::Cancelled => "cancelled",
        ScheduledStatus::DeliveryUnknown => "delivery_unknown",
    }
}

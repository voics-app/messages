# Notifier queue and burst check

The notifier admits at most `PUSH_QUEUE_CAPACITY` waiting jobs and `PUSH_MAX_IN_FLIGHT` active
jobs across APNs and FCM together. The pump acquires a permit before receiving a job; the former
receive-first order retained one additional waiting job outside the configured buffer. A blocked
provider regression reproduced that extra acceptance before the fix.

Each active job owns its permit through retries/backoff and token cleanup. In-flight accounting
uses a drop guard, so cancellation and unwinding decrement it too. The queue's depth and active
jobs have separate gauges; configured limits are exposed as `notifier_queue_capacity` and
`notifier_in_flight_limit`. Outcome counters are initialized for both providers so a live scrape
can distinguish zero drops from absent telemetry. Manual submissions update queue depth and use
the same queue; their bounded wait does not bypass admission limits.

## Reproduce the engineered burst

```sh
cargo test -p discordy-messages-notifier --lib application::dispatcher::tests::ten_times_capacity_burst_is_bounded_and_reports_drops -- --exact --nocapture
```

The sizing baseline is the shipped 10,000-job queue, not a measured production forecast. Hold all
100 send slots in a synthetic blocked provider, then submit a burst of 100,000 additional jobs,
alternating APNs and FCM. The test uses the real dispatcher and Prometheus recorder. Expected:

```text
burst=100000 queued=10000 dropped=90000 peak_in_flight=100 sent=10100 queue_after=0
```

Each provider exports 45,000 dropped pushes. Releasing the provider drains the queued work and
returns both depth and active gauges to zero. The first 100 active jobs explain why sent is
10,100 while queued is 10,000. Retained jobs are bounded by 10,100; this is not an RSS benchmark,
real provider throughput or device-delivery proof.

Other coverage:

```sh
cargo test -p discordy-messages-notifier --lib application::dispatcher::tests
cargo test -p discordy-messages-notifier --test pipeline
cargo test -p discordy-notifier --bins
```

These cover concurrency, exact full-buffer drops, retries/backoff, retry limits, invalid-token
removal, manual broadcast pacing, config rejection, and APNs/FCM HTTP protocol/privacy behavior.
Admin Platform health reads aggregate Prometheus telemetry through its existing authenticated
series endpoint. The drop chart estimates the preceding five minutes per point, preserving
counter-reset handling; missing samples are not zero drops.

This change does not make the notifier safe to replicate. Its consumer still starts at the end
of the topic without a durable consumer-group offset; retain singleton/Recreate semantics.
API/messages Snowflake allocation and notifier event-consumption ownership are separate concerns.

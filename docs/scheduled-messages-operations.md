# Scheduled messages configuration and recovery

The scheduling implementation is in progress under messages#84 / issue #61 and ADR0028.
Enable only after backend, compatible client surfaces and real two-account acceptance have
been reviewed. This document does not authorize a deployment.

| Variable | Default | Effect |
| --- | --- | --- |
| `SCHEDULED_MESSAGES_ENABLED` | `false` | Enables both author routes and the local worker. Disabled routes return 503; no schedule schema or worker starts. |
| `SCYLLA_SCHEDULED_MESSAGE_KEYSPACE` | `<SCYLLA_KEYSPACE>_scheduled` | Dedicated vnode keyspace on the same Scylla cluster; exact replication must match the main keyspace. Identifier is at most 48 ASCII letters/digits/underscores and cannot start with a digit. |

The scheduler requires the API account-eligibility change from api#150. All accepting nodes
must have the compatible implementation and feature flag. No new secret or server is needed.
Readiness checks the three schedule tables and worker task liveness; a stopped task is unhealthy.
Shutdown stops discovery, drops unclaimed buffered keys and waits up to 30 seconds for claimed
work. A killed or timed-out task can leave an uncertain claim; recovery never resends it.

Each process polls once per second, rotates four of 64 shards, reads at most 100 candidates
per shard and retains at most 400 unique waiting/running keys. Four dispatches may run at once.
Discovery has a five-second timeout; cursors safely revisit partially processed pages. A full
buffer does not advance past an unretained candidate. These bounds do not bound the persisted
dataset or promise a full-pass deadline under arbitrary load. Dispatching/preparing recovery
is eligible after two minutes and requires discovery to reach the row. Pre-claim dependency
backoff is 1–60 seconds and expires at 24 hours overdue. No post-claim insertion is retried.

With healthy, fast dependencies and at most one due page per shard, a shard is visited every
16 nominal one-second ticks. A job becoming due immediately after its visit can therefore
wait almost 16 seconds before dispatch preparation, plus database/API time. A full hot-shard
page needs another rotation for the next page; a candidate inserted behind a persisted cursor
needs a completed wrap. Buffer saturation, slow discovery and outages add delay. There is no
five-second dispatch promise: the ADR's five seconds bounds the post-claim authorization lookup.
Neither the polling cadence nor the recovery eligibility threshold is a wall-clock guarantee.

Accepted pending records have durable due entries. A crash between reservation and the first
index write has not acknowledged acceptance and can leave an unindexed preparation. It is
reconciled by the next author create, HTTP inspection or cancel operation, reading at most 100 active rows,
even after send permission is lost. Without a later author operation that unaccepted row can
remain stored; the worker does not enumerate every author partition. It cannot dispatch or
consume an additional accepted slot after an operation has reconciled its expired reservation.

Application `get` and `list` are read-only queries. The authenticated HTTP inspection handlers
explicitly run `reconcile_author` before querying; invalid paths/cursors/limits are rejected
first. Another author's inspection cannot reconcile or reveal the original author's work.

Metrics have bounded labels and contain no author/channel/schedule/message IDs:

- `scheduled_in_flight` and `scheduled_buffered` describe this process's bounded work.
- `scheduled_scanned_candidates_total`, `scheduled_recovered_total`,
  `scheduled_observed_state_total{status}` count scan observations, not a global census.
- `scheduled_observed_oldest_due_seconds` retains the last observed due age for each shard;
  `scheduled_shard_pass_seconds` measures each completed cursor wrap. Observe both, since a
  stale observation during a dependency outage does not prove that due age stopped growing.
- `scheduled_observed_oldest_claim_seconds{status}` observes preparing/dispatching age before
  recovery. `scheduled_last_successful_scan_unixtime_seconds` exposes discovery freshness.
- `scheduled_scan_errors_total`, `scheduled_dispatch_errors_total`,
  `scheduled_quota_refusals_total` expose discovery, execution and admission failures.
- `scheduled_state_transitions_total{from,to}`, `scheduled_sent_total`, `scheduled_failed_total`
  and `scheduled_delivery_unknown_total` count acknowledged durable journal transitions,
  including unknown recovery by another worker or an author operation. Idempotent completion
  replays do not count again. `scheduled_claim_to_outcome_seconds{outcome}` measures the
  interval from dispatch claim to its first acknowledged outcome, including recovered unknown.
  A lost successful mutation result can omit a metric, so combine these with scan observations;
  metrics are not an exactly-once audit log.

The ADR's alert thresholds are an observed due age above 60 seconds for five minutes,
preparing/dispatching age beyond two minutes plus a completed scan pass, or any unknown outcome.
Watch unknown transition increases together with unknown scan observations, and investigate
scan freshness older than 30 seconds alongside scan errors/readiness. An outage can stop
observations completely. Current observation counters must not be presented as a global
census or delivery SLO. Deployment-specific alert rules belong to the reviewed rollout.

Back up the schedule journal, due indexes and cursors together with message and read-state
keyspaces. Restore with execution **disabled** until all nonterminal schedules are reconciled
against possible later writes. An old backup's `pending` state can predate a successful send;
enabling it blindly could send twice. Never repair uncertainty by changing it to pending or
allocating a new request key. Preserve evidence and certify only a proven original attempt,
or leave the outcome uncertain for its author to inspect. Paired backups and this restore
fence are required in addition to the existing read-state old-writer drain procedure.

Reproduce the HTTP/schema acceptance locally with installed canonical contracts dependencies:

```sh
SCHEDULED_HTTP_CAPTURE=/tmp/scheduled-http.json cargo test -p discordy-messages-interface --test scheduled_http
node scripts/check-scheduled-http-schema.cjs ../contracts /tmp/scheduled-http.json
python scripts/check-scheduled-contracts.py --canonical ../contracts
```

The capture contains synthetic test data only. Generated wire files are imported by the
canonical Typify executable using `scripts/check-scheduled-contracts.py --import-revision ...
--canonical ... --generator ...`; the recorded full commit and LF digests are checked in CI.
Conditional rules remain in the distributed schema. DTOs cannot enforce every JSON Schema
constraint, so the HTTP boundary also validates strict UUIDv4, revision range and null edits.

Local coupled evidence uses the production `Scheduler` start/timer/shutdown code, TCP HTTP
router, real Scylla journal/index/message repositories and two worker instances. Authentication,
channel access, user directory, reactions, read-state/accounting, search, media and event sink
are fixture adapters. The test drops the author's HTTP client, rejects early delivery, observes
one sent message through the second principal's history, excludes a cancelled intent, and
checks readiness after shutdown. It does not prove production API/JWT/gateway/push/client UI
integration. One local run observed 14.520 seconds from virtual due to sent; this matches the
rotation cadence and is not a production SLO.

The separate real-Scylla load test accepts 225 jobs for nine authors (25 per channel each),
all in one shard, and paces scans at one second. A local run accepted them in 5.023 seconds and
finished the hot-shard pass in 32.453 seconds, reading pages of 100/100/25 with at most four
in flight and 96 waiting; all 225 writes/events completed and all active quota was released.
Those are measurements of the named fixture on one machine, not throughput guarantees.

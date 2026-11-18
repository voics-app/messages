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

Metrics have bounded labels and contain no author/channel/schedule/message IDs:

- `scheduled_in_flight` and `scheduled_buffered` describe this process's bounded work.
- `scheduled_scanned_candidates_total`, `scheduled_recovered_total`,
  `scheduled_observed_state_total{status}` count scan observations, not a global census.
- `scheduled_observed_oldest_due_seconds` retains the last observed due age for each shard;
  `scheduled_shard_pass_seconds` measures each completed cursor wrap. Observe both, since a
  stale observation during a dependency outage does not prove that due age stopped growing.
- `scheduled_scan_errors_total`, `scheduled_dispatch_errors_total`,
  `scheduled_quota_refusals_total`, `scheduled_sent_total`, `scheduled_failed_total` and
  `scheduled_delivery_unknown_total` expose the corresponding process outcomes. The outcome
  counters currently describe completed dispatcher results; recovery is counted separately.

The ADR's alert thresholds are an observed due age above 60 seconds for five minutes,
preparing/dispatching age beyond two minutes plus a completed scan pass, or any unknown outcome.
Final claim-age/outcome instrumentation and alert integration remain part of acceptance; the
current observation counters alone must not be presented as a global count or delivery SLO.

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

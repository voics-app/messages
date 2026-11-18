# Replica-safe message IDs

Messages acquires a committed timestamp interval from authenticated
`POST /internal/v1/id-reservations` before it starts. The API must have migration 0060 and the
reservation endpoint. `SNOWFLAKE_WORKER_ID` is a preference in 64–127; allocation chooses a free
worker in 65–127. Worker 64 remains permanently reserved for legacy binaries. Replicas can use
the same configuration; at most 63 live owners can hold message ranges. Startup fails when
allocation is unavailable. There is no static fallback.

Each startup uses a fresh random UUID owner. The API commits worker timestamp ranges under a
PostgreSQL row lock; no different owners share a worker timestamp. The shared `voics-snowflake`
crate checks wall time, a conservative request-start monotonic deadline, clock rollback and
the 4,096-ID/millisecond limit. Calls return errors rather than panic, borrow future time or
reset sequence state. Storage mutations propagate allocation failures as 503.

Renewal runs every five seconds using the same owner and worker, with a five-second HTTP
timeout. The maximum remaining committed window is 30,000 ms; request latency consumes this
window. Failed or replayed renewals do not extend it. On expiry, generation and `/readyz` fail
closed. After resolving the API/database/clock issue, restart with a fresh owner. Automatic
reacquisition is deliberately absent. Logs show worker numbers and redacted renewal failures.

Before overlapping legacy and new binaries, prove every old messages process effectively uses
worker 64 and every old API uses 0. ConfigMap defaults alone are insufficient. Otherwise stop
and fence old issuers before rollout. Start the upgraded API before messages. PostgreSQL restore
requires stopping/fencing all issuers and preserving the live maximum reserved timestamp outside
the backup, then applying API's `scripts/restore-id-barrier.sql` and waiting for verified UTC to
reach its persisted floor. A fixed sleep alone cannot account for clock rollback or lost history.
Follow API's `docs/id-reservations.md`; do not delete reservation rows or run cloned allocators.

`cargo test -p discordy-messages-infrastructure --lib system::tests` exercises two independent
runtime issuers with the same preference through a real authenticated HTTP transport. Shared
crate and API integration tests cover timestamp boundaries, actual PostgreSQL concurrency,
reassignment, restore fencing and unavailable-ID writes.

CI verifies the imported shared crate with `python3 scripts/check-snowflake.py`. To compare its
recorded source digests and exact commit with the canonical contracts checkout, run
`python scripts/check-snowflake.py --canonical ../contracts`. Updates use the same command with
`--import-revision FULL_SHA` in both services. Never edit the imported source directly.

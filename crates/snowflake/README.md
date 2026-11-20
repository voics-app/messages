# Committed Snowflake intervals

`voics-snowflake` is the shared, fallible issuer used by API and messages. It preserves the
2026-01-01 epoch, 10 worker bits and 12 sequence bits. The signed PostgreSQL bigint ceiling
limits the timestamp to 41 bits. It does not allocate workers or open database connections.

Services import a revision-pinned snapshot into `crates/snowflake` so ordinary CI and container
builds require no cross-repository credentials. Their `scripts/check-snowflake.py` checks the
source, manifest, README and formatter configuration against `UPSTREAM.json` SHA-256 digests;
`--canonical PATH` also verifies those bytes against the exact canonical Git commit. Update
both services together with `--canonical PATH --import-revision FULL_SHA`, then run all tests.
Never edit the imported crate directly.

API reserves workers 1–63 and messages 65–127. Workers 0 and 64 remain permanently excluded
for legacy binaries. A configured worker is a preference, so replicas can use the same config.
At most 63 live owners per service can hold ranges. A fresh process creates a fresh UUID owner
and constructs exactly one `LeasedIds` after a successful allocation commit. Never persist,
restore, share, or duplicate a process owner's issuer state.

The store must serialize allocation by worker and retain each exclusive `end_ms` forever.
A new owner's `start_ms` must be at least the previous owner's end. Renewal retains owner,
worker and start, requires a still-live interval, and may never lower the end or issued time.
The request/response wire format is in `schemas/internal/id-reservation*.json`; the authenticated
HTTP endpoint accepts messages only. API uses the same DTOs through its database adapter.

`next_id` checks both wall time inside the committed interval and monotonic elapsed time from
the **start of the allocation/renewal request**, bounded by `end_ms - issued_at_ms`. Network
delay consumes this bound. Maximum remaining time is 30,000 ms. A clock rollback, exhausted
4,096-ID millisecond, expired interval, poisoned state, or invalid renewal returns an error.
There is no logical-time borrowing, sequence reset on renewal, or static-worker fallback.
Repeated or reordered renewal payloads cannot restart the deadline. Only a committed fresh
response advances it; unsuccessful renewals leave it unchanged.

## Rollout and restore requirements

Excluding 0/64 permits overlap with legacy binaries **only after effective runtime worker IDs
of every old issuer have been verified**. ConfigMap values alone are insufficient. Otherwise
stop and fence all old API/messages issuers before the rollout. Gateway/voice UUID generators
are outside this allocator. Start the new API before messages, and check readiness and distinct
allocated workers before scaling. Never allow an old binary on an allocatable worker.

Safety requires one authoritative database history. Never run two restored/cloned databases
as independent allocators for the same ID space. Never delete, truncate or reset reservation
rows while any issuer can run. Before restoring PostgreSQL, stop/fence all issuers and capture
the live maximum committed `end_ms` in a durable location outside that backup. After restore,
API's `scripts/restore-id-barrier.sql` raises a persistent global floor and every worker end to
at least the captured maximum, restored maximum, prior guard, and verified database UTC plus
the maximum reservation window. It invalidates owners and prevents allocation until that floor.

A fixed sleep by itself is not a restore guarantee: prior wall-clock advances or a refreshed
backup can retain higher ends. If the high-water capture is unavailable, derive a conservative
upper bound from trustworthy time and the maximum committed future window before allowing a
restart. If clock rollback makes that bound uncertain, keep issuance stopped until the actual
high-water mark is recovered. The API runbook supplies commands and the tested restore script.

Runtime adapters renew every five seconds. On expiry, issuance and readiness fail closed;
they do not reacquire automatically. Restart after the database/clock problem is resolved to
obtain a fresh owner and range. This behavior is deliberate and must inform deployment probes.

## Checks

`cargo test -p voics-snowflake` exercises interval bounds, signed range, legacy exclusions,
4,096 IDs per millisecond, rollback, replayed renewals, delayed responses and old-process resume.
The API tests use real PostgreSQL for concurrency, capacity, reassignment and the restore script;
messages tests exercise the authenticated HTTP transport and independent replica issuers.

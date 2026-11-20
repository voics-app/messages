# Mention accounting and read state

Versioned snapshot follow-up (contracts#48, docs ADR0027): migration0009 adds only the nullable
companion `read_state_version` column. Old rows read at version0; successful metadata/ledger
mutations increment atomically, and duplicate accounting/stale ACK does not increment.
Fence all writers that leave this column unchanged before enabling the versioned writer.
MESSAGE_ACK and READ_STATE_UPDATE contain full versioned snapshots; the internal array
endpoint certifies support through `X-Read-State-Versioned: 1`. Deployment needs compatible
gateway hydration during identify/resume and client version comparison. Publication remains
best effort; a subsequent snapshot repairs a missed event, not an accounting write that failed.
After any database restore, invalidate gateway sessions/resume buffers before reopening so
fresh READY can establish the restored version baseline. Include the version column in paired
backups; never reset it while certified clients remain connected. Unversioned binary rollback
requires fencing those sessions as well as the existing data-conversion/restore procedure.

The message writer obtains current permitted recipients from API's
`GET /internal/v1/channels/{id}/notification-recipients` (contracts#44/API#146), independently
of push tokens. Direct-only guild mentions use one-candidate exact-ID seeks. Roles, broadcasts
and DMs follow every advancing page, including empty permission-filtered pages, with at most
16 concurrent recipient writes. A plain guild post does not scan its audience.

Mutes and level 2 disable counts. Direct users, matching unsuppressed roles and allowed
broadcasts count once per message; `@here` needs observed online state. Level 0 still does not
make a plain guild post a mention. DMs count as mentions. System, ephemeral, silent and
author-side deliveries do not count. Suppressing an implicit reply ping preserves explicit
mentions. Policy is current per page; it is not a historical or cross-page snapshot and does
not rewrite old badges after preference changes. Existing author-side mention eligibility is
unchanged.

## Storage and migration

Scylla 2025.1 tablet tables cannot run the required LWT. Migration `0008_read_accounting.cql`
is applied separately by the read-state adapter in a distinct vnode keyspace on the **same
cluster**. `SCYLLA_READ_STATE_KEYSPACE` defaults to `<SCYLLA_KEYSPACE>_reads`, must be distinct,
and is validated before CQL interpolation. Bootstrap inherits the full main replication map,
rejects mismatches, checks LWT and preserves the original keyspace and tables. It never switches
the shared session's current keyspace. `/readyz` probes both read-state tables at quorum.

Metadata, delivery ledger and generation-scoped contributions share one user/channel
partition. Conditional batches use `SERIAL` and `QUORUM`; metadata revision checks serialize
changes. Delivery IDs are deduplicated, normal ACK retains newer contributions, and stale ACK
cannot move the marker backwards or clear a new count. Manual unread replaces marker/count
and the active generation while retaining deduplication and a monotonic acknowledged watermark.
ACK and manual cleanup are part of the same conditional transaction. Conflicts retry at most
64 times, then return an error; there is no unconditional fallback.

Each repository also queues operations for the same user/channel locally, using a cache capped
at 4096 partition gates with 60-second idle expiry. This avoids starving older requests with
the process's own new writes. The gate is an optimization: independent instances, eviction and
all mutations still use Scylla revision/ledger checks. Conflicts use jittered exponential delays
capped at 128 ms; the 64-attempt bound remains.

The old `read_states` table remains the per-user channel index. Initialization writes discovery
first, then inserts metadata once with the legacy aggregate as an untracked baseline. Legacy
counts have no historical message IDs or preferences and cannot be reconstructed exactly.
An advancing normal ACK clears that baseline, preserving newer tracked contributions. READY
uses authoritative companion metadata for initialized rows, with at most 16 concurrent reads.
Legacy count columns remain intact for migration evidence; they are not current rollback data.

## Release and recovery requirements

Land contracts#44 and API#146 before this writer. Fence requests, stop/drain **all** old
unconditional writers, and wait for in-flight CreateMessage/ACK before the new writer starts.
Prevent old replicas from restarting. Mixed old/new writers are unsupported. Capture a coherent
backup while fenced and prove recovery in isolation first.

Back up and restore **both keyspaces together**, including schema, tables, migration markers,
replication settings and the configured-name manifest. Do not change to an empty companion,
restore one keyspace alone or drop/recreate existing data to change tablet mode. Replication
changes and repair must cover both keyspaces. Root owns rollout and recovery; no additional
server is required.

After any new writes, a binary-only rollback exposes stale legacy read states. Prefer a
compatible forward fix. Otherwise remain fenced and restore the complete pre-change recovery
set with explicit acceptance of intervening data loss, or build and verify an intentional
companion-to-legacy conversion. No automatic conversion tool is provided. Preserve evidence.

See the central [rollout and recovery runbook](https://github.com/discordy-voics/docs/blob/develop/runbooks/read-accounting.md)
and [ADR 0026](https://github.com/discordy-voics/docs/blob/develop/adr/0026-atomic-read-accounting.md).

## Operational limits and proof

Message storage and accounting remain separate best-effort steps. A process crash, policy-page
failure or database outage can leave missing counts; failure logs carry message/channel IDs.
A page can fail after earlier pages were counted. There is no durable retry/outbox or
exactly-once end-to-end guarantee. A retry of an already recorded message does not add a second
count. Unknown LWT outcomes are safe to retry using the same message ID.

Database state and event publication are also not one transaction. In-flight MESSAGE_ACK
snapshots and MESSAGE_CREATE delivery need client ordering/reconciliation QA; a client that
sets a snapshot and later increments an already-included create can temporarily diverge.
The unchanged wire payload has no accounting revision or snapshot-delivery watermark. This
change proves authoritative stored/READY state, not exactly-once live UI state.

Each unread unique message contributes one ledger row and one active-generation row. Manual
unread retains necessary ledger rows while removing obsolete contributions; ACK deletes
acknowledged ranges atomically. No TTL silently re-enables duplicates. ACK count scans and
long-unread partitions remain scaling limits. Observe `read_state_cas_conflicts_total`,
`read_state_cas_exhausted_total`, failed-accounting logs and latency/partition growth.

Regression tests exercise CreateMessage/ACK and current policy. `notification_recipients` tests
use real HTTP, including malformed/private pages. `read_state_accounting` uses actual isolated
Scylla 2025.1 containers and independent repository instances for concurrency, replay, ACK,
manual unread, legacy initialization, bootstrap rejection and companion readiness. No live
device, production throughput or crash-recovery guarantee is inferred from those tests.

The hot-partition regression submits 1024 unique messages twice each across four independent
repositories with 16 operations in flight, using one disposable Scylla 2025.1 shard (750 MiB).
The initial implementation exhausted 64 retries, including with jitter alone. Local admission
plus jitter completed a measured run in 19.477 s with 1462 CAS conflicts and zero exhaustion;
ACK of the first 512 took 9 ms. Live rows went 2049 (metadata + ledger/contribution pairs),
1025 (half ACK), 513 (manual unread retains deduplication) and 1 (full ACK). These are local
observations, not a production latency or capacity promise.

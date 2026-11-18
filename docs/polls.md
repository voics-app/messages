# Persistent polls

The protocol and rollout requirements are defined in docs ADR0029 and
`specs/2026-11-18-polls.md`; the canonical schemas are contracts PR54. This draft
connects creation, immutable message storage, voting, author and automatic expiry,
deletion fences and HTTP/event serialization. Creation defaults to disabled;
consumer rollout and complete client/gateway acceptance remain separate gates.

Polls require Standard delivery. Creation refreshes VIEW_CHANNEL, the applicable
SEND_MESSAGES permission and SEND_POLLS; the normal attachment and TTS checks
still apply. Reads, votes and author expiry refresh VIEW_CHANNEL and
READ_MESSAGE_HISTORY. Public bots use the same creation checks and cannot vote.
Internal messages, webhooks, interactions, system messages and forwards cannot
create polls. Unicode answer emoji are supported; custom emoji are rejected.

`GET /channels/{channel}/polls/{message}` returns complete counts with the
caller's `vote_version` and explicit `me_voted` facts. It never closes overdue
polls or repairs storage. PUT/DELETE on `answers/{answer}/@me` require the
expected voter revision. An already-satisfied retry is a no-op; a stale request
that would change the choice returns 409. Single-choice replacement is atomic.
Only the author can POST `expire`; ordinary moderation uses message deletion.

Counts, final/deleted status and voter revisions share a serially read partition
in a companion vnode keyspace on the same Scylla cluster. The main message stores
only the immutable definition. Initialization reserves metadata for 24 hours,
writes a durable 64-shard expiry index, then removes the metadata TTL before the
main message may be accepted. If that index write fails, the unaccepted
reservation expires. A post-activation create failure can leave a content-free
orphan; finalization does not recreate its message.

Vote events carry complete versioned results and the actor's complete selection,
not count deltas. They omit the recipient's vote facts. A single-choice change
emits REMOVE and ADD with the same final snapshot. Finalization emits a message
update. Publication is best effort after persistence; foreground/reconnect GET
must reconcile visible polls. Votes never create unread counts or push alerts.

Deletion installs a durable content-free fence before removing participant rows,
the due index and the main message. Backups/restores and old-writer drain must
cover both keyspaces; do not change the primary keyspace's replication/tablet
configuration. Keep the companion keyspace when disabling future creation.

## Runtime and recovery

`POLLS_ENABLED=false` gates only new poll creation. A compatible binary always
opens the companion repository, serves existing snapshots/votes and runs expiry.
`SCYLLA_POLL_KEYSPACE` defaults to `<SCYLLA_KEYSPACE>_polls`; it must be distinct
from the main keyspace, use vnode tables and match the main replication map.
Startup validates these requirements. Readiness requires the worker to be alive
and bounded read-only queries against both companion tables to succeed. Disabling
creation does not hide a broken existing-poll store from readiness.

The worker scans four of 64 shards each second, with at most 100 entries per page,
four active finalizations and 400 total retained tasks per replica. It refills a
free completion slot immediately. A full queue never advances past an entry it
could not retain. Cursors reset at shard end and restart at the head after process
restart; failed entries remain in the durable index. An idle rotation takes a
nominal 16 ticks, plus IO and backlog. A continuously growing backlog can extend
a complete shard pass; the limits are not a production latency guarantee.

Discovery has a five-second timeout, each finalization a ten-second timeout, and
shutdown stops discovery/discards buffered tasks before draining active work for
at most fifteen seconds. Storage may commit before a timeout or lost publication;
the authoritative GET resolves the result. A vanished/private channel or an
unaccepted content-free reservation may finalize without a message broadcast.
Deleted fences allow a later scan to retry participant/index cleanup.

Monitor `poll_cas_conflicts_total`, `poll_cas_exhausted_total`,
`poll_expiry_in_flight`, `poll_expiry_buffered`, `poll_finalized_total`,
`poll_expiry_errors_total`, `poll_scan_errors_total`,
`poll_scanned_candidates_total`, `poll_shard_pass_seconds`,
`poll_last_successful_scan_unixtime_seconds` and
`poll_observed_oldest_due_seconds`. The age gauge describes bounded pages recently
observed across the shards, not a whole-store count. Investigate a stale scan
timestamp, growing shard-pass duration/backlog or repeated CAS exhaustion before
increasing replicas; every replica still serializes a hot poll through Scylla.

## Verification

`cargo test -p discordy-messages-infrastructure --test poll_persistence` starts
one disposable Scylla fixture. It covers concurrent voters across independent
repositories, stale requests, close/delete races, bounded index paging and
reservation TTL failure. The application/HTTP suites cover policy and lifecycle.
`cargo test -p discordy-messages --test poll_runtime -- --nocapture` uses one
disposable Scylla fixture, actual TCP routes, production main/companion adapters
and two production expiry workers. Auth/access/user-directory and final event
transport are explicitly fixture adapters. It verifies last-shard rotation,
read-only overdue GET, restart with creation disabled, a lost final publication,
persisted final results, deletion and missing-table readiness. It does not prove
real API/gateway/client integration.

The generated poll types are imported using `scripts/check-poll-contracts.py`;
its provenance file records the canonical commit and schema/type hashes. Capture
actual HTTP responses and production event encoding with
`POLL_HTTP_CAPTURE=/tmp/polls.json cargo test -p discordy-messages-interface --test http_api`,
then run `node scripts/check-poll-http-schema.cjs <contracts checkout> /tmp/polls.json`.
The latter validates all eleven captured responses/events using canonical AJV,
and rejects a required-field counterexample for every capture. Authentication,
API directory, storage and final event transport in this HTTP suite use explicit
in-memory fixtures; it does not prove a coupled deployment or client behavior.

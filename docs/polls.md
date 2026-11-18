# Persistent polls

The protocol and rollout requirements are defined in docs ADR0029 and
`specs/2026-11-18-polls.md`; the canonical schemas are contracts PR54. This draft
currently connects creation, immutable message storage, voting, author expiry,
deletion fences and HTTP/event serialization. Composition-root enablement and
the automatic expiry worker are still being completed. No deployment is enabled
by this checkpoint.

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

## Verification

`cargo test -p discordy-messages-infrastructure --test poll_persistence` starts
one disposable Scylla fixture. It covers concurrent voters across independent
repositories, stale requests, close/delete races, bounded index paging and
reservation TTL failure. The application/HTTP suites cover policy and lifecycle.

The generated poll types are imported using `scripts/check-poll-contracts.py`;
its provenance file records the canonical commit and schema/type hashes. Capture
actual HTTP responses and production event encoding with
`POLL_HTTP_CAPTURE=/tmp/polls.json cargo test -p discordy-messages-interface --test http_api`,
then run `node scripts/check-poll-http-schema.cjs <contracts checkout> /tmp/polls.json`.
The latter validates all eleven captured responses/events using canonical AJV,
and rejects a required-field counterexample for every capture. Authentication,
API directory, storage and final event transport in this HTTP suite use explicit
in-memory fixtures; it does not prove a coupled deployment or client behavior.

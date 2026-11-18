# messages

The Voics messages service (Rust, axum, ScyllaDB). It stores message history in 10-day
ScyllaDB buckets, and serves reactions, pins, read states and attachment uploads. It publishes
every change to `discordy.message.v1` on Redpanda. Only `standard` delivery-mode channels are
accepted; other modes get `409` with code `50160`.

## Endpoints (port 8082)

Public, under `/api/v1` (Bearer JWT or Bot token):
`GET|POST /channels/{id}/messages`, `PATCH|DELETE /channels/{id}/messages/{mid}`,
`POST /channels/{id}/messages/bulk-delete`, `POST /channels/{id}/messages/{mid}/ack`,
`DELETE /channels/{id}/messages/{mid}/reactions`,
`PUT|DELETE /channels/{id}/messages/{mid}/reactions/{emoji}/@me`,
`GET /channels/{id}/pins`, `PUT|DELETE /channels/{id}/pins/{mid}` (pinning also posts the type 6 system message),
`POST /channels/{id}/attachments`.

Creation stores `tts` (default false); `tts: true` requires `SEND_TTS_MESSAGES` alongside
the usual send permissions. History and MESSAGE_CREATE/MESSAGE_UPDATE carry the saved value.
Internal callers enforce that permission before relaying; system messages always have TTS off.
Migration `0007_message_tts` adds the nullable column; older rows read as false.

Authors can PATCH `attachments: [{id, description?, flags?}]` to change existing files' alt
text or spoiler state. Missing files/fields remain unchanged; `description: null` clears alt
text. Only IS_SPOILER (8) is changed by `flags`; explicit zero clears it, including on files
named `SPOILER_*`. Object keys and signed URLs remain intact. Duplicate/unknown IDs, more than
10 edits, alt text over 1024 characters, and attempts to change immutable file fields are
rejected atomically. Attachment metadata changes emit MESSAGE_UPDATE and set the edit timestamp.

Internal, under `/internal/v1` (header `X-Internal-Token`):
`POST /channels/{id}/messages` (bot, webhook and interaction messages; body `author_id`, `content`,
`embeds`, `components`; skips the user permission check, still refuses non-standard channels with 409
code 50160; `type` posts a system message: 1-2 and 4-5 group DM events, 3 call with `call`
`{participants, ended_timestamp}`, 6 pin, 7 member join, 18 thread created with `thread_id`),
`PATCH /channels/{id}/messages/{mid}/call` (records `ended_timestamp` and final `participants` of a
call message), `GET /channels/{id}/messages/{mid}` (one stored message, used by api to snapshot a reported message),
`GET /channels/{id}/last-message-id`, `GET /users/{id}/read-states`, `GET /admin/stats`.

Operational: `/healthz`, `/readyz` (ScyllaDB and Redpanda), `/metrics`. Metrics: `http_requests_total` and `http_request_duration_seconds` (method, route, status class), `db_query_duration_seconds` (db, operation), `messages_created_total`, `messages_edited_total`, `messages_deleted_total`, `kafka_produce_duration_seconds` and `kafka_produce_errors_total` (topic).

## Notifier (push notifications)

`bin/notifier` (crate `crates/notifier`, image `messages-notifier` from `Dockerfile.notifier`, port
8085) reads `discordy.message.v1` from the end of the topic. For each `MESSAGE_CREATE`, the api's
`GET /internal/v1/channels/{id}/push-targets` resolves viewers who hold a push token and are not
the author. Notification rules apply mute, level, direct/role/everyone mentions, `@here`, `@silent`
and each device's only-mentions preference. APNs uses token auth; FCM uses HTTP v1 with cached
OAuth credentials. A provider without credentials counts `unsupported` while the other still
works. A live gateway socket does not suppress push because it can belong to a desktop or a
backgrounded phone; the client handles foreground duplicates.

- **Privacy.** Only a channel whose delivery mode is exactly `standard` may put text (and the author, channel and server names) into a push; anything else, including an unknown mode, gets the title `New message` and no body. A device that turned previews off gets `New message` as the body in standard chats.
- **Back-pressure (ADR 0016).** The pump acquires a send slot before removing a queued job, so
  `PUSH_QUEUE_CAPACITY` is the exact waiting-job bound. `PUSH_MAX_IN_FLIGHT` bounds active jobs
  across providers together, including retry waits; it also bounds each provider connection.
  A full queue drops new message pushes and counts `notifier_pushes_total{outcome="dropped"}`.
  Errors retry with exponential backoff up to `PUSH_MAX_ATTEMPTS`; invalid tokens are removed
  through `DELETE /internal/v1/push-tokens`. Queue/active gauges and configured limits are exposed
  for the admin Platform health panels. See [the burst check](docs/notifier-backpressure.md).
- **Sandbox and production.** Each token carries the environment it was issued for (development builds register `sandbox`); the sender uses `api.sandbox.push.apple.com` or `api.push.apple.com` per token. `BadDeviceToken` is what APNs answers when a token reaches the wrong host (an ad-hoc build can carry either kind), so the other host gets one try before the token is removed, and the host that worked is remembered. `APNS_ENVIRONMENT` forces one host for every token and disables the fallback.
- **Manual push.** `POST /internal/v1/manual-push` (same port, header `X-Internal-Token`, cluster only: no ingress route, a NetworkPolicy admits only the admin pods) sends a notification the owner wrote in the admin portal: `{"target": {"user_id": "123"} | "all", "title" (1-64 chars), "body" (1-240 chars), "dry_run"?: bool, "actor"?: "admin email"}`. Devices come from the api (`GET /internal/v1/users/{id}/push-tokens`, or `GET /internal/v1/push-tokens` in pages) and go through the same bounded queue and `PUSH_MAX_IN_FLIGHT` cap as message pushes; a broadcast waits up to 30 seconds per device for queue room, then drops and counts. Answer: `{devices, queued, dropped, truncated}` (`dry_run` only counts). Only one broadcast to everyone runs at a time (409), and one request covers at most 50000 devices (`truncated`). The push is an alert with data `type=manual` and no channel. Each request is logged (`manual push`, with actor, target, title, counts; never the body) and each queued device counts `notifier_pushes_total{outcome="manual"}`.
- **One replica.** There is no stored offset and no consumer group: the notifier starts at the end of the topic and ignores events older than `PUSH_MAX_EVENT_AGE_SECS`, so run exactly one replica (the gitops deployment uses `Recreate`).
- **VoIP (calls).** The call system message that the gateway posts when a DM call starts (`MESSAGE_CREATE`, type 3, `call.ended_timestamp` null, author = the caller) rings the phone: the notifier asks the api for the targets of the channel and sends `OutboundPush::Voip` to every PushKit token (`voip_tokens` of each target) of the members who are not the caller, not already in the call (`call.participants`) and have not muted the conversation. Online status and the message level are ignored on purpose (a backgrounded phone keeps a live gateway session for a while, and a call that does not ring is worse than one that rings twice). A call is never an alert, and a call event older than 20 seconds, an ended call, an ephemeral message and any call in a server are not rung. The payload is `{type: "call", channel_id, call_message_id, caller_id, private, ring_seconds, caller_name?}` and never carries message text; `caller_name` is present only when the delivery mode is exactly `standard`, and `private: true` tells the app to show an unnamed call. It goes to the `<topic>.voip` topic with `apns-push-type: voip` and `apns-expiration: 0` (APNs tries once, a ring is never delivered late), and a ring is retried for 10 seconds at most. Calls rung later with "Add to call" (gateway op 26) produce no new event and so reach open apps only. A rejected PushKit token is removed through the same `DELETE /internal/v1/push-tokens` call.

| Variable | Required | Default | Purpose |
|---|---|---|---|
| `KAFKA_BROKERS` | yes | | Redpanda bootstrap addresses |
| `API_INTERNAL_URL` | yes | | api base URL |
| `INTERNAL_SERVICE_TOKEN` | yes | | `X-Internal-Token` for the api |
| `APNS_KEY_FILE` | yes (or `APNS_KEY`) | | Path of the mounted `.p8` key (PKCS#8 PEM); `APNS_KEY` holds the PEM text for local runs |
| `APNS_KEY_ID` | yes | | Key ID of the key |
| `APNS_TEAM_ID` | yes | | Apple Team ID |
| `APNS_TOPIC` | no | `nl.voics.app` | iOS bundle identifier |
| `APNS_ENVIRONMENT` | no | per token | `production` or `sandbox` |
| `BIND_ADDR` | no | `0.0.0.0:8085` | probes and metrics |
| `LOG_LEVEL` | no | `info` | tracing filter |
| `PUSH_QUEUE_CAPACITY` | no | `10000` | Bounded buffer; a full buffer drops pushes |
| `PUSH_MAX_IN_FLIGHT` | no | `100` | Concurrent provider requests |
| `PUSH_MAX_ATTEMPTS` | no | `3` | Sends per push, retries included |
| `PUSH_BASE_BACKOFF_MS` / `PUSH_MAX_BACKOFF_MS` | no | `500` / `10000` | Retry backoff, doubled per attempt |
| `PUSH_MAX_EVENT_AGE_SECS` | no | `120` | Older events are not pushed |
| `PUSH_DEDUPE_CAPACITY` | no | `10000` | Recent event ids remembered |

Endpoints: `/healthz`, `/readyz` (Redpanda), `/metrics`, `POST /internal/v1/manual-push` (internal token). Metrics: `notifier_events_total{result}`, `notifier_decisions_total{verdict}`, `notifier_pushes_total{provider,outcome}` (sent, invalid_token, failed, dropped, unsupported, manual), `notifier_push_retries_total`, `notifier_push_duration_seconds`, `notifier_queue_depth`, `notifier_queue_capacity`, `notifier_in_flight`, `notifier_in_flight_limit`, `notifier_apns_errors_total{status,reason}`, `notifier_consumer_errors_total`.

## Run locally

Start the compose stack from the `gitops` repo (`gitops/local`), then:

```sh
cp .env.example .env   # set JWT_PUBLIC_KEY to the api dev public key
make dev
```

## Test

```sh
make test   # unit tests plus integration tests (Docker required)
make lint   # cargo fmt --check, cargo clippy -D warnings
make docker
```

The integration tests start ScyllaDB, Redpanda and MinIO with testcontainers on random ports
(containers are named `messages-it-*`) and stub the api service in process.

## Configuration

All configuration comes from environment variables and is validated at startup; every problem
is reported at once.

| Variable | Required | Default | Purpose |
|---|---|---|---|
| `DISCORDY_ENV` | no | `local` | `local` or `production` (JSON logs; `local` also creates the MinIO bucket) |
| `LOG_LEVEL` | no | `info` | tracing filter |
| `BIND_ADDR` | no | `0.0.0.0:8082` | listen address |
| `INTERNAL_SERVICE_TOKEN` | yes | | shared secret for `/internal/*`, in both directions |
| `JWT_PUBLIC_KEY` | yes | | Ed25519 public key (PEM) verifying access tokens |
| `SNOWFLAKE_WORKER_ID` | yes | | 64 to 127 |
| `KAFKA_BROKERS` | yes | | comma-separated brokers |
| `SCYLLA_NODES` | yes | | comma-separated `host:port` |
| `SCYLLA_KEYSPACE` | no | `discordy` | keyspace (created and migrated at startup) |
| `API_INTERNAL_URL` | yes | | api service base URL |
| `MINIO_ENDPOINT` | yes | | MinIO API URL (server-side calls) |
| `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY` | yes | | MinIO credentials |
| `MINIO_BUCKET_ATTACHMENTS` | no | `discordy-attachments` | attachments bucket |
| `CDN_PUBLIC_URL` | yes | | public CDN base URL; presigned upload and signed attachment URLs use it |
| `ATTACHMENT_SIGNING_KEY` | yes | | HMAC key (32+ chars, secret) signing attachment download URLs |
| `ATTACHMENT_URL_TTL_SECONDS` | no | `86400` | lifetime of a signed download URL |
| `CORS_ALLOWED_ORIGINS` | no | `null,http://localhost:5173` | comma-separated origins allowed by CORS (`null` is the packaged `file://` client) |

## Pending owner actions

- The local Caddy route only forwards `^/api/v1/channels/[^/]+/messages` to this service.
  `/channels/{id}/pins` and `/channels/{id}/attachments` (also owned by messages, per P9) would
  reach the api service. The ingress rule in `gitops` needs to match those paths too.
- `OTEL_EXPORTER_OTLP_ENDPOINT` is not consumed yet; metrics are exposed on `/metrics`.

See [ARCHITECTURE.md](ARCHITECTURE.md) for the design.

## Git hooks

Tests run locally, not in CI. Install [lefthook](https://github.com/evilmartians/lefthook) once and run `make hooks` (or `lefthook install`) to enable the pre-push hook in `lefthook.yml`: it runs this repository's lint and unit tests before every push. CI repeats only the slim gate on pull requests. Skip once with `LEFTHOOK=0 git push`.

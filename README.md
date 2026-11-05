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

Internal, under `/internal/v1` (header `X-Internal-Token`):
`POST /channels/{id}/messages` (bot, webhook and interaction messages; body `author_id`, `content`,
`embeds`, `components`; skips the user permission check, still refuses non-standard channels with 409
code 50160; `type` posts a system message: 1-2 and 4-5 group DM events, 3 call with `call`
`{participants, ended_timestamp}`, 6 pin, 7 member join, 18 thread created with `thread_id`),
`PATCH /channels/{id}/messages/{mid}/call` (records `ended_timestamp` and final `participants` of a
call message), `GET /channels/{id}/last-message-id`, `GET /users/{id}/read-states`, `GET /admin/stats`.

Operational: `/healthz`, `/readyz` (ScyllaDB and Redpanda), `/metrics`. Metrics: `http_requests_total` and `http_request_duration_seconds` (method, route, status class), `db_query_duration_seconds` (db, operation), `messages_created_total`, `messages_edited_total`, `messages_deleted_total`, `kafka_produce_duration_seconds` and `kafka_produce_errors_total` (topic).

## Notifier (push notifications)

`bin/notifier` (crate `crates/notifier`, image `messages-notifier` from `Dockerfile.notifier`, port 8085) is a second worker in this workspace. It reads `discordy.message.v1` from the end of the topic, and for every `MESSAGE_CREATE` asks the api (`GET /internal/v1/channels/{id}/push-targets`) for the members who can view the channel, hold a push token and are not the author. It then applies the clients' notification rules (`decide`: a live gateway session, mute, level, mention, role, `@everyone`, `@here`, `@silent`) and sends the push through APNs with token auth. `fcm` tokens are counted as `unsupported` and not sent yet.

- **Privacy.** Only a channel whose delivery mode is exactly `standard` may put text (and the author, channel and server names) into a push; anything else, including an unknown mode, gets the title `New message` and no body. A device that turned previews off gets `New message` as the body in standard chats.
- **Back-pressure (ADR 0016).** Pushes go through a bounded queue (`PUSH_QUEUE_CAPACITY`) and at most `PUSH_MAX_IN_FLIGHT` provider requests at once; a full queue drops the push and counts it (`notifier_pushes_total{outcome="dropped"}`). Provider errors are retried with exponential backoff up to `PUSH_MAX_ATTEMPTS`; a rejected token (APNs 410, `BadDeviceToken`, `DeviceTokenNotForTopic`) is removed through `DELETE /internal/v1/push-tokens`.
- **Sandbox and production.** Each token carries the environment it was issued for (development builds register `sandbox`); the sender uses `api.sandbox.push.apple.com` or `api.push.apple.com` per token. `BadDeviceToken` is what APNs answers when a token reaches the wrong host (an ad-hoc build can carry either kind), so the other host gets one try before the token is removed, and the host that worked is remembered. `APNS_ENVIRONMENT` forces one host for every token and disables the fallback.
- **One replica.** There is no stored offset and no consumer group: the notifier starts at the end of the topic and ignores events older than `PUSH_MAX_EVENT_AGE_SECS`, so run exactly one replica (the gitops deployment uses `Recreate`).
- **VoIP.** `OutboundPush::Voip` and the `.voip` topic are in the sender for the call ticket; nothing produces them yet.

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

Endpoints: `/healthz`, `/readyz` (Redpanda), `/metrics`. Metrics: `notifier_events_total{result}`, `notifier_decisions_total{verdict}`, `notifier_pushes_total{provider,outcome}` (sent, invalid_token, failed, dropped, unsupported), `notifier_push_retries_total`, `notifier_push_duration_seconds`, `notifier_queue_depth`, `notifier_apns_errors_total{status,reason}`, `notifier_consumer_errors_total`.

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

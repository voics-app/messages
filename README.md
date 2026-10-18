# messages

The Discordy messages service (Rust, axum, ScyllaDB). It stores message history in 10-day
ScyllaDB buckets, and serves reactions, pins, read states and attachment uploads. It publishes
every change to `discordy.message.v1` on Redpanda. Only `standard` delivery-mode channels are
accepted; other modes get `409` with code `50160`.

## Endpoints (port 8082)

Public, under `/api/v1` (Bearer JWT or Bot token):
`GET|POST /channels/{id}/messages`, `PATCH|DELETE /channels/{id}/messages/{mid}`,
`POST /channels/{id}/messages/bulk-delete`, `POST /channels/{id}/messages/{mid}/ack`,
`DELETE /channels/{id}/messages/{mid}/reactions`,
`PUT|DELETE /channels/{id}/messages/{mid}/reactions/{emoji}/@me`,
`GET /channels/{id}/pins`, `PUT|DELETE /channels/{id}/pins/{mid}`,
`POST /channels/{id}/attachments`.

Internal, under `/internal/v1` (header `X-Internal-Token`):
`POST /channels/{id}/messages` (bot, webhook and interaction messages; body `author_id`, `content`,
`embeds`, `components`; skips the user permission check, still refuses non-standard channels with 409
code 50160), `GET /channels/{id}/last-message-id`, `GET /users/{id}/read-states`, `GET /admin/stats`.

Operational: `/healthz`, `/readyz` (ScyllaDB and Redpanda), `/metrics`.

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
| `CDN_PUBLIC_URL` | yes | | public CDN base URL; presigned upload and attachment URLs use it |
| `CORS_ALLOWED_ORIGINS` | no | `null,http://localhost:5173` | comma-separated origins allowed by CORS (`null` is the packaged `file://` client) |

## Pending owner actions

- The local Caddy route only forwards `^/api/v1/channels/[^/]+/messages` to this service.
  `/channels/{id}/pins` and `/channels/{id}/attachments` (also owned by messages, per P9) would
  reach the api service. The ingress rule in `gitops` needs to match those paths too.
- `OTEL_EXPORTER_OTLP_ENDPOINT` is not consumed yet; metrics are exposed on `/metrics`.

See [ARCHITECTURE.md](ARCHITECTURE.md) for the design.

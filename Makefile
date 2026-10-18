.PHONY: dev test lint build docker fmt

IMAGE ?= ghcr.io/discordy-voics/messages
VERSION ?= $(shell sed -n 's/.*"\.": "\(.*\)".*/\1/p' .release-please-manifest.json)

# Runs the service against the local compose stack (gitops/local). Reads .env.
dev:
	set -a && . ./.env && set +a && cargo run --bin messages

# Unit tests plus the integration tests, which start ScyllaDB, Redpanda and MinIO
# through testcontainers on random ports (Docker required).
test:
	cargo test --workspace

lint:
	cargo fmt --check
	cargo clippy --workspace --all-targets -- -D warnings

fmt:
	cargo fmt

build:
	cargo build --release --bin messages

docker:
	docker build --build-arg VERSION=$(VERSION) --build-arg REVISION=$$(git rev-parse --short HEAD 2>/dev/null || echo unknown) -t $(IMAGE):$(VERSION) .

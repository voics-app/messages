# syntax=docker/dockerfile:1.7
# cargo-chef builds the dependency layer once, from a recipe of the manifests only (also reused through the
# registry layer cache in CI, which does not export cache mounts).
FROM rust:1.99-slim-bookworm AS chef
ARG CHEF_VERSION=0.1.78
RUN --mount=type=cache,target=/usr/local/cargo/registry     cargo install cargo-chef --locked --version "${CHEF_VERSION}"
WORKDIR /src

FROM chef AS planner
COPY Cargo.toml Cargo.lock rustfmt.toml ./
COPY crates ./crates
COPY bin ./bin
COPY migrations ./migrations
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS build
COPY --from=planner /src/recipe.json recipe.json
RUN --mount=type=cache,target=/usr/local/cargo/registry     --mount=type=cache,target=/usr/local/cargo/git     cargo chef cook --release --locked --bin messages --recipe-path recipe.json
COPY Cargo.toml Cargo.lock rustfmt.toml ./
COPY crates ./crates
COPY bin ./bin
COPY migrations ./migrations
RUN --mount=type=cache,target=/usr/local/cargo/registry     --mount=type=cache,target=/usr/local/cargo/git     cargo build --release --locked --bin messages  && cp target/release/messages /messages

FROM gcr.io/distroless/cc-debian12:nonroot AS runtime
ARG VERSION=0.1.0
ARG REVISION=unknown
LABEL org.opencontainers.image.title="messages" \
      org.opencontainers.image.description="Discordy messages service: message history, reactions, pins, read states and attachments" \
      org.opencontainers.image.source="https://github.com/discordy-voics/messages" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${REVISION}" \
      org.opencontainers.image.licenses="UNLICENSED"
COPY --from=build /messages /usr/local/bin/messages
USER nonroot:nonroot
EXPOSE 8082
ENTRYPOINT ["/usr/local/bin/messages"]

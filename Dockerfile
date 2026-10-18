# syntax=docker/dockerfile:1
FROM rust:1.99-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock rustfmt.toml ./
COPY crates ./crates
COPY bin ./bin
COPY migrations ./migrations
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked --bin messages \
 && cp target/release/messages /messages

FROM debian:bookworm-slim
ARG VERSION=0.1.0
ARG REVISION=unknown
LABEL org.opencontainers.image.title="messages" \
      org.opencontainers.image.description="Discordy messages service: message history, reactions, pins, read states and attachments" \
      org.opencontainers.image.source="https://github.com/discordy-voics/messages" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${REVISION}" \
      org.opencontainers.image.licenses="UNLICENSED"
RUN useradd --system --uid 10001 --no-create-home --shell /usr/sbin/nologin messages
COPY --from=build /messages /usr/local/bin/messages
USER 10001:10001
EXPOSE 8082
ENTRYPOINT ["/usr/local/bin/messages"]

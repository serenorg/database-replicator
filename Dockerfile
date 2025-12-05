# syntax=docker/dockerfile:1

FROM rust:1.82-slim AS builder
WORKDIR /app

# Install build dependencies for OpenSSL / libpq bindings
RUN apt-get update && \
    apt-get install -y --no-install-recommends pkg-config libssl-dev libpq-dev && \
    rm -rf /var/lib/apt/lists/*

# Cache dependencies
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY README.md .
RUN cargo build --release --bin database-replicator

FROM debian:bookworm-slim
LABEL org.opencontainers.image.title="database-replicator" \
      org.opencontainers.image.description="Seren database replicator CLI" \
      org.opencontainers.image.source="https://github.com/serenorg/database-replicator"

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates libssl3 libpq5 && \
    rm -rf /var/lib/apt/lists/* && \
    useradd -m replicator

COPY --from=builder /app/target/release/database-replicator /usr/local/bin/database-replicator
USER replicator
ENTRYPOINT ["database-replicator"]
CMD ["--help"]

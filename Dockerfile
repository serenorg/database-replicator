# syntax=docker/dockerfile:1

FROM ubuntu:24.04 AS downloader
ARG VERSION=latest
ENV BINARY_NAME=database-replicator-linux-x64-binary
ENV RELEASE_ROOT=https://github.com/serenorg/database-replicator/releases

RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates && rm -rf /var/lib/apt/lists/*

RUN set -eux; \
    if [ "$VERSION" = "latest" ]; then \
        URL="$RELEASE_ROOT/latest/download/$BINARY_NAME"; \
    else \
        URL="$RELEASE_ROOT/download/$VERSION/$BINARY_NAME"; \
    fi; \
    curl -fL "$URL" -o /tmp/database-replicator && \
    chmod +x /tmp/database-replicator

FROM ubuntu:24.04
LABEL org.opencontainers.image.title="database-replicator" \
      org.opencontainers.image.description="Seren database replicator CLI" \
      org.opencontainers.image.source="https://github.com/serenorg/database-replicator"

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates libsqlite3-0 libssl3 libpq5 postgresql-client && \
    rm -rf /var/lib/apt/lists/* && \
    useradd -m replicator

COPY --from=downloader /tmp/database-replicator /usr/local/bin/database-replicator
USER replicator
ENTRYPOINT ["database-replicator"]
CMD ["--help"]

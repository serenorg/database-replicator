# syntax=docker/dockerfile:1

FROM ubuntu:24.04 AS downloader
ARG VERSION=latest
ENV REPLICATOR_ASSET=database-replicator-linux-x64-binary
ENV WATCHER_ASSET=sqlite-watcher-linux-x64
ENV RELEASE_ROOT=https://github.com/serenorg/database-replicator/releases

RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates && rm -rf /var/lib/apt/lists/*

RUN set -eux; \
    if [ "$VERSION" = "latest" ]; then \
        REP_URL="$RELEASE_ROOT/latest/download/$REPLICATOR_ASSET"; \
        WATCH_URL="$RELEASE_ROOT/latest/download/$WATCHER_ASSET"; \
    else \
        REP_URL="$RELEASE_ROOT/download/$VERSION/$REPLICATOR_ASSET"; \
        WATCH_URL="$RELEASE_ROOT/download/$VERSION/$WATCHER_ASSET"; \
    fi; \
    curl -fL "$REP_URL" -o /tmp/database-replicator && chmod +x /tmp/database-replicator && \
    curl -fL "$WATCH_URL" -o /tmp/sqlite-watcher && chmod +x /tmp/sqlite-watcher

FROM ubuntu:24.04
LABEL org.opencontainers.image.title="database-replicator" \
      org.opencontainers.image.description="Seren database replicator CLI" \
      org.opencontainers.image.source="https://github.com/serenorg/database-replicator"

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates libsqlite3-0 libssl3 libpq5 postgresql-client && \
    rm -rf /var/lib/apt/lists/* && \
    useradd -m replicator

COPY --from=downloader /tmp/database-replicator /usr/local/bin/database-replicator
COPY --from=downloader /tmp/sqlite-watcher /usr/local/bin/sqlite-watcher
USER replicator
ENTRYPOINT ["database-replicator"]
CMD ["--help"]

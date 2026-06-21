# ── Stage 1: build ──────────────────────────────────────────────────────────
FROM rust:1.82-slim-bookworm AS builder

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Cache dependency compilation separately from application source.
COPY Cargo.toml Cargo.lock ./
RUN mkdir -p src && \
    printf 'fn main(){}' > src/main.rs && \
    printf '' > src/lib.rs && \
    cargo build --release --locked && \
    rm -f target/release/deps/rustyqueue*

# Build real source
COPY src ./src
COPY migrations ./migrations
COPY config.toml ./
RUN touch src/main.rs src/lib.rs && cargo build --release --locked

# ── Stage 2: runtime ─────────────────────────────────────────────────────────
FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app/target/release/rustyqueue ./rustyqueue
COPY --from=builder /app/migrations ./migrations
COPY --from=builder /app/config.toml ./config.toml

EXPOSE 8080
ENTRYPOINT ["./rustyqueue"]

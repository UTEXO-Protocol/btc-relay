FROM rust:1.91-bookworm AS builder
WORKDIR /app

# Build dependencies first to improve layer caching.
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY tests ./tests
RUN cargo build --release

FROM debian:bookworm-slim AS runtime
WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates tzdata \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/btc-relayer /usr/local/bin/btc-relayer

# Keep default state file parent directory available in container.
RUN mkdir -p /app/artifacts

ENV RUST_LOG=info
ENV METRICS_BIND_ADDR=0.0.0.0:9090

EXPOSE 9090

ENTRYPOINT ["/usr/local/bin/btc-relayer"]

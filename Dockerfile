# Multi-stage Dockerfile for building and running tycho-tonapi
# Build stage
FROM rust:1.89-bullseye AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    clang \
    pkg-config \
    libssl-dev \
    zlib1g-dev \
    protobuf-compiler \
    ca-certificates \
    git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Cache dependencies
COPY Cargo.toml Cargo.lock ./
COPY rustfmt.toml build.rs ./
COPY proto ./proto
# Create a dummy main to cache deps compile
RUN mkdir -p src && echo "fn main() {}" > src/main.rs
RUN cargo build --release --locked || true

# Now copy the full source and build the real binary
COPY src ./src
RUN cargo build --release --locked

# Runtime stage
FROM debian:bullseye-slim AS runtime

# Install minimal runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libssl1.1 \
    zlib1g \
    wget \
    && rm -rf /var/lib/apt/lists/* \
    && update-ca-certificates

# Create non-root user
RUN useradd -m -u 10001 -s /usr/sbin/nologin appuser

# Prepare app directory and permissions
RUN mkdir -p /app && chown -R appuser:appuser /app

# Copy binary
COPY --from=builder /build/target/release/tycho-tonapi /usr/local/bin/tycho-tonapi

# Copy entrypoint script
COPY scripts/docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

# Workdir and data/config locations
WORKDIR /app
VOLUME ["/app/data"]

# Expose default ports (UDP/TCP as used by the app)
# Node UDP port
EXPOSE 30000/udp
# Prometheus metrics
EXPOSE 10000/tcp
# gRPC API
EXPOSE 50051/tcp

USER appuser

# Use entrypoint that initializes config and downloads global config as per README
ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
CMD []

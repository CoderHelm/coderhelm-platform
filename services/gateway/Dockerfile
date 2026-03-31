# ---------------------------------------------------------------------------
# Stage 1: builder — compile gateway for aarch64-unknown-linux-musl via Zig
# ---------------------------------------------------------------------------
FROM rust:1-bookworm AS builder

ARG ZIG_VERSION=0.14.1
ARG CARGO_ZIGBUILD_VERSION=0.19.8

# Install Zig
RUN curl -fsSL "https://ziglang.org/download/${ZIG_VERSION}/zig-linux-x86_64-${ZIG_VERSION}.tar.xz" \
    | tar -xJ -C /usr/local --strip-components=1

# Install cargo-zigbuild
RUN pip3 install --break-system-packages cargo-zigbuild==${CARGO_ZIGBUILD_VERSION}

# Add the cross-compilation target
RUN rustup target add aarch64-unknown-linux-musl

WORKDIR /build

# --- dependency caching layer ---
# Copy workspace manifests and lock file
COPY Cargo.toml Cargo.lock ./
COPY gateway/Cargo.toml gateway/Cargo.toml
COPY worker/Cargo.toml worker/Cargo.toml

# Create stub main.rs files so `cargo zigbuild` can resolve the workspace
RUN mkdir -p gateway/src worker/src \
    && echo "fn main() {}" > gateway/src/main.rs \
    && echo "fn main() {}" > worker/src/main.rs

# Pre-build dependencies (cached until Cargo.toml/Cargo.lock change)
RUN cargo zigbuild -p gateway --release --target aarch64-unknown-linux-musl || true

# --- real source layer ---
# Remove the stubs and copy real gateway source
RUN rm -rf gateway/src
COPY gateway/src gateway/src

# Touch main.rs so Cargo detects the source change
RUN touch gateway/src/main.rs

# Build the gateway binary
RUN cargo zigbuild -p gateway --release --target aarch64-unknown-linux-musl

# ---------------------------------------------------------------------------
# Stage 2: artifact — minimal image with the compiled binary
# ---------------------------------------------------------------------------
FROM debian:bookworm-slim AS artifact

COPY --from=builder /build/target/aarch64-unknown-linux-musl/release/gateway /bootstrap

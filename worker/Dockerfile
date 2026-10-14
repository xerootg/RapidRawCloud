# Headless rrcloud-worker image. rrcloud-core-only (no tauri/gtk/webkit): the
# worker generates linear-DNG proxies via the pinned rawler in rrcloud-core, so
# color parity with the app needs no app code. Slim runtime: just the binary +
# CA certs for TLS to the S3 endpoint.
#
# rrcloud-core takes its wire types from the generated protocol SDK
# (protocol/gen/rust/rrcloud-proto, a path dependency), so the build context
# is the REPOSITORY ROOT:
#   docker build -f worker/Dockerfile .
# (CI passes that context; see .github/workflows/worker-image.yml).
FROM rust:1.97-bookworm AS builder
WORKDIR /build
# rrcloud-core declares rust-version 1.97, edition 2021. rawler is a git dep
# fetched during the build (needs network in CI). tests/ MUST be copied even
# though a --bin build compiles no tests/dev-deps: the crate declares explicit
# [[test]] targets (state_crash, transfer_crash), and cargo requires their
# source files to exist to PARSE the manifest, before target selection.
# Same relative layout as the repository, so the
# `../../../protocol/gen/rust/rrcloud-proto` path dependency resolves.
COPY protocol/gen/rust/rrcloud-proto ./protocol/gen/rust/rrcloud-proto
COPY src-tauri/crates/rrcloud-core/Cargo.toml src-tauri/crates/rrcloud-core/Cargo.lock ./src-tauri/crates/rrcloud-core/
COPY src-tauri/crates/rrcloud-core/src ./src-tauri/crates/rrcloud-core/src
COPY src-tauri/crates/rrcloud-core/tests ./src-tauri/crates/rrcloud-core/tests
WORKDIR /build/src-tauri/crates/rrcloud-core
# Build only the worker bin in release.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/build/src-tauri/crates/rrcloud-core/target \
    cargo build --release --locked --bin rrcloud-worker \
 && cp target/release/rrcloud-worker /usr/local/bin/rrcloud-worker

FROM debian:bookworm-slim AS runtime
# TLS roots for HTTPS S3 endpoints (Garage public ingress, AWS, B2, R2).
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/*
# Non-root; the state dir is a mounted volume owned by this uid in the manifest.
RUN useradd --uid 65532 --user-group --no-create-home --shell /usr/sbin/nologin rrcloud
COPY --from=builder /usr/local/bin/rrcloud-worker /usr/local/bin/rrcloud-worker
USER 65532:65532
ENV RRCLOUD_STATE_DIR=/state
ENTRYPOINT ["/usr/local/bin/rrcloud-worker"]
CMD ["--once"]

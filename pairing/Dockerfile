# RapidRawCloud pairing/discovery service image.
#
# Tiny, standalone crate (no rrcloud-core / raw-processing deps): OIDC token
# check + read/write a JSON config doc in the S3 "admin" bucket + a small
# HTML form. Its document types come from the generated protocol SDK
# (protocol/gen/rust/rrcloud-proto, a path dependency), so the build context
# is the REPOSITORY ROOT:
#   docker build -f pairing/Dockerfile .
# (CI passes that context; see .github/workflows/pairing-image.yml).
# Latest stable Rust: some transitive deps (e.g. rand_pcg 0.10.x) ship a
# Cargo.toml needing a recent cargo to parse; an old pin fails at manifest
# parse time.
FROM rust:1-bookworm AS builder
WORKDIR /build
# Same relative layout as the repository, so the `../protocol/...` path dependency resolves.
COPY protocol/gen/rust/rrcloud-proto ./protocol/gen/rust/rrcloud-proto
COPY pairing/Cargo.toml pairing/Cargo.lock ./pairing/
COPY pairing/src ./pairing/src
WORKDIR /build/pairing
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/build/pairing/target \
    cargo build --release --locked --bin rrcloud-pairing \
 && cp /build/pairing/target/release/rrcloud-pairing /usr/local/bin/rrcloud-pairing

FROM debian:bookworm-slim AS runtime
# TLS roots for HTTPS to the S3 endpoint and the Authentik userinfo endpoint.
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/*
RUN useradd --uid 65532 --user-group --no-create-home --shell /usr/sbin/nologin rrcloud
COPY --from=builder /usr/local/bin/rrcloud-pairing /usr/local/bin/rrcloud-pairing
USER 65532:65532
ENV BIND_ADDR=0.0.0.0:8080
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/rrcloud-pairing"]

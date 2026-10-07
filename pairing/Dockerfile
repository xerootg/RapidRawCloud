# RapidRawCloud pairing/discovery service image.
#
# Tiny, standalone crate (no rrcloud-core / raw-processing deps): OIDC token
# check + read/write a JSON config doc in the S3 "admin" bucket + a small
# HTML form. Build context is this directory:
#   docker build -f pairing/Dockerfile pairing
# (CI passes that context; see .github/workflows/pairing-image.yml).
FROM rust:1.83-bookworm AS builder
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/build/target \
    cargo build --release --locked --bin rrcloud-pairing \
 && cp /build/target/release/rrcloud-pairing /usr/local/bin/rrcloud-pairing

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

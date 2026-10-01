#!/usr/bin/env bash
# Fetches the Garage v2.2.0 static binary into <crate>/.garage/garage, the
# drop point the integration-test harness (tests/common/garage.rs) checks
# right after $GARAGE_BIN. Idempotent: a present binary whose SHA-256
# matches is left untouched and nothing is downloaded.
#
# x86_64 Linux only (the only artifact pinned below). On any other platform,
# obtain a Garage v2.2.0 binary yourself and `export GARAGE_BIN=/path/to/garage`.
set -euo pipefail

GARAGE_VERSION="2.2.0"
GARAGE_URL="https://garagehq.deuxfleurs.fr/_releases/v2.2.0/x86_64-unknown-linux-musl/garage"
GARAGE_SHA256="ec761bb996e8453e86fe68ccc1cf222c73bb1ef05ae0b540bd4827e7d1931aab"

# Resolve paths from this script's location, never from the caller's cwd.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CRATE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
DEST_DIR="${CRATE_DIR}/.garage"
DEST="${DEST_DIR}/garage"

die() {
  echo "fetch-garage: $*" >&2
  exit 1
}

arch="$(uname -m)"
os="$(uname -s)"
if [ "${arch}" != "x86_64" ] || [ "${os}" != "Linux" ]; then
  echo "fetch-garage: this script only fetches the x86_64 Linux binary (found ${arch} ${os})." >&2
  echo "fetch-garage: install a Garage v${GARAGE_VERSION} binary for your platform and set" >&2
  echo "fetch-garage:   export GARAGE_BIN=/path/to/garage" >&2
  echo "fetch-garage: before running 'cargo test'." >&2
  exit 1
fi

sha256_of() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | cut -d' ' -f1
  elif command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$1" | cut -d' ' -f1
  else
    die "need sha256sum or shasum on PATH"
  fi
}

if [ -f "${DEST}" ] && [ "$(sha256_of "${DEST}")" = "${GARAGE_SHA256}" ]; then
  chmod 755 "${DEST}"
  echo "fetch-garage: ${DEST} already present and verified (v${GARAGE_VERSION}); nothing to do."
  exit 0
fi

mkdir -p "${DEST_DIR}"
# Download beside the destination so the final mv is an atomic same-fs rename,
# and never leave a partial or unverified file behind.
tmp="$(mktemp "${DEST_DIR}/garage.download.XXXXXX")"
trap 'rm -f "${tmp}"' EXIT

echo "fetch-garage: downloading ${GARAGE_URL}"
if command -v curl >/dev/null 2>&1; then
  curl --fail --silent --show-error --location --retry 3 --output "${tmp}" "${GARAGE_URL}" \
    || die "download failed (${GARAGE_URL})"
elif command -v wget >/dev/null 2>&1; then
  wget --quiet --tries=3 --output-document="${tmp}" "${GARAGE_URL}" \
    || die "download failed (${GARAGE_URL})"
else
  die "need curl or wget on PATH"
fi

actual="$(sha256_of "${tmp}")"
if [ "${actual}" != "${GARAGE_SHA256}" ]; then
  rm -f "${tmp}"
  echo "fetch-garage: SHA-256 MISMATCH for the downloaded Garage binary; deleted it." >&2
  echo "fetch-garage:   expected ${GARAGE_SHA256}" >&2
  echo "fetch-garage:   actual   ${actual}" >&2
  exit 1
fi

chmod 755 "${tmp}"
mv -f "${tmp}" "${DEST}"
trap - EXIT
echo "fetch-garage: installed ${DEST} (v${GARAGE_VERSION}, sha256 verified)."

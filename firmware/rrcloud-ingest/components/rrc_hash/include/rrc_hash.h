/*
 * rrc_hash — small, dependency-free hash/encoding primitives shared by the
 * S3 signer (SHA-256 / HMAC-SHA256 / Content-MD5 / Base64) and the gzip
 * manifest writer (CRC-32). Pure C99; builds unchanged on the host for tests.
 *
 * mbedTLS is available on the device, but keeping these self-contained means
 * the signer and the protocol codec are byte-for-byte identical in the host
 * test-suite and in the firmware — which is the whole point of the tests.
 */
#pragma once
#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ---- SHA-256 -------------------------------------------------------------- */
#ifdef ESP_PLATFORM
/* On the dock the hashes ride the SoC engines through mbedTLS (SHA-256 is DMA
 * driven; MD5 is mbedTLS' optimised software), which is 20x the portable code. */
#include "mbedtls/sha256.h"
#include "mbedtls/md5.h"
typedef struct { mbedtls_sha256_context m; } rrc_sha256_ctx;
typedef struct { mbedtls_md5_context m; } rrc_md5_ctx;
#else
typedef struct {
    uint32_t h[8];
    uint64_t total_len;
    uint8_t buf[64];
    size_t buf_len;
} rrc_sha256_ctx;
#endif

void rrc_sha256_init(rrc_sha256_ctx *c);
void rrc_sha256_update(rrc_sha256_ctx *c, const void *data, size_t len);
void rrc_sha256_final(rrc_sha256_ctx *c, uint8_t out[32]);
void rrc_sha256(const void *data, size_t len, uint8_t out[32]);

/* ---- HMAC-SHA256 ---------------------------------------------------------- */
void rrc_hmac_sha256(const void *key, size_t key_len, const void *data, size_t data_len, uint8_t out[32]);

/* ---- MD5 (Content-MD5 only; not used for any security purpose) ------------ */
#ifndef ESP_PLATFORM
typedef struct {
    uint32_t a, b, c, d;
    uint64_t total_len;
    uint8_t buf[64];
    size_t buf_len;
} rrc_md5_ctx;
#endif

void rrc_md5_init(rrc_md5_ctx *c);
void rrc_md5_update(rrc_md5_ctx *c, const void *data, size_t len);
void rrc_md5_final(rrc_md5_ctx *c, uint8_t out[16]);
void rrc_md5(const void *data, size_t len, uint8_t out[16]);

/* ---- CRC-32 (IEEE 802.3 / zlib, reflected, init 0xFFFFFFFF, final xor) ---- */
uint32_t rrc_crc32_update(uint32_t crc, const void *data, size_t len);
static inline uint32_t rrc_crc32(const void *data, size_t len) { return rrc_crc32_update(0, data, len); }

/* ---- Encodings ----------------------------------------------------------- */
/* Lowercase hex. `out` must hold 2*len+1 bytes; returns `out`. */
char *rrc_hex_lower(const uint8_t *in, size_t len, char *out);
/* Standard Base64 with padding. `out` must hold 4*ceil(len/3)+1 bytes; returns length written. */
size_t rrc_base64_encode(const uint8_t *in, size_t len, char *out);
/* Base64url (RFC 4648 §5) without padding — used for OAuth PKCE/state. */
size_t rrc_base64url_encode_nopad(const uint8_t *in, size_t len, char *out);

#ifdef __cplusplus
}
#endif

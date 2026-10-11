/* Device backend for rrc_hash: SHA-256 on the SHA engine (DMA for large
 * inputs) and MD5 from mbedTLS. Same API as the portable rrc_sha256.c /
 * rrc_md5.c, which the host tests keep using. */
#include "rrc_hash.h"

void rrc_sha256_init(rrc_sha256_ctx *c) { mbedtls_sha256_init(&c->m); mbedtls_sha256_starts(&c->m, 0); }
void rrc_sha256_update(rrc_sha256_ctx *c, const void *data, size_t len) { if (len) mbedtls_sha256_update(&c->m, data, len); }
void rrc_sha256_final(rrc_sha256_ctx *c, uint8_t out[32]) { mbedtls_sha256_finish(&c->m, out); mbedtls_sha256_free(&c->m); }
void rrc_sha256(const void *data, size_t len, uint8_t out[32]) { mbedtls_sha256(data, len, out, 0); }

void rrc_md5_init(rrc_md5_ctx *c) { mbedtls_md5_init(&c->m); mbedtls_md5_starts(&c->m); }
void rrc_md5_update(rrc_md5_ctx *c, const void *data, size_t len) { if (len) mbedtls_md5_update(&c->m, data, len); }
void rrc_md5_final(rrc_md5_ctx *c, uint8_t out[16]) { mbedtls_md5_finish(&c->m, out); mbedtls_md5_free(&c->m); }
void rrc_md5(const void *data, size_t len, uint8_t out[16]) { mbedtls_md5(data, len, out); }

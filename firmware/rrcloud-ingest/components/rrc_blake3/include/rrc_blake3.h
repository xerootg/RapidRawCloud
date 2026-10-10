/*
 * rrc_blake3 — compact portable BLAKE3 (unkeyed hash mode only), written from
 * the BLAKE3 specification's reference algorithm. The sync protocol keys
 * originals by `blake3(bytes)` (`content_id`, architecture §1.2) and every
 * journal entry carries the blake3 of the uploaded bytes (§2.2/§2.4), so the
 * firmware must produce exactly the same digests as the Rust engine.
 * Verified against the `blake3` Python package in host_tests/.
 */
#pragma once
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define RRC_BLAKE3_OUT_LEN 32
#define RRC_BLAKE3_HEX_LEN 64

typedef struct {
    uint32_t cv[8];
    uint64_t chunk_counter;
    uint8_t block[64];
    uint8_t block_len;
    uint8_t blocks_compressed;
} rrc_blake3_chunk_state;

typedef struct {
    rrc_blake3_chunk_state chunk;
    uint32_t cv_stack[54][8]; /* enough for 2^54 chunks (a 2^64-byte input) */
    uint8_t cv_stack_len;
} rrc_blake3_ctx;

void rrc_blake3_init(rrc_blake3_ctx *h);
void rrc_blake3_update(rrc_blake3_ctx *h, const void *data, size_t len);
void rrc_blake3_final(const rrc_blake3_ctx *h, uint8_t out[RRC_BLAKE3_OUT_LEN]);
void rrc_blake3(const void *data, size_t len, uint8_t out[RRC_BLAKE3_OUT_LEN]);
/* Finalize to lowercase hex (65-byte buffer incl. NUL). */
void rrc_blake3_final_hex(const rrc_blake3_ctx *h, char out[RRC_BLAKE3_HEX_LEN + 1]);

#ifdef __cplusplus
}
#endif

#include "rrc_blake3.h"
#include <string.h>

#define CHUNK_LEN 1024
#define BLOCK_LEN 64

enum { CHUNK_START = 1, CHUNK_END = 2, PARENT = 4, ROOT = 8 };

static const uint32_t IV[8] = {0x6A09E667, 0xBB67AE85, 0x3C6EF372, 0xA54FF53A,
                               0x510E527F, 0x9B05688C, 0x1F83D9AB, 0x5BE0CD19};

static inline uint32_t rotr(uint32_t x, int n) { return (x >> n) | (x << (32 - n)); }
static inline uint32_t load32(const uint8_t *p)
{
    return (uint32_t)p[0] | ((uint32_t)p[1] << 8) | ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24);
}
static inline void store32(uint8_t *p, uint32_t v)
{
    p[0] = (uint8_t)v; p[1] = (uint8_t)(v >> 8); p[2] = (uint8_t)(v >> 16); p[3] = (uint8_t)(v >> 24);
}

static void compress(const uint32_t cv[8], const uint8_t block[BLOCK_LEN], uint64_t counter,
                     uint32_t block_len, uint32_t flags, uint32_t out[16])
{
    /* Fully unrolled: state and message words live in registers, and the message
     * permutation is folded into which word each G mixes. The array-indexed
     * reference form kept the state in memory and ran at 7 MB/s on the dock. */
    uint32_t m0 = load32(block), m1 = load32(block + 4), m2 = load32(block + 8), m3 = load32(block + 12),
             m4 = load32(block + 16), m5 = load32(block + 20), m6 = load32(block + 24), m7 = load32(block + 28),
             m8 = load32(block + 32), m9 = load32(block + 36), m10 = load32(block + 40), m11 = load32(block + 44),
             m12 = load32(block + 48), m13 = load32(block + 52), m14 = load32(block + 56), m15 = load32(block + 60);
    uint32_t v0 = cv[0], v1 = cv[1], v2 = cv[2], v3 = cv[3], v4 = cv[4], v5 = cv[5], v6 = cv[6], v7 = cv[7],
             v8 = IV[0], v9 = IV[1], v10 = IV[2], v11 = IV[3],
             v12 = (uint32_t)counter, v13 = (uint32_t)(counter >> 32), v14 = block_len, v15 = flags;
    /* round 1 */
    v0 += v4 + m0; v12 = rotr(v12 ^ v0, 16); v8 += v12; v4 = rotr(v4 ^ v8, 12);
    v0 += v4 + m1; v12 = rotr(v12 ^ v0, 8);  v8 += v12; v4 = rotr(v4 ^ v8, 7);
    v1 += v5 + m2; v13 = rotr(v13 ^ v1, 16); v9 += v13; v5 = rotr(v5 ^ v9, 12);
    v1 += v5 + m3; v13 = rotr(v13 ^ v1, 8);  v9 += v13; v5 = rotr(v5 ^ v9, 7);
    v2 += v6 + m4; v14 = rotr(v14 ^ v2, 16); v10 += v14; v6 = rotr(v6 ^ v10, 12);
    v2 += v6 + m5; v14 = rotr(v14 ^ v2, 8);  v10 += v14; v6 = rotr(v6 ^ v10, 7);
    v3 += v7 + m6; v15 = rotr(v15 ^ v3, 16); v11 += v15; v7 = rotr(v7 ^ v11, 12);
    v3 += v7 + m7; v15 = rotr(v15 ^ v3, 8);  v11 += v15; v7 = rotr(v7 ^ v11, 7);
    v0 += v5 + m8; v15 = rotr(v15 ^ v0, 16); v10 += v15; v5 = rotr(v5 ^ v10, 12);
    v0 += v5 + m9; v15 = rotr(v15 ^ v0, 8);  v10 += v15; v5 = rotr(v5 ^ v10, 7);
    v1 += v6 + m10; v12 = rotr(v12 ^ v1, 16); v11 += v12; v6 = rotr(v6 ^ v11, 12);
    v1 += v6 + m11; v12 = rotr(v12 ^ v1, 8);  v11 += v12; v6 = rotr(v6 ^ v11, 7);
    v2 += v7 + m12; v13 = rotr(v13 ^ v2, 16); v8 += v13; v7 = rotr(v7 ^ v8, 12);
    v2 += v7 + m13; v13 = rotr(v13 ^ v2, 8);  v8 += v13; v7 = rotr(v7 ^ v8, 7);
    v3 += v4 + m14; v14 = rotr(v14 ^ v3, 16); v9 += v14; v4 = rotr(v4 ^ v9, 12);
    v3 += v4 + m15; v14 = rotr(v14 ^ v3, 8);  v9 += v14; v4 = rotr(v4 ^ v9, 7);
    /* round 2 */
    v0 += v4 + m2; v12 = rotr(v12 ^ v0, 16); v8 += v12; v4 = rotr(v4 ^ v8, 12);
    v0 += v4 + m6; v12 = rotr(v12 ^ v0, 8);  v8 += v12; v4 = rotr(v4 ^ v8, 7);
    v1 += v5 + m3; v13 = rotr(v13 ^ v1, 16); v9 += v13; v5 = rotr(v5 ^ v9, 12);
    v1 += v5 + m10; v13 = rotr(v13 ^ v1, 8);  v9 += v13; v5 = rotr(v5 ^ v9, 7);
    v2 += v6 + m7; v14 = rotr(v14 ^ v2, 16); v10 += v14; v6 = rotr(v6 ^ v10, 12);
    v2 += v6 + m0; v14 = rotr(v14 ^ v2, 8);  v10 += v14; v6 = rotr(v6 ^ v10, 7);
    v3 += v7 + m4; v15 = rotr(v15 ^ v3, 16); v11 += v15; v7 = rotr(v7 ^ v11, 12);
    v3 += v7 + m13; v15 = rotr(v15 ^ v3, 8);  v11 += v15; v7 = rotr(v7 ^ v11, 7);
    v0 += v5 + m1; v15 = rotr(v15 ^ v0, 16); v10 += v15; v5 = rotr(v5 ^ v10, 12);
    v0 += v5 + m11; v15 = rotr(v15 ^ v0, 8);  v10 += v15; v5 = rotr(v5 ^ v10, 7);
    v1 += v6 + m12; v12 = rotr(v12 ^ v1, 16); v11 += v12; v6 = rotr(v6 ^ v11, 12);
    v1 += v6 + m5; v12 = rotr(v12 ^ v1, 8);  v11 += v12; v6 = rotr(v6 ^ v11, 7);
    v2 += v7 + m9; v13 = rotr(v13 ^ v2, 16); v8 += v13; v7 = rotr(v7 ^ v8, 12);
    v2 += v7 + m14; v13 = rotr(v13 ^ v2, 8);  v8 += v13; v7 = rotr(v7 ^ v8, 7);
    v3 += v4 + m15; v14 = rotr(v14 ^ v3, 16); v9 += v14; v4 = rotr(v4 ^ v9, 12);
    v3 += v4 + m8; v14 = rotr(v14 ^ v3, 8);  v9 += v14; v4 = rotr(v4 ^ v9, 7);
    /* round 3 */
    v0 += v4 + m3; v12 = rotr(v12 ^ v0, 16); v8 += v12; v4 = rotr(v4 ^ v8, 12);
    v0 += v4 + m4; v12 = rotr(v12 ^ v0, 8);  v8 += v12; v4 = rotr(v4 ^ v8, 7);
    v1 += v5 + m10; v13 = rotr(v13 ^ v1, 16); v9 += v13; v5 = rotr(v5 ^ v9, 12);
    v1 += v5 + m12; v13 = rotr(v13 ^ v1, 8);  v9 += v13; v5 = rotr(v5 ^ v9, 7);
    v2 += v6 + m13; v14 = rotr(v14 ^ v2, 16); v10 += v14; v6 = rotr(v6 ^ v10, 12);
    v2 += v6 + m2; v14 = rotr(v14 ^ v2, 8);  v10 += v14; v6 = rotr(v6 ^ v10, 7);
    v3 += v7 + m7; v15 = rotr(v15 ^ v3, 16); v11 += v15; v7 = rotr(v7 ^ v11, 12);
    v3 += v7 + m14; v15 = rotr(v15 ^ v3, 8);  v11 += v15; v7 = rotr(v7 ^ v11, 7);
    v0 += v5 + m6; v15 = rotr(v15 ^ v0, 16); v10 += v15; v5 = rotr(v5 ^ v10, 12);
    v0 += v5 + m5; v15 = rotr(v15 ^ v0, 8);  v10 += v15; v5 = rotr(v5 ^ v10, 7);
    v1 += v6 + m9; v12 = rotr(v12 ^ v1, 16); v11 += v12; v6 = rotr(v6 ^ v11, 12);
    v1 += v6 + m0; v12 = rotr(v12 ^ v1, 8);  v11 += v12; v6 = rotr(v6 ^ v11, 7);
    v2 += v7 + m11; v13 = rotr(v13 ^ v2, 16); v8 += v13; v7 = rotr(v7 ^ v8, 12);
    v2 += v7 + m15; v13 = rotr(v13 ^ v2, 8);  v8 += v13; v7 = rotr(v7 ^ v8, 7);
    v3 += v4 + m8; v14 = rotr(v14 ^ v3, 16); v9 += v14; v4 = rotr(v4 ^ v9, 12);
    v3 += v4 + m1; v14 = rotr(v14 ^ v3, 8);  v9 += v14; v4 = rotr(v4 ^ v9, 7);
    /* round 4 */
    v0 += v4 + m10; v12 = rotr(v12 ^ v0, 16); v8 += v12; v4 = rotr(v4 ^ v8, 12);
    v0 += v4 + m7; v12 = rotr(v12 ^ v0, 8);  v8 += v12; v4 = rotr(v4 ^ v8, 7);
    v1 += v5 + m12; v13 = rotr(v13 ^ v1, 16); v9 += v13; v5 = rotr(v5 ^ v9, 12);
    v1 += v5 + m9; v13 = rotr(v13 ^ v1, 8);  v9 += v13; v5 = rotr(v5 ^ v9, 7);
    v2 += v6 + m14; v14 = rotr(v14 ^ v2, 16); v10 += v14; v6 = rotr(v6 ^ v10, 12);
    v2 += v6 + m3; v14 = rotr(v14 ^ v2, 8);  v10 += v14; v6 = rotr(v6 ^ v10, 7);
    v3 += v7 + m13; v15 = rotr(v15 ^ v3, 16); v11 += v15; v7 = rotr(v7 ^ v11, 12);
    v3 += v7 + m15; v15 = rotr(v15 ^ v3, 8);  v11 += v15; v7 = rotr(v7 ^ v11, 7);
    v0 += v5 + m4; v15 = rotr(v15 ^ v0, 16); v10 += v15; v5 = rotr(v5 ^ v10, 12);
    v0 += v5 + m0; v15 = rotr(v15 ^ v0, 8);  v10 += v15; v5 = rotr(v5 ^ v10, 7);
    v1 += v6 + m11; v12 = rotr(v12 ^ v1, 16); v11 += v12; v6 = rotr(v6 ^ v11, 12);
    v1 += v6 + m2; v12 = rotr(v12 ^ v1, 8);  v11 += v12; v6 = rotr(v6 ^ v11, 7);
    v2 += v7 + m5; v13 = rotr(v13 ^ v2, 16); v8 += v13; v7 = rotr(v7 ^ v8, 12);
    v2 += v7 + m8; v13 = rotr(v13 ^ v2, 8);  v8 += v13; v7 = rotr(v7 ^ v8, 7);
    v3 += v4 + m1; v14 = rotr(v14 ^ v3, 16); v9 += v14; v4 = rotr(v4 ^ v9, 12);
    v3 += v4 + m6; v14 = rotr(v14 ^ v3, 8);  v9 += v14; v4 = rotr(v4 ^ v9, 7);
    /* round 5 */
    v0 += v4 + m12; v12 = rotr(v12 ^ v0, 16); v8 += v12; v4 = rotr(v4 ^ v8, 12);
    v0 += v4 + m13; v12 = rotr(v12 ^ v0, 8);  v8 += v12; v4 = rotr(v4 ^ v8, 7);
    v1 += v5 + m9; v13 = rotr(v13 ^ v1, 16); v9 += v13; v5 = rotr(v5 ^ v9, 12);
    v1 += v5 + m11; v13 = rotr(v13 ^ v1, 8);  v9 += v13; v5 = rotr(v5 ^ v9, 7);
    v2 += v6 + m15; v14 = rotr(v14 ^ v2, 16); v10 += v14; v6 = rotr(v6 ^ v10, 12);
    v2 += v6 + m10; v14 = rotr(v14 ^ v2, 8);  v10 += v14; v6 = rotr(v6 ^ v10, 7);
    v3 += v7 + m14; v15 = rotr(v15 ^ v3, 16); v11 += v15; v7 = rotr(v7 ^ v11, 12);
    v3 += v7 + m8; v15 = rotr(v15 ^ v3, 8);  v11 += v15; v7 = rotr(v7 ^ v11, 7);
    v0 += v5 + m7; v15 = rotr(v15 ^ v0, 16); v10 += v15; v5 = rotr(v5 ^ v10, 12);
    v0 += v5 + m2; v15 = rotr(v15 ^ v0, 8);  v10 += v15; v5 = rotr(v5 ^ v10, 7);
    v1 += v6 + m5; v12 = rotr(v12 ^ v1, 16); v11 += v12; v6 = rotr(v6 ^ v11, 12);
    v1 += v6 + m3; v12 = rotr(v12 ^ v1, 8);  v11 += v12; v6 = rotr(v6 ^ v11, 7);
    v2 += v7 + m0; v13 = rotr(v13 ^ v2, 16); v8 += v13; v7 = rotr(v7 ^ v8, 12);
    v2 += v7 + m1; v13 = rotr(v13 ^ v2, 8);  v8 += v13; v7 = rotr(v7 ^ v8, 7);
    v3 += v4 + m6; v14 = rotr(v14 ^ v3, 16); v9 += v14; v4 = rotr(v4 ^ v9, 12);
    v3 += v4 + m4; v14 = rotr(v14 ^ v3, 8);  v9 += v14; v4 = rotr(v4 ^ v9, 7);
    /* round 6 */
    v0 += v4 + m9; v12 = rotr(v12 ^ v0, 16); v8 += v12; v4 = rotr(v4 ^ v8, 12);
    v0 += v4 + m14; v12 = rotr(v12 ^ v0, 8);  v8 += v12; v4 = rotr(v4 ^ v8, 7);
    v1 += v5 + m11; v13 = rotr(v13 ^ v1, 16); v9 += v13; v5 = rotr(v5 ^ v9, 12);
    v1 += v5 + m5; v13 = rotr(v13 ^ v1, 8);  v9 += v13; v5 = rotr(v5 ^ v9, 7);
    v2 += v6 + m8; v14 = rotr(v14 ^ v2, 16); v10 += v14; v6 = rotr(v6 ^ v10, 12);
    v2 += v6 + m12; v14 = rotr(v14 ^ v2, 8);  v10 += v14; v6 = rotr(v6 ^ v10, 7);
    v3 += v7 + m15; v15 = rotr(v15 ^ v3, 16); v11 += v15; v7 = rotr(v7 ^ v11, 12);
    v3 += v7 + m1; v15 = rotr(v15 ^ v3, 8);  v11 += v15; v7 = rotr(v7 ^ v11, 7);
    v0 += v5 + m13; v15 = rotr(v15 ^ v0, 16); v10 += v15; v5 = rotr(v5 ^ v10, 12);
    v0 += v5 + m3; v15 = rotr(v15 ^ v0, 8);  v10 += v15; v5 = rotr(v5 ^ v10, 7);
    v1 += v6 + m0; v12 = rotr(v12 ^ v1, 16); v11 += v12; v6 = rotr(v6 ^ v11, 12);
    v1 += v6 + m10; v12 = rotr(v12 ^ v1, 8);  v11 += v12; v6 = rotr(v6 ^ v11, 7);
    v2 += v7 + m2; v13 = rotr(v13 ^ v2, 16); v8 += v13; v7 = rotr(v7 ^ v8, 12);
    v2 += v7 + m6; v13 = rotr(v13 ^ v2, 8);  v8 += v13; v7 = rotr(v7 ^ v8, 7);
    v3 += v4 + m4; v14 = rotr(v14 ^ v3, 16); v9 += v14; v4 = rotr(v4 ^ v9, 12);
    v3 += v4 + m7; v14 = rotr(v14 ^ v3, 8);  v9 += v14; v4 = rotr(v4 ^ v9, 7);
    /* round 7 */
    v0 += v4 + m11; v12 = rotr(v12 ^ v0, 16); v8 += v12; v4 = rotr(v4 ^ v8, 12);
    v0 += v4 + m15; v12 = rotr(v12 ^ v0, 8);  v8 += v12; v4 = rotr(v4 ^ v8, 7);
    v1 += v5 + m5; v13 = rotr(v13 ^ v1, 16); v9 += v13; v5 = rotr(v5 ^ v9, 12);
    v1 += v5 + m0; v13 = rotr(v13 ^ v1, 8);  v9 += v13; v5 = rotr(v5 ^ v9, 7);
    v2 += v6 + m1; v14 = rotr(v14 ^ v2, 16); v10 += v14; v6 = rotr(v6 ^ v10, 12);
    v2 += v6 + m9; v14 = rotr(v14 ^ v2, 8);  v10 += v14; v6 = rotr(v6 ^ v10, 7);
    v3 += v7 + m8; v15 = rotr(v15 ^ v3, 16); v11 += v15; v7 = rotr(v7 ^ v11, 12);
    v3 += v7 + m6; v15 = rotr(v15 ^ v3, 8);  v11 += v15; v7 = rotr(v7 ^ v11, 7);
    v0 += v5 + m14; v15 = rotr(v15 ^ v0, 16); v10 += v15; v5 = rotr(v5 ^ v10, 12);
    v0 += v5 + m10; v15 = rotr(v15 ^ v0, 8);  v10 += v15; v5 = rotr(v5 ^ v10, 7);
    v1 += v6 + m2; v12 = rotr(v12 ^ v1, 16); v11 += v12; v6 = rotr(v6 ^ v11, 12);
    v1 += v6 + m12; v12 = rotr(v12 ^ v1, 8);  v11 += v12; v6 = rotr(v6 ^ v11, 7);
    v2 += v7 + m3; v13 = rotr(v13 ^ v2, 16); v8 += v13; v7 = rotr(v7 ^ v8, 12);
    v2 += v7 + m4; v13 = rotr(v13 ^ v2, 8);  v8 += v13; v7 = rotr(v7 ^ v8, 7);
    v3 += v4 + m7; v14 = rotr(v14 ^ v3, 16); v9 += v14; v4 = rotr(v4 ^ v9, 12);
    v3 += v4 + m13; v14 = rotr(v14 ^ v3, 8);  v9 += v14; v4 = rotr(v4 ^ v9, 7);
    out[0] = v0 ^ v8;  out[1] = v1 ^ v9;  out[2] = v2 ^ v10; out[3] = v3 ^ v11;
    out[4] = v4 ^ v12; out[5] = v5 ^ v13; out[6] = v6 ^ v14; out[7] = v7 ^ v15;
    out[8] = v8 ^ cv[0];  out[9] = v9 ^ cv[1];  out[10] = v10 ^ cv[2]; out[11] = v11 ^ cv[3];
    out[12] = v12 ^ cv[4]; out[13] = v13 ^ cv[5]; out[14] = v14 ^ cv[6]; out[15] = v15 ^ cv[7];
}

static void chunk_init(rrc_blake3_chunk_state *c, uint64_t counter)
{
    memcpy(c->cv, IV, sizeof IV);
    c->chunk_counter = counter;
    c->block_len = 0;
    c->blocks_compressed = 0;
}

static inline size_t chunk_len(const rrc_blake3_chunk_state *c)
{
    return (size_t)c->blocks_compressed * BLOCK_LEN + c->block_len;
}

static inline uint32_t chunk_start_flag(const rrc_blake3_chunk_state *c)
{
    return c->blocks_compressed == 0 ? CHUNK_START : 0;
}

static void chunk_update(rrc_blake3_chunk_state *c, const uint8_t *in, size_t len)
{
    while (len > 0) {
        if (c->block_len == BLOCK_LEN) {
            uint32_t out[16];
            compress(c->cv, c->block, c->chunk_counter, BLOCK_LEN, chunk_start_flag(c), out);
            memcpy(c->cv, out, 32);
            c->blocks_compressed++;
            c->block_len = 0;
        }
        size_t want = BLOCK_LEN - c->block_len;
        size_t take = len < want ? len : want;
        memcpy(c->block + c->block_len, in, take);
        c->block_len += (uint8_t)take;
        in += take; len -= take;
    }
}

/* A pending output: the node that can be finalized as either a chaining value
 * or (with ROOT) the final output. */
typedef struct {
    uint32_t cv[8];
    uint8_t block[BLOCK_LEN];
    uint8_t block_len;
    uint64_t counter;
    uint32_t flags;
} output_t;

static void chunk_output(const rrc_blake3_chunk_state *c, output_t *o)
{
    memcpy(o->cv, c->cv, 32);
    memcpy(o->block, c->block, BLOCK_LEN);
    memset(o->block + c->block_len, 0, BLOCK_LEN - c->block_len);
    o->block_len = c->block_len;
    o->counter = c->chunk_counter;
    o->flags = chunk_start_flag(c) | CHUNK_END;
}

static void output_cv(const output_t *o, uint32_t cv[8])
{
    uint32_t s[16];
    compress(o->cv, o->block, o->counter, o->block_len, o->flags, s);
    memcpy(cv, s, 32);
}

static void parent_output(const uint32_t l[8], const uint32_t r[8], output_t *o)
{
    memcpy(o->cv, IV, 32);
    for (int i = 0; i < 8; i++) { store32(o->block + 4 * i, l[i]); store32(o->block + 32 + 4 * i, r[i]); }
    o->block_len = BLOCK_LEN;
    o->counter = 0;
    o->flags = PARENT;
}

void rrc_blake3_init(rrc_blake3_ctx *h)
{
    chunk_init(&h->chunk, 0);
    h->cv_stack_len = 0;
}

static void add_chunk_cv(rrc_blake3_ctx *h, uint32_t new_cv[8], uint64_t total_chunks)
{
    while ((total_chunks & 1) == 0) {
        output_t o;
        h->cv_stack_len--;
        parent_output(h->cv_stack[h->cv_stack_len], new_cv, &o);
        output_cv(&o, new_cv);
        total_chunks >>= 1;
    }
    memcpy(h->cv_stack[h->cv_stack_len++], new_cv, 32);
}

void rrc_blake3_update(rrc_blake3_ctx *h, const void *data, size_t len)
{
    const uint8_t *in = (const uint8_t *)data;
    while (len > 0) {
        if (chunk_len(&h->chunk) == CHUNK_LEN) {
            output_t o;
            uint32_t cv[8];
            chunk_output(&h->chunk, &o);
            output_cv(&o, cv);
            uint64_t total = h->chunk.chunk_counter + 1;
            add_chunk_cv(h, cv, total);
            chunk_init(&h->chunk, total);
        }
        size_t want = CHUNK_LEN - chunk_len(&h->chunk);
        size_t take = len < want ? len : want;
        chunk_update(&h->chunk, in, take);
        in += take; len -= take;
    }
}

void rrc_blake3_final(const rrc_blake3_ctx *h, uint8_t out[RRC_BLAKE3_OUT_LEN])
{
    output_t o;
    chunk_output(&h->chunk, &o);
    int remaining = h->cv_stack_len;
    while (remaining > 0) {
        remaining--;
        uint32_t cv[8];
        output_cv(&o, cv);
        parent_output(h->cv_stack[remaining], cv, &o);
    }
    uint32_t s[16];
    compress(o.cv, o.block, 0, o.block_len, o.flags | ROOT, s);
    for (int i = 0; i < 8; i++) store32(out + 4 * i, s[i]);
}

void rrc_blake3(const void *data, size_t len, uint8_t out[RRC_BLAKE3_OUT_LEN])
{
    rrc_blake3_ctx h;
    rrc_blake3_init(&h);
    rrc_blake3_update(&h, data, len);
    rrc_blake3_final(&h, out);
}

void rrc_blake3_final_hex(const rrc_blake3_ctx *h, char out[RRC_BLAKE3_HEX_LEN + 1])
{
    static const char hx[] = "0123456789abcdef";
    uint8_t d[32];
    rrc_blake3_final(h, d);
    for (int i = 0; i < 32; i++) { out[2 * i] = hx[d[i] >> 4]; out[2 * i + 1] = hx[d[i] & 15]; }
    out[64] = 0;
}

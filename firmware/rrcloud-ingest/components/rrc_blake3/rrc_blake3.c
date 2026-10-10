#include "rrc_blake3.h"
#include <string.h>

#define CHUNK_LEN 1024
#define BLOCK_LEN 64

enum { CHUNK_START = 1, CHUNK_END = 2, PARENT = 4, ROOT = 8 };

static const uint32_t IV[8] = {0x6A09E667, 0xBB67AE85, 0x3C6EF372, 0xA54FF53A,
                               0x510E527F, 0x9B05688C, 0x1F83D9AB, 0x5BE0CD19};
static const uint8_t MSG_PERMUTATION[16] = {2, 6, 3, 10, 7, 0, 4, 13, 1, 11, 12, 5, 9, 14, 15, 8};

static inline uint32_t rotr(uint32_t x, int n) { return (x >> n) | (x << (32 - n)); }
static inline uint32_t load32(const uint8_t *p)
{
    return (uint32_t)p[0] | ((uint32_t)p[1] << 8) | ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24);
}
static inline void store32(uint8_t *p, uint32_t v)
{
    p[0] = (uint8_t)v; p[1] = (uint8_t)(v >> 8); p[2] = (uint8_t)(v >> 16); p[3] = (uint8_t)(v >> 24);
}

static inline void g(uint32_t *s, int a, int b, int c, int d, uint32_t mx, uint32_t my)
{
    s[a] = s[a] + s[b] + mx; s[d] = rotr(s[d] ^ s[a], 16);
    s[c] = s[c] + s[d];      s[b] = rotr(s[b] ^ s[c], 12);
    s[a] = s[a] + s[b] + my; s[d] = rotr(s[d] ^ s[a], 8);
    s[c] = s[c] + s[d];      s[b] = rotr(s[b] ^ s[c], 7);
}

static void round_fn(uint32_t *s, const uint32_t *m)
{
    g(s, 0, 4, 8, 12, m[0], m[1]);   g(s, 1, 5, 9, 13, m[2], m[3]);
    g(s, 2, 6, 10, 14, m[4], m[5]);  g(s, 3, 7, 11, 15, m[6], m[7]);
    g(s, 0, 5, 10, 15, m[8], m[9]);  g(s, 1, 6, 11, 12, m[10], m[11]);
    g(s, 2, 7, 8, 13, m[12], m[13]); g(s, 3, 4, 9, 14, m[14], m[15]);
}

static void compress(const uint32_t cv[8], const uint8_t block[BLOCK_LEN], uint64_t counter,
                     uint32_t block_len, uint32_t flags, uint32_t out[16])
{
    uint32_t m[16], p[16];
    for (int i = 0; i < 16; i++) m[i] = load32(block + 4 * i);
    uint32_t s[16] = {cv[0], cv[1], cv[2], cv[3], cv[4], cv[5], cv[6], cv[7],
                      IV[0], IV[1], IV[2], IV[3], (uint32_t)counter, (uint32_t)(counter >> 32), block_len, flags};
    for (int r = 0; r < 7; r++) {
        round_fn(s, m);
        if (r == 6) break;
        for (int i = 0; i < 16; i++) p[i] = m[MSG_PERMUTATION[i]];
        memcpy(m, p, sizeof m);
    }
    for (int i = 0; i < 8; i++) { s[i] ^= s[i + 8]; s[i + 8] ^= cv[i]; }
    memcpy(out, s, 64);
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

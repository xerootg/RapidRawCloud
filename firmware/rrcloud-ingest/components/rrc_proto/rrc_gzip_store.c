#include "rrc_proto.h"
#include "rrc_hash.h"
#include <string.h>

/* RFC 1952 member with RFC 1951 "stored" (BTYPE=00) blocks only. Any inflater
 * (flate2's MultiGzDecoder on the readers, gzip(1), browsers) accepts it; the
 * manifest is small and written rarely, so compression ratio is irrelevant and
 * a 20-line writer beats a zlib dependency on the device. */

static int emit(rrc_gzip_store *g, const void *d, size_t n)
{
    if (g->err) return g->err;
    int rc = g->sink(g->sink_ctx, (const uint8_t *)d, n);
    if (rc) g->err = rc;
    return rc;
}

int rrc_gzip_store_begin(rrc_gzip_store *g, rrc_gzip_sink sink, void *ctx)
{
    memset(g, 0, sizeof *g);
    g->sink = sink; g->sink_ctx = ctx;
    /* ID1 ID2 CM=8 FLG=0 MTIME=0 XFL=0 OS=255(unknown) */
    static const uint8_t hdr[10] = {0x1f, 0x8b, 0x08, 0x00, 0, 0, 0, 0, 0x00, 0xff};
    return emit(g, hdr, sizeof hdr);
}

int rrc_gzip_store_write(rrc_gzip_store *g, const void *data, size_t len)
{
    const uint8_t *p = (const uint8_t *)data;
    while (len > 0) {
        size_t n = len > 65535 ? 65535 : len;
        uint8_t bh[5] = {0x00, (uint8_t)n, (uint8_t)(n >> 8), (uint8_t)~(uint8_t)n, (uint8_t)~(uint8_t)(n >> 8)};
        if (emit(g, bh, 5) || emit(g, p, n)) return g->err;
        g->crc = rrc_crc32_update(g->crc, p, n);
        g->isize += (uint32_t)n;
        p += n; len -= n;
    }
    return 0;
}

int rrc_gzip_store_end(rrc_gzip_store *g)
{
    static const uint8_t final_block[5] = {0x01, 0x00, 0x00, 0xff, 0xff};
    if (emit(g, final_block, 5)) return g->err;
    uint8_t tr[8] = {(uint8_t)g->crc, (uint8_t)(g->crc >> 8), (uint8_t)(g->crc >> 16), (uint8_t)(g->crc >> 24),
                     (uint8_t)g->isize, (uint8_t)(g->isize >> 8), (uint8_t)(g->isize >> 16), (uint8_t)(g->isize >> 24)};
    return emit(g, tr, 8);
}

#include "rrc_proto.h"
#include "rrcloud_proto.h"
#include <string.h>
#include <stdio.h>

static void cp(char *dst, size_t cap, const char *src) { snprintf(dst, cap, "%s", src ? src : ""); }

int rrc_manifest_header_encode(int64_t written_server_ts, const char *self_device, uint64_t published_cursor, char *out, size_t cap)
{
    rrcp_manifest_header_t h;
    rrcp_manifest_header_init(&h);
    h.written_server_ts = written_server_ts;
    h.proto = RRCP_MANIFEST_PROTO;
    if (published_cursor && rrcp_applied_cursors_set(&h.cursors, self_device, published_cursor) != RRCP_OK) return -1;
    int n = rrcp_manifest_header_encode(&h, out, cap);
    return n < 0 ? -1 : n;
}

int rrc_manifest_row_encode(const rrc_manifest_row_original *r, char *out, size_t cap)
{
    if (!r->relkey || !r->blake3_hex || !r->device || !r->content_id_hex || r->vv_self == 0) return -1;
    rrcp_manifest_row_t row;
    rrcp_manifest_row_init(&row);
    cp(row.key, sizeof row.key, r->relkey);
    row.kind = RRCP_KIND_ORIGINAL;
    row.size = r->size;
    row.has_blake3 = true; cp(row.blake3, sizeof row.blake3, r->blake3_hex);
    if (rrcp_version_vector_set(&row.vv, r->device, r->vv_self) != RRCP_OK) return -1;
    row.has_device = true; cp(row.device, sizeof row.device, r->device);
    row.has_content_id = true; cp(row.content_id, sizeof row.content_id, r->content_id_hex);
    if (r->has_mtime) { row.has_mtime = true; row.mtime = r->mtime; }
    row.has_ts = true; row.ts = r->ts;
    int n = rrcp_manifest_row_encode(&row, out, cap);
    return n < 0 ? -1 : n;
}

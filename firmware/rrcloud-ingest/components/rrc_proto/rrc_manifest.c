#include "rrc_proto.h"
#include <string.h>

int rrc_manifest_header_encode(int64_t written_server_ts, const char *self_device, uint64_t published_cursor, char *out, size_t cap)
{
    rrc_jsonw w;
    rrc_jsonw_init(&w, out, cap);
    rrc_jsonw_raw(&w, "{");
    rrc_jsonw_key(&w, "written_server_ts", true); rrc_jsonw_i64(&w, written_server_ts);
    rrc_jsonw_key(&w, "cursors", false); rrc_jsonw_raw(&w, "{");
    if (published_cursor) { rrc_jsonw_key(&w, self_device, true); rrc_jsonw_u64(&w, published_cursor); }
    rrc_jsonw_raw(&w, "}");
    rrc_jsonw_key(&w, "proto", false); rrc_jsonw_u64(&w, RRC_MANIFEST_PROTO);
    rrc_jsonw_raw(&w, "}");
    return rrc_jsonw_finish(&w);
}

int rrc_manifest_row_encode(const rrc_manifest_row_original *r, char *out, size_t cap)
{
    if (!r->relkey || !r->blake3_hex || !r->device || !r->content_id_hex || r->vv_self == 0) return -1;
    rrc_jsonw w;
    rrc_jsonw_init(&w, out, cap);
    rrc_jsonw_raw(&w, "{");
    rrc_jsonw_key(&w, "key", true); rrc_jsonw_str(&w, r->relkey);
    rrc_jsonw_key(&w, "kind", false); rrc_jsonw_str(&w, "original");
    rrc_jsonw_key(&w, "size", false); rrc_jsonw_u64(&w, r->size);
    rrc_jsonw_key(&w, "blake3", false); rrc_jsonw_str(&w, r->blake3_hex);
    rrc_jsonw_key(&w, "vv", false); rrc_jsonw_raw(&w, "{"); rrc_jsonw_key(&w, r->device, true); rrc_jsonw_u64(&w, r->vv_self); rrc_jsonw_raw(&w, "}");
    rrc_jsonw_key(&w, "device", false); rrc_jsonw_str(&w, r->device);
    rrc_jsonw_key(&w, "content_id", false); rrc_jsonw_str(&w, r->content_id_hex);
    if (r->has_mtime) { rrc_jsonw_key(&w, "mtime", false); rrc_jsonw_i64(&w, r->mtime); }
    rrc_jsonw_key(&w, "ts", false); rrc_jsonw_i64(&w, r->ts);
    rrc_jsonw_raw(&w, "}");
    return rrc_jsonw_finish(&w);
}

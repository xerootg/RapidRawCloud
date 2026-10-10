#include "rrc_proto.h"
#include <string.h>

static void vv_single(rrc_jsonw *w, const char *device, uint32_t count)
{
    rrc_jsonw_raw(w, "{");
    rrc_jsonw_key(w, device, true);
    rrc_jsonw_u64(w, count);
    rrc_jsonw_raw(w, "}");
}

int rrc_journal_encode_put_original(const rrc_journal_put_original *e, char *out, size_t cap)
{
    if (!e->device || !e->bucket_key || !e->blake3_hex || !e->content_id_hex || e->vv_self == 0) return -1;
    if (strlen(e->blake3_hex) != 64 || strlen(e->content_id_hex) != 64) return -1;
    rrc_jsonw w;
    rrc_jsonw_init(&w, out, cap);
    rrc_jsonw_raw(&w, "{");
    rrc_jsonw_key(&w, "v", true); rrc_jsonw_u64(&w, RRC_JOURNAL_VERSION);
    rrc_jsonw_key(&w, "seq", false); rrc_jsonw_u64(&w, e->seq);
    rrc_jsonw_key(&w, "ts", false); rrc_jsonw_i64(&w, e->ts);
    rrc_jsonw_key(&w, "device", false); rrc_jsonw_str(&w, e->device);
    rrc_jsonw_key(&w, "op", false); rrc_jsonw_str(&w, "put");
    rrc_jsonw_key(&w, "kind", false); rrc_jsonw_str(&w, "original");
    rrc_jsonw_key(&w, "key", false); rrc_jsonw_str(&w, e->bucket_key);
    rrc_jsonw_key(&w, "vv", false); vv_single(&w, e->device, e->vv_self);
    rrc_jsonw_key(&w, "size", false); rrc_jsonw_u64(&w, e->size);
    rrc_jsonw_key(&w, "blake3", false); rrc_jsonw_str(&w, e->blake3_hex);
    rrc_jsonw_key(&w, "content_id", false); rrc_jsonw_str(&w, e->content_id_hex);
    if (e->has_mtime) { rrc_jsonw_key(&w, "mtime", false); rrc_jsonw_i64(&w, e->mtime); }
    rrc_jsonw_raw(&w, "}");
    return rrc_jsonw_finish(&w);
}

int rrc_device_entry_encode(const rrc_device_entry *d, char *out, size_t cap)
{
    rrc_jsonw w;
    rrc_jsonw_init(&w, out, cap);
    rrc_jsonw_raw(&w, "{");
    rrc_jsonw_key(&w, "name", true); rrc_jsonw_str(&w, d->name);
    rrc_jsonw_key(&w, "platform", false); rrc_jsonw_str(&w, d->platform);
    rrc_jsonw_key(&w, "created", false); rrc_jsonw_i64(&w, d->created);
    rrc_jsonw_key(&w, "last_seen_server_ts", false); rrc_jsonw_i64(&w, d->last_seen_server_ts);
    rrc_jsonw_key(&w, "applied", false); rrc_jsonw_raw(&w, "{");
    for (size_t i = 0; i < d->n_applied; i++) { rrc_jsonw_key(&w, d->applied_devices[i], i == 0); rrc_jsonw_u64(&w, d->applied_seqs[i]); }
    rrc_jsonw_raw(&w, "}");
    rrc_jsonw_key(&w, "proto", false); rrc_jsonw_raw(&w, "{\"read\":[1],\"write\":1}");
    rrc_jsonw_raw(&w, "}");
    return rrc_jsonw_finish(&w);
}

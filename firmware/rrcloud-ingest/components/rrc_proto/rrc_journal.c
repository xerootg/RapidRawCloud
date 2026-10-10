/* Thin adapters over the generated protocol SDK (protocol/gen/c): the firmware's
 * convenience structs are filled into the generated records, which do the encoding. */
#include "rrc_proto.h"
#include "rrcloud_proto.h"
#include <string.h>
#include <stdio.h>

static void cp(char *dst, size_t cap, const char *src) { snprintf(dst, cap, "%s", src ? src : ""); }

int rrc_journal_encode_put_original(const rrc_journal_put_original *e, char *out, size_t cap)
{
    if (!e->device || !e->bucket_key || !e->blake3_hex || !e->content_id_hex || e->vv_self == 0) return -1;
    rrcp_journal_entry_t j;
    rrcp_journal_entry_init(&j);
    j.v = RRCP_JOURNAL_VERSION;
    j.seq = e->seq;
    j.ts = e->ts;
    cp(j.device, sizeof j.device, e->device);
    j.op = RRCP_OP_PUT;
    j.kind = RRCP_KIND_ORIGINAL;
    cp(j.key, sizeof j.key, e->bucket_key);
    if (rrcp_version_vector_set(&j.vv, e->device, e->vv_self) != RRCP_OK) return -1;
    j.has_size = true; j.size = e->size;
    j.has_blake3 = true; cp(j.blake3, sizeof j.blake3, e->blake3_hex);
    j.has_content_id = true; cp(j.content_id, sizeof j.content_id, e->content_id_hex);
    if (e->has_mtime) { j.has_mtime = true; j.mtime = e->mtime; }
    int n = rrcp_journal_entry_encode(&j, out, cap);
    return n < 0 ? -1 : n;
}

int rrc_device_entry_encode(const rrc_device_entry *d, char *out, size_t cap)
{
    rrcp_device_entry_t de;
    rrcp_device_entry_init(&de);
    cp(de.name, sizeof de.name, d->name);
    cp(de.platform, sizeof de.platform, d->platform);
    de.created = d->created;
    de.last_seen_server_ts = d->last_seen_server_ts;
    for (size_t i = 0; i < d->n_applied; i++) {
        if (rrcp_applied_cursors_set(&de.applied, d->applied_devices[i], d->applied_seqs[i]) != RRCP_OK) return -1;
    }
    de.proto.read_len = RRCP_PROTO_READ_LEN;
    for (size_t i = 0; i < RRCP_PROTO_READ_LEN; i++) de.proto.read[i] = RRCP_PROTO_READ[i];
    de.proto.write = RRCP_PROTO_WRITE;
    int n = rrcp_device_entry_encode(&de, out, cap);
    return n < 0 ? -1 : n;
}

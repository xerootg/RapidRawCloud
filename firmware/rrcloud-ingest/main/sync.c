#include "sync.h"
#include "util.h"
#include <stdarg.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include "esp_log.h"
#include "esp_timer.h"
#include "esp_heap_caps.h"
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/queue.h"
#include "freertos/semphr.h"
#include "rrc_s3_client.h"
#include "rrc_blake3.h"
#include "rrc_hash.h"
#include "rrc_glob.h"
#include "rrc_proto.h"
#include "app_config.h"
#include "store.h"
#include "net.h"
#include "log_ring.h"

static const char *TAG = "sync";

#define PART_BUF_BYTES   (8u * 1024u * 1024u)   /* one multipart part / max single PUT */
#define READ_CHUNK       (256u * 1024u)
#define SEGMENT_FREEZE_ENTRIES 900
#define SEGMENT_FREEZE_BYTES   (900u * 1024u)
#define HEARTBEAT_ACTIVE_S  (15 * 60)
#define HEARTBEAT_IDLE_S    (60 * 60)
#define COMPACT_CHECK_S     (24 * 3600)
#define MANIFEST_RECONFIRM_S (24 * 3600)   /* §2.10 rule 2 */
#define LAGGARD_CAP_S       (14 * 24 * 3600) /* §2.10 rule 3 */

typedef enum { CMD_ATTACH, CMD_DETACH, CMD_SYNC_NOW, CMD_CANCEL, CMD_TICK, CMD_CONFIG } cmd_t;
typedef struct { cmd_t cmd; cam_source_t *src; } msg_t;

static QueueHandle_t q;
static SemaphoreHandle_t st_mtx;
static sync_status_t st;
static rrc_s3_t s3;
static bool s3_ready;
static cam_source_t *cam;             /* attached camera (or NULL) */
static volatile bool cancel_flag;
static uint8_t *part_buf;
static esp_timer_handle_t tick_timer;
static int64_t last_tick_pending_check;

#define ST_LOCK() xSemaphoreTake(st_mtx, portMAX_DELAY)
#define ST_UNLOCK() xSemaphoreGive(st_mtx)

static void set_phase(sync_phase_t p) { ST_LOCK(); st.phase = p; ST_UNLOCK(); }
static void set_error(const char *fmt, ...) __attribute__((format(printf, 1, 2)));
static void set_error(const char *fmt, ...)
{
    char buf[160];
    va_list ap; va_start(ap, fmt); vsnprintf(buf, sizeof buf, fmt, ap); va_end(ap);
    ST_LOCK(); scpy(st.last_error, sizeof st.last_error, buf); ST_UNLOCK();
    log_ring_printf("error: %s", buf);
}

/* ---- S3 configuration ----------------------------------------------------- */
static void configure_s3(void)
{
    const app_config_t *c = app_config_get();
    rrc_s3_config_t cfg = {0};
    scpy(cfg.endpoint, sizeof cfg.endpoint, c->s3_endpoint);
    scpy(cfg.bucket, sizeof cfg.bucket, c->s3_bucket);
    scpy(cfg.region, sizeof cfg.region, c->s3_region);
    scpy(cfg.access_key, sizeof cfg.access_key, c->s3_access_key);
    app_config_get_secret(cfg.secret_key, sizeof cfg.secret_key);
    cfg.tls_insecure = c->s3_tls_insecure;
    cfg.timeout_ms = 60000;
    bool was = s3_ready;
    bool had_offset = s3.have_server_offset;
    int64_t off = s3.server_offset_s;
    s3_ready = rrc_s3_init(&s3, &cfg) == ESP_OK;
    if (s3_ready && had_offset) { s3.have_server_offset = true; s3.server_offset_s = off; }
    /* digest-probe result is per backend; remembered in the store */
    char key[80];
    uint32_t h = rrc_crc32(cfg.endpoint, strlen(cfg.endpoint)) ^ rrc_crc32(cfg.bucket, strlen(cfg.bucket));
    snprintf(key, sizeof key, "digest_%08x", (unsigned)h);
    char v[8];
    if (store_kv_get_str(key, v, sizeof v)) { s3.digest_check_known = true; s3.backend_rejects_bad_md5 = v[0] == '1'; }
    ST_LOCK();
    st.s3_ready = s3_ready;
    st.digest_check_known = s3.digest_check_known;
    st.backend_rejects_bad_md5 = s3.backend_rejects_bad_md5;
    if (st.phase == SYNC_UNCONFIGURED || !s3_ready) st.phase = s3_ready ? SYNC_IDLE : SYNC_UNCONFIGURED;
    ST_UNLOCK();
    if (s3_ready != was) log_ring_printf(s3_ready ? "cloud configured: %s/%s" : "cloud not configured", cfg.endpoint, cfg.bucket);
}

static void run_digest_probe_if_needed(void)
{
    if (!s3_ready || s3.digest_check_known) return;
    if (rrc_s3_probe_digest_check(&s3, app_device_id()) == ESP_OK) {
        char key[80];
        uint32_t h = rrc_crc32(s3.cfg.endpoint, strlen(s3.cfg.endpoint)) ^ rrc_crc32(s3.cfg.bucket, strlen(s3.cfg.bucket));
        snprintf(key, sizeof key, "digest_%08x", (unsigned)h);
        store_kv_set_str(key, s3.backend_rejects_bad_md5 ? "1" : "0");
        log_ring_printf("backend %s a wrong Content-MD5", s3.backend_rejects_bad_md5 ? "rejects" : "ACCEPTS");
        ST_LOCK(); st.digest_check_known = true; st.backend_rejects_bad_md5 = s3.backend_rejects_bad_md5; ST_UNLOCK();
    }
}

/* ---- journal / registry / manifest ---------------------------------------- */
static esp_err_t publish_frozen_segments(void)
{
    uint64_t seqs[32];
    int n = store_journal_frozen_list(seqs, 32);
    for (int i = 0; i < n; i++) {
        uint8_t *bytes; size_t len; uint64_t max_seq;
        if (store_journal_frozen_read(seqs[i], &bytes, &len, &max_seq) != ESP_OK) continue;
        char key[160];
        rrc_key_journal_segment(app_device_id(), seqs[i], key, sizeof key);
        rrc_s3_result_t res;
        esp_err_t e = rrc_s3_put(&s3, key, bytes, len, "application/x-ndjson", NULL, 0, &res);
        free(bytes);
        if (e != ESP_OK) { char b[96]; set_error("segment %016llx publish failed: %s", (unsigned long long)seqs[i], rrc_s3_result_str(&res, b, sizeof b)); return e; }
        int64_t ts = res.server_date > 0 ? res.server_date : rrc_s3_server_now(&s3);
        store_journal_mark_published(seqs[i], max_seq, ts);
        log_ring_printf("journal segment %016llx..%llu published", (unsigned long long)seqs[i], (unsigned long long)max_seq);
    }
    return ESP_OK;
}

static esp_err_t flush_journal(void)
{
    if (store_journal_pending_entries() > 0) {
        uint64_t first;
        if (store_journal_freeze(&first) != ESP_OK) return ESP_FAIL;
    }
    return publish_frozen_segments();
}

static esp_err_t heartbeat(void)
{
    const app_config_t *c = app_config_get();
    int64_t created = app_device_created();
    int64_t now = rrc_s3_server_now(&s3);
    if (created <= 0) created = now;
    rrc_device_entry d = {.name = c->device_name[0] ? c->device_name : "Camera dock", .platform = "esp32", .created = created, .last_seen_server_ts = now};
    char body[512];
    int n = rrc_device_entry_encode(&d, body, sizeof body);
    if (n < 0) return ESP_FAIL;
    char key[160];
    rrc_key_device_registry(app_device_id(), key, sizeof key);
    rrc_s3_result_t res;
    esp_err_t e = rrc_s3_put(&s3, key, body, (size_t)n, "application/json", NULL, 0, &res);
    if (e != ESP_OK) { char b[96]; set_error("heartbeat failed: %s", rrc_s3_result_str(&res, b, sizeof b)); return e; }
    ST_LOCK(); st.last_heartbeat_ts = now; ST_UNLOCK();
    return ESP_OK;
}

typedef struct { uint8_t *buf; size_t len, cap; } growbuf_t;
static int grow_sink(void *ctx, const uint8_t *d, size_t n)
{
    growbuf_t *g = ctx;
    if (g->len + n > g->cap) {
        size_t nc = g->cap ? g->cap * 2 : 65536;
        while (nc < g->len + n) nc *= 2;
        uint8_t *nb = heap_caps_realloc(g->buf, nc, MALLOC_CAP_SPIRAM | MALLOC_CAP_8BIT);
        if (!nb) return -1;
        g->buf = nb; g->cap = nc;
    }
    memcpy(g->buf + g->len, d, n); g->len += n;
    return 0;
}

typedef struct { rrc_gzip_store *gz; int err; char line[2048]; } manifest_ctx_t;
static int manifest_row(void *ctx, const rrc_ledger_rec *r)
{
    manifest_ctx_t *m = ctx;
    rrc_manifest_row_original row = {.relkey = r->relkey, .size = r->size, .blake3_hex = r->blake3_hex, .device = app_device_id(), .vv_self = 1,
                                     .content_id_hex = r->blake3_hex, .has_mtime = r->mtime > 0, .mtime = r->mtime, .ts = r->ts};
    int n = rrc_manifest_row_encode(&row, m->line, sizeof m->line);
    if (n < 0) return 0; /* skip unencodable row */
    m->line[n++] = '\n';
    if (rrc_gzip_store_write(m->gz, m->line, (size_t)n)) { m->err = 1; return 1; }
    return 0;
}

static esp_err_t publish_manifest(void)
{
    growbuf_t gb = {0};
    rrc_gzip_store gz;
    rrc_gzip_store_begin(&gz, grow_sink, &gb);
    int64_t now = rrc_s3_server_now(&s3);
    uint64_t cursor = store_journal_published_cursor();
    char line[256];
    int n = rrc_manifest_header_encode(now, app_device_id(), cursor, line, sizeof line);
    line[n++] = '\n';
    rrc_gzip_store_write(&gz, line, (size_t)n);
    manifest_ctx_t mc = {.gz = &gz};
    store_ledger_foreach_uploaded(manifest_row, &mc);
    rrc_gzip_store_end(&gz);
    if (gz.err || mc.err) { free(gb.buf); return ESP_ERR_NO_MEM; }
    char key[160];
    rrc_key_manifest(app_device_id(), key, sizeof key);
    rrc_s3_result_t res;
    esp_err_t e = rrc_s3_put(&s3, key, gb.buf, gb.len, "application/gzip", NULL, 0, &res);
    free(gb.buf);
    if (e != ESP_OK) { char b[96]; set_error("manifest publish failed: %s", rrc_s3_result_str(&res, b, sizeof b)); return e; }
    int64_t ts = res.server_date > 0 ? res.server_date : now;
    store_kv_set_str("manifest_etag", res.etag);
    store_kv_set_i64("manifest_ts", ts);
    store_kv_set_u64("manifest_cursor", cursor);
    ST_LOCK(); st.last_manifest_ts = ts; ST_UNLOCK();
    log_ring_printf("manifest published (%u rows, %u bytes gz, cursor %llu)", (unsigned)store_ledger_uploaded_count(), (unsigned)gb.len, (unsigned long long)cursor);
    return ESP_OK;
}

/* §2.10 segment compaction, own prefix only: rule 1 (manifest covers), rule 2
 * (manifest ≥ 24 h old and re-confirmed by HEAD with matching ETag), rule 3
 * via the 14-day laggard cap (this participant reads no peer registry, so the
 * fast path is never taken — segments live at least 14 days). */
static void compact_segments(void)
{
    int64_t mts = store_kv_get_i64("manifest_ts", 0);
    uint64_t mcursor = store_kv_get_u64("manifest_cursor", 0);
    char metag[80];
    if (!mts || !store_kv_get_str("manifest_etag", metag, sizeof metag)) return;
    int64_t now = rrc_s3_server_now(&s3);
    if (now - mts < MANIFEST_RECONFIRM_S) return;
    store_segment_t segs[64];
    int n = store_segments_list(segs, 64);
    bool any = false;
    for (int i = 0; i < n; i++) {
        if (segs[i].max_seq > mcursor) continue;                         /* rule 1 */
        if (segs[i].published_server_ts <= 0 || now - segs[i].published_server_ts <= LAGGARD_CAP_S) continue; /* rule 3 (cap) */
        any = true; break;
    }
    if (!any) return;
    char key[160];
    rrc_key_manifest(app_device_id(), key, sizeof key);
    rrc_s3_result_t res;
    if (rrc_s3_head(&s3, key, &res) != ESP_OK || res.status != 200 || strcmp(res.etag, metag) != 0) {   /* rule 2 re-confirm */
        log_ring_printf("compaction: manifest read-back mismatch; skipping this pass");
        return;
    }
    for (int i = 0; i < n; i++) {
        if (segs[i].max_seq > mcursor) continue;
        if (segs[i].published_server_ts <= 0 || now - segs[i].published_server_ts <= LAGGARD_CAP_S) continue;
        rrc_key_journal_segment(app_device_id(), segs[i].first_seq, key, sizeof key);
        if (rrc_s3_delete(&s3, key, &res) == ESP_OK) {
            store_segments_remove(segs[i].first_seq);
            log_ring_printf("compacted journal segment %016llx", (unsigned long long)segs[i].first_seq);
        }
    }
}

/* ---- upload ---------------------------------------------------------------- */
typedef struct { rrc_blake3_ctx b3; uint64_t n; } hash_sink_t;
static int hash_sink(void *ctx, const uint8_t *d, size_t n) { hash_sink_t *h = ctx; rrc_blake3_update(&h->b3, d, n); h->n += n; return 0; }

/* Uploads one camera object to `key`; fills blake3 hex. Returns ESP_OK when the
 * object is verified in the bucket. */
static esp_err_t upload_object(cam_source_t *src, const cam_object_t *o, const char *key, char blake3_hex[65], uint64_t *out_size)
{
    void *fh = NULL;
    esp_err_t e = src->open(src, o, &fh);
    if (e != ESP_OK) return e;
    rrc_kv meta[1] = {{"rrc-device", app_device_id()}};
    rrc_blake3_ctx b3;
    rrc_blake3_init(&b3);
    bool multipart = o->size > PART_BUF_BYTES;
    char upload_id[160] = "";
    char (*etags)[40] = NULL;
    int parts = 0;
    rrc_s3_result_t res = {0};
    uint64_t total = 0;
    char single_md5_hex[33] = "";
    if (multipart) {
        e = rrc_s3_multipart_create(&s3, key, "application/octet-stream", meta, 1, upload_id, sizeof upload_id, &res);
        if (e != ESP_OK) { char b[96]; set_error("multipart create failed: %s", rrc_s3_result_str(&res, b, sizeof b)); goto fail; }
        etags = heap_caps_malloc(1024 * 40, MALLOC_CAP_SPIRAM | MALLOC_CAP_8BIT);
        if (!etags) { e = ESP_ERR_NO_MEM; goto fail; }
    }
    for (;;) {
        /* fill one part */
        size_t filled = 0;
        while (filled < PART_BUF_BYTES && total + filled < o->size) {
            if (cancel_flag || !src->connected(src)) { e = ESP_ERR_INVALID_STATE; goto fail; }
            size_t want = PART_BUF_BYTES - filled;
            if (want > READ_CHUNK) want = READ_CHUNK;
            if (want > o->size - total - filled) want = (size_t)(o->size - total - filled);
            size_t got = 0;
            e = src->read(src, fh, total + filled, part_buf + filled, want, &got);
            if (e != ESP_OK) { set_error("camera read failed at %llu: %s", (unsigned long long)(total + filled), esp_err_to_name(e)); goto fail; }
            if (got == 0) break; /* unexpected EOF */
            filled += got;
            ST_LOCK(); st.current_done = total + filled; ST_UNLOCK();
        }
        if (filled == 0 && total > 0) break;
        rrc_blake3_update(&b3, part_buf, filled);
        if (multipart) {
            if (parts >= 1024) { e = ESP_ERR_INVALID_SIZE; goto fail; }
            e = rrc_s3_upload_part(&s3, key, upload_id, parts + 1, part_buf, filled, etags[parts], 40, &res);
            if (e != ESP_OK) { char b[96]; set_error("part %d failed: %s", parts + 1, rrc_s3_result_str(&res, b, sizeof b)); goto fail; }
            parts++;
        } else {
            e = rrc_s3_put(&s3, key, part_buf, filled, "application/octet-stream", meta, 1, &res);
            if (e != ESP_OK) { char b[96]; set_error("put failed: %s", rrc_s3_result_str(&res, b, sizeof b)); goto fail; }
            uint8_t d[16];
            rrc_md5(part_buf, filled, d);
            rrc_hex_lower(d, 16, single_md5_hex);
        }
        total += filled;
        if (total >= o->size || filled < PART_BUF_BYTES) break;
    }
    if (total != o->size) { set_error("short read from camera: %llu of %llu bytes", (unsigned long long)total, (unsigned long long)o->size); e = ESP_FAIL; goto fail; }
    if (multipart) {
        const char *etag_ptrs[1024];
        for (int i = 0; i < parts; i++) etag_ptrs[i] = etags[i];
        e = rrc_s3_multipart_complete(&s3, key, upload_id, etag_ptrs, parts, &res);
        if (e != ESP_OK) { char b[96]; set_error("multipart complete failed: %s", rrc_s3_result_str(&res, b, sizeof b)); goto fail; }
        upload_id[0] = 0;
    }
    rrc_blake3_final_hex(&b3, blake3_hex);
    /* §2.4 verifying: HEAD size; single-part ETag == MD5; multipart without a
     * digest-checking backend → full read-back re-hash. */
    rrc_s3_result_t h;
    if (rrc_s3_head(&s3, key, &h) != ESP_OK || h.status != 200) { set_error("verify HEAD failed (%d)", h.status); e = ESP_FAIL; goto fail_nomp; }
    if (h.content_length >= 0 && (uint64_t)h.content_length != total) { set_error("verify size mismatch %lld != %llu", (long long)h.content_length, (unsigned long long)total); e = ESP_FAIL; goto fail_nomp; }
    if (!multipart) {
        if (strcasecmp(h.etag, single_md5_hex) != 0) { set_error("verify ETag != MD5 (%s vs %s)", h.etag, single_md5_hex); e = ESP_FAIL; goto fail_nomp; }
    } else if (!s3.digest_check_known || !s3.backend_rejects_bad_md5) {
        hash_sink_t hs = {0};
        rrc_blake3_init(&hs.b3);
        rrc_s3_result_t gr;
        if (rrc_s3_get_stream(&s3, key, hash_sink, &hs, &gr) != ESP_OK) { set_error("read-back failed (%d)", gr.status); e = ESP_FAIL; goto fail_nomp; }
        char rb[65];
        rrc_blake3_final_hex(&hs.b3, rb);
        if (hs.n != total || strcmp(rb, blake3_hex) != 0) { set_error("read-back blake3 mismatch"); e = ESP_FAIL; goto fail_nomp; }
    }
    *out_size = total;
    free(etags);
    src->close(src, fh);
    return ESP_OK;
fail:
    if (multipart && upload_id[0]) { rrc_s3_result_t ar; rrc_s3_multipart_abort(&s3, key, upload_id, &ar); }
fail_nomp:
    free(etags);
    if (fh) src->close(src, fh);
    return e == ESP_OK ? ESP_FAIL : e;
}

/* ---- enumeration ----------------------------------------------------------- */
typedef struct { cam_object_t *items; size_t n, cap; cam_source_t *src; } enum_ctx_t;

static int enum_cb(void *ctx, const cam_object_t *o)
{
    enum_ctx_t *c = ctx;
    const app_config_t *cfg = app_config_get();
    if (cancel_flag) return 1;
    if (o->size == 0 || o->size == 0xFFFFFFFFull) return 0;
    if (o->size < (uint64_t)cfg->min_size_kb * 1024) return 0;
    char inc[512];
    snprintf(inc, sizeof inc, "%s%s", cfg->include_globs, cfg->upload_videos ? " *.mov *.mp4" : "");
    if (!rrc_glob_selected(inc, cfg->exclude_globs, o->path)) return 0;
    if (store_ledger_lookup(c->src->source_id, o->path, o->size)) return 0; /* seen before */
    if (c->n == c->cap) {
        size_t nc = c->cap ? c->cap * 2 : 256;
        cam_object_t *ni = heap_caps_realloc(c->items, nc * sizeof *ni, MALLOC_CAP_SPIRAM | MALLOC_CAP_8BIT);
        if (!ni) return 1;
        c->items = ni; c->cap = nc;
    }
    c->items[c->n++] = *o;
    return 0;
}

static int cmp_obj(const void *a, const void *b)
{
    const cam_object_t *x = a, *y = b;
    if (x->mtime != y->mtime) return x->mtime < y->mtime ? -1 : 1;
    return strcmp(x->path, y->path);
}

/* ---- one sync run ---------------------------------------------------------- */
static void sync_run(cam_source_t *src)
{
    if (!s3_ready) { set_error("cloud not configured — pair or enter S3 settings first"); return; }
    if (!net_has_ip()) { set_phase(SYNC_WAITING_NETWORK); set_error("no network"); return; }
    cancel_flag = false;
    ST_LOCK(); st.phase = SYNC_ENUMERATING; st.run_total = st.run_done = st.run_skipped = st.run_failed = 0; st.run_bytes = 0; st.last_error[0] = 0; ST_UNLOCK();
    run_digest_probe_if_needed();
    log_ring_printf("sync: scanning %s (%s)", src->model, src->source_id);
    enum_ctx_t ec = {.src = src};
    esp_err_t e = src->enumerate(src, enum_cb, &ec);
    if (e != ESP_OK) { set_error("camera enumeration failed: %s", esp_err_to_name(e)); free(ec.items); set_phase(SYNC_ERROR); return; }
    if (ec.n > 1) qsort(ec.items, ec.n, sizeof ec.items[0], cmp_obj);
    ST_LOCK(); st.run_total = (uint32_t)ec.n; st.phase = SYNC_UPLOADING; ST_UNLOCK();
    log_ring_printf("sync: %u new file(s) to upload", (unsigned)ec.n);
    const app_config_t *cfg = app_config_get();
    int consecutive_failures = 0;
    for (size_t i = 0; i < ec.n && !cancel_flag && src->connected(src); i++) {
        if (consecutive_failures >= 5) { set_error("aborting run after %d consecutive failures", consecutive_failures); break; }
        cam_object_t *o = &ec.items[i];
        ST_LOCK(); scpy(st.current_file, sizeof st.current_file, o->path); st.current_size = o->size; st.current_done = 0; ST_UNLOCK();
        /* key */
        char dir[512] = "";
        const char *slash = strrchr(o->path, '/');
        if (slash) { size_t dl = (size_t)(slash - o->path); if (dl >= sizeof dir) dl = sizeof dir - 1; memcpy(dir, o->path, dl); dir[dl] = 0; }
        rrc_template_vars tv = {.name = o->name, .path = dir, .model = src->model, .serial = src->serial, .when = o->mtime};
        char relkey[RRC_RELKEY_MAX], key[RRC_RELKEY_MAX + 16];
        rrc_relkey_err re = rrc_template_expand(cfg->key_template, &tv, relkey, sizeof relkey);
        if (re != RRC_RELKEY_OK) { set_error("%s: unsyncable key (%s)", o->path, rrc_relkey_err_str(re)); ST_LOCK(); st.run_failed++; ST_UNLOCK(); continue; }
        rrc_key_library(relkey, key, sizeof key);
        /* HEAD: already in the bucket? */
        rrc_s3_result_t hr;
        if (rrc_s3_head(&s3, key, &hr) != ESP_OK) { set_error("%s: HEAD failed (%d)", relkey, hr.status); ST_LOCK(); st.run_failed++; ST_UNLOCK(); consecutive_failures++; continue; }
        rrc_ledger_rec lr = {.source_id = src->source_id, .source_path = o->path, .size = o->size, .mtime = o->mtime, .relkey = relkey, .blake3_hex = "", .seq = 0, .ts = 0};
        if (hr.status == 200 && strcmp(hr.meta_device, app_device_id()) != 0) {
            if (hr.content_length >= 0 && (uint64_t)hr.content_length == o->size) {
                lr.status = RRC_LEDGER_REMOTE_EXISTS;
                log_ring_printf("skip %s: already in library (another device)", relkey);
            } else {
                lr.status = RRC_LEDGER_COLLISION;
                log_ring_printf("skip %s: a different file already uses this key (%lld bytes vs %llu)", relkey, (long long)hr.content_length, (unsigned long long)o->size);
            }
            store_ledger_append(&lr);
            ST_LOCK(); st.run_skipped++; ST_UNLOCK();
            continue;
        }
        /* 404, or a 200 that is OUR earlier (unjournaled) upload → (re)upload idempotently. */
        char b3hex[65];
        uint64_t size = 0;
        e = upload_object(src, o, key, b3hex, &size);
        if (e != ESP_OK) {
            ST_LOCK(); st.run_failed++; ST_UNLOCK();
            consecutive_failures++;
            if (!src->connected(src) || cancel_flag) break;
            continue;
        }
        /* journal (§2.1.5 order: object verified → journal entry → ledger commit) */
        uint64_t seq = store_journal_alloc_seq();
        int64_t ts = rrc_s3_server_now(&s3);
        rrc_journal_put_original je = {.seq = seq, .ts = ts, .device = app_device_id(), .bucket_key = key, .vv_self = 1, .size = size,
                                       .blake3_hex = b3hex, .content_id_hex = b3hex, .has_mtime = o->mtime > 0, .mtime = o->mtime};
        char line[1200];
        if (rrc_journal_encode_put_original(&je, line, sizeof line) < 0 || store_journal_append_pending(line) != ESP_OK) {
            set_error("%s: journal append failed", relkey);
            ST_LOCK(); st.run_failed++; ST_UNLOCK();
            continue;
        }
        lr.status = RRC_LEDGER_UPLOADED; lr.blake3_hex = b3hex; lr.seq = seq; lr.ts = ts;
        store_ledger_append(&lr);
        consecutive_failures = 0;
        ST_LOCK(); st.run_done++; st.run_bytes += size; st.lifetime_uploaded++; st.lifetime_bytes += size; ST_UNLOCK();
        log_ring_printf("uploaded %s (%llu bytes)", relkey, (unsigned long long)size);
        if (store_journal_pending_entries() >= SEGMENT_FREEZE_ENTRIES || store_journal_pending_bytes() >= SEGMENT_FREEZE_BYTES) {
            uint64_t first; store_journal_freeze(&first);
        }
    }
    free(ec.items);
    ST_LOCK(); st.current_file[0] = 0; st.phase = SYNC_PUBLISHING; ST_UNLOCK();
    bool uploaded_any;
    ST_LOCK(); uploaded_any = st.run_done > 0; ST_UNLOCK();
    if (flush_journal() == ESP_OK) {
        heartbeat();
        if (uploaded_any || store_kv_get_i64("manifest_ts", 0) == 0) publish_manifest();
    }
    ST_LOCK();
    st.last_run_ts = (int64_t)time(NULL);
    st.phase = st.run_failed ? SYNC_ERROR : SYNC_IDLE;
    log_ring_printf("sync done: %u uploaded, %u skipped, %u failed, %llu bytes", (unsigned)st.run_done, (unsigned)st.run_skipped, (unsigned)st.run_failed, (unsigned long long)st.run_bytes);
    ST_UNLOCK();
}

/* ---- periodic tick ---------------------------------------------------------- */
static void on_tick(void)
{
    if (!s3_ready || !net_has_ip()) return;
    int64_t now = (int64_t)time(NULL);
    int64_t hb;
    bool attached;
    ST_LOCK(); hb = st.last_heartbeat_ts; attached = st.camera_attached; ST_UNLOCK();
    int64_t interval = attached ? HEARTBEAT_ACTIVE_S : HEARTBEAT_IDLE_S;
    /* leftovers from a crash: pending entries / frozen segments */
    if (store_journal_pending_entries() > 0 && now - last_tick_pending_check > 60) { last_tick_pending_check = now; flush_journal(); }
    else publish_frozen_segments();
    if (hb == 0 || rrc_s3_server_now(&s3) - hb >= interval) {
        run_digest_probe_if_needed();
        if (heartbeat() == ESP_OK && store_kv_get_i64("manifest_ts", 0) == 0 && store_ledger_uploaded_count() > 0) publish_manifest();
    }
    int64_t last_compact = store_kv_get_i64("last_compact_check", 0);
    if (now - last_compact >= COMPACT_CHECK_S) {
        store_kv_set_i64("last_compact_check", now);
        compact_segments();
    }
}

/* ---- task ------------------------------------------------------------------- */
static void sync_task(void *arg)
{
    (void)arg;
    msg_t m;
    for (;;) {
        if (xQueueReceive(q, &m, portMAX_DELAY) != pdTRUE) continue;
        switch (m.cmd) {
        case CMD_ATTACH:
            cam = m.src;
            if (!strcmp(cam->kind, "ptp")) {
                esp_err_t e = source_ptp_identify(cam);
                if (e != ESP_OK) log_ring_printf("ptp: identify failed (%s) — is the camera in MTP/PTP USB mode and awake?", esp_err_to_name(e));
            }
            ST_LOCK();
            st.camera_attached = true;
            scpy(st.camera_kind, sizeof st.camera_kind, cam->kind);
            scpy(st.camera_model, sizeof st.camera_model, cam->model);
            scpy(st.camera_serial, sizeof st.camera_serial, cam->serial);
            ST_UNLOCK();
            log_ring_printf("camera attached: %s [%s]", cam->model, cam->source_id);
            if (app_config_get()->auto_sync) sync_run(cam);
            break;
        case CMD_DETACH:
            if (cam) { cam->release(cam); }
            cam = NULL;
            ST_LOCK(); st.camera_attached = false; st.camera_kind[0] = 0; st.camera_model[0] = 0; st.camera_serial[0] = 0; st.current_file[0] = 0;
            if (st.phase == SYNC_ENUMERATING || st.phase == SYNC_UPLOADING) st.phase = SYNC_IDLE;
            ST_UNLOCK();
            log_ring_printf("camera detached");
            break;
        case CMD_SYNC_NOW:
            if (cam && cam->connected(cam)) sync_run(cam);
            else { set_error("no camera attached"); }
            break;
        case CMD_CANCEL: break; /* flag already set */
        case CMD_TICK: on_tick(); break;
        case CMD_CONFIG: configure_s3(); break;
        }
    }
}

static void tick_cb(void *arg) { (void)arg; msg_t m = {.cmd = CMD_TICK}; xQueueSend(q, &m, 0); }

void sync_on_camera_event(cam_event_t ev, cam_source_t *src, void *arg)
{
    (void)arg;
    if (ev == CAM_EV_DETACHED) cancel_flag = true;
    msg_t m = {.cmd = ev == CAM_EV_ATTACHED ? CMD_ATTACH : CMD_DETACH, .src = src};
    xQueueSend(q, &m, pdMS_TO_TICKS(100));
}

esp_err_t sync_init(void)
{
    st_mtx = xSemaphoreCreateMutex();
    q = xQueueCreate(16, sizeof(msg_t));
    part_buf = heap_caps_malloc(PART_BUF_BYTES, MALLOC_CAP_SPIRAM | MALLOC_CAP_8BIT);
    if (!part_buf) { ESP_LOGE(TAG, "cannot allocate %u-byte part buffer in PSRAM", PART_BUF_BYTES); return ESP_ERR_NO_MEM; }
    ST_LOCK();
    st.lifetime_uploaded = (uint32_t)store_ledger_uploaded_count();
    st.lifetime_bytes = store_ledger_uploaded_bytes();
    st.last_manifest_ts = store_kv_get_i64("manifest_ts", 0);
    ST_UNLOCK();
    configure_s3();
    if (xTaskCreatePinnedToCore(sync_task, "sync", 24576, NULL, 4, NULL, tskNO_AFFINITY) != pdPASS) return ESP_ERR_NO_MEM;
    const esp_timer_create_args_t ta = {.callback = tick_cb, .name = "sync_tick"};
    esp_timer_create(&ta, &tick_timer);
    esp_timer_start_periodic(tick_timer, 60ull * 1000 * 1000);
    return ESP_OK;
}

void sync_get_status(sync_status_t *out) { ST_LOCK(); *out = st; out->pending_entries = 0; ST_UNLOCK(); out->pending_entries = store_journal_pending_entries(); }
void sync_request_now(void) { msg_t m = {.cmd = CMD_SYNC_NOW}; xQueueSend(q, &m, 0); }
void sync_cancel(void) { cancel_flag = true; msg_t m = {.cmd = CMD_CANCEL}; xQueueSend(q, &m, 0); }
void sync_config_changed(void) { msg_t m = {.cmd = CMD_CONFIG}; xQueueSend(q, &m, 0); }

static const char *phase_str(sync_phase_t p)
{
    switch (p) {
    case SYNC_UNCONFIGURED: return "unconfigured";
    case SYNC_IDLE: return "idle";
    case SYNC_WAITING_NETWORK: return "waiting_network";
    case SYNC_ENUMERATING: return "enumerating";
    case SYNC_UPLOADING: return "uploading";
    case SYNC_PUBLISHING: return "publishing";
    case SYNC_ERROR: return "error";
    }
    return "?";
}

int sync_status_json(char *out, size_t cap)
{
    sync_status_t s;
    sync_get_status(&s);
    rrc_jsonw w;
    rrc_jsonw_init(&w, out, cap);
    rrc_jsonw_raw(&w, "{");
    rrc_jsonw_key(&w, "phase", true); rrc_jsonw_str(&w, phase_str(s.phase));
    rrc_jsonw_key(&w, "s3_ready", false); rrc_jsonw_raw(&w, s.s3_ready ? "true" : "false");
    rrc_jsonw_key(&w, "camera_attached", false); rrc_jsonw_raw(&w, s.camera_attached ? "true" : "false");
    rrc_jsonw_key(&w, "camera_kind", false); rrc_jsonw_str(&w, s.camera_kind);
    rrc_jsonw_key(&w, "camera_model", false); rrc_jsonw_str(&w, s.camera_model);
    rrc_jsonw_key(&w, "camera_serial", false); rrc_jsonw_str(&w, s.camera_serial);
    rrc_jsonw_key(&w, "current_file", false); rrc_jsonw_str(&w, s.current_file);
    rrc_jsonw_key(&w, "current_size", false); rrc_jsonw_u64(&w, s.current_size);
    rrc_jsonw_key(&w, "current_done", false); rrc_jsonw_u64(&w, s.current_done);
    rrc_jsonw_key(&w, "run_total", false); rrc_jsonw_u64(&w, s.run_total);
    rrc_jsonw_key(&w, "run_done", false); rrc_jsonw_u64(&w, s.run_done);
    rrc_jsonw_key(&w, "run_skipped", false); rrc_jsonw_u64(&w, s.run_skipped);
    rrc_jsonw_key(&w, "run_failed", false); rrc_jsonw_u64(&w, s.run_failed);
    rrc_jsonw_key(&w, "run_bytes", false); rrc_jsonw_u64(&w, s.run_bytes);
    rrc_jsonw_key(&w, "last_run_ts", false); rrc_jsonw_i64(&w, s.last_run_ts);
    rrc_jsonw_key(&w, "last_heartbeat_ts", false); rrc_jsonw_i64(&w, s.last_heartbeat_ts);
    rrc_jsonw_key(&w, "last_manifest_ts", false); rrc_jsonw_i64(&w, s.last_manifest_ts);
    rrc_jsonw_key(&w, "last_error", false); rrc_jsonw_str(&w, s.last_error);
    rrc_jsonw_key(&w, "digest_check_known", false); rrc_jsonw_raw(&w, s.digest_check_known ? "true" : "false");
    rrc_jsonw_key(&w, "backend_rejects_bad_md5", false); rrc_jsonw_raw(&w, s.backend_rejects_bad_md5 ? "true" : "false");
    rrc_jsonw_key(&w, "lifetime_uploaded", false); rrc_jsonw_u64(&w, s.lifetime_uploaded);
    rrc_jsonw_key(&w, "lifetime_bytes", false); rrc_jsonw_u64(&w, s.lifetime_bytes);
    rrc_jsonw_key(&w, "pending_entries", false); rrc_jsonw_u64(&w, s.pending_entries);
    rrc_jsonw_key(&w, "published_cursor", false); rrc_jsonw_u64(&w, store_journal_published_cursor());
    rrc_jsonw_key(&w, "ledger_records", false); rrc_jsonw_u64(&w, store_ledger_count());
    rrc_jsonw_raw(&w, "}");
    return rrc_jsonw_finish(&w);
}

#include "sync.h"
#include "ota.h"
#include "util.h"
#include <stdarg.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include "esp_log.h"
#include "esp_attr.h"
#include "esp_system.h"
#include "esp_timer.h"
#include "lwip/sockets.h"
#include "lwip/inet.h"
#include "esp_heap_caps.h"
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/queue.h"
#include "freertos/semphr.h"
#include "rrc_s3_client.h"
#include "rrc_ptp.h"
#include "nvs.h"
#include "rrc_blake3.h"
#include "rrc_hash.h"
#include "rrc_sigv4.h"
#include "rrc_glob.h"
#include "rrc_proto.h"
#include "app_config.h"
#include "store.h"
#include "net.h"
#include "log_ring.h"
#include "upload_engine.h"

static const char *TAG = "sync";

#define PART_BUF_BYTES   UE_PART_BYTES   /* diagnostics: the size of one part buffer */
/* The sync task runs the S3 client and its TLS session (mbedTLS handshake and
 * record buffers on the stack) under several KiB of path/key/journal-line locals;
 * 24 KiB overflowed by ~6 KiB on hardware (stack protection fault in "sync"). */
#define SYNC_TASK_STACK  (40u * 1024u)   /* measured peak use ~30 KB (TLS handshake included) */
/* One GetPartialObject per READ_CHUNK. The Z 7II pays a fixed latency per request: 256 KiB requests
 * top out at 8 MB/s, 4 MiB requests reach 25 MB/s. Cancel latency is one chunk (~0.15 s). */
#define READ_CHUNK       (4u * 1024u * 1024u)
#define SEGMENT_FREEZE_ENTRIES 900
#define SEGMENT_FREEZE_BYTES   (900u * 1024u)
#define HEARTBEAT_ACTIVE_S  (15 * 60)
#define HEARTBEAT_IDLE_S    (60 * 60)
#define COMPACT_CHECK_S     (24 * 3600)
#define MANIFEST_RECONFIRM_S (24 * 3600)   /* §2.10 rule 2 */
#define LAGGARD_CAP_S       (14 * 24 * 3600) /* §2.10 rule 3 */

typedef enum { CMD_ATTACH, CMD_DETACH, CMD_SYNC_NOW, CMD_CANCEL, CMD_TICK, CMD_CONFIG, CMD_DEBUG_LS, CMD_DEBUG_HASH, CMD_DEBUG_SPEED, CMD_DEBUG_TCP, CMD_DEBUG_PIPE, CMD_DEBUG_READ, CMD_DEBUG_CAMPIPE, CMD_DEBUG_MULTI, CMD_DEBUG_PIPEMULTI } cmd_t;
typedef struct { cmd_t cmd; cam_source_t *src; char *arg; int repeat; int port; } msg_t;

static QueueHandle_t q;
static SemaphoreHandle_t st_mtx;
static sync_status_t st;
static rrc_s3_t s3;
static rrc_s3_t s3b, s3c;             /* one connection per upload task; `s3` is the control connection */
static bool s3_ready;
static cam_source_t *cam;             /* attached camera (or NULL) */
static volatile bool cancel_flag;
static uint8_t *part_buf;                /* the engine's first part buffer; scratch for the diagnostics */
static esp_timer_handle_t tick_timer;
static int64_t last_tick_pending_check;
static bool cam_identified;      /* PTP: OpenSession + DeviceInfo succeeded (source_id is final) */
static bool run_deferred;        /* a sync was wanted but network/cloud was not ready */

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
    if (s3_ready) { rrc_s3_disconnect(&s3); rrc_s3_disconnect(&s3b); rrc_s3_disconnect(&s3c); }
    rrc_s3_init(&s3b, &cfg);
    rrc_s3_init(&s3c, &cfg);
    s3_ready = rrc_s3_init(&s3, &cfg) == ESP_OK;
    if (s3_ready && had_offset) { s3.have_server_offset = true; s3.server_offset_s = off; }
    /* digest-probe result is per backend; remembered in the store */
    char key[80];
    uint32_t h = rrc_crc32(cfg.endpoint, strlen(cfg.endpoint)) ^ rrc_crc32(cfg.bucket, strlen(cfg.bucket));
    snprintf(key, sizeof key, "digest_%08x", (unsigned)h);
    char v[8];
    if (store_kv_get_str(key, v, sizeof v)) { s3.digest_check_known = true; s3.backend_rejects_bad_md5 = v[0] == '1'; }
    snprintf(key, sizeof key, "sha256_%08x", (unsigned)h);
    if (store_kv_get_str(key, v, sizeof v)) { s3.sha256_check_known = true; s3.backend_checks_sha256 = v[0] == '1'; }
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
    if (s3_ready && !s3.sha256_check_known && rrc_s3_probe_sha256_check(&s3, app_device_id()) == ESP_OK) {
        char key[80];
        uint32_t h = rrc_crc32(s3.cfg.endpoint, strlen(s3.cfg.endpoint)) ^ rrc_crc32(s3.cfg.bucket, strlen(s3.cfg.bucket));
        snprintf(key, sizeof key, "sha256_%08x", (unsigned)h);
        store_kv_set_str(key, s3.backend_checks_sha256 ? "1" : "0");
        log_ring_printf("backend %s a wrong x-amz-checksum-sha256%s", s3.backend_checks_sha256 ? "rejects" : "ACCEPTS", s3.backend_checks_sha256 ? "; parts use the hardware SHA engine" : "; parts use Content-MD5");
    }
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

/* One live row, copied out of the ledger scan (the scan's strings point into a
 * reused line buffer) so the rows can be sorted before encoding. §2.3 readers
 * merge by key, so order is not load-bearing for correctness, but ascending
 * relkey is what every other writer emits and what makes two manifests of the
 * same library diff cleanly. */
typedef struct { char *relkey; char blake3_hex[65]; uint64_t size; int64_t mtime, ts; } mrow_t;
typedef struct { mrow_t *rows; size_t n, cap; int err; } mrows_t;

static int manifest_collect(void *ctx, const rrc_ledger_rec *r)
{
    mrows_t *m = ctx;
    if (m->n == m->cap) {
        size_t nc = m->cap ? m->cap * 2 : 256;
        mrow_t *nr = heap_caps_realloc(m->rows, nc * sizeof *nr, MALLOC_CAP_SPIRAM | MALLOC_CAP_8BIT);
        if (!nr) { m->err = 1; return 1; }
        m->rows = nr; m->cap = nc;
    }
    mrow_t *row = &m->rows[m->n];
    size_t kl = strlen(r->relkey);
    row->relkey = heap_caps_malloc(kl + 1, MALLOC_CAP_SPIRAM | MALLOC_CAP_8BIT);
    if (!row->relkey) { m->err = 1; return 1; }
    memcpy(row->relkey, r->relkey, kl + 1);
    scpy(row->blake3_hex, sizeof row->blake3_hex, r->blake3_hex);
    row->size = r->size; row->mtime = r->mtime; row->ts = r->ts;
    m->n++;
    return 0;
}

static int mrow_cmp(const void *a, const void *b) { return strcmp(((const mrow_t *)a)->relkey, ((const mrow_t *)b)->relkey); }

static void mrows_free(mrows_t *m)
{
    for (size_t i = 0; i < m->n; i++) free(m->rows[i].relkey);
    free(m->rows);
    memset(m, 0, sizeof *m);
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
    if (n < 0) { free(gb.buf); set_error("manifest header encode failed"); return ESP_FAIL; }
    line[n++] = '\n';
    rrc_gzip_store_write(&gz, line, (size_t)n);
    mrows_t rows = {0};
    store_ledger_foreach_uploaded(manifest_collect, &rows);
    if (rows.err) { mrows_free(&rows); free(gb.buf); set_error("manifest: out of memory collecting %u rows", (unsigned)rows.n); return ESP_ERR_NO_MEM; }
    qsort(rows.rows, rows.n, sizeof *rows.rows, mrow_cmp);
    char *rline = heap_caps_malloc(2048, MALLOC_CAP_SPIRAM | MALLOC_CAP_8BIT);
    if (!rline) { mrows_free(&rows); free(gb.buf); return ESP_ERR_NO_MEM; }
    int werr = 0;
    for (size_t i = 0; i < rows.n && !werr; i++) {
        const mrow_t *r = &rows.rows[i];
        rrc_manifest_row_original row = {.relkey = r->relkey, .size = r->size, .blake3_hex = r->blake3_hex, .device = app_device_id(), .vv_self = 1,
                                         .content_id_hex = r->blake3_hex, .has_mtime = r->mtime > 0, .mtime = r->mtime, .ts = r->ts};
        int rn = rrc_manifest_row_encode(&row, rline, 2048);
        if (rn < 0) continue; /* skip unencodable row */
        rline[rn++] = '\n';
        if (rrc_gzip_store_write(&gz, rline, (size_t)rn)) werr = 1;
    }
    free(rline);
    size_t nrows = rows.n;
    mrows_free(&rows);
    rrc_gzip_store_end(&gz);
    if (gz.err || werr) { free(gb.buf); return ESP_ERR_NO_MEM; }
    char key[160];
    rrc_key_manifest(app_device_id(), key, sizeof key);
    rrc_s3_result_t res;
    esp_err_t e = rrc_s3_put(&s3, key, gb.buf, gb.len, "application/gzip", NULL, 0, &res);
    free(gb.buf);
    if (e != ESP_OK) { char b[96]; set_error("manifest publish failed: %s", rrc_s3_result_str(&res, b, sizeof b)); return e; }
    int64_t ts = res.server_date > 0 ? res.server_date : now;
    /* One record, one durable write: etag, server ts and covered cursor always
     * describe the same manifest (three separate keys could be torn by a crash). */
    char rec[160];
    snprintf(rec, sizeof rec, "%s\t%lld\t%llu", res.etag, (long long)ts, (unsigned long long)cursor);
    store_kv_set_str("manifest", rec);
    store_kv_set_u64("manifest_dirty", 0);
    ST_LOCK(); st.last_manifest_ts = ts; ST_UNLOCK();
    log_ring_printf("manifest published (%u rows, %u bytes gz, cursor %llu)", (unsigned)nrows, (unsigned)gb.len, (unsigned long long)cursor);
    return ESP_OK;
}

/* Reads the combined manifest record written by publish_manifest. */
static bool manifest_record(char *etag, size_t etag_cap, int64_t *ts, uint64_t *cursor)
{
    char rec[160];
    if (!store_kv_get_str("manifest", rec, sizeof rec)) return false;
    char *t1 = strchr(rec, '\t'); if (!t1) return false; *t1 = 0;
    char *t2 = strchr(t1 + 1, '\t'); if (!t2) return false; *t2 = 0;
    scpy(etag, etag_cap, rec);
    *ts = strtoll(t1 + 1, NULL, 10);
    *cursor = strtoull(t2 + 1, NULL, 10);
    return *ts > 0;
}

static int64_t manifest_ts(void) { char e[80]; int64_t ts; uint64_t c; return manifest_record(e, sizeof e, &ts, &c) ? ts : 0; }

/* §2.10 segment compaction, own prefix only: rule 1 (manifest covers), rule 2
 * (manifest ≥ 24 h old and re-confirmed by HEAD with matching ETag), rule 3
 * via the 14-day laggard cap (this participant reads no peer registry, so the
 * fast path is never taken — segments live at least 14 days). */
static void compact_segments(void)
{
    int64_t mts; uint64_t mcursor; char metag[80];
    if (!manifest_record(metag, sizeof metag, &mts, &mcursor)) return;
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

/* ---- upload (the pipeline lives in upload_engine.c) --------------------------------- */
typedef struct { rrc_blake3_ctx b3; uint64_t n; } hash_sink_t;
static int hash_sink(void *ctx, const uint8_t *d, size_t n) { hash_sink_t *h = ctx; rrc_blake3_update(&h->b3, d, n); h->n += n; return 0; }

static int64_t run_t0_us, run_end_us;   /* run_end_us == 0 while a run is going */

static void on_progress(void *ctx, const char *path, uint64_t size, uint64_t done)
{
    (void)ctx;
    ST_LOCK();
    /* the reader works through one file at a time, so a drop in `done` (or a new path) starts a file */
    st.run_read_bytes += (strcmp(st.current_file, path) == 0 && done >= st.current_done) ? done - st.current_done : done;
    scpy(st.current_file, sizeof st.current_file, path); st.current_size = size; st.current_done = done;
    ST_UNLOCK();
}

/* One object through the engine, start to finish (diagnostics; a sync keeps several in flight). */
static esp_err_t upload_one(cam_source_t *src, const cam_object_t *o, const char *key, char b3hex[65], uint64_t *size)
{
    char err[96] = "";
    ue_job_t *j = ue_submit(src, o, key, NULL, err, sizeof err);
    if (!j) { set_error("%s", err); return ESP_FAIL; }
    ue_job_t *d = ue_wait(UINT32_MAX);
    esp_err_t e = ue_finish(d, b3hex, size, err, sizeof err);
    if (e != ESP_OK) set_error("%s", err);
    return e;
}

/* ---- enumeration ----------------------------------------------------------- */
/* The run's file list. A card can carry tens of thousands of objects and a
 * cam_object_t is 790 bytes, so the list keeps one small record per file and
 * the path in a string pool, both grown in modest chunks: no allocation ever
 * needs megabytes of contiguous PSRAM, which a long run leaves fragmented (the
 * old doubling realloc failed silently at 8192 entries on a Z f). The name is
 * the path's last component, which every source guarantees. */
typedef struct { uint64_t size; int64_t mtime; uint32_t handle; uint32_t path; /* pool << 16 | offset */ } item_t;
#define ITEM_CHUNK 2048u                      /* records per chunk (48 KiB) */
#define POOL_CHUNK (64u * 1024u)              /* path bytes per pool chunk */
#define MAX_CHUNKS 64                         /* 131072 files, 4 MiB of paths */
typedef struct {
    item_t *chunks[MAX_CHUNKS];
    char *pools[MAX_CHUNKS];
    size_t n, nchunks, npools, pool_used;
    uint32_t *order;                          /* sorted record indices */
    bool oom;
    cam_source_t *src;
} enum_ctx_t;

static void enum_free(enum_ctx_t *c)
{
    for (size_t i = 0; i < c->nchunks; i++) free(c->chunks[i]);
    for (size_t i = 0; i < c->npools; i++) free(c->pools[i]);
    free(c->order);
    c->nchunks = c->npools = c->n = 0; c->order = NULL;
}

static inline item_t *item_at(const enum_ctx_t *c, size_t i) { return &c->chunks[i / ITEM_CHUNK][i % ITEM_CHUNK]; }
static inline const char *item_path(const enum_ctx_t *c, const item_t *it) { return c->pools[it->path >> 16] + (it->path & 0xFFFF); }

static bool item_add(enum_ctx_t *c, const cam_object_t *o)
{
    size_t plen = strlen(o->path) + 1;
    if (plen > POOL_CHUNK) return false;
    if (c->npools == 0 || c->pool_used + plen > POOL_CHUNK) {
        if (c->npools == MAX_CHUNKS) return false;
        char *pool = heap_caps_malloc(POOL_CHUNK, MALLOC_CAP_SPIRAM | MALLOC_CAP_8BIT);
        if (!pool) return false;
        c->pools[c->npools++] = pool; c->pool_used = 0;
    }
    if (c->n % ITEM_CHUNK == 0) {
        if (c->nchunks == MAX_CHUNKS) return false;
        item_t *ch = heap_caps_malloc(ITEM_CHUNK * sizeof *ch, MALLOC_CAP_SPIRAM | MALLOC_CAP_8BIT);
        if (!ch) return false;
        c->chunks[c->nchunks++] = ch;
    }
    memcpy(c->pools[c->npools - 1] + c->pool_used, o->path, plen);
    item_t *it = item_at(c, c->n);
    it->size = o->size; it->mtime = o->mtime; it->handle = o->handle;
    it->path = (uint32_t)((c->npools - 1) << 16 | c->pool_used);
    c->pool_used += plen;
    c->n++;
    return true;
}

/* Rebuilds the full cam_object_t for one record. */
static void item_fill(const enum_ctx_t *c, size_t i, cam_object_t *o)
{
    const item_t *it = item_at(c, i);
    memset(o, 0, sizeof *o);
    scpy(o->path, sizeof o->path, item_path(c, it));
    const char *slash = strrchr(o->path, '/');
    scpy(o->name, sizeof o->name, slash ? slash + 1 : o->path);
    o->size = it->size; o->mtime = it->mtime; o->handle = it->handle;
}

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
    if (!item_add(c, o)) { c->oom = true; return 1; }
    return 0;
}

/* qsort has no context argument; a run enumerates on one task at a time. */
static const enum_ctx_t *sort_ctx;
static int cmp_item(const void *a, const void *b)
{
    const item_t *x = item_at(sort_ctx, *(const uint32_t *)a), *y = item_at(sort_ctx, *(const uint32_t *)b);
    if (x->mtime != y->mtime) return x->mtime < y->mtime ? -1 : 1;
    return strcmp(item_path(sort_ctx, x), item_path(sort_ctx, y));
}

/* ---- one sync run ---------------------------------------------------------- */
#define RUN_WINDOW 3
typedef struct {
    ue_job_t *job;
    const cam_object_t *o;                    /* &obj while in flight */
    cam_object_t obj;
    char relkey[RRC_RELKEY_MAX];
    char key[RRC_RELKEY_MAX + 16];
} flight_t;

typedef struct {
    cam_source_t *src;
    const app_config_t *cfg;
    flight_t *fl;
    int inflight, consecutive_failures;
    int interrupted;   /* in-flight files stopped by a cancel or the camera going away: not failures */
    bool warned;
} run_t;

/* Next finished job, aborting the engine if the run is cancelled or the camera goes away meanwhile. */
static ue_job_t *run_wait(run_t *rc)
{
    for (;;) {
        ue_job_t *j = ue_wait(200);
        if (j) return j;
        if (cancel_flag || !rc->src->connected(rc->src)) ue_set_abort(true);
    }
}

/* A job whose parts have all been uploaded (or failed): complete it, verify it, then the journal entry and
 * the ledger commit (§2.1.5 order: object verified -> journal entry -> ledger commit). */
static void run_finish(run_t *rc, ue_job_t *dj)
{
    flight_t *f = ue_job_user(dj);
    const cam_object_t *o = f->o;
    char b3hex[65], err[96] = "";
    uint64_t size = 0;
    rc->inflight--; ST_LOCK(); st.run_inflight = (uint32_t)rc->inflight; ST_UNLOCK();
    esp_err_t e = ue_finish(dj, b3hex, &size, err, sizeof err);
    f->job = NULL;
    rrc_ledger_rec lr = {.source_id = rc->src->source_id, .source_path = o->path, .size = o->size, .mtime = o->mtime, .relkey = f->relkey, .blake3_hex = "", .seq = 0, .ts = 0};
    if (e != ESP_OK) {
        /* Stopped by a cancel or an unplugged camera: nothing went wrong with the
         * file, and the ledger has no record of it, so the next run uploads it. */
        if (cancel_flag || !rc->src->connected(rc->src)) { rc->interrupted++; return; }
        set_error("%s: %s", f->relkey, err);
        ST_LOCK(); st.run_failed++; ST_UNLOCK();
        rc->consecutive_failures++;
        return;
    }
    /* Peek, encode, append: the seq is committed by the append (store.h), so an encode/append failure leaves
     * no gap and a reboot cannot reuse it. */
    uint64_t seq = store_journal_next_seq();
    int64_t ts = rrc_s3_server_now(&s3);
    rrc_journal_put_original je = {.seq = seq, .ts = ts, .device = app_device_id(), .bucket_key = f->key, .vv_self = 1, .size = size,
                                   .blake3_hex = b3hex, .content_id_hex = b3hex, .has_mtime = o->mtime > 0, .mtime = o->mtime};
    char line[1200];
    if (rrc_journal_encode_put_original(&je, line, sizeof line) < 0 || store_journal_append_pending(line) != ESP_OK) {
        set_error("%s: journal append failed (flash full?) — object is in the bucket and will be adopted by the worker", f->relkey);
        ST_LOCK(); st.run_failed++; ST_UNLOCK();
        return;
    }
    lr.status = RRC_LEDGER_UPLOADED; lr.blake3_hex = b3hex; lr.seq = seq; lr.ts = ts;
    store_ledger_append(&lr);
    store_kv_set_u64("manifest_dirty", 1);
    rc->consecutive_failures = 0;
    ST_LOCK(); st.run_done++; st.run_bytes += size; st.lifetime_uploaded++; st.lifetime_bytes += size; ST_UNLOCK();
    log_ring_printf("uploaded %s (%llu bytes)", f->relkey, (unsigned long long)size);
    if (store_journal_pending_entries() >= SEGMENT_FREEZE_ENTRIES || store_journal_pending_bytes() >= SEGMENT_FREEZE_BYTES) {
        uint64_t first; store_journal_freeze(&first);
    }
}

static void run_complete_one(run_t *rc) { run_finish(rc, run_wait(rc)); }

/* Decides what to do with one camera object (skip it, or start uploading it). */
static bool run_submit(run_t *rc, const enum_ctx_t *ec, size_t item, bool may_defer)
{
    cam_source_t *src = rc->src;
    flight_t *f = NULL;
    for (int k = 0; k < RUN_WINDOW; k++) if (!rc->fl[k].job) { f = &rc->fl[k]; break; }
    if (!f) return true;   /* cannot happen: the caller checks the window */
    item_fill(ec, item, &f->obj);
    const cam_object_t *o = &f->obj;
    char dir[512] = "";
    const char *slash = strrchr(o->path, '/');
    if (slash) { size_t dl = (size_t)(slash - o->path); if (dl >= sizeof dir) dl = sizeof dir - 1; memcpy(dir, o->path, dl); dir[dl] = 0; }
    rrc_template_vars tv = {.name = o->name, .path = dir, .model = src->model, .serial = src->serial, .when = o->mtime};
    rrc_relkey_err re = rrc_template_expand(rc->cfg->key_template, &tv, f->relkey, sizeof f->relkey);
    if (re != RRC_RELKEY_OK) { set_error("%s: unsyncable key (%s)", o->path, rrc_relkey_err_str(re)); ST_LOCK(); st.run_failed++; ST_UNLOCK(); return true; }
    rrc_key_library(f->relkey, f->key, sizeof f->key);
    /* A photo that is still uploading from the other card slot has to land (and be recorded) before this
     * one is judged: if that upload failed, this copy must still go. */
    for (;;) {
        bool twin = false;
        for (int k = 0; k < RUN_WINDOW; k++) if (rc->fl[k].job && &rc->fl[k] != f && !strcmp(rc->fl[k].relkey, f->relkey)) twin = true;
        if (!twin) break;
        if (may_defer) return false;   /* let the caller try a different file first */
        run_complete_one(rc);
    }
    /* Dual-slot bodies (Nikon "backup" to the second card) list the same photo twice under two storages;
     * the copies are not byte-identical, so only the first one seen is the library's. Record the mirror so
     * it is never retried. */
    if (store_ledger_relkey_uploaded(f->relkey, o->size)) {
        rrc_ledger_rec mirror = {.source_id = src->source_id, .source_path = o->path, .size = o->size, .mtime = o->mtime, .relkey = f->relkey, .blake3_hex = "", .seq = 0, .ts = 0, .status = RRC_LEDGER_REMOTE_EXISTS};
        log_ring_printf("skip %s: the same photo was already uploaded from this camera's other card slot", f->relkey);
        store_ledger_append(&mirror);
        ST_LOCK(); st.run_skipped++; st.run_total_bytes -= o->size; ST_UNLOCK();
        return true;
    }
    /* HEAD: already in the bucket? */
    rrc_s3_result_t hr;
    if (rrc_s3_head(&s3, f->key, &hr) != ESP_OK) { set_error("%s: HEAD failed (%d)", f->relkey, hr.status); ST_LOCK(); st.run_failed++; ST_UNLOCK(); rc->consecutive_failures++; return true; }
    if (hr.status == 200 && strcmp(hr.meta_device, app_device_id()) != 0) {
        rrc_ledger_rec lr = {.source_id = src->source_id, .source_path = o->path, .size = o->size, .mtime = o->mtime, .relkey = f->relkey, .blake3_hex = "", .seq = 0, .ts = 0};
        if (hr.content_length >= 0 && (uint64_t)hr.content_length == o->size) {
            lr.status = RRC_LEDGER_REMOTE_EXISTS;
            log_ring_printf("skip %s: already in library (another device)", f->relkey);
        } else {
            lr.status = RRC_LEDGER_COLLISION;
            log_ring_printf("skip %s: a different file already uses this key (%lld bytes vs %llu)", f->relkey, (long long)hr.content_length, (unsigned long long)o->size);
        }
        store_ledger_append(&lr);
        ST_LOCK(); st.run_skipped++; st.run_total_bytes -= o->size; ST_UNLOCK();
        return true;
    }
    /* 404, or a 200 that is OUR earlier (unjournaled) upload -> (re)upload idempotently. */
    char err[96] = "";
    f->o = o;
    f->job = ue_submit(src, o, f->key, f, err, sizeof err);
    if (!f->job) { set_error("%s: %s", f->relkey, err); ST_LOCK(); st.run_failed++; ST_UNLOCK(); rc->consecutive_failures++; return true; }
    rc->inflight++; ST_LOCK(); st.run_inflight = (uint32_t)rc->inflight; ST_UNLOCK();
    return true;
}


/* A run interrupted by a crash reset (panic or watchdog) picks itself up again
 * when the camera comes back, even with auto_sync off: the operator started it.
 * The marker lives in retained RAM, which survives those resets but not power
 * loss; the ledger makes the resumed run skip everything already uploaded. */
#define RUN_MAGIC 0x52554e31u   /* "RUN1" */
#define RESUME_MAX_TRIES 3
#define RECOVERY_MAGIC 0x52435652u   /* "RCVR" */
static RTC_NOINIT_ATTR uint32_t rtc_run_magic;
static RTC_NOINIT_ATTR uint32_t rtc_resume_tries;
static RTC_NOINIT_ATTR uint32_t rtc_recovery_restart;
static bool resume_pending;
/* RESUME_MAX_TRIES crashes in a row during runs: no run starts by itself (neither a
 * resume nor auto_sync on the still-attached camera, which turned a crash that recurs
 * mid-run into a boot loop) until the operator asks for one or a run completes. */
static bool crash_hold;

void sync_mark_recovery_restart(void) { rtc_recovery_restart = RECOVERY_MAGIC; }

static void resume_check_at_boot(void)
{
    esp_reset_reason_t r = esp_reset_reason();
    /* A software restart counts like a crash only when the firmware itself
     * restarted to recover (sync_mark_recovery_restart, e.g. the Ethernet
     * receive watchdog); a console or web-UI reboot never resumes. */
    bool recovery = r == ESP_RST_SW && rtc_recovery_restart == RECOVERY_MAGIC;
    rtc_recovery_restart = 0;
    bool crash = recovery || r == ESP_RST_PANIC || r == ESP_RST_INT_WDT || r == ESP_RST_TASK_WDT || r == ESP_RST_WDT;
    if (!crash) { rtc_run_magic = 0; rtc_resume_tries = 0; return; }
    if (rtc_run_magic != RUN_MAGIC) return;
    rtc_run_magic = 0;
    if (rtc_resume_tries >= RESUME_MAX_TRIES) {
        crash_hold = true;
        log_ring_printf("sync: %u restarts in a row during runs; automatic syncs are paused until one is started by hand (Sync now)", (unsigned)rtc_resume_tries);
        return;
    }
    rtc_resume_tries++;
    resume_pending = true;
    log_ring_printf("sync: restarted %s during a run; it resumes when the camera is back", recovery ? "to recover the network" : "after a crash");
}

static void sync_run_impl(cam_source_t *src);
static bool run_began;   /* sync_run_impl got past its preconditions (not merely deferred) */

static void sync_run(cam_source_t *src)
{
    /* A firmware update restarts the dock when it is done: no run starts under it
     * (the tick retries once the update failed or was cancelled), and none can start
     * one while this run holds the reservation, from identifying the camera on. */
    if (!ota_net_begin()) { run_deferred = true; set_error("firmware update in progress; the sync starts when it is done"); return; }
    rtc_run_magic = RUN_MAGIC;
    run_began = false;
    sync_run_impl(src);
    rtc_run_magic = 0;
    if (run_began) {
        /* a run that ran to its end, however it ended, clears the crash count and the pause;
         * one that was only deferred (no network yet, camera asleep) does not */
        rtc_resume_tries = 0;
        crash_hold = false;
    }
    ota_net_end();
}

/* Whether a run may start without the operator asking. */
static bool auto_start_allowed(void) { return (app_config_get()->auto_sync && !crash_hold) || resume_pending; }

static void sync_run_impl(cam_source_t *src)
{
    if (!s3_ready) { run_deferred = true; set_error("cloud not configured — pair or enter S3 settings first; the sync starts once it is"); return; }
    if (!net_has_ip()) { run_deferred = true; set_phase(SYNC_WAITING_NETWORK); set_error("waiting for the network"); return; }
    if (!strcmp(src->kind, "ptp") && !cam_identified) {
        if (source_ptp_identify(src) != ESP_OK) { run_deferred = true; set_error("camera did not answer OpenSession/GetDeviceInfo — is it awake and in MTP/PTP USB mode? retrying"); return; }
        cam_identified = true;
        ST_LOCK(); scpy(st.camera_model, sizeof st.camera_model, src->model); scpy(st.camera_serial, sizeof st.camera_serial, src->serial); ST_UNLOCK();
    }
    run_deferred = false;
    resume_pending = false;
    cancel_flag = false;
    run_began = true;
    ST_LOCK(); st.phase = SYNC_ENUMERATING; st.run_total = st.run_done = st.run_skipped = st.run_failed = 0; st.run_bytes = 0; st.run_total_bytes = st.run_read_bytes = 0; st.run_inflight = 0; st.run_elapsed_ms = 0; st.current_file[0] = 0; st.current_done = 0; st.last_error[0] = 0; ST_UNLOCK();
    memset(ue_stats(), 0, sizeof *ue_stats());
    run_t0_us = esp_timer_get_time(); run_end_us = 0;
    run_digest_probe_if_needed();
    log_ring_printf("sync: scanning %s (%s)", src->model, src->source_id);
    enum_ctx_t ec = {.src = src};
    esp_err_t e = src->enumerate(src, enum_cb, &ec);
    if (e != ESP_OK) { set_error("camera enumeration failed: %s", esp_err_to_name(e)); enum_free(&ec); set_phase(SYNC_ERROR); return; }
    if (ec.oom) log_ring_printf("sync: the file list stopped at %u entries (out of memory); the rest is picked up by the next run", (unsigned)ec.n);
    ec.order = heap_caps_malloc((ec.n ? ec.n : 1) * sizeof *ec.order, MALLOC_CAP_SPIRAM | MALLOC_CAP_8BIT);
    if (!ec.order) { set_error("out of memory"); enum_free(&ec); set_phase(SYNC_ERROR); return; }
    for (size_t i = 0; i < ec.n; i++) ec.order[i] = (uint32_t)i;
    if (ec.n > 1) { sort_ctx = &ec; qsort(ec.order, ec.n, sizeof ec.order[0], cmp_item); sort_ctx = NULL; }
    uint64_t tb = 0;
    for (size_t i = 0; i < ec.n; i++) tb += item_at(&ec, i)->size;
    ST_LOCK(); st.run_total = (uint32_t)ec.n; st.run_total_bytes = tb; st.phase = SYNC_UPLOADING; ST_UNLOCK();
    log_ring_printf("sync: %u new file(s) to upload", (unsigned)ec.n);
    run_t rc = {.src = src, .cfg = app_config_get()};
    rc.fl = heap_caps_calloc(RUN_WINDOW, sizeof *rc.fl, MALLOC_CAP_SPIRAM | MALLOC_CAP_8BIT);
    if (!rc.fl) { set_error("out of memory"); enum_free(&ec); set_phase(SYNC_ERROR); return; }
    /* Up to RUN_WINDOW files in flight: while earlier ones upload, the next are checked against the
     * bucket and read from the camera, so neither the camera nor the network waits on a round trip. */
    size_t next = 0;
    int defers = 0;
    while (true) {
        while (next < ec.n && rc.inflight < RUN_WINDOW && !cancel_flag && src->connected(src) && rc.consecutive_failures < 5) {
            size_t idx = next++;
            /* Dual-slot twins sit next to each other; instead of idling until the first copy lands, move on
             * to another file and come back (at most RUN_WINDOW swaps in a row, so this cannot spin). */
            if (!run_submit(&rc, &ec, ec.order[idx], next < ec.n && defers < RUN_WINDOW)) {
                uint32_t tmp = ec.order[idx]; ec.order[idx] = ec.order[next]; ec.order[next] = tmp;
                next = idx; defers++;
            } else defers = 0;
        }
        if (rc.inflight == 0) break;
        run_complete_one(&rc);
        if (rc.consecutive_failures >= 5 && !rc.warned) { rc.warned = true; set_error("aborting run after %d consecutive failures", rc.consecutive_failures); }
    }
    ue_set_abort(false);
    free(rc.fl);
    enum_free(&ec);
    ST_LOCK(); st.current_file[0] = 0; st.phase = SYNC_PUBLISHING; ST_UNLOCK();
    bool uploaded_any;
    ST_LOCK(); uploaded_any = st.run_done > 0; ST_UNLOCK();
    if (flush_journal() == ESP_OK) {
        heartbeat();
        if (uploaded_any || store_kv_get_u64("manifest_dirty", 0) || manifest_ts() == 0) publish_manifest();
    }
    bool stopped = cancel_flag || !src->connected(src);
    ST_LOCK();
    run_end_us = esp_timer_get_time();
    st.last_run_ts = (int64_t)time(NULL);
    st.phase = st.run_failed ? SYNC_ERROR : SYNC_IDLE;
    if (stopped)
        log_ring_printf("sync stopped (%s): %u uploaded, %u skipped, %u failed, %llu bytes; %d in flight left for the next run",
                        cancel_flag ? "cancelled" : "camera disconnected", (unsigned)st.run_done, (unsigned)st.run_skipped,
                        (unsigned)st.run_failed, (unsigned long long)st.run_bytes, rc.interrupted);
    else
        log_ring_printf("sync done: %u uploaded, %u skipped, %u failed, %llu bytes", (unsigned)st.run_done, (unsigned)st.run_skipped, (unsigned)st.run_failed, (unsigned long long)st.run_bytes);
    ST_UNLOCK();
}

/* ---- periodic tick ---------------------------------------------------------- */
static void tick_maintenance(void);
static void on_tick(void)
{
    /* A run that could not start (no network yet, cloud unconfigured, camera asleep)
     * is retried here instead of waiting for a replug. */
    if (run_deferred && cam && cam->connected(cam) && auto_start_allowed()) sync_run(cam);
    if (!s3_ready || !net_has_ip()) return;
    /* bucket traffic: not beside a firmware download or flash write (ota.h) */
    if (!ota_net_begin()) return;
    tick_maintenance();
    ota_net_end();
}

static void tick_maintenance(void)
{
    int64_t now = (int64_t)time(NULL);
    int64_t hb;
    bool attached;
    ST_LOCK(); hb = st.last_heartbeat_ts; attached = st.camera_attached; ST_UNLOCK();
    int64_t interval = attached ? HEARTBEAT_ACTIVE_S : HEARTBEAT_IDLE_S;
    /* leftovers from a crash: pending entries / frozen segments */
    if (store_journal_pending_entries() > 0 && now - last_tick_pending_check > 60) { last_tick_pending_check = now; flush_journal(); }
    else publish_frozen_segments();
    { uint64_t frozen[1]; if (store_kv_get_u64("manifest_dirty", 0) && store_journal_pending_entries() == 0 && store_journal_frozen_list(frozen, 1) == 0) publish_manifest(); }
    if (hb == 0 || rrc_s3_server_now(&s3) - hb >= interval) {
        run_digest_probe_if_needed();
        if (heartbeat() == ESP_OK && manifest_ts() == 0 && store_ledger_uploaded_count() > 0) publish_manifest();
    }
    int64_t last_compact = store_kv_get_i64("last_compact_check", 0);
    if (now - last_compact >= COMPACT_CHECK_S) {
        store_kv_set_i64("last_compact_check", now);
        compact_segments();
    }
}

/* ---- console diagnostics (run on the sync task: it owns the camera session) -- */
static int debug_ls_cb(void *ctx, const cam_object_t *o)
{
    size_t *n = ctx;
    (*n)++;
    printf("  %08lx %12llu  %s\n", (unsigned long)o->handle, (unsigned long long)o->size, o->path);
    return 0;
}

static void debug_ls(void)
{
    if (!cam || !cam->connected(cam)) { printf("no camera attached\n"); return; }
    if (!strcmp(cam->kind, "ptp") && !cam_identified) {
        if (source_ptp_identify(cam) != ESP_OK) { printf("camera did not answer OpenSession/GetDeviceInfo\n"); return; }
        cam_identified = true;
    }
    printf("objects on %s [%s] (handle, size, path):\n", cam->model, cam->source_id);
    size_t n = 0;
    esp_err_t e = cam->enumerate(cam, debug_ls_cb, &n);
    printf("%u objects (%s)\n", (unsigned)n, esp_err_to_name(e));
}

/* Reads one object end to end `repeat` times through the normal source read
 * path and prints the BLAKE3 of each pass: identical hashes prove the camera
 * read path is deterministic; the hash can be compared with the file on the
 * card. `arg` is a hex PTP handle or an MSC path. */
static void debug_hash(const char *arg, int repeat)
{
    if (!cam || !cam->connected(cam)) { printf("no camera attached\n"); return; }
    cam_object_t o = {0};
    char *end = NULL;
    unsigned long h = strtoul(arg, &end, 16);
    if (end && *end == 0 && h) o.handle = (uint32_t)h;
    scpy(o.path, sizeof o.path, arg);
    const char *slash = strrchr(arg, '/');
    scpy(o.name, sizeof o.name, slash ? slash + 1 : arg);
    o.size = 0xFFFFFFFFFFFFFFFFull; /* unknown: read to EOF */
    for (int r = 0; r < (repeat > 0 ? repeat : 1); r++) {
        void *fh = NULL;
        esp_err_t e = cam->open(cam, &o, &fh);
        if (e != ESP_OK) { printf("open failed: %s\n", esp_err_to_name(e)); return; }
        rrc_blake3_ctx b3;
        rrc_blake3_init(&b3);
        uint64_t total = 0;
        int64_t t0 = esp_timer_get_time();
        for (;;) {
            size_t got = 0;
            e = cam->read(cam, fh, total, part_buf, READ_CHUNK, &got);
            if (e != ESP_OK) { printf("read failed at %llu: %s\n", (unsigned long long)total, esp_err_to_name(e)); break; }
            if (got == 0) break;
            rrc_blake3_update(&b3, part_buf, got);
            total += got;
            if (got < READ_CHUNK) break;
        }
        cam->close(cam, fh);
        char hex[65];
        rrc_blake3_final_hex(&b3, hex);
        int64_t ms = (esp_timer_get_time() - t0) / 1000;
        printf("pass %d: %llu bytes blake3 %s in %lld ms (%.2f MB/s)\n", r + 1, (unsigned long long)total, hex, (long long)ms,
               ms > 0 ? (double)total / 1048576.0 * 1000.0 / (double)ms : 0.0);
    }
}

/* Raw camera read rate with no hashing and no upload: reads `mb` MiB of one object
 * through the real read path into the part buffer, once with the zero-copy USB
 * DMA path and once with the copying path, so the difference is the DMA win. */
static void debug_read(const char *arg, int mb, int chunk_kib)
{
    if (!cam || !cam->connected(cam)) { printf("no camera attached\n"); return; }
    cam_object_t o = {0};
    char *end = NULL;
    unsigned long h = strtoul(arg, &end, 16);
    if (end && *end == 0 && h) o.handle = (uint32_t)h;
    scpy(o.path, sizeof o.path, arg);
    const char *slash = strrchr(arg, '/');
    scpy(o.name, sizeof o.name, slash ? slash + 1 : arg);
    o.size = 0xFFFFFFFFFFFFFFFFull;
    uint64_t limit = (uint64_t)(mb > 0 ? mb : 16) * 1024u * 1024u;
    bool was = rrc_ptp_direct_enabled();
    for (int mode = 1; mode >= 0; mode--) {
        rrc_ptp_set_direct(mode == 1);
        void *fh = NULL;
        esp_err_t e = cam->open(cam, &o, &fh);
        if (e != ESP_OK) { printf("open failed: %s\n", esp_err_to_name(e)); break; }
        uint64_t total = 0; uint32_t crc = 0;
        int64_t t0 = esp_timer_get_time();
        while (total < limit) {
            size_t got = 0;
            size_t slot_off = (size_t)(total % PART_BUF_BYTES);
            size_t want = chunk_kib > 0 ? (size_t)chunk_kib * 1024u : READ_CHUNK;
            if (want > PART_BUF_BYTES - slot_off) want = PART_BUF_BYTES - slot_off;
            e = cam->read(cam, fh, total, part_buf + slot_off, want, &got);
            if (e != ESP_OK) { printf("read failed at %llu: %s\n", (unsigned long long)total, esp_err_to_name(e)); break; }
            if (got == 0) break;
            crc = rrc_crc32_update(crc, part_buf + slot_off, got < 64 ? got : 64); /* touches the data, cheap */
            total += got;
            if (got < want) break;
        }
        int64_t us = esp_timer_get_time() - t0;
        cam->close(cam, fh);
        printf("%s: %llu bytes in %.2f s = %.2f MB/s\n", mode ? "usb dma straight into psram" : "bounce buffer + cpu copy ", (unsigned long long)total,
               (double)us / 1e6, us > 0 ? (double)total / 1048576.0 / ((double)us / 1e6) : 0.0);
        (void)crc;
        if (e != ESP_OK) break;
    }
    rrc_ptp_set_direct(was);
}

/* ---- synthetic camera for the pipeline diagnostic ---------------------------
 * Serves a deterministic pattern at an optional emulated rate so the whole
 * read-ahead/upload pipeline can be timed and verified without a camera. */
typedef struct { uint64_t size; int rate_kib_s; } synth_impl_t;
static void synth_fill(uint8_t *buf, uint64_t offset, size_t n)
{
    while (n) {
        size_t in_block = 4096 - (size_t)(offset & 4095);
        size_t k = n < in_block ? n : in_block;
        memset(buf, (int)(0x20 + ((offset >> 12) * 7919u) % 200u), k);
        buf += k; offset += k; n -= k;
    }
}
static esp_err_t synth_enumerate(cam_source_t *s, cam_enum_cb_t cb, void *ctx) { (void)s; (void)cb; (void)ctx; return ESP_OK; }
static esp_err_t synth_open(cam_source_t *s, const cam_object_t *o, void **fh) { (void)s; (void)o; *fh = (void *)1; return ESP_OK; }
static esp_err_t synth_read(cam_source_t *s, void *fh, uint64_t offset, uint8_t *buf, size_t want, size_t *got)
{
    (void)fh;
    synth_impl_t *im = s->impl;
    if (offset >= im->size) { *got = 0; return ESP_OK; }
    size_t n = want > im->size - offset ? (size_t)(im->size - offset) : want;
    synth_fill(buf, offset, n);
    if (im->rate_kib_s > 0) {
        uint32_t ms = (uint32_t)((uint64_t)n * 1000u / ((uint64_t)im->rate_kib_s * 1024u));
        if (ms) vTaskDelay(pdMS_TO_TICKS(ms));
    }
    *got = n;
    return ESP_OK;
}
static void synth_close(cam_source_t *s, void *fh) { (void)s; (void)fh; }
static bool synth_connected(cam_source_t *s) { (void)s; return true; }
static void synth_release(cam_source_t *s) { (void)s; }

static void debug_pipe(int mb, int rate_mb_s)
{
    if (!s3_ready) { printf("cloud not configured\n"); return; }
    synth_impl_t im = {.size = (uint64_t)(mb > 0 ? mb : 24) * 1024u * 1024u, .rate_kib_s = rate_mb_s > 0 ? rate_mb_s * 1024 : 0};
    cam_source_t src = {.kind = "synth", .source_id = "synth", .model = "synthetic", .serial = "0", .impl = &im,
                        .enumerate = synth_enumerate, .open = synth_open, .read = synth_read, .close = synth_close,
                        .connected = synth_connected, .release = synth_release};
    cam_object_t o = {.name = "pipetest.bin", .path = "pipetest.bin", .size = im.size, .mtime = 0, .handle = 0};
    /* expected hash, straight from the generator (also times BLAKE3 on this board) */
    char expect[65];
    {
        rrc_blake3_ctx b3; rrc_blake3_init(&b3);
        int64_t t0 = esp_timer_get_time(), gen_us = 0;
        for (uint64_t off = 0; off < im.size; off += READ_CHUNK) {
            size_t n = im.size - off > READ_CHUNK ? READ_CHUNK : (size_t)(im.size - off);
            int64_t g0 = esp_timer_get_time(); synth_fill(part_buf, off, n); gen_us += esp_timer_get_time() - g0;
            rrc_blake3_update(&b3, part_buf, n);
        }
        rrc_blake3_final_hex(&b3, expect);
        int64_t hash_us = esp_timer_get_time() - t0 - gen_us;
        printf("blake3 over %d MiB: %lld ms = %.1f MB/s\n", mb, (long long)(hash_us / 1000), hash_us > 0 ? (double)im.size / 1048576.0 / ((double)hash_us / 1e6) : 0.0);
    }
    char key[160];
    snprintf(key, sizeof key, ".rrcloud/tmp/%s-pipetest", app_device_id());
    cancel_flag = false;
    char got_hex[65] = ""; uint64_t size = 0;
    memset(ue_stats(), 0, sizeof *ue_stats());
    int64_t t0 = esp_timer_get_time();
    esp_err_t e = upload_one(&src, &o, key, got_hex, &size);
    int64_t t1 = esp_timer_get_time();
    double secs = (double)(t1 - t0) / 1e6;
    printf("pipeline %d MiB at %s: %s in %.2f s = %.2f MB/s%s\n", mb, rate_mb_s > 0 ? "emulated camera rate" : "generator speed",
           esp_err_to_name(e), secs, (double)im.size / 1048576.0 / secs, rate_mb_s > 0 ? "" : " (upload-bound)");
    if (rate_mb_s > 0) printf("  camera alone would take %.2f s at %d MB/s; serial read-then-upload would add the upload time on top\n", (double)mb / rate_mb_s, rate_mb_s);
    printf("  stages (overlapping): camera read %.2f s, blake3 %.2f s, md5 %.2f s, upload %.2f s\n",
           (double)ue_stats()->read_us / 1e6, (double)ue_stats()->hash_us / 1e6, (double)ue_stats()->md5_us / 1e6, (double)ue_stats()->upload_us / 1e6);
    if (e != ESP_OK) { ST_LOCK(); printf("  error: %s\n", st.last_error); ST_UNLOCK(); return; }
    printf("  pipeline blake3 %s expected\n", strcmp(got_hex, expect) == 0 ? "matches" : "DIFFERS FROM");
    hash_sink_t hs = {0}; rrc_blake3_init(&hs.b3);
    rrc_s3_result_t gr;
    t0 = esp_timer_get_time();
    e = rrc_s3_get_stream(&s3, key, hash_sink, &hs, &gr);
    t1 = esp_timer_get_time();
    char rb[65]; rrc_blake3_final_hex(&hs.b3, rb);
    printf("  read-back: %s (http %d) %llu bytes in %.2f s = %.2f MB/s, blake3 %s expected\n", esp_err_to_name(e), gr.status,
           (unsigned long long)hs.n, (double)(t1 - t0) / 1e6, (double)hs.n / 1048576.0 / ((double)(t1 - t0) / 1e6),
           strcmp(rb, expect) == 0 ? "matches" : "DIFFERS FROM");
    rrc_s3_result_t dr;
    rrc_s3_delete(&s3, key, &dr);
}

/* One real camera object through the real read/hash/upload pipeline to a scratch key
 * under the control prefix (never the photo library), verified by the same checks a
 * sync uses plus a read-back BLAKE3, then deleted. */
static void debug_campipe(const char *arg, uint64_t size)
{
    if (!s3_ready) { printf("cloud not configured\n"); return; }
    run_digest_probe_if_needed();
    if (!cam || !cam->connected(cam)) { printf("no camera attached\n"); return; }
    cam_object_t o = {0};
    o.handle = (uint32_t)strtoul(arg, NULL, 16);
    scpy(o.path, sizeof o.path, arg);
    scpy(o.name, sizeof o.name, arg);
    o.size = size;
    char key[160];
    snprintf(key, sizeof key, ".rrcloud/tmp/%s-campipe", app_device_id());
    cancel_flag = false;
    ue_camera_attached();
    char hex[65] = ""; uint64_t got = 0;
    memset(ue_stats(), 0, sizeof *ue_stats());
    int64_t t0 = esp_timer_get_time();
    esp_err_t e = upload_one(cam, &o, key, hex, &got);
    double secs = (double)(esp_timer_get_time() - t0) / 1e6;
    printf("camera -> bucket %llu bytes: %s in %.2f s = %.2f MB/s (usb direct %s)\n", (unsigned long long)got, esp_err_to_name(e), secs,
           secs > 0 ? (double)got / 1048576.0 / secs : 0.0, rrc_ptp_direct_enabled() ? "on" : "off");
    printf("  stages (overlapping): camera read %.2f s, blake3 %.2f s, md5 %.2f s, upload %.2f s\n",
           (double)ue_stats()->read_us / 1e6, (double)ue_stats()->hash_us / 1e6, (double)ue_stats()->md5_us / 1e6, (double)ue_stats()->upload_us / 1e6);
    if (e != ESP_OK) { ST_LOCK(); printf("  error: %s\n", st.last_error); ST_UNLOCK(); }
    else printf("  blake3 %s\n", hex);
    printf("  sync task stack: %u of %u bytes never used; internal heap free %u, largest block %u\n", (unsigned)uxTaskGetStackHighWaterMark(NULL), (unsigned)SYNC_TASK_STACK,
           (unsigned)heap_caps_get_free_size(MALLOC_CAP_INTERNAL), (unsigned)heap_caps_get_largest_free_block(MALLOC_CAP_INTERNAL));
    rrc_s3_result_t dr;
    rrc_s3_delete(&s3, key, &dr);
}

/* Several real camera objects through the engine at once ("handle:size handle:size ..."), to scratch keys
 * under the control prefix (never the library), verified and deleted: what a sync's window of files does. */
static void debug_multi(const char *list)
{
    if (!s3_ready) { printf("cloud not configured\n"); return; }
    if (!cam || !cam->connected(cam)) { printf("no camera attached\n"); return; }
    run_digest_probe_if_needed();
    cam_object_t objs[8]; char keys[8][160]; int n = 0;
    char *copy = strdup(list), *save = NULL;
    for (char *tok = strtok_r(copy, " ,", &save); tok && n < 8; tok = strtok_r(NULL, " ,", &save)) {
        char *colon = strchr(tok, ':');
        if (!colon) continue;
        memset(&objs[n], 0, sizeof objs[n]);
        objs[n].handle = (uint32_t)strtoul(tok, NULL, 16);
        scpy(objs[n].path, sizeof objs[n].path, tok); scpy(objs[n].name, sizeof objs[n].name, tok);
        objs[n].size = strtoull(colon + 1, NULL, 10);
        snprintf(keys[n], sizeof keys[n], ".rrcloud/tmp/%s-multi-%d", app_device_id(), n);
        n++;
    }
    free(copy);
    ue_set_abort(false);
    memset(ue_stats(), 0, sizeof *ue_stats());
    uint64_t bytes = 0; int ok = 0, inflight = 0, next = 0;
    int64_t t0 = esp_timer_get_time();
    while (next < n || inflight > 0) {
        while (next < n && inflight < RUN_WINDOW) {
            char err[96] = "";
            if (ue_submit(cam, &objs[next], keys[next], &keys[next], err, sizeof err)) inflight++; else printf("  submit %d failed: %s\n", next, err);
            next++;
        }
        if (!inflight) break;
        ue_job_t *dj = ue_wait(UINT32_MAX);
        char *key = ue_job_user(dj), hex[65], err[96] = "";
        uint64_t sz = 0;
        esp_err_t e = ue_finish(dj, hex, &sz, err, sizeof err);
        inflight--;
        printf("  %s: %s at %.2f s, %llu bytes%s%s\n", key + strlen(".rrcloud/tmp/"), esp_err_to_name(e), (double)(esp_timer_get_time() - t0) / 1e6, (unsigned long long)sz, e ? " — " : "", e ? err : "");
        if (e == ESP_OK) { ok++; bytes += sz; }
        rrc_s3_result_t dr; rrc_s3_delete(&s3, key, &dr);
    }
    double secs = (double)(esp_timer_get_time() - t0) / 1e6;
    printf("%d of %d files, %.1f MB in %.2f s = %.2f MB/s (window %d)\n", ok, n, (double)bytes / 1048576.0, secs, secs > 0 ? (double)bytes / 1048576.0 / secs : 0.0, RUN_WINDOW);
    printf("  stages (summed over files): camera read %.2f s, blake3 %.2f s, checksum %.2f s, upload %.2f s; internal heap %u free\n",
           (double)ue_stats()->read_us / 1e6, (double)ue_stats()->hash_us / 1e6, (double)ue_stats()->md5_us / 1e6, (double)ue_stats()->upload_us / 1e6,
           (unsigned)heap_caps_get_free_size(MALLOC_CAP_INTERNAL));
}

/* `count` synthetic files of `mb` MiB each through the engine with a window of RUN_WINDOW, to scratch keys,
 * verified by size/checksum and deleted: concurrent small files and concurrent large files, no camera. */
static void debug_pipemulti(int count, int mb)
{
    if (!s3_ready) { printf("cloud not configured\n"); return; }
    run_digest_probe_if_needed();
    if (count < 1) count = 12;
    if (count > 40) count = 40;
    synth_impl_t im = {.size = (uint64_t)(mb > 0 ? mb : 2) * 1024u * 1024u, .rate_kib_s = 0};
    cam_source_t src = {.kind = "synth", .source_id = "synth", .model = "synthetic", .serial = "0", .impl = &im,
                        .enumerate = synth_enumerate, .open = synth_open, .read = synth_read, .close = synth_close,
                        .connected = synth_connected, .release = synth_release};
    char (*keys)[96] = calloc((size_t)count, 96);
    cam_object_t o = {.name = "m.bin", .path = "m.bin", .size = im.size};
    ue_set_abort(false);
    memset(ue_stats(), 0, sizeof *ue_stats());
    int ok = 0, bad = 0, inflight = 0, next = 0;
    int64_t t0 = esp_timer_get_time();
    while (next < count || inflight > 0) {
        while (next < count && inflight < RUN_WINDOW) {
            snprintf(keys[next], 96, ".rrcloud/tmp/%s-pm-%d", app_device_id(), next);
            char err[96] = "";
            if (ue_submit(&src, &o, keys[next], keys[next], err, sizeof err)) inflight++; else { bad++; printf("  submit failed: %s\n", err); }
            next++;
        }
        if (!inflight) break;
        ue_job_t *dj = ue_wait(UINT32_MAX);
        char *key = ue_job_user(dj), hex[65], err[96] = "";
        uint64_t sz = 0;
        esp_err_t e = ue_finish(dj, hex, &sz, err, sizeof err);
        inflight--;
        if (e == ESP_OK) ok++; else { bad++; printf("  %s failed: %s\n", key, err); }
        rrc_s3_result_t dr; rrc_s3_delete(&s3, key, &dr);
    }
    double secs = (double)(esp_timer_get_time() - t0) / 1e6;
    double mbytes = (double)ok * (double)im.size / 1048576.0;
    printf("%d x %d MiB, window %d: %d ok, %d failed, %.1f MB in %.2f s = %.2f MB/s (%.2f files/s); internal heap %u free\n", count, mb, RUN_WINDOW, ok, bad, mbytes, secs,
           secs > 0 ? mbytes / secs : 0.0, secs > 0 ? ok / secs : 0.0, (unsigned)heap_caps_get_free_size(MALLOC_CAP_INTERNAL));
    free(keys);
}

/* Pure cloud-path throughput: PUT `mb` MiB of pattern from the part buffer to a
 * scratch key under the control prefix, then delete it. Camera reads are not
 * involved, so this isolates TLS + HTTP + TCP on the dock from the camera. */
static void debug_speed(int mb)
{
    if (!s3_ready) { printf("cloud not configured\n"); return; }
    size_t len = (size_t)(mb > 0 ? mb : 8) * 1024u * 1024u;
    if (len > PART_BUF_BYTES) len = PART_BUF_BYTES;
    for (size_t i = 0; i < len; i += 4096) memset(part_buf + i, (int)(0x30 + (i / 4096) % 64), 4096);
    char key[160];
    snprintf(key, sizeof key, ".rrcloud/tmp/%s-speedtest", app_device_id());
    rrc_s3_result_t res;
    int64_t t0 = esp_timer_get_time();
    esp_err_t e = rrc_s3_head(&s3, key, &res);
    int64_t t1 = esp_timer_get_time();
    printf("HEAD (connect + TLS handshake + round trip): %lld ms (%s, http %d)\n", (long long)((t1 - t0) / 1000), esp_err_to_name(e), res.status);
    {   /* the single-object PUT hashes its body twice before the first byte leaves */
        uint8_t d[32]; char hex[65];
        t0 = esp_timer_get_time(); rrc_md5(part_buf, len, d); t1 = esp_timer_get_time();
        printf("  content-md5 over %u MiB: %lld ms\n", (unsigned)(len >> 20), (long long)((t1 - t0) / 1000));
        t0 = esp_timer_get_time(); rrc_sigv4_sha256_hex(part_buf, len, hex); t1 = esp_timer_get_time();
        printf("  sigv4 payload sha256 over %u MiB: %lld ms (multipart parts skip this)\n", (unsigned)(len >> 20), (long long)((t1 - t0) / 1000));
    }
    t0 = esp_timer_get_time();
    e = rrc_s3_put(&s3, key, part_buf, len, "application/octet-stream", NULL, 0, &res);
    t1 = esp_timer_get_time();
    double secs = (double)(t1 - t0) / 1e6;
    printf("PUT %u MiB: %s (http %d) in %.2f s = %.2f MB/s\n", (unsigned)(len >> 20), esp_err_to_name(e), res.status, secs, (double)len / 1048576.0 / secs);
    printf("  of which: in write() %lld ms for %u bytes = %.2f MB/s (socket send %lld ms, TLS cpu %lld ms); waiting for the response %lld ms\n",
           (long long)(s3.stat_tx_us / 1000), (unsigned)s3.stat_tx_bytes,
           s3.stat_tx_us > 0 ? (double)s3.stat_tx_bytes / 1048576.0 / ((double)s3.stat_tx_us / 1e6) : 0.0,
           (long long)(s3.stat_send_us / 1000), (long long)((s3.stat_tx_us - s3.stat_send_us) / 1000), (long long)(s3.stat_wait_us / 1000));
    if (e == ESP_OK) {
        t0 = esp_timer_get_time();
        rrc_s3_delete(&s3, key, &res);
        t1 = esp_timer_get_time();
        printf("DELETE: %lld ms\n", (long long)((t1 - t0) / 1000));
    }
}

/* Raw TCP: connect, blast `mb` MiB from the part buffer in 64 KiB sends, report.
 * No HTTP, no TLS — the number is lwIP + EMAC + the wire. */
static void debug_tcp(const char *host, int port, int mb)
{
    size_t len = (size_t)(mb > 0 ? mb : 8) * 1024u * 1024u;
    if (len > PART_BUF_BYTES) len = PART_BUF_BYTES;
    struct sockaddr_in sa = {.sin_family = AF_INET, .sin_port = htons((uint16_t)port)};
    if (inet_pton(AF_INET, host, &sa.sin_addr) != 1) { printf("tcp-speed: need a dotted IPv4 address\n"); return; }
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) { printf("socket failed\n"); return; }
    int one = 1; setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
    if (net_eth_ip()[0]) { /* pin the test to the wire even when Wi-Fi shares the subnet */
        struct sockaddr_in src = {.sin_family = AF_INET, .sin_port = 0};
        inet_pton(AF_INET, net_eth_ip(), &src.sin_addr);
        if (bind(fd, (struct sockaddr *)&src, sizeof src) != 0) printf("bind to %s failed: errno %d\n", net_eth_ip(), errno);
    }
    int64_t t0 = esp_timer_get_time();
    if (connect(fd, (struct sockaddr *)&sa, sizeof sa) != 0) { printf("connect failed: errno %d\n", errno); close(fd); return; }
    int64_t t1 = esp_timer_get_time();
    size_t sent = 0; int err = 0;
    while (sent < len) {
        size_t n = len - sent > 65536 ? 65536 : len - sent;
        int w = send(fd, part_buf + sent, n, 0);
        if (w <= 0) { err = errno; break; }
        sent += (size_t)w;
    }
    int64_t t2 = esp_timer_get_time();
    close(fd);
    double secs = (double)(t2 - t1) / 1e6;
    printf("tcp-speed %s:%d: connect %lld ms, sent %u MiB in %.2f s = %.2f MB/s%s%d\n", host, port, (long long)((t1 - t0) / 1000), (unsigned)(sent >> 20), secs, (double)sent / 1048576.0 / secs, err ? ", errno " : "", err);
}

/* ---- task ------------------------------------------------------------------- */
static void sync_task(void *arg)
{
    (void)arg;
    msg_t m;
    for (;;) {
        if (xQueueReceive(q, &m, portMAX_DELAY) != pdTRUE) continue;
        /* console diagnostics move data over the network and USB: not beside an update (ota.h) */
        bool diag = m.cmd >= CMD_DEBUG_LS;
        if (diag && !ota_net_begin()) { printf("a firmware update is in progress; try again when it is done\n"); free(m.arg); continue; }
        switch (m.cmd) {
        case CMD_ATTACH:
            cam = m.src;
            cam_identified = strcmp(cam->kind, "ptp") != 0; /* MSC needs no session */
            run_deferred = false;
            ST_LOCK();
            st.camera_attached = true;
            scpy(st.camera_kind, sizeof st.camera_kind, cam->kind);
            scpy(st.camera_model, sizeof st.camera_model, cam->model);
            scpy(st.camera_serial, sizeof st.camera_serial, cam->serial);
            ST_UNLOCK();
            log_ring_printf("camera attached: %s [%s]", cam->model, cam->source_id);
            if (auto_start_allowed()) sync_run(cam);
            else if (crash_hold) set_error("automatic syncs are paused after repeated crashes; press Sync now to start one");
            break;
        case CMD_DETACH:
            if (cam) { cam->release(cam); }
            cam = NULL;
            run_deferred = false;
            ST_LOCK(); st.camera_attached = false; st.camera_kind[0] = 0; st.camera_model[0] = 0; st.camera_serial[0] = 0; st.current_file[0] = 0;
            if (st.phase == SYNC_ENUMERATING || st.phase == SYNC_UPLOADING) st.phase = SYNC_IDLE;
            ST_UNLOCK();
            log_ring_printf("camera detached");
            break;
        case CMD_SYNC_NOW:
            crash_hold = false;   /* the operator asked */
            if (cam && cam->connected(cam)) sync_run(cam);
            else { set_error("no camera attached"); }
            break;
        case CMD_CANCEL: break; /* flag already set */
        case CMD_TICK: on_tick(); break;
        case CMD_CONFIG: configure_s3(); break;
        case CMD_DEBUG_LS: debug_ls(); break;
        case CMD_DEBUG_HASH: debug_hash(m.arg ? m.arg : "", m.repeat); free(m.arg); break;
        case CMD_DEBUG_SPEED: debug_speed(m.repeat); break;
        case CMD_DEBUG_PIPE: debug_pipe(m.repeat, m.port); break;
        case CMD_DEBUG_PIPEMULTI: debug_pipemulti(m.repeat, m.port); break;
        case CMD_DEBUG_MULTI: debug_multi(m.arg ? m.arg : ""); free(m.arg); break;
        case CMD_DEBUG_CAMPIPE: debug_campipe(m.arg ? m.arg : "", (uint64_t)m.repeat * 1u); free(m.arg); break;
        case CMD_DEBUG_READ: debug_read(m.arg ? m.arg : "", m.repeat, m.port); free(m.arg); break;
        case CMD_DEBUG_TCP: { char *colon = m.arg ? strrchr(m.arg, ':') : NULL; if (colon) { *colon = 0; debug_tcp(m.arg, atoi(colon + 1), m.repeat); } free(m.arg); break; }
        }
        if (diag) ota_net_end();
    }
}

static void tick_cb(void *arg) { (void)arg; msg_t m = {.cmd = CMD_TICK}; xQueueSend(q, &m, 0); }

void sync_on_camera_event(cam_event_t ev, cam_source_t *src, void *arg)
{
    (void)arg;
    if (ev == CAM_EV_DETACHED) cancel_flag = true;
    if (ev == CAM_EV_ATTACHED) ue_camera_attached();   /* a new body gets its own zero-copy USB check */
    msg_t m = {.cmd = ev == CAM_EV_ATTACHED ? CMD_ATTACH : CMD_DETACH, .src = src};
    xQueueSend(q, &m, pdMS_TO_TICKS(100));
}

esp_err_t sync_init(void)
{
    resume_check_at_boot();
    st_mtx = xSemaphoreCreateMutex();
    q = xQueueCreate(16, sizeof(msg_t));
    ue_config_t uc = {.ctl = &s3, .conn = {&s3b, &s3c}, .device_id = app_device_id(), .progress = on_progress};
    esp_err_t ue = ue_init(&uc);
    if (ue != ESP_OK) { ESP_LOGE(TAG, "upload engine failed to start: %s", esp_err_to_name(ue)); return ue; }
    part_buf = ue_slot_buf(0);
    ST_LOCK();
    st.lifetime_uploaded = (uint32_t)store_ledger_uploaded_count();
    st.lifetime_bytes = store_ledger_uploaded_bytes();
    st.last_manifest_ts = manifest_ts();
    ST_UNLOCK();
    configure_s3();
    if (xTaskCreatePinnedToCore(sync_task, "sync", SYNC_TASK_STACK, NULL, 4, NULL, tskNO_AFFINITY) != pdPASS) return ESP_ERR_NO_MEM;
    const esp_timer_create_args_t ta = {.callback = tick_cb, .name = "sync_tick"};
    esp_timer_create(&ta, &tick_timer);
    esp_timer_start_periodic(tick_timer, 60ull * 1000 * 1000);
    return ESP_OK;
}

void sync_get_status(sync_status_t *out)
{
    if (!st_mtx) { memset(out, 0, sizeof *out); return; }   /* asked before sync_init() */
    ST_LOCK(); *out = st; out->pending_entries = 0; ST_UNLOCK();
    out->pending_entries = store_journal_pending_entries();
}
void sync_request_now(void) { msg_t m = {.cmd = CMD_SYNC_NOW}; xQueueSend(q, &m, 0); }
void sync_cancel(void) { cancel_flag = true; msg_t m = {.cmd = CMD_CANCEL}; xQueueSend(q, &m, 0); }
void sync_config_changed(void) { msg_t m = {.cmd = CMD_CONFIG}; xQueueSend(q, &m, 0); }
void sync_debug_list(void) { msg_t m = {.cmd = CMD_DEBUG_LS}; xQueueSend(q, &m, 0); }
void sync_debug_speed(int mb) { msg_t m = {.cmd = CMD_DEBUG_SPEED, .repeat = mb}; xQueueSend(q, &m, 0); }
void sync_debug_read(const char *arg, int mb, int chunk_kib)
{
    char *a = strdup(arg ? arg : "");
    if (!a) return;
    msg_t m = {.cmd = CMD_DEBUG_READ, .arg = a, .repeat = mb, .port = chunk_kib};
    if (xQueueSend(q, &m, 0) != pdTRUE) free(a);
}
void sync_debug_pipemulti(int count, int mb) { msg_t m = {.cmd = CMD_DEBUG_PIPEMULTI, .repeat = count, .port = mb}; xQueueSend(q, &m, 0); }
void sync_debug_multi(const char *list)
{
    char *a = strdup(list ? list : "");
    if (!a) return;
    msg_t m = {.cmd = CMD_DEBUG_MULTI, .arg = a};
    if (xQueueSend(q, &m, 0) != pdTRUE) free(a);
}
void sync_debug_campipe(const char *handle, int size_bytes)
{
    char *a = strdup(handle ? handle : "");
    if (!a) return;
    msg_t m = {.cmd = CMD_DEBUG_CAMPIPE, .arg = a, .repeat = size_bytes};
    if (xQueueSend(q, &m, 0) != pdTRUE) free(a);
}
void sync_debug_pipe(int mb, int rate_mb_s) { msg_t m = {.cmd = CMD_DEBUG_PIPE, .repeat = mb, .port = rate_mb_s}; xQueueSend(q, &m, 0); }
void sync_debug_tcp_speed(const char *host, int port, int mb)
{
    char *arg = malloc(80);
    if (!arg) return;
    snprintf(arg, 80, "%s:%d", host, port);
    msg_t m = {.cmd = CMD_DEBUG_TCP, .arg = arg, .repeat = mb};
    if (xQueueSend(q, &m, 0) != pdTRUE) free(arg);
}
void sync_debug_hash(const char *handle_or_path, int repeat)
{
    msg_t m = {.cmd = CMD_DEBUG_HASH, .arg = strdup(handle_or_path ? handle_or_path : ""), .repeat = repeat};
    if (xQueueSend(q, &m, 0) != pdTRUE) free(m.arg);
}

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
    if (run_t0_us) {
        const ue_stats_t *u = ue_stats();
        s.run_elapsed_ms = (uint32_t)(((run_end_us ? run_end_us : esp_timer_get_time()) - run_t0_us) / 1000);
        s.stage_read_s = (double)u->read_us / 1e6; s.stage_hash_s = (double)u->hash_us / 1e6;
        s.stage_sum_s = (double)u->md5_us / 1e6; s.stage_upload_s = (double)u->upload_us / 1e6;
    }
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
    rrc_jsonw_key(&w, "run_total_bytes", false); rrc_jsonw_u64(&w, s.run_total_bytes);
    rrc_jsonw_key(&w, "run_read_bytes", false); rrc_jsonw_u64(&w, s.run_read_bytes);
    rrc_jsonw_key(&w, "run_inflight", false); rrc_jsonw_u64(&w, s.run_inflight);
    rrc_jsonw_key(&w, "run_elapsed_ms", false); rrc_jsonw_u64(&w, s.run_elapsed_ms);
    rrc_jsonw_key(&w, "stage_read_ms", false); rrc_jsonw_u64(&w, (uint64_t)(s.stage_read_s * 1000.0));
    rrc_jsonw_key(&w, "stage_hash_ms", false); rrc_jsonw_u64(&w, (uint64_t)(s.stage_hash_s * 1000.0));
    rrc_jsonw_key(&w, "stage_sum_ms", false); rrc_jsonw_u64(&w, (uint64_t)(s.stage_sum_s * 1000.0));
    rrc_jsonw_key(&w, "stage_upload_ms", false); rrc_jsonw_u64(&w, (uint64_t)(s.stage_upload_s * 1000.0));
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

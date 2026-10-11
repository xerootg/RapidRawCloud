#include "sync.h"
#include "util.h"
#include <stdarg.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include "esp_log.h"
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

static const char *TAG = "sync";

#define PART_BUF_BYTES   (8u * 1024u * 1024u)   /* one multipart part / max single PUT */
/* The sync task runs the S3 client and its TLS session (mbedTLS handshake and
 * record buffers on the stack) under several KiB of path/key/journal-line locals;
 * 24 KiB overflowed by ~6 KiB on hardware (stack protection fault in "sync"). */
#define SYNC_TASK_STACK  (48u * 1024u)
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

typedef enum { CMD_ATTACH, CMD_DETACH, CMD_SYNC_NOW, CMD_CANCEL, CMD_TICK, CMD_CONFIG, CMD_DEBUG_LS, CMD_DEBUG_HASH, CMD_DEBUG_SPEED, CMD_DEBUG_TCP, CMD_DEBUG_PIPE, CMD_DEBUG_READ, CMD_DEBUG_CAMPIPE } cmd_t;
typedef struct { cmd_t cmd; cam_source_t *src; char *arg; int repeat; int port; } msg_t;

static QueueHandle_t q;
static SemaphoreHandle_t st_mtx;
static sync_status_t st;
static rrc_s3_t s3;
static rrc_s3_t s3b;                  /* second connection, see upload_worker */
static bool s3_ready;
static cam_source_t *cam;             /* attached camera (or NULL) */
static volatile bool cancel_flag;
#define PART_SLOTS 3
static uint8_t *part_bufs[PART_SLOTS];   /* read-ahead pipeline buffers, PSRAM */
static uint8_t *part_buf;                /* = part_bufs[0]; scratch for the diagnostics */
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
    if (s3_ready) { rrc_s3_disconnect(&s3); rrc_s3_disconnect(&s3b); }
    rrc_s3_init(&s3b, &cfg);
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

/* ---- upload ---------------------------------------------------------------- */
typedef struct { rrc_blake3_ctx b3; uint64_t n; } hash_sink_t;
static int hash_sink(void *ctx, const uint8_t *d, size_t n) { hash_sink_t *h = ctx; rrc_blake3_update(&h->b3, d, n); h->n += n; return 0; }

/* ---- read-ahead pipeline ----------------------------------------------------
 * Reading a part from the camera (PTP runs at a few MB/s), hashing it and pushing
 * the previous part through TLS are independent, so they overlap on the two
 * cores: reader -> hasher -> uploader (the sync task), PART_SLOTS buffers in
 * flight. The reader sits in the camera transfer most of the time, the hasher
 * runs BLAKE3 (~11 MB/s in software) and MD5, the uploader does TLS. The tasks
 * only meet at the queues; the reader's final `last` message travels through
 * the hasher, and the uploader waits for it before touching the source again. */
typedef struct {
    int slot;        /* index into part_bufs */
    size_t len;      /* bytes in the buffer */
    esp_err_t err;   /* read error; len bytes before it are still valid */
    bool last;       /* nothing follows (EOF, short part, or error) */
    uint8_t md5[16]; /* filled by the hasher: Content-MD5 / single-PUT ETag */
} part_msg_t;

/* Where the last upload_object spent its time, per stage (microseconds). The
 * stages overlap, so the sum exceeds the wall time; pipe-test prints them. */
static struct { int64_t read_us, hash_us, md5_us, upload_us; } pipe_stats;

typedef struct {
    cam_source_t *src;
    void *fh;
    uint64_t size;
    rrc_blake3_ctx *b3;
    QueueHandle_t free_q, hash_q, md5_q, full_q;
    TaskHandle_t reader_h, hasher_h, md5_h;
    volatile bool abort;   /* uploader gave up; stop reading */
    uint64_t fail_at;
    esp_err_t fail_err;
} reader_job_t;

#define READER_TASK_STACK 16384

static void reader_task(void *arg)
{
    reader_job_t *j = arg;
    uint64_t total = 0;
    for (;;) {
        int slot = 0;
        xQueueReceive(j->free_q, &slot, portMAX_DELAY);
        part_msg_t m = {.slot = slot};
        uint8_t *buf = part_bufs[slot];
        while (m.len < PART_BUF_BYTES && total + m.len < j->size) {
            if (j->abort || cancel_flag || !j->src->connected(j->src)) { m.err = ESP_ERR_INVALID_STATE; break; }
            size_t want = PART_BUF_BYTES - m.len;
            if (want > READ_CHUNK) want = READ_CHUNK;
            if (want > j->size - total - m.len) want = (size_t)(j->size - total - m.len);
            size_t got = 0;
            int64_t t0 = esp_timer_get_time();
            esp_err_t e = j->src->read(j->src, j->fh, total + m.len, buf + m.len, want, &got);
            pipe_stats.read_us += esp_timer_get_time() - t0;
            if (e != ESP_OK) { j->fail_at = total + m.len; j->fail_err = e; m.err = e; break; }
            if (got == 0) break; /* unexpected EOF: a short part ends the object */
            m.len += got;
            ST_LOCK(); st.current_done = total + m.len; ST_UNLOCK();
        }
        total += m.len;
        m.last = m.err != ESP_OK || total >= j->size || m.len < PART_BUF_BYTES;
        xQueueSend(j->hash_q, &m, portMAX_DELAY);
        if (m.last) break;
    }
    for (;;) vTaskSuspend(NULL);   /* the uploader deletes this task (its stack is a caps allocation) */
}

#define HASHER_TASK_STACK 4096

static void hasher_task(void *arg)
{
    reader_job_t *j = arg;
    for (;;) {
        part_msg_t m;
        xQueueReceive(j->hash_q, &m, portMAX_DELAY);
        if (m.err == ESP_OK && m.len && !j->abort) {
            int64_t t0 = esp_timer_get_time();
            rrc_blake3_update(j->b3, part_bufs[m.slot], m.len);
            pipe_stats.hash_us += esp_timer_get_time() - t0;
        }
        xQueueSend(j->md5_q, &m, portMAX_DELAY);
        if (m.last) break;
    }
    for (;;) vTaskSuspend(NULL);
}

/* MD5 is its own stage: BLAKE3 and MD5 in one task capped the pipeline at ~6 MB/s. */
static void md5_task(void *arg)
{
    reader_job_t *j = arg;
    for (;;) {
        part_msg_t m;
        xQueueReceive(j->md5_q, &m, portMAX_DELAY);
        if (m.err == ESP_OK && !j->abort) {
            int64_t t0 = esp_timer_get_time();
            rrc_md5(part_bufs[m.slot], m.len, m.md5);
            pipe_stats.md5_us += esp_timer_get_time() - t0;
        }
        xQueueSend(j->full_q, &m, portMAX_DELAY);
        if (m.last) break;
    }
    for (;;) vTaskSuspend(NULL);
}

/* The reader and hasher run on PSRAM stacks: they never touch flash, and 16 KiB of
 * internal RAM matters (the hardware AES driver and TLS fail to allocate without it).
 * The sync task itself keeps an internal stack because it writes NVS and LittleFS. */
static void job_destroy(reader_job_t *j)
{
    if (j->reader_h) vTaskDeleteWithCaps(j->reader_h);
    if (j->hasher_h) vTaskDeleteWithCaps(j->hasher_h);
    if (j->md5_h) vTaskDeleteWithCaps(j->md5_h);
    if (j->md5_q) vQueueDelete(j->md5_q);
    if (j->free_q) vQueueDelete(j->free_q);
    if (j->hash_q) vQueueDelete(j->hash_q);
    if (j->full_q) vQueueDelete(j->full_q);
    free(j);
}

/* Stops the reader (if still running) and waits for its final message. `held`
 * is a slot the uploader still owns, handed back so a blocked reader can exit. */
static void reader_join(reader_job_t *j, bool *done, int held)
{
    j->abort = true;
    if (held >= 0) xQueueSend(j->free_q, &held, 0);
    while (!*done) {
        part_msg_t m;
        xQueueReceive(j->full_q, &m, portMAX_DELAY);
        *done = m.last;
    }
}

/* ---- second upload connection ------------------------------------------------
 * One TCP stream over this ~40 ms WAN path tops out near 6 MB/s (a lone Reno-style
 * sender loses ground to the occasional drop; the laptop shows the same: 9 MB/s on
 * one stream, 12-13 on two or more). Multipart parts are independent, so a worker task
 * with its own connection uploads every other part while the sync task uploads the
 * rest. The worker is persistent (one TLS handshake, then keep-alive across files)
 * and has an internal-RAM stack because the hardware AES-GCM engine only takes
 * DMA-capable buffers and mbedTLS keeps its AAD on the stack. */
typedef struct {
    const char *key, *upload_id;
    int part_no, slot;
    const uint8_t *data, *md5;
    size_t len;
    char *etag;
    QueueHandle_t free_q;
} part_job_t;

#define WORKER_STACK (24u * 1024u)
static uint8_t pj_md5[PART_SLOTS][16];   /* the MD5 must outlive the message that carried it */
static QueueHandle_t wq;
static SemaphoreHandle_t w_idle;
static volatile bool w_busy;
static volatile esp_err_t w_err;
static char w_errmsg[96];
static bool worker_ready;

static void upload_worker(void *arg)
{
    (void)arg;
    for (;;) {
        part_job_t j;
        xQueueReceive(wq, &j, portMAX_DELAY);
        s3b.have_server_offset = s3.have_server_offset;
        s3b.server_offset_s = s3.server_offset_s;
        rrc_s3_result_t res = {0};
        int64_t t0 = esp_timer_get_time();
        esp_err_t e = rrc_s3_upload_part(&s3b, j.key, j.upload_id, j.part_no, j.data, j.len, j.md5, j.etag, 40, &res);
        pipe_stats.upload_us += esp_timer_get_time() - t0;
        if (e != ESP_OK && w_err == ESP_OK) {
            char b[64];
            snprintf(w_errmsg, sizeof w_errmsg, "part %d failed: %s", j.part_no, rrc_s3_result_str(&res, b, sizeof b));
            w_err = e;
        }
        xQueueSend(j.free_q, &j.slot, 0);
        w_busy = false;
        xSemaphoreGive(w_idle);
    }
}

/* Waits (bounded by the S3 timeout) until the worker has finished its part. */
static void worker_quiesce(void)
{
    for (int i = 0; w_busy && i < 700; i++) xSemaphoreTake(w_idle, pdMS_TO_TICKS(100));
}

/* ---- zero-copy USB read self-check ---------------------------------------------
 * The USB controller DMAs straight into the part buffers and the stack does cache
 * maintenance on line-rounded ranges, so a mistake there would silently corrupt a
 * photo (the upload verifies the hash of what it read, not what the camera sent).
 * Once per camera attach, before the first upload, the first part of the first
 * file is read both ways and compared byte for byte, with guard patterns around
 * the destination to catch stray writes. A mismatch turns the DMA path off for
 * the rest of the session and says so in the log. */
static bool direct_checked;

/* NVS "usb_direct": 0 verified/unknown, 1 a check was running (a crash during it
 * leaves this at 1, so the next boot treats the path as broken instead of looping),
 * 2 disabled by a failed or crashed check. `usb-direct on` clears it. */
static uint8_t direct_state_get(void)
{
    nvs_handle_t h; uint8_t v = 0;
    if (nvs_open("rrc", NVS_READONLY, &h) == ESP_OK) { nvs_get_u8(h, "usb_direct", &v); nvs_close(h); }
    return v;
}
void sync_usb_direct_state_set(uint8_t v)
{
    nvs_handle_t h;
    if (nvs_open("rrc", NVS_READWRITE, &h) == ESP_OK) { nvs_set_u8(h, "usb_direct", v); nvs_commit(h); nvs_close(h); }
}
static void direct_state_boot(void)
{
    uint8_t v = direct_state_get();
    if (v == 1) { sync_usb_direct_state_set(2); v = 2; log_ring_printf("usb: the previous zero-copy DMA check did not finish (reset during it); copying path in use until 'usb-direct on'"); }
    if (v == 2) rrc_ptp_set_direct(false);
}

static void direct_selfcheck(cam_source_t *src, const cam_object_t *o, void *fh)
{
    if (direct_checked || strcmp(src->kind, "ptp") != 0 || !rrc_ptp_direct_enabled() || o->size < 256 * 1024) return;
    direct_checked = true;
    sync_usb_direct_state_set(1);
    const size_t n = (size_t)(o->size < 1024 * 1024 + 4100 ? o->size : 1024 * 1024 + 4100);   /* ends mid cache line on purpose */
    uint8_t *a = part_bufs[0], *b = part_bufs[1];
    memset(a - 12, 0xA5, 12 + n + 192); memset(b - 12, 0x5A, 12 + n + 192);
    size_t ga = 0, gb = 0;
    rrc_ptp_set_direct(true);
    esp_err_t ea = src->read(src, fh, 0, a, n, &ga);
    rrc_ptp_set_direct(false);
    esp_err_t eb = src->read(src, fh, 0, b, n, &gb);
    rrc_ptp_set_direct(true);
    bool guards = true;
    for (int i = -12; i < 0; i++) guards &= a[i] == 0xA5;       /* the 12 bytes the container header lands on */
    for (size_t i = n; i < n + 192; i++) guards &= a[i] == 0xA5; /* nothing past the end */
    bool same = ea == ESP_OK && eb == ESP_OK && ga == n && gb == n && memcmp(a, b, n) == 0;
    if (same && guards) {
        sync_usb_direct_state_set(0);
        log_ring_printf("usb: zero-copy DMA read verified against the copying path (%u bytes identical)", (unsigned)n);
        return;
    }
    size_t first = 0;
    if (ea == ESP_OK && eb == ESP_OK) while (first < n && a[first] == b[first]) first++;
    rrc_ptp_set_direct(false);
    sync_usb_direct_state_set(2);
    log_ring_printf("usb: zero-copy DMA read FAILED its check (%s/%s, got %u/%u of %u, first diff at %u, guards %s); using the copying path",
                    esp_err_to_name(ea), esp_err_to_name(eb), (unsigned)ga, (unsigned)gb, (unsigned)n, (unsigned)first, guards ? "ok" : "corrupted");
}

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
    reader_job_t *job = NULL;
    bool reader_done = true;
    int held = -1;
    memset(&pipe_stats, 0, sizeof pipe_stats);
    w_err = ESP_OK;
    if (multipart) {
        e = rrc_s3_multipart_create(&s3, key, "application/octet-stream", meta, 1, upload_id, sizeof upload_id, &res);
        if (e != ESP_OK) { char b[96]; set_error("multipart create failed: %s", rrc_s3_result_str(&res, b, sizeof b)); goto fail; }
        etags = heap_caps_malloc(1024 * 40, MALLOC_CAP_SPIRAM | MALLOC_CAP_8BIT);
        if (!etags) { e = ESP_ERR_NO_MEM; goto fail; }
    }
    direct_selfcheck(src, o, fh);
    job = calloc(1, sizeof *job);
    if (!job) { e = ESP_ERR_NO_MEM; goto fail; }
    job->src = src; job->fh = fh; job->size = o->size; job->b3 = &b3;
    job->free_q = xQueueCreate(PART_SLOTS, sizeof(int));
    job->hash_q = xQueueCreate(PART_SLOTS, sizeof(part_msg_t));
    job->md5_q = xQueueCreate(PART_SLOTS, sizeof(part_msg_t));
    job->full_q = xQueueCreate(PART_SLOTS, sizeof(part_msg_t));
    if (!job->free_q || !job->hash_q || !job->md5_q || !job->full_q) { e = ESP_ERR_NO_MEM; goto fail; }
    for (int i = 0; i < PART_SLOTS; i++) xQueueSend(job->free_q, &i, 0);
    const uint32_t stack_caps = MALLOC_CAP_SPIRAM | MALLOC_CAP_8BIT;
    if (xTaskCreatePinnedToCoreWithCaps(md5_task, "cam_md5", HASHER_TASK_STACK, job, 4, &job->md5_h, tskNO_AFFINITY, stack_caps) != pdPASS) { set_error("cannot start the md5 stage"); e = ESP_ERR_NO_MEM; goto fail; }
    reader_done = false; /* from here the stages own a message stream that must end with `last` */
    part_msg_t end = {.slot = 0, .len = 0, .err = ESP_ERR_NO_MEM, .last = true};
    if (xTaskCreatePinnedToCoreWithCaps(hasher_task, "cam_hash", HASHER_TASK_STACK, job, 4, &job->hasher_h, tskNO_AFFINITY, stack_caps) != pdPASS) {
        xQueueSend(job->md5_q, &end, portMAX_DELAY);
        set_error("cannot start the hasher"); e = ESP_ERR_NO_MEM; goto fail;
    }
    if (xTaskCreatePinnedToCoreWithCaps(reader_task, "cam_read", READER_TASK_STACK, job, 4, &job->reader_h, tskNO_AFFINITY, stack_caps) != pdPASS) {
        xQueueSend(job->hash_q, &end, portMAX_DELAY);
        set_error("cannot start the camera reader"); e = ESP_ERR_NO_MEM; goto fail;
    }
    for (;;) {
        part_msg_t m;
        xQueueReceive(job->full_q, &m, portMAX_DELAY);
        reader_done = m.last;
        held = m.slot;
        if (m.err == ESP_ERR_INVALID_STATE) { e = m.err; goto fail; }
        if (m.err != ESP_OK) { set_error("camera read failed at %llu: %s", (unsigned long long)job->fail_at, esp_err_to_name(m.err)); e = m.err; goto fail; }
        uint8_t *buf = part_bufs[m.slot];
        if (m.len == 0 && total > 0) break;
        int64_t t_up = esp_timer_get_time();
        if (multipart) {
            if (parts >= 1024) { e = ESP_ERR_INVALID_SIZE; goto fail; }
            if (w_err != ESP_OK) { set_error("%s", w_errmsg); e = w_err; goto fail; }
            if (worker_ready && !w_busy) {
                /* hand this part to the second connection; the worker releases the slot */
                part_job_t pj = {.key = key, .upload_id = upload_id, .part_no = parts + 1, .slot = m.slot, .data = buf, .md5 = m.md5, .len = m.len,
                                 .etag = etags[parts], .free_q = job->free_q};
                memcpy(pj_md5[m.slot], m.md5, 16);
                pj.md5 = pj_md5[m.slot];
                w_busy = true;
                xSemaphoreTake(w_idle, 0);
                xQueueSend(wq, &pj, 0);
                parts++;
                total += m.len;
                held = -1;
                if (m.last) break;
                continue;
            }
            e = rrc_s3_upload_part(&s3, key, upload_id, parts + 1, buf, m.len, m.md5, etags[parts], 40, &res);
            if (e != ESP_OK) { char b[96]; set_error("part %d failed: %s", parts + 1, rrc_s3_result_str(&res, b, sizeof b)); goto fail; }
            parts++;
        } else {
            e = rrc_s3_put_md5(&s3, key, buf, m.len, "application/octet-stream", meta, 1, m.md5, &res);
            if (e != ESP_OK) { char b[96]; set_error("put failed: %s", rrc_s3_result_str(&res, b, sizeof b)); goto fail; }
            rrc_hex_lower(m.md5, 16, single_md5_hex);
        }
        pipe_stats.upload_us += esp_timer_get_time() - t_up;
        total += m.len;
        xQueueSend(job->free_q, &m.slot, 0);
        held = -1;
        if (m.last) break;
    }
    if (!reader_done) reader_join(job, &reader_done, held);
    held = -1;
    worker_quiesce();
    if (w_err != ESP_OK) { set_error("%s", w_errmsg); e = w_err; goto fail; }
    if (total != o->size) { set_error("short read from camera: %llu of %llu bytes", (unsigned long long)total, (unsigned long long)o->size); e = ESP_FAIL; goto fail; }
    if (multipart) {
        /* Heap, not stack: up to 1024 pointers (4 KiB) on top of the TLS session
         * below this frame overflowed the sync task on hardware. */
        const char **etag_ptrs = calloc((size_t)(parts > 0 ? parts : 1), sizeof *etag_ptrs);
        if (!etag_ptrs) { set_error("multipart complete: out of memory"); e = ESP_ERR_NO_MEM; goto fail; }
        for (int i = 0; i < parts; i++) etag_ptrs[i] = etags[i];
        e = rrc_s3_multipart_complete(&s3, key, upload_id, etag_ptrs, parts, &res);
        free(etag_ptrs);
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
    job_destroy(job);
    src->close(src, fh);
    return ESP_OK;
fail:
    worker_quiesce();
    if (multipart && upload_id[0]) { rrc_s3_result_t ar; rrc_s3_multipart_abort(&s3, key, upload_id, &ar); }
fail_nomp:
    if (job) {
        if (!reader_done) reader_join(job, &reader_done, held);
        job_destroy(job);
    }
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
    if (!s3_ready) { run_deferred = true; set_error("cloud not configured — pair or enter S3 settings first; the sync starts once it is"); return; }
    if (!net_has_ip()) { run_deferred = true; set_phase(SYNC_WAITING_NETWORK); set_error("waiting for the network"); return; }
    if (!strcmp(src->kind, "ptp") && !cam_identified) {
        if (source_ptp_identify(src) != ESP_OK) { run_deferred = true; set_error("camera did not answer OpenSession/GetDeviceInfo — is it awake and in MTP/PTP USB mode? retrying"); return; }
        cam_identified = true;
        ST_LOCK(); scpy(st.camera_model, sizeof st.camera_model, src->model); scpy(st.camera_serial, sizeof st.camera_serial, src->serial); ST_UNLOCK();
    }
    run_deferred = false;
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
        /* Dual-slot bodies (Nikon "backup" to the second card) list the same photo
         * twice under two storages; the copies are not byte-identical, so only the
         * first one seen is the library's. Record the mirror so it is never retried. */
        if (store_ledger_relkey_uploaded(relkey, o->size)) {
            rrc_ledger_rec mirror = {.source_id = src->source_id, .source_path = o->path, .size = o->size, .mtime = o->mtime, .relkey = relkey, .blake3_hex = "", .seq = 0, .ts = 0, .status = RRC_LEDGER_REMOTE_EXISTS};
            log_ring_printf("skip %s: the same photo was already uploaded from this camera's other card slot", relkey);
            store_ledger_append(&mirror);
            ST_LOCK(); st.run_skipped++; ST_UNLOCK();
            continue;
        }
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
        /* Peek, encode, append: the seq is committed by the append (store.h), so
         * an encode/append failure leaves no gap and a reboot cannot reuse it. */
        uint64_t seq = store_journal_next_seq();
        int64_t ts = rrc_s3_server_now(&s3);
        rrc_journal_put_original je = {.seq = seq, .ts = ts, .device = app_device_id(), .bucket_key = key, .vv_self = 1, .size = size,
                                       .blake3_hex = b3hex, .content_id_hex = b3hex, .has_mtime = o->mtime > 0, .mtime = o->mtime};
        char line[1200];
        if (rrc_journal_encode_put_original(&je, line, sizeof line) < 0 || store_journal_append_pending(line) != ESP_OK) {
            set_error("%s: journal append failed (flash full?) — object is in the bucket and will be adopted by the worker", relkey);
            ST_LOCK(); st.run_failed++; ST_UNLOCK();
            continue;
        }
        lr.status = RRC_LEDGER_UPLOADED; lr.blake3_hex = b3hex; lr.seq = seq; lr.ts = ts;
        store_ledger_append(&lr);
        store_kv_set_u64("manifest_dirty", 1);
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
        if (uploaded_any || store_kv_get_u64("manifest_dirty", 0) || manifest_ts() == 0) publish_manifest();
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
    /* A run that could not start (no network yet, cloud unconfigured, camera asleep)
     * is retried here instead of waiting for a replug. */
    if (run_deferred && cam && cam->connected(cam) && app_config_get()->auto_sync) sync_run(cam);
    if (!s3_ready || !net_has_ip()) return;
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
    int64_t t0 = esp_timer_get_time();
    esp_err_t e = upload_object(&src, &o, key, got_hex, &size);
    int64_t t1 = esp_timer_get_time();
    double secs = (double)(t1 - t0) / 1e6;
    printf("pipeline %d MiB at %s: %s in %.2f s = %.2f MB/s%s\n", mb, rate_mb_s > 0 ? "emulated camera rate" : "generator speed",
           esp_err_to_name(e), secs, (double)im.size / 1048576.0 / secs, rate_mb_s > 0 ? "" : " (upload-bound)");
    if (rate_mb_s > 0) printf("  camera alone would take %.2f s at %d MB/s; serial read-then-upload would add the upload time on top\n", (double)mb / rate_mb_s, rate_mb_s);
    printf("  stages (overlapping): camera read %.2f s, blake3 %.2f s, md5 %.2f s, upload %.2f s\n",
           (double)pipe_stats.read_us / 1e6, (double)pipe_stats.hash_us / 1e6, (double)pipe_stats.md5_us / 1e6, (double)pipe_stats.upload_us / 1e6);
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
    if (!cam || !cam->connected(cam)) { printf("no camera attached\n"); return; }
    cam_object_t o = {0};
    o.handle = (uint32_t)strtoul(arg, NULL, 16);
    scpy(o.path, sizeof o.path, arg);
    scpy(o.name, sizeof o.name, arg);
    o.size = size;
    char key[160];
    snprintf(key, sizeof key, ".rrcloud/tmp/%s-campipe", app_device_id());
    cancel_flag = false;
    direct_checked = false;
    char hex[65] = ""; uint64_t got = 0;
    int64_t t0 = esp_timer_get_time();
    esp_err_t e = upload_object(cam, &o, key, hex, &got);
    double secs = (double)(esp_timer_get_time() - t0) / 1e6;
    printf("camera -> bucket %llu bytes: %s in %.2f s = %.2f MB/s (usb direct %s)\n", (unsigned long long)got, esp_err_to_name(e), secs,
           secs > 0 ? (double)got / 1048576.0 / secs : 0.0, rrc_ptp_direct_enabled() ? "on" : "off");
    printf("  stages (overlapping): camera read %.2f s, blake3 %.2f s, md5 %.2f s, upload %.2f s\n",
           (double)pipe_stats.read_us / 1e6, (double)pipe_stats.hash_us / 1e6, (double)pipe_stats.md5_us / 1e6, (double)pipe_stats.upload_us / 1e6);
    if (e != ESP_OK) { ST_LOCK(); printf("  error: %s\n", st.last_error); ST_UNLOCK(); }
    else printf("  blake3 %s\n", hex);
    printf("  sync task stack: %u of %u bytes never used; internal heap free %u, largest block %u\n", (unsigned)uxTaskGetStackHighWaterMark(NULL), (unsigned)SYNC_TASK_STACK,
           (unsigned)heap_caps_get_free_size(MALLOC_CAP_INTERNAL), (unsigned)heap_caps_get_largest_free_block(MALLOC_CAP_INTERNAL));
    rrc_s3_result_t dr;
    rrc_s3_delete(&s3, key, &dr);
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
            if (app_config_get()->auto_sync) sync_run(cam);
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
        case CMD_DEBUG_CAMPIPE: debug_campipe(m.arg ? m.arg : "", (uint64_t)m.repeat * 1u); free(m.arg); break;
        case CMD_DEBUG_READ: debug_read(m.arg ? m.arg : "", m.repeat, m.port); free(m.arg); break;
        case CMD_DEBUG_TCP: { char *colon = m.arg ? strrchr(m.arg, ':') : NULL; if (colon) { *colon = 0; debug_tcp(m.arg, atoi(colon + 1), m.repeat); } free(m.arg); break; }
        }
    }
}

static void tick_cb(void *arg) { (void)arg; msg_t m = {.cmd = CMD_TICK}; xQueueSend(q, &m, 0); }

void sync_on_camera_event(cam_event_t ev, cam_source_t *src, void *arg)
{
    (void)arg;
    if (ev == CAM_EV_DETACHED) cancel_flag = true;
    if (ev == CAM_EV_ATTACHED) direct_checked = false;   /* a new body gets its own check */
    msg_t m = {.cmd = ev == CAM_EV_ATTACHED ? CMD_ATTACH : CMD_DETACH, .src = src};
    xQueueSend(q, &m, pdMS_TO_TICKS(100));
}

esp_err_t sync_init(void)
{
    st_mtx = xSemaphoreCreateMutex();
    q = xQueueCreate(16, sizeof(msg_t));
    /* The USB controller DMAs PTP data straight into these (see rrc_ptp_read_partial_direct):
     * the 12-byte container header precedes the payload in the stream, so the payload
     * starts 12 bytes into a cache-line-aligned block, and the block has slack for the
     * line rounding the USB stack applies to the last transfer. */
    wq = xQueueCreate(1, sizeof(part_job_t));
    w_idle = xSemaphoreCreateBinary();
    worker_ready = wq && w_idle && xTaskCreate(upload_worker, "s3_up2", WORKER_STACK, NULL, 4, NULL) == pdPASS;
    if (!worker_ready) ESP_LOGW(TAG, "second upload connection unavailable; single stream");
    direct_state_boot();
    size_t al = rrc_ptp_direct_align();
    for (int i = 0; i < PART_SLOTS; i++) {
        uint8_t *base = heap_caps_aligned_alloc(al, PART_BUF_BYTES + 12 + 2 * al + 1024, MALLOC_CAP_SPIRAM | MALLOC_CAP_8BIT);
        if (!base) { ESP_LOGE(TAG, "cannot allocate %u-byte part buffer %d in PSRAM", PART_BUF_BYTES, i); return ESP_ERR_NO_MEM; }
        part_bufs[i] = base + 12;
    }
    part_buf = part_bufs[0];
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

void sync_get_status(sync_status_t *out) { ST_LOCK(); *out = st; out->pending_entries = 0; ST_UNLOCK(); out->pending_entries = store_journal_pending_entries(); }
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

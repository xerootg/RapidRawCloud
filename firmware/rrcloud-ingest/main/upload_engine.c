#include "upload_engine.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <strings.h>
#include <stdarg.h>
#include "esp_log.h"
#include "esp_timer.h"
#include "esp_heap_caps.h"
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/queue.h"
#include "freertos/semphr.h"
#include "nvs.h"
#include "rrc_ptp.h"
#include "rrc_blake3.h"
#include "rrc_hash.h"
#include "log_ring.h"

static const char *TAG = "upload";

#define READ_CHUNK       (4u * 1024u * 1024u)   /* one GetPartialObject: the Z 7II pays a fixed latency per request */
#define READER_STACK     16384                  /* reader and hash tasks: PSRAM stacks, they never touch flash */
#define HASH_STACK       4096
#define UPLOADER_STACK   (24u * 1024u)          /* TLS handshake uses ~21 KB; INTERNAL RAM, see README */
#define N_UP             2

struct ue_job {
    cam_source_t *src;
    cam_object_t obj;
    char *key;
    void *user;
    bool multipart, use_sha;
    char upload_id[160];
    char (*etags)[40];
    int etag_cap;
    rrc_blake3_ctx b3;
    uint64_t hashed;
    char b3hex[65];
    uint64_t total;
    void *fh;
    int nparts, done_parts;          /* guarded by g.mx */
    bool have_last;
    volatile esp_err_t err;
    char errmsg[96];
    char single_md5_hex[33];
};

typedef struct {
    ue_job_t *job;
    int slot;                /* -1: a terminal error message that carries no data */
    size_t len;
    int part_no;
    bool last;
    esp_err_t err;
    uint8_t md5[16];
    uint8_t sha[32];
} msg_t;

#define SUB_CHUNKS 1024u
#define SUB_BYTES  (SUB_CHUNKS * 1024u)
#define MAX_SUBS   (UE_PART_BYTES / SUB_BYTES)
typedef struct { const uint8_t *buf; uint64_t first_chunk; volatile int next; uint32_t cv[MAX_SUBS][8]; } h2_work_t;

static struct {
    ue_config_t cfg;
    uint8_t *bufs[UE_SLOTS];
    QueueHandle_t job_q, free_q, hash_q, sum_q, full_q, done_q, h2_q, h2_out;
    SemaphoreHandle_t mx;
    volatile bool abort;
    bool direct_checked;
    ue_stats_t stats;
    bool up;
} g;

ue_stats_t *ue_stats(void) { return &g.stats; }
uint8_t *ue_slot_buf(int i) { return g.bufs[i]; }
void ue_camera_attached(void) { g.direct_checked = false; }
void ue_set_abort(bool on) { g.abort = on; }
void *ue_job_user(const ue_job_t *j) { return j->user; }

static void job_fail(ue_job_t *j, esp_err_t e, const char *fmt, ...) __attribute__((format(printf, 3, 4)));
static void job_fail(ue_job_t *j, esp_err_t e, const char *fmt, ...)
{
    xSemaphoreTake(g.mx, portMAX_DELAY);
    if (j->err == ESP_OK) {
        j->err = e;
        va_list ap; va_start(ap, fmt); vsnprintf(j->errmsg, sizeof j->errmsg, fmt, ap); va_end(ap);
    }
    xSemaphoreGive(g.mx);
}

/* ---- zero-copy USB read self-check ---------------------------------------------
 * The USB controller DMAs straight into the part buffers and the stack does cache maintenance on
 * line-rounded ranges, so a mistake there would silently corrupt a photo (the upload verifies the hash of
 * what it read, not what the camera sent). Once per camera attach, before the first upload, the start of
 * the first file is read both ways and compared byte for byte, with guard patterns around the destination.
 * A mismatch turns the DMA path off for the session; a reset during the check disables it on the next
 * boot (NVS "usb_direct": 0 ok/unknown, 1 running, 2 disabled; `usb-direct on` clears it). */
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

static void direct_selfcheck(ue_job_t *j)
{
    if (g.direct_checked || strcmp(j->src->kind, "ptp") != 0 || !rrc_ptp_direct_enabled() || j->obj.size < 256 * 1024) return;
    g.direct_checked = true;
    int sa = -1, sb = -1;   /* borrow two part buffers; they come back when the check is done */
    xQueueReceive(g.free_q, &sa, portMAX_DELAY);
    xQueueReceive(g.free_q, &sb, portMAX_DELAY);
    sync_usb_direct_state_set(1);
    const size_t n = (size_t)(j->obj.size < 1024 * 1024 + 4100 ? j->obj.size : 1024 * 1024 + 4100);   /* ends mid cache line on purpose */
    uint8_t *a = g.bufs[sa], *b = g.bufs[sb];
    memset(a - 12, 0xA5, 12 + n + 192); memset(b - 12, 0x5A, 12 + n + 192);
    size_t ga = 0, gb = 0;
    rrc_ptp_set_direct(true);
    esp_err_t ea = j->src->read(j->src, j->fh, 0, a, n, &ga);
    rrc_ptp_set_direct(false);
    esp_err_t eb = j->src->read(j->src, j->fh, 0, b, n, &gb);
    rrc_ptp_set_direct(true);
    bool guards = true;
    for (int i = -12; i < 0; i++) guards &= a[i] == 0xA5;
    for (size_t i = n; i < n + 192; i++) guards &= a[i] == 0xA5;
    bool same = ea == ESP_OK && eb == ESP_OK && ga == n && gb == n && memcmp(a, b, n) == 0;
    if (same && guards) {
        sync_usb_direct_state_set(0);
        log_ring_printf("usb: zero-copy DMA read verified against the copying path (%u bytes identical)", (unsigned)n);
    } else {
        size_t first = 0;
        if (ea == ESP_OK && eb == ESP_OK) while (first < n && a[first] == b[first]) first++;
        rrc_ptp_set_direct(false);
        sync_usb_direct_state_set(2);
        log_ring_printf("usb: zero-copy DMA read FAILED its check (%s/%s, got %u/%u of %u, first diff at %u, guards %s); using the copying path",
                        esp_err_to_name(ea), esp_err_to_name(eb), (unsigned)ga, (unsigned)gb, (unsigned)n, (unsigned)first, guards ? "ok" : "corrupted");
    }
    xQueueSend(g.free_q, &sa, 0);
    xQueueSend(g.free_q, &sb, 0);
}

/* ---- reader --------------------------------------------------------------------- */
static void reader_task(void *arg)
{
    (void)arg;
    for (;;) {
        ue_job_t *j;
        xQueueReceive(g.job_q, &j, portMAX_DELAY);
        cam_source_t *src = j->src;
        esp_err_t e = ESP_OK;
        if (g.abort || !src->connected(src)) e = ESP_ERR_INVALID_STATE;
        else if ((e = src->open(src, &j->obj, &j->fh)) != ESP_OK) job_fail(j, e, "cannot open on the camera: %s", esp_err_to_name(e));
        if (e == ESP_OK) direct_selfcheck(j);
        int part_no = 0;
        uint64_t total = 0;
        bool sent_last = false;
        while (e == ESP_OK && !sent_last) {
            int slot;
            xQueueReceive(g.free_q, &slot, portMAX_DELAY);
            uint8_t *buf = g.bufs[slot];
            size_t len = 0;
            while (len < UE_PART_BYTES && total + len < j->obj.size) {
                if (g.abort || !src->connected(src)) { e = ESP_ERR_INVALID_STATE; break; }
                size_t want = UE_PART_BYTES - len;
                if (want > READ_CHUNK) want = READ_CHUNK;
                if (want > j->obj.size - total - len) want = (size_t)(j->obj.size - total - len);
                size_t got = 0;
                int64_t t0 = esp_timer_get_time();
                esp_err_t re = src->read(src, j->fh, total + len, buf + len, want, &got);
                g.stats.read_us += esp_timer_get_time() - t0;
                if (re != ESP_OK) { e = re; job_fail(j, e, "camera read failed at %llu: %s", (unsigned long long)(total + len), esp_err_to_name(re)); break; }
                if (got == 0) break;
                len += got;
                if (g.cfg.progress) g.cfg.progress(g.cfg.progress_ctx, j->obj.path, j->obj.size, total + len);
            }
            if (e != ESP_OK) { xQueueSend(g.free_q, &slot, 0); break; }
            total += len;
            bool last = total >= j->obj.size || len < UE_PART_BYTES;
            msg_t m = {.job = j, .slot = slot, .len = len, .part_no = part_no++, .last = last};
            if (last && total != j->obj.size) { m.err = ESP_FAIL; job_fail(j, ESP_FAIL, "short read from camera: %llu of %llu bytes", (unsigned long long)total, (unsigned long long)j->obj.size); }
            j->total = total;
            xQueueSend(g.hash_q, &m, portMAX_DELAY);
            sent_last = last;
        }
        if (j->fh) { src->close(src, j->fh); j->fh = NULL; }
        if (!sent_last) {   /* error before the end: one terminal message so the job's accounting completes */
            if (e != ESP_OK && j->err == ESP_OK) job_fail(j, e, "stopped");
            msg_t m = {.job = j, .slot = -1, .part_no = part_no, .last = true, .err = e != ESP_OK ? e : ESP_FAIL};
            xQueueSend(g.hash_q, &m, portMAX_DELAY);
        }
    }
}

/* ---- BLAKE3: both cores ------------------------------------------------------------
 * A full part that starts on a part boundary splits into aligned 1 MiB subtrees; both cores pull subtrees
 * off a shared counter and the results are appended to the running hash in order. */
static void h2_run(h2_work_t *w)
{
    for (;;) {
        int i = __atomic_fetch_add(&w->next, 1, __ATOMIC_RELAXED);
        if (i >= (int)MAX_SUBS) return;
        rrc_blake3_subtree_cv(w->buf + (size_t)i * SUB_BYTES, SUB_CHUNKS, w->first_chunk + (uint64_t)i * SUB_CHUNKS, w->cv[i]);
    }
}

static void hash2_task(void *arg)
{
    (void)arg;
    for (;;) {
        h2_work_t *w;
        xQueueReceive(g.h2_q, &w, portMAX_DELAY);
        h2_run(w);
        uint32_t done = 1;
        xQueueSend(g.h2_out, &done, portMAX_DELAY);
    }
}

static void hash_task(void *arg)
{
    (void)arg;
    h2_work_t *w = heap_caps_malloc(sizeof *w, MALLOC_CAP_SPIRAM | MALLOC_CAP_8BIT);
    for (;;) {
        msg_t m;
        xQueueReceive(g.hash_q, &m, portMAX_DELAY);
        ue_job_t *j = m.job;
        if (m.err == ESP_OK && j->err == ESP_OK) {
            int64_t t0 = esp_timer_get_time();
            if (m.len) {
                const uint8_t *buf = g.bufs[m.slot];
                if (w && m.len == UE_PART_BYTES && j->hashed % UE_PART_BYTES == 0) {
                    w->buf = buf; w->first_chunk = j->hashed / 1024; w->next = 0;
                    xQueueSend(g.h2_q, &w, portMAX_DELAY);
                    h2_run(w);
                    uint32_t done;
                    xQueueReceive(g.h2_out, &done, portMAX_DELAY);
                    for (size_t i = 0; i < MAX_SUBS; i++) {
                        if (rrc_blake3_add_subtree(&j->b3, w->cv[i], SUB_CHUNKS) != 0) {
                            if (i == 0) { rrc_blake3_update(&j->b3, buf, m.len); break; }   /* refused before consuming anything */
                            abort();                                                        /* aligned by construction */
                        }
                    }
                } else {
                    rrc_blake3_update(&j->b3, buf, m.len);
                }
                j->hashed += m.len;
            }
            if (m.last) rrc_blake3_final_hex(&j->b3, j->b3hex);
            g.stats.hash_us += esp_timer_get_time() - t0;
        }
        xQueueSend(g.sum_q, &m, portMAX_DELAY);
    }
}

/* ---- per-part checksum: SHA-256 on the hardware engine for multipart parts, MD5 otherwise -- */
static void sum_task(void *arg)
{
    (void)arg;
    for (;;) {
        msg_t m;
        xQueueReceive(g.sum_q, &m, portMAX_DELAY);
        ue_job_t *j = m.job;
        if (m.err == ESP_OK && j->err == ESP_OK && m.slot >= 0) {
            int64_t t0 = esp_timer_get_time();
            if (j->use_sha) rrc_sha256(g.bufs[m.slot], m.len, m.sha);
            else rrc_md5(g.bufs[m.slot], m.len, m.md5);
            g.stats.md5_us += esp_timer_get_time() - t0;
        }
        xQueueSend(g.full_q, &m, portMAX_DELAY);
    }
}

/* ---- uploaders ---------------------------------------------------------------------- */
static void uploader_task(void *arg)
{
    int idx = (int)(intptr_t)arg;
    rrc_s3_t *c = g.cfg.conn[idx];
    for (;;) {
        msg_t m;
        xQueueReceive(g.full_q, &m, portMAX_DELAY);
        ue_job_t *j = m.job;
        if (m.err == ESP_OK && j->err == ESP_OK && m.slot >= 0) {
            c->have_server_offset = g.cfg.ctl->have_server_offset;
            c->server_offset_s = g.cfg.ctl->server_offset_s;
            rrc_s3_result_t res = {0};
            char b[64];
            int64_t t0 = esp_timer_get_time();
            const uint8_t *buf = g.bufs[m.slot];
            esp_err_t e;
            if (j->multipart) {
                e = rrc_s3_upload_part(c, j->key, j->upload_id, m.part_no + 1, buf, m.len, m.md5, j->use_sha ? m.sha : NULL, j->etags[m.part_no], 40, &res);
                if (e != ESP_OK) job_fail(j, e, "part %d failed: %s", m.part_no + 1, rrc_s3_result_str(&res, b, sizeof b));
            } else {
                rrc_kv meta[1] = {{"rrc-device", g.cfg.device_id}};
                e = rrc_s3_put_md5(c, j->key, buf, m.len, "application/octet-stream", meta, 1, m.md5, &res);
                if (e != ESP_OK) job_fail(j, e, "put failed: %s", rrc_s3_result_str(&res, b, sizeof b));
                else rrc_hex_lower(m.md5, 16, j->single_md5_hex);
            }
            g.stats.upload_us += esp_timer_get_time() - t0;
        }
        if (m.slot >= 0) xQueueSend(g.free_q, &(int){m.slot}, 0);
        bool fin;
        xSemaphoreTake(g.mx, portMAX_DELAY);
        j->done_parts++;
        if (m.last) { j->have_last = true; j->nparts = m.part_no + 1; }
        fin = j->have_last && j->done_parts == j->nparts;
        xSemaphoreGive(g.mx);
        if (fin) xQueueSend(g.done_q, &j, portMAX_DELAY);
    }
}

/* ---- public API ------------------------------------------------------------------- */
esp_err_t ue_init(const ue_config_t *cfg)
{
    if (g.up) return ESP_OK;
    g.cfg = *cfg;
    g.mx = xSemaphoreCreateMutex();
    g.job_q = xQueueCreate(4, sizeof(ue_job_t *));
    g.free_q = xQueueCreate(UE_SLOTS, sizeof(int));
    g.hash_q = xQueueCreate(UE_SLOTS + 2, sizeof(msg_t));
    g.sum_q = xQueueCreate(UE_SLOTS + 2, sizeof(msg_t));
    g.full_q = xQueueCreate(UE_SLOTS + 2, sizeof(msg_t));
    g.done_q = xQueueCreate(16, sizeof(ue_job_t *));
    g.h2_q = xQueueCreate(1, sizeof(void *));
    g.h2_out = xQueueCreate(1, sizeof(uint32_t));
    if (!g.mx || !g.job_q || !g.free_q || !g.hash_q || !g.sum_q || !g.full_q || !g.done_q || !g.h2_q || !g.h2_out) return ESP_ERR_NO_MEM;
    direct_state_boot();
    /* The USB controller DMAs straight into these (see rrc_ptp_read_partial_direct): the 12-byte container
     * header precedes the payload in the stream, so the payload starts 12 bytes into a cache-line-aligned
     * block, which also has slack for the line rounding the USB stack applies to the last transfer. */
    size_t al = rrc_ptp_direct_align();
    for (int i = 0; i < UE_SLOTS; i++) {
        uint8_t *base = heap_caps_aligned_alloc(al, UE_PART_BYTES + 12 + 2 * al + 1024, MALLOC_CAP_SPIRAM | MALLOC_CAP_8BIT);
        if (!base) { ESP_LOGE(TAG, "cannot allocate part buffer %d (%u bytes) in PSRAM", i, (unsigned)UE_PART_BYTES); return ESP_ERR_NO_MEM; }
        g.bufs[i] = base + 12;
        xQueueSend(g.free_q, &i, 0);
    }
    const uint32_t sc = MALLOC_CAP_SPIRAM | MALLOC_CAP_8BIT;
    BaseType_t ok = pdPASS;
    ok &= xTaskCreatePinnedToCoreWithCaps(reader_task, "ue_read", READER_STACK, NULL, 4, NULL, tskNO_AFFINITY, sc);
    ok &= xTaskCreatePinnedToCoreWithCaps(hash_task, "ue_hash", HASH_STACK, NULL, 4, NULL, tskNO_AFFINITY, sc);
    ok &= xTaskCreatePinnedToCoreWithCaps(hash2_task, "ue_hash2", HASH_STACK, NULL, 4, NULL, tskNO_AFFINITY, sc);
    ok &= xTaskCreatePinnedToCoreWithCaps(sum_task, "ue_sum", HASH_STACK, NULL, 4, NULL, tskNO_AFFINITY, sc);
    for (int i = 0; i < N_UP; i++) ok &= xTaskCreate(uploader_task, i ? "ue_up1" : "ue_up0", UPLOADER_STACK, (void *)(intptr_t)i, 4, NULL);
    if (ok != pdPASS) return ESP_ERR_NO_MEM;
    g.up = true;
    return ESP_OK;
}

ue_job_t *ue_submit(cam_source_t *src, const cam_object_t *o, const char *key, void *user, char *err, size_t errcap)
{
    ue_job_t *j = heap_caps_calloc(1, sizeof *j, MALLOC_CAP_SPIRAM | MALLOC_CAP_8BIT);
    if (!j) { snprintf(err, errcap, "out of memory"); return NULL; }
    j->src = src; j->obj = *o; j->user = user;
    j->key = strdup(key);
    j->multipart = o->size > UE_PART_BYTES;
    rrc_s3_t *ctl = g.cfg.ctl;
    j->use_sha = j->multipart && ctl->sha256_check_known && ctl->backend_checks_sha256;
    rrc_blake3_init(&j->b3);
    if (j->multipart) {
        j->etag_cap = (int)(o->size / UE_PART_BYTES) + 2;
        j->etags = heap_caps_calloc((size_t)j->etag_cap, 40, MALLOC_CAP_SPIRAM | MALLOC_CAP_8BIT);
        rrc_s3_result_t res = {0};
        rrc_kv meta[1] = {{"rrc-device", g.cfg.device_id}};
        esp_err_t e = j->etags ? rrc_s3_multipart_create(ctl, j->key, "application/octet-stream", meta, 1, j->upload_id, sizeof j->upload_id, &res) : ESP_ERR_NO_MEM;
        if (e != ESP_OK) {
            char b[96];
            snprintf(err, errcap, "multipart create failed: %s", rrc_s3_result_str(&res, b, sizeof b));
            free(j->etags); free(j->key); free(j);
            return NULL;
        }
    }
    xQueueSend(g.job_q, &j, portMAX_DELAY);
    return j;
}

ue_job_t *ue_wait(uint32_t timeout_ms)
{
    ue_job_t *j = NULL;
    if (xQueueReceive(g.done_q, &j, timeout_ms == UINT32_MAX ? portMAX_DELAY : pdMS_TO_TICKS(timeout_ms)) != pdTRUE) return NULL;
    return j;
}

typedef struct { rrc_blake3_ctx b3; uint64_t n; } hash_sink_t;
static int hash_sink(void *ctx, const uint8_t *d, size_t n) { hash_sink_t *h = ctx; rrc_blake3_update(&h->b3, d, n); h->n += n; return 0; }

static void job_free(ue_job_t *j) { free(j->etags); free(j->key); free(j); }

esp_err_t ue_finish(ue_job_t *j, char b3hex[65], uint64_t *size, char *err, size_t errcap)
{
    rrc_s3_t *s3 = g.cfg.ctl;
    rrc_s3_result_t res = {0};
    char b[96];
    esp_err_t e = j->err;
    if (e != ESP_OK) { snprintf(err, errcap, "%s", j->errmsg); goto fail; }
    if (j->multipart) {
        /* Heap, not stack: up to a thousand pointers on top of the TLS session overflowed the sync task once. */
        const char **ptrs = calloc((size_t)(j->nparts > 0 ? j->nparts : 1), sizeof *ptrs);
        if (!ptrs) { e = ESP_ERR_NO_MEM; snprintf(err, errcap, "multipart complete: out of memory"); goto fail; }
        for (int i = 0; i < j->nparts; i++) ptrs[i] = j->etags[i];
        e = rrc_s3_multipart_complete(s3, j->key, j->upload_id, ptrs, j->nparts, &res);
        free(ptrs);
        if (e != ESP_OK) { snprintf(err, errcap, "multipart complete failed: %s", rrc_s3_result_str(&res, b, sizeof b)); goto fail; }
        j->upload_id[0] = 0;
    }
    /* verify: HEAD size; single-part ETag == MD5; multipart on a backend that checked neither a per-part
     * SHA-256 nor a bad Content-MD5 -> read the object back and re-hash it. */
    rrc_s3_result_t h;
    if (rrc_s3_head(s3, j->key, &h) != ESP_OK || h.status != 200) { snprintf(err, errcap, "verify HEAD failed (%d)", h.status); e = ESP_FAIL; goto fail; }
    if (h.content_length >= 0 && (uint64_t)h.content_length != j->total) { snprintf(err, errcap, "verify size mismatch %lld != %llu", (long long)h.content_length, (unsigned long long)j->total); e = ESP_FAIL; goto fail; }
    if (!j->multipart) {
        if (strcasecmp(h.etag, j->single_md5_hex) != 0) { snprintf(err, errcap, "verify ETag != MD5 (%s vs %s)", h.etag, j->single_md5_hex); e = ESP_FAIL; goto fail; }
    } else if (!j->use_sha && (!s3->digest_check_known || !s3->backend_rejects_bad_md5)) {
        hash_sink_t hs = {0};
        rrc_blake3_init(&hs.b3);
        rrc_s3_result_t gr;
        if (rrc_s3_get_stream(s3, j->key, hash_sink, &hs, &gr) != ESP_OK) { snprintf(err, errcap, "read-back failed (%d)", gr.status); e = ESP_FAIL; goto fail; }
        char rb[65];
        rrc_blake3_final_hex(&hs.b3, rb);
        if (hs.n != j->total || strcmp(rb, j->b3hex) != 0) { snprintf(err, errcap, "read-back blake3 mismatch"); e = ESP_FAIL; goto fail; }
    }
    memcpy(b3hex, j->b3hex, 65);
    *size = j->total;
    job_free(j);
    return ESP_OK;
fail:
    if (j->multipart && j->upload_id[0]) { rrc_s3_result_t ar; rrc_s3_multipart_abort(s3, j->key, j->upload_id, &ar); }
    job_free(j);
    return e == ESP_OK ? ESP_FAIL : e;
}

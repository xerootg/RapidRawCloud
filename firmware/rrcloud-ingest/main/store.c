#include "store.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <dirent.h>
#include <unistd.h>
#include <sys/stat.h>
#include <errno.h>
#include "esp_log.h"
#include "esp_littlefs.h"
#include "esp_heap_caps.h"
#include "freertos/FreeRTOS.h"
#include "freertos/semphr.h"

static const char *TAG = "store";
#define LEDGER STORE_BASE "/ledger.tsv"
#define JDIR STORE_BASE "/journal"
#define PENDING JDIR "/pending"
#define SEGMENTS STORE_BASE "/segments.tsv"
#define KV STORE_BASE "/state.kv"

static SemaphoreHandle_t mtx;
#define LOCK() xSemaphoreTake(mtx, portMAX_DELAY)
#define UNLOCK() xSemaphoreGive(mtx)

/* ---- ledger index: open-addressing hash of (source_id, path, size) -> status */
/* Ledger index: one 64-bit word per record. Bits 63..2 are the FNV-1a 64 hash of
 * (source_id, path, size); bits 1..0 carry the status (1=U, 2=R, 3=C), so a slot is
 * never 0 (0 = empty). The index is only a membership filter in front of the on-flash
 * ledger: a false "seen" would silently skip a photo, so 62 hash bits (N/2^62) rather
 * than 32 (N/2^32, which would lose one photo per few hundred thousand). */
static uint64_t *idx;
static size_t idx_cap, idx_len;
static size_t ledger_count, uploaded_count, pending_entries_cache;
static void recover_last_seq(void);
static uint64_t last_seq;   /* high-water mark of every seq ever written (see store.h) */
static uint64_t uploaded_bytes;

static uint64_t fnv1a64(const char *s, uint64_t h) { while (*s) { h ^= (uint8_t)*s++; h *= 1099511628211ull; } return h; }
static uint64_t key_hash(const char *sid, const char *path, uint64_t size)
{
    uint64_t h = fnv1a64(sid, 14695981039346656037ull);
    h = fnv1a64("\x1f", h);
    h = fnv1a64(path, h);
    char sz[24]; snprintf(sz, sizeof sz, "\x1f%llu", (unsigned long long)size);
    return fnv1a64(sz, h) & ~3ull;
}
/* Second namespace in the same index: (RELKEY_NS, relkey, size) of every uploaded
 * record, so a camera that mirrors files to a second card slot (Nikon "backup")
 * does not upload the mirrored copy over the primary one. */
#define RELKEY_NS "\x1erelkey"
static uint64_t status_bits(char status) { return status == RRC_LEDGER_UPLOADED ? 1 : status == RRC_LEDGER_REMOTE_EXISTS ? 2 : 3; }
static char status_from_bits(uint64_t b) { return (b & 3) == 1 ? RRC_LEDGER_UPLOADED : (b & 3) == 2 ? RRC_LEDGER_REMOTE_EXISTS : RRC_LEDGER_COLLISION; }

static bool idx_grow(void)
{
    size_t ncap = idx_cap ? idx_cap * 2 : 4096;
    uint64_t *n = heap_caps_calloc(ncap, sizeof *n, MALLOC_CAP_SPIRAM | MALLOC_CAP_8BIT);
    if (!n) n = calloc(ncap, sizeof *n);
    if (!n) return false;
    for (size_t i = 0; i < idx_cap; i++) {
        if (!idx[i]) continue;
        size_t p = (size_t)((idx[i] >> 2) & (ncap - 1));
        while (n[p]) p = (p + 1) & (ncap - 1);
        n[p] = idx[i];
    }
    free(idx); idx = n; idx_cap = ncap;
    return true;
}

static void idx_put(uint64_t h, char status)
{
    if ((idx_len + 1) * 10 > idx_cap * 7 && !idx_grow()) return;
    size_t p = (size_t)((h >> 2) & (idx_cap - 1));
    while (idx[p] && (idx[p] & ~3ull) != h) p = (p + 1) & (idx_cap - 1);
    if (!idx[p]) idx_len++;
    idx[p] = h | status_bits(status);
}

static char idx_get(uint64_t h)
{
    if (!idx_cap) return 0;
    size_t p = (size_t)((h >> 2) & (idx_cap - 1));
    while (idx[p]) { if ((idx[p] & ~3ull) == h) return status_from_bits(idx[p]); p = (p + 1) & (idx_cap - 1); }
    return 0;
}

static void fsync_file(FILE *f) { fflush(f); fsync(fileno(f)); }
static void pending_stats(size_t *entries, size_t *bytes);

esp_err_t store_init(void)
{
    mtx = xSemaphoreCreateMutex();
    esp_vfs_littlefs_conf_t conf = {
        .base_path = STORE_BASE,
        .partition_label = "storage",
        .format_if_mount_failed = true,
        .dont_mount = false,
    };
    esp_err_t e = esp_vfs_littlefs_register(&conf);
    if (e != ESP_OK) { ESP_LOGE(TAG, "littlefs mount failed: %s", esp_err_to_name(e)); return e; }
    mkdir(JDIR, 0777);
    { size_t e2, b2; pending_stats(&e2, &b2); pending_entries_cache = e2; }
    recover_last_seq();
    size_t total, used;
    store_usage(&total, &used);
    ESP_LOGI(TAG, "littlefs mounted: %u/%u KiB used", (unsigned)(used / 1024), (unsigned)(total / 1024));
    return ESP_OK;
}

void store_usage(size_t *total, size_t *used)
{
    size_t t = 0, u = 0;
    esp_littlefs_info("storage", &t, &u);
    if (total) *total = t;
    if (used) *used = u;
}

/* ---- kv: rewrite-whole-file key=value lines ----------------------------- */
static bool kv_get(const char *key, char *out, size_t cap)
{
    FILE *f = fopen(KV, "r");
    if (!f) return false;
    char line[320];
    bool found = false;
    size_t kl = strlen(key);
    while (fgets(line, sizeof line, f)) {
        if (!strncmp(line, key, kl) && line[kl] == '=') {
            line[strcspn(line, "\r\n")] = 0;
            strncpy(out, line + kl + 1, cap - 1); out[cap - 1] = 0;
            found = true;
        }
    }
    fclose(f);
    return found;
}

static esp_err_t kv_set(const char *key, const char *val)
{
    struct stat st;
    size_t cap = (stat(KV, &st) == 0 ? (size_t)st.st_size : 0) + strlen(key) + strlen(val) + 64;
    char *buf = malloc(cap);
    if (!buf) return ESP_ERR_NO_MEM;
    size_t len = 0;
    FILE *f = fopen(KV, "r");
    size_t kl = strlen(key);
    if (f) {
        char line[320];
        while (fgets(line, sizeof line, f)) {
            if (!strncmp(line, key, kl) && line[kl] == '=') continue;
            size_t n = strlen(line);
            if (len + n >= cap) { fclose(f); free(buf); return ESP_ERR_NO_MEM; } /* never drop a key silently */
            memcpy(buf + len, line, n); len += n;
        }
        fclose(f);
    }
    int n = snprintf(buf + len, cap - len, "%s=%s\n", key, val);
    if (n < 0 || (size_t)n >= cap - len) { free(buf); return ESP_ERR_NO_MEM; }
    len += (size_t)n;
    f = fopen(KV ".tmp", "w");
    if (!f) { free(buf); return ESP_FAIL; }
    bool ok = fwrite(buf, 1, len, f) == len;
    fsync_file(f);
    fclose(f);
    free(buf);
    if (!ok) { unlink(KV ".tmp"); return ESP_FAIL; }
    if (rename(KV ".tmp", KV) != 0) return ESP_FAIL;
    return ESP_OK;
}

esp_err_t store_kv_set_u64(const char *key, uint64_t v) { char b[24]; snprintf(b, sizeof b, "%llu", (unsigned long long)v); LOCK(); esp_err_t e = kv_set(key, b); UNLOCK(); return e; }
uint64_t store_kv_get_u64(const char *key, uint64_t def) { char b[64]; LOCK(); bool ok = kv_get(key, b, sizeof b); UNLOCK(); return ok ? strtoull(b, NULL, 10) : def; }
esp_err_t store_kv_set_i64(const char *key, int64_t v) { char b[24]; snprintf(b, sizeof b, "%lld", (long long)v); LOCK(); esp_err_t e = kv_set(key, b); UNLOCK(); return e; }
int64_t store_kv_get_i64(const char *key, int64_t def) { char b[64]; LOCK(); bool ok = kv_get(key, b, sizeof b); UNLOCK(); return ok ? strtoll(b, NULL, 10) : def; }
esp_err_t store_kv_set_str(const char *key, const char *v) { LOCK(); esp_err_t e = kv_set(key, v); UNLOCK(); return e; }
bool store_kv_get_str(const char *key, char *out, size_t cap) { LOCK(); bool ok = kv_get(key, out, cap); UNLOCK(); return ok; }

/* ---- ledger ------------------------------------------------------------- */
esp_err_t store_ledger_load(void)
{
    LOCK();
    FILE *f = fopen(LEDGER, "r");
    if (!f) { UNLOCK(); return ESP_OK; }
    char *line = malloc(2048);
    if (!line) { fclose(f); UNLOCK(); return ESP_ERR_NO_MEM; }
    while (fgets(line, 2048, f)) {
        rrc_ledger_rec r;
        if (rrc_ledger_parse(line, &r) != 0) continue;
        idx_put(key_hash(r.source_id, r.source_path, r.size), r.status);
        ledger_count++;
        if (r.status == RRC_LEDGER_UPLOADED) { uploaded_count++; uploaded_bytes += r.size; idx_put(key_hash(RELKEY_NS, r.relkey, r.size), r.status); }
    }
    free(line);
    fclose(f);
    UNLOCK();
    ESP_LOGI(TAG, "ledger: %u records (%u uploaded)", (unsigned)ledger_count, (unsigned)uploaded_count);
    return ESP_OK;
}

char store_ledger_lookup(const char *sid, const char *path, uint64_t size)
{
    LOCK();
    char s = idx_get(key_hash(sid, path, size));
    UNLOCK();
    return s;
}

bool store_ledger_relkey_uploaded(const char *relkey, uint64_t size)
{
    LOCK();
    char s = idx_get(key_hash(RELKEY_NS, relkey, size));
    UNLOCK();
    return s == RRC_LEDGER_UPLOADED;
}

esp_err_t store_ledger_append(const rrc_ledger_rec *r)
{
    char line[2048];
    int n = rrc_ledger_format(r, line, sizeof line);
    if (n < 0) return ESP_ERR_INVALID_ARG;
    LOCK();
    FILE *f = fopen(LEDGER, "a");
    if (!f) { UNLOCK(); return ESP_FAIL; }
    bool ok = fwrite(line, 1, (size_t)n, f) == (size_t)n;
    fsync_file(f);
    fclose(f);
    if (ok) {
        idx_put(key_hash(r->source_id, r->source_path, r->size), r->status);
        ledger_count++;
        if (r->status == RRC_LEDGER_UPLOADED) { uploaded_count++; uploaded_bytes += r->size; idx_put(key_hash(RELKEY_NS, r->relkey, r->size), r->status); }
    }
    UNLOCK();
    return ok ? ESP_OK : ESP_FAIL;
}

esp_err_t store_ledger_foreach_uploaded(store_ledger_cb cb, void *ctx)
{
    LOCK();
    FILE *f = fopen(LEDGER, "r");
    if (!f) { UNLOCK(); return ESP_OK; }
    char *line = malloc(2048);
    if (!line) { fclose(f); UNLOCK(); return ESP_ERR_NO_MEM; }
    while (fgets(line, 2048, f)) {
        rrc_ledger_rec r;
        if (rrc_ledger_parse(line, &r) != 0 || r.status != RRC_LEDGER_UPLOADED) continue;
        if (cb(ctx, &r)) break;
    }
    free(line);
    fclose(f);
    UNLOCK();
    return ESP_OK;
}

size_t store_ledger_count(void) { return ledger_count; }
size_t store_ledger_uploaded_count(void) { return uploaded_count; }
uint64_t store_ledger_uploaded_bytes(void) { return uploaded_bytes; }

/* ---- journal ------------------------------------------------------------ */
static uint64_t line_seq(const char *line);

/* Highest "seq":N in an NDJSON file (0 when absent/empty). */
static uint64_t file_max_seq(const char *path)
{
    FILE *f = fopen(path, "r");
    if (!f) return 0;
    char *line = malloc(2048);
    uint64_t mx = 0;
    if (line) {
        while (fgets(line, 2048, f)) { uint64_t s = line_seq(line); if (s > mx) mx = s; }
        free(line);
    }
    fclose(f);
    return mx;
}

/* Boot: the seq high-water mark is the max over every durable trace of a seq.
 * Readers dedupe by (device, seq), so handing a used seq out again would make
 * two different entries collide; a gap, by contrast, is harmless. */
static void recover_last_seq(void)
{
    char b[32];
    uint64_t mx = kv_get("last_seq", b, sizeof b) ? strtoull(b, NULL, 10) : 0;
    uint64_t cur = kv_get("published_cursor", b, sizeof b) ? strtoull(b, NULL, 10) : 0;
    if (cur > mx) mx = cur;
    uint64_t p = file_max_seq(PENDING);
    if (p > mx) mx = p;
    DIR *d = opendir(JDIR);
    if (d) {
        struct dirent *de;
        while ((de = readdir(d)) != NULL) {
            if (strlen(de->d_name) != 26 || strcmp(de->d_name + 16, ".v1.ndjson")) continue;
            char name[96];
            snprintf(name, sizeof name, JDIR "/%s", de->d_name);
            uint64_t s = file_max_seq(name);
            if (s > mx) mx = s;
        }
        closedir(d);
    }
    last_seq = mx;
    ESP_LOGI(TAG, "journal seq high-water mark %llu", (unsigned long long)last_seq);
}

uint64_t store_journal_next_seq(void)
{
    LOCK();
    uint64_t n = last_seq + 1;
    UNLOCK();
    return n;
}

esp_err_t store_journal_append_pending(const char *json_line)
{
    LOCK();
    FILE *f = fopen(PENDING, "a");
    if (!f) { UNLOCK(); return ESP_FAIL; }
    bool ok = fputs(json_line, f) >= 0 && fputc('\n', f) == '\n';
    fsync_file(f);
    fclose(f);
    if (ok) {
        pending_entries_cache++;
        uint64_t s = line_seq(json_line);
        if (s > last_seq) last_seq = s;
    }
    UNLOCK();
    return ok ? ESP_OK : ESP_FAIL;
}

static void pending_stats(size_t *entries, size_t *bytes)
{
    *entries = 0; *bytes = 0;
    struct stat st;
    if (stat(PENDING, &st) != 0) return;
    *bytes = (size_t)st.st_size;
    FILE *f = fopen(PENDING, "r");
    if (!f) return;
    int c;
    while ((c = fgetc(f)) != EOF) if (c == '\n') (*entries)++;
    fclose(f);
}

size_t store_journal_pending_entries(void) { LOCK(); size_t e = pending_entries_cache; UNLOCK(); return e; }
size_t store_journal_pending_bytes(void) { size_t e, b; LOCK(); pending_stats(&e, &b); UNLOCK(); return b; }

/* "seq":N from an entry line */
static uint64_t line_seq(const char *line)
{
    const char *p = strstr(line, "\"seq\":");
    return p ? strtoull(p + 6, NULL, 10) : 0;
}

esp_err_t store_journal_freeze(uint64_t *first_seq_out)
{
    LOCK();
    FILE *f = fopen(PENDING, "r");
    if (!f) { UNLOCK(); return ESP_ERR_NOT_FOUND; }
    char *line = malloc(2048);
    uint64_t first = 0;
    if (line && fgets(line, 2048, f)) first = line_seq(line);
    free(line);
    fclose(f);
    if (!first) { UNLOCK(); return ESP_ERR_NOT_FOUND; }
    char name[96];
    char fn[32];
    rrc_segment_filename(first, fn);
    snprintf(name, sizeof name, JDIR "/%s", fn);
    int rc = rename(PENDING, name);
    if (rc == 0) {
        pending_entries_cache = 0;
        /* Cheap boot shortcut for recover_last_seq(); the segment file is the
         * authority until it is published, then published_cursor is. */
        char b[32];
        snprintf(b, sizeof b, "%llu", (unsigned long long)last_seq);
        kv_set("last_seq", b);
    }
    UNLOCK();
    if (rc != 0) return ESP_FAIL;
    if (first_seq_out) *first_seq_out = first;
    return ESP_OK;
}

int store_journal_frozen_list(uint64_t *seqs, size_t cap)
{
    LOCK();
    DIR *d = opendir(JDIR);
    int n = 0;
    if (d) {
        struct dirent *de;
        while ((de = readdir(d)) != NULL && (size_t)n < cap) {
            if (strlen(de->d_name) == 26 && !strcmp(de->d_name + 16, ".v1.ndjson")) seqs[n++] = strtoull(de->d_name, NULL, 16);
        }
        closedir(d);
    }
    UNLOCK();
    /* insertion sort ascending */
    for (int i = 1; i < n; i++) { uint64_t v = seqs[i]; int j = i - 1; while (j >= 0 && seqs[j] > v) { seqs[j + 1] = seqs[j]; j--; } seqs[j + 1] = v; }
    return n;
}

esp_err_t store_journal_frozen_read(uint64_t first_seq, uint8_t **bytes, size_t *len, uint64_t *max_seq)
{
    char name[96], fn[32];
    rrc_segment_filename(first_seq, fn);
    snprintf(name, sizeof name, JDIR "/%s", fn);
    LOCK();
    FILE *f = fopen(name, "r");
    if (!f) { UNLOCK(); return ESP_ERR_NOT_FOUND; }
    struct stat st;
    stat(name, &st);
    uint8_t *buf = heap_caps_malloc((size_t)st.st_size + 1, MALLOC_CAP_SPIRAM | MALLOC_CAP_8BIT);
    if (!buf) buf = malloc((size_t)st.st_size + 1);
    if (!buf) { fclose(f); UNLOCK(); return ESP_ERR_NO_MEM; }
    size_t got = fread(buf, 1, (size_t)st.st_size, f);
    fclose(f);
    UNLOCK();
    buf[got] = 0;
    /* max seq = seq of the last line */
    uint64_t mx = first_seq;
    const char *p = (const char *)buf;
    for (;;) {
        const char *nl = strchr(p, '\n');
        uint64_t s = line_seq(p);
        if (s > mx) mx = s;
        if (!nl || !nl[1]) break;
        p = nl + 1;
    }
    *bytes = buf; *len = got; if (max_seq) *max_seq = mx;
    return ESP_OK;
}

esp_err_t store_journal_mark_published(uint64_t first_seq, uint64_t max_seq, int64_t server_ts)
{
    char name[96], fn[32];
    rrc_segment_filename(first_seq, fn);
    snprintf(name, sizeof name, JDIR "/%s", fn);
    LOCK();
    FILE *f = fopen(SEGMENTS, "a");
    if (!f) { UNLOCK(); return ESP_FAIL; }
    fprintf(f, "%llu\t%llu\t%lld\n", (unsigned long long)first_seq, (unsigned long long)max_seq, (long long)server_ts);
    fsync_file(f);
    fclose(f);
    char b[32];
    uint64_t cur = kv_get("published_cursor", b, sizeof b) ? strtoull(b, NULL, 10) : 0;
    if (max_seq > cur) { snprintf(b, sizeof b, "%llu", (unsigned long long)max_seq); kv_set("published_cursor", b); }
    unlink(name);
    UNLOCK();
    return ESP_OK;
}

uint64_t store_journal_published_cursor(void) { return store_kv_get_u64("published_cursor", 0); }

int store_segments_list(store_segment_t *out, size_t cap)
{
    LOCK();
    FILE *f = fopen(SEGMENTS, "r");
    int n = 0;
    if (f) {
        char line[96];
        while (fgets(line, sizeof line, f) && (size_t)n < cap) {
            unsigned long long a, b; long long c;
            if (sscanf(line, "%llu\t%llu\t%lld", &a, &b, &c) == 3) { out[n].first_seq = a; out[n].max_seq = b; out[n].published_server_ts = c; n++; }
        }
        fclose(f);
    }
    UNLOCK();
    return n;
}

esp_err_t store_segments_remove(uint64_t first_seq)
{
    LOCK();
    FILE *f = fopen(SEGMENTS, "r");
    if (!f) { UNLOCK(); return ESP_OK; }
    FILE *t = fopen(SEGMENTS ".tmp", "w");
    if (!t) { fclose(f); UNLOCK(); return ESP_FAIL; }
    char line[96];
    while (fgets(line, sizeof line, f)) {
        unsigned long long a;
        if (sscanf(line, "%llu", &a) == 1 && a == first_seq) continue;
        fputs(line, t);
    }
    fclose(f);
    fsync_file(t);
    fclose(t);
    int rc = rename(SEGMENTS ".tmp", SEGMENTS);
    UNLOCK();
    return rc == 0 ? ESP_OK : ESP_FAIL;
}

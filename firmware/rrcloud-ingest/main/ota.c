/*
 * ota — updates of the dock's own firmware; see ota.h for the design.
 */
#include "ota.h"
#include <stdio.h>
#include <string.h>
#include <strings.h>
#include <stdlib.h>
#include <stdarg.h>
#include "sdkconfig.h"
#include "esp_log.h"
#include "esp_system.h"
#include "esp_timer.h"
#include "esp_heap_caps.h"
#include "esp_app_desc.h"
#include "esp_app_format.h"
#include "esp_ota_ops.h"
#include "esp_partition.h"
#include "esp_http_client.h"
#include "esp_crt_bundle.h"
#include "hal/efuse_hal.h"
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/semphr.h"
#include "mbedtls/sha256.h"
#include "cJSON.h"
#include "app_config.h"
#include "board.h"
#include "net.h"
#include "sync.h"
#include "log_ring.h"
#include "rrc_proto.h"
#include "util.h"

static const char *TAG = "ota";

/* ---- this image's identity ----------------------------------------------------------
 * Placed right after esp_app_desc_t (section .rodata_custom_desc, ESP-IDF's place for
 * application data next to the description), so it sits at a fixed offset in every
 * image: the ESP image header (24 bytes), the first segment's header (8) and the
 * 256-byte esp_app_desc_t come before it. An image whose tag names other hardware is
 * refused; the variant (e.g. the PoE board with or without the DSI display) may change. */
#define FW_TAG_MAGIC  0x47544652u   /* "RFTG" */
#define FW_TAG_OFFSET (sizeof(esp_image_header_t) + sizeof(esp_image_segment_header_t) + sizeof(esp_app_desc_t))

typedef struct {
    uint32_t magic;
    char hardware[16];   /* board family: what the image can run on */
    char variant[24];    /* the build: which release asset this dock installs */
    uint32_t reserved[4];
} fw_tag_t;

__attribute__((used)) const __attribute__((section(".rodata_custom_desc"))) fw_tag_t rrc_fw_tag = {
    .magic = FW_TAG_MAGIC, .hardware = BOARD_OTA_HARDWARE, .variant = BOARD_OTA_VARIANT,
};

_Static_assert(sizeof(esp_image_header_t) == 24 && sizeof(esp_image_segment_header_t) == 8 && sizeof(esp_app_desc_t) == 256,
               "the firmware tag offset assumes the ESP-IDF image layout");

/* ---- state ------------------------------------------------------------------------- */
typedef enum { ST_IDLE, ST_CHECKING, ST_DOWNLOADING, ST_RECEIVING, ST_VERIFYING, ST_WRITING, ST_RESTARTING, ST_FAILED } ota_state_t;
static const char *const state_names[] = {"idle", "checking", "downloading", "receiving", "verifying", "writing", "restarting", "failed"};

static struct {
    ota_state_t state;
    char msg[160];
    uint32_t done, total;
    /* the last check */
    bool checked, avail;
    char avail_version[32], avail_tag[48], avail_commit[16], avail_url[300], avail_sha[65];
    uint32_t avail_size;
    char checked_channel[8];
    /* this boot */
    bool trial, rolled_back;
    char rolled_back_version[32];
    /* an upload in progress */
    uint8_t *buf;
    size_t size, got;
    char sha[65];
    int64_t last_chunk_us;
} st;

static SemaphoreHandle_t mtx;
#define LOCK() xSemaphoreTake(mtx, portMAX_DELAY)
#define UNLOCK() xSemaphoreGive(mtx)

static void set_state(ota_state_t s, const char *fmt, ...) __attribute__((format(printf, 2, 3)));
static void set_state(ota_state_t s, const char *fmt, ...)
{
    char m[160];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(m, sizeof m, fmt, ap);
    va_end(ap);
    LOCK();
    st.state = s;
    scpy(st.msg, sizeof st.msg, m);
    UNLOCK();
    log_ring_printf("firmware update%s: %s", s == ST_FAILED ? " failed" : "", m);
}

static bool busy_locked(void) { return st.state >= ST_CHECKING && st.state <= ST_RESTARTING; }

bool ota_busy(void)
{
    if (!mtx) return false;
    LOCK();
    bool b = st.state >= ST_DOWNLOADING && st.state <= ST_RESTARTING;
    UNLOCK();
    return b;
}

static bool sync_running(void)
{
    sync_status_t s;
    sync_get_status(&s);
    return s.phase == SYNC_ENUMERATING || s.phase == SYNC_UPLOADING || s.phase == SYNC_PUBLISHING;
}

/* ---- trial boot -------------------------------------------------------------------- */
#define HEALTH_MIN_UP_US   (60 * 1000000LL)
#define HEALTH_GIVE_UP_US  (5 * 60 * 1000000LL)

static void health_tick(void *arg)
{
    esp_timer_handle_t self = *(esp_timer_handle_t *)arg;
    int64_t up = esp_timer_get_time();
    if (up >= HEALTH_MIN_UP_US && net_has_ip()) {
        esp_ota_mark_app_valid_cancel_rollback();
        LOCK(); st.trial = false; UNLOCK();
        log_ring_printf("firmware update: %s kept (up %lld s with the network working)", esp_app_get_description()->version, (long long)(up / 1000000));
        esp_timer_stop(self);
        return;
    }
    if (up >= HEALTH_GIVE_UP_US) {
        log_ring_printf("firmware update: no network %lld s after starting %s; going back to the previous firmware",
                        (long long)(up / 1000000), esp_app_get_description()->version);
        vTaskDelay(pdMS_TO_TICKS(500));   /* let the line reach the console */
        esp_ota_mark_app_invalid_rollback_and_reboot();
    }
}

void ota_init(void)
{
    if (!mtx) mtx = xSemaphoreCreateMutex();
    const esp_partition_t *running = esp_ota_get_running_partition();
    esp_ota_img_states_t s;
    if (running && esp_ota_get_state_partition(running, &s) == ESP_OK && s == ESP_OTA_IMG_PENDING_VERIFY) {
        st.trial = true;
        log_ring_printf("firmware update: trying %s; it is kept after a minute with the network up, otherwise the previous firmware comes back",
                        esp_app_get_description()->version);
        static esp_timer_handle_t t;
        const esp_timer_create_args_t ta = {.callback = health_tick, .arg = &t, .name = "ota_health"};
        if (esp_timer_create(&ta, &t) != ESP_OK || esp_timer_start_periodic(t, 5 * 1000000) != ESP_OK) {
            /* Without the check the trial can never succeed; keep the image rather than
             * strand the dock on a timer failure. */
            esp_ota_mark_app_valid_cancel_rollback();
            st.trial = false;
        }
    }
    const esp_partition_t *bad = esp_ota_get_last_invalid_partition();
    esp_app_desc_t d;
    if (bad && esp_ota_get_partition_description(bad, &d) == ESP_OK) {
        st.rolled_back = true;
        scpy(st.rolled_back_version, sizeof st.rolled_back_version, d.version);
        log_ring_printf("firmware update: %s did not start cleanly and was rolled back; running %s", d.version, esp_app_get_description()->version);
    }
}

/* ---- status ------------------------------------------------------------------------ */
int ota_status_json(char *out, size_t cap)
{
    if (!mtx) mtx = xSemaphoreCreateMutex();
    const esp_app_desc_t *app = esp_app_get_description();
    LOCK();
    rrc_jsonw w;
    rrc_jsonw_init(&w, out, cap);
    rrc_jsonw_raw(&w, "{");
    rrc_jsonw_key(&w, "running", true); rrc_jsonw_str(&w, app->version);
    rrc_jsonw_key(&w, "built", false);
    char built[40];
    snprintf(built, sizeof built, "%s %s", app->date, app->time);
    rrc_jsonw_str(&w, built);
    rrc_jsonw_key(&w, "hardware", false); rrc_jsonw_str(&w, rrc_fw_tag.hardware);
    rrc_jsonw_key(&w, "variant", false); rrc_jsonw_str(&w, rrc_fw_tag.variant);
    rrc_jsonw_key(&w, "chip_rev", false); rrc_jsonw_u64(&w, efuse_hal_chip_revision());
    rrc_jsonw_key(&w, "channel", false); rrc_jsonw_str(&w, app_config_get()->ota_dev_channel ? "dev" : "release");
    rrc_jsonw_key(&w, "state", false); rrc_jsonw_str(&w, state_names[st.state]);
    rrc_jsonw_key(&w, "message", false); rrc_jsonw_str(&w, st.msg);
    rrc_jsonw_key(&w, "done", false); rrc_jsonw_u64(&w, st.done);
    rrc_jsonw_key(&w, "total", false); rrc_jsonw_u64(&w, st.total);
    rrc_jsonw_key(&w, "trial", false); rrc_jsonw_raw(&w, st.trial ? "true" : "false");
    rrc_jsonw_key(&w, "rolled_back", false); rrc_jsonw_str(&w, st.rolled_back ? st.rolled_back_version : "");
    rrc_jsonw_key(&w, "checked", false); rrc_jsonw_raw(&w, st.checked ? "true" : "false");
    rrc_jsonw_key(&w, "available", false);
    if (st.avail) {
        rrc_jsonw_raw(&w, "{");
        rrc_jsonw_key(&w, "version", true); rrc_jsonw_str(&w, st.avail_version);
        rrc_jsonw_key(&w, "tag", false); rrc_jsonw_str(&w, st.avail_tag);
        rrc_jsonw_key(&w, "commit", false); rrc_jsonw_str(&w, st.avail_commit);
        rrc_jsonw_key(&w, "channel", false); rrc_jsonw_str(&w, st.checked_channel);
        rrc_jsonw_key(&w, "size", false); rrc_jsonw_u64(&w, st.avail_size);
        rrc_jsonw_key(&w, "newer", false); rrc_jsonw_raw(&w, strcmp(st.avail_version, app->version) ? "true" : "false");
        rrc_jsonw_raw(&w, "}");
    } else {
        rrc_jsonw_raw(&w, "null");
    }
    rrc_jsonw_raw(&w, "}");
    UNLOCK();
    return rrc_jsonw_finish(&w);
}

/* ---- checks on a complete image ---------------------------------------------------- */
static void hex_of(const uint8_t dig[32], char out[65])
{
    for (int i = 0; i < 32; i++) snprintf(out + 2 * i, 3, "%02x", dig[i]);
}

/* Everything that can be known before flashing: a mismatch here leaves the flash and
 * the running image untouched. */
static bool image_acceptable(const uint8_t *img, size_t len, const char *want_sha, char *why, size_t why_cap)
{
    const esp_partition_t *target = esp_ota_get_next_update_partition(NULL);
    if (!target) { snprintf(why, why_cap, "no update partition (the partition table has no second app slot)"); return false; }
    if (len > target->size) { snprintf(why, why_cap, "image is %u bytes, the app slot holds %u", (unsigned)len, (unsigned)target->size); return false; }
    if (len < FW_TAG_OFFSET + sizeof(fw_tag_t)) { snprintf(why, why_cap, "file is too short to be a firmware image (%u bytes)", (unsigned)len); return false; }
    if (want_sha[0]) {
        uint8_t dig[32];
        char hex[65];
        mbedtls_sha256(img, len, dig, 0);
        hex_of(dig, hex);
        if (strcasecmp(hex, want_sha)) { snprintf(why, why_cap, "SHA-256 mismatch: got %.16s…, expected %.16s…", hex, want_sha); return false; }
    }
    esp_image_header_t h;
    memcpy(&h, img, sizeof h);
    if (h.magic != ESP_IMAGE_HEADER_MAGIC) { snprintf(why, why_cap, "not an ESP application image (magic 0x%02x)", h.magic); return false; }
    if (h.chip_id != ESP_CHIP_ID_ESP32P4) { snprintf(why, why_cap, "image is for another chip (id %u)", (unsigned)h.chip_id); return false; }
    uint32_t rev = efuse_hal_chip_revision();
    if (rev < h.min_chip_rev_full || (h.max_chip_rev_full != 0xFFFF && rev > h.max_chip_rev_full)) {
        snprintf(why, why_cap, "image supports chip revisions v%u.%u to v%u.%u; this chip is v%u.%u (the other chip-revision build is the one to install)",
                 h.min_chip_rev_full / 100, h.min_chip_rev_full % 100, h.max_chip_rev_full / 100, h.max_chip_rev_full % 100,
                 (unsigned)(rev / 100), (unsigned)(rev % 100));
        return false;
    }
    esp_app_desc_t d;
    memcpy(&d, img + sizeof(esp_image_header_t) + sizeof(esp_image_segment_header_t), sizeof d);
    if (d.magic_word != ESP_APP_DESC_MAGIC_WORD) { snprintf(why, why_cap, "image has no application description"); return false; }
    if (strncmp(d.project_name, esp_app_get_description()->project_name, sizeof d.project_name)) {
        snprintf(why, why_cap, "image is \"%.32s\", not this dock's firmware", d.project_name);
        return false;
    }
    fw_tag_t tag;
    memcpy(&tag, img + FW_TAG_OFFSET, sizeof tag);
    if (tag.magic != FW_TAG_MAGIC) { snprintf(why, why_cap, "image does not say which board it is for (built before over-the-air updates?)"); return false; }
    tag.hardware[sizeof tag.hardware - 1] = 0;
    tag.variant[sizeof tag.variant - 1] = 0;
    if (strcmp(tag.hardware, rrc_fw_tag.hardware)) { snprintf(why, why_cap, "image is for the %s board, this is %s", tag.hardware, rrc_fw_tag.hardware); return false; }
    return true;
}

/* ---- flashing ---------------------------------------------------------------------- */
typedef struct {
    uint8_t *img;        /* PSRAM, owned */
    size_t len;
    char sha[65];
    char url[300];       /* download source, or "" for an image that is already in img */
} job_t;

static void restart_cb(void *arg) { (void)arg; esp_restart(); }

/* Runs on an internal-RAM stack (flash writes cannot run from a PSRAM stack). */
static esp_err_t flash_image(const uint8_t *img, size_t len)
{
    const esp_partition_t *target = esp_ota_get_next_update_partition(NULL);
    esp_ota_handle_t h = 0;
    LOCK(); st.done = 0; st.total = (uint32_t)len; UNLOCK();
    set_state(ST_WRITING, "writing %u bytes to %s", (unsigned)len, target->label);
    esp_err_t e = esp_ota_begin(target, OTA_WITH_SEQUENTIAL_WRITES, &h);
    if (e != ESP_OK) { set_state(ST_FAILED, "could not open %s: %s", target->label, esp_err_to_name(e)); return e; }
    for (size_t off = 0; off < len;) {
        size_t n = len - off < 16384 ? len - off : 16384;
        e = esp_ota_write(h, img + off, n);
        if (e != ESP_OK) { esp_ota_abort(h); set_state(ST_FAILED, "write failed at %u: %s", (unsigned)off, esp_err_to_name(e)); return e; }
        off += n;
        LOCK(); st.done = (uint32_t)off; UNLOCK();
    }
    e = esp_ota_end(h);   /* verifies the written image (segments, checksum, SHA-256 appended by the build) */
    if (e != ESP_OK) { set_state(ST_FAILED, "the written image does not verify: %s", esp_err_to_name(e)); return e; }
    e = esp_ota_set_boot_partition(target);
    if (e != ESP_OK) { set_state(ST_FAILED, "could not select %s for the next boot: %s", target->label, esp_err_to_name(e)); return e; }
    return ESP_OK;
}

static esp_err_t download(const char *url, uint8_t **out, size_t *out_len, size_t max, bool progress)
{
    *out = NULL;
    *out_len = 0;
    esp_http_client_config_t cfg = {
        .url = url,
        .timeout_ms = 30000,
        .crt_bundle_attach = esp_crt_bundle_attach,
        .buffer_size = 4096,        /* GitHub's redirect to its asset CDN carries a ~1 KB Location */
        .buffer_size_tx = 2048,
        .max_redirection_count = 4,
        .user_agent = "rrcloud-ingest",
    };
    esp_http_client_handle_t c = esp_http_client_init(&cfg);
    if (!c) return ESP_ERR_NO_MEM;
    esp_err_t e = ESP_FAIL;
    uint8_t *buf = NULL;
    int status = 0;
    int64_t len = -1;
    /* esp_http_client_open() does not follow redirects by itself: re-open on 3xx. */
    for (int hop = 0; hop <= 4; hop++) {
        if (esp_http_client_open(c, 0) != ESP_OK) { set_state(ST_FAILED, "cannot connect to %.80s", url); goto out; }
        len = esp_http_client_fetch_headers(c);
        status = esp_http_client_get_status_code(c);
        if (status != 301 && status != 302 && status != 303 && status != 307 && status != 308) break;
        if (esp_http_client_set_redirection(c) != ESP_OK) break;
        esp_http_client_close(c);
    }
    if (status != 200) { set_state(ST_FAILED, "HTTP %d from %.80s", status, url); goto out; }
    if (len > (int64_t)max) { set_state(ST_FAILED, "download is %lld bytes, more than the %u allowed", (long long)len, (unsigned)max); goto out; }
    size_t cap = len > 0 ? (size_t)len : max;
    buf = heap_caps_malloc(cap + 1, MALLOC_CAP_SPIRAM | MALLOC_CAP_8BIT);
    if (!buf) { set_state(ST_FAILED, "not enough free memory for %u bytes (restart the dock and try again)", (unsigned)cap); goto out; }
    if (progress) { LOCK(); st.done = 0; st.total = len > 0 ? (uint32_t)len : 0; UNLOCK(); }
    size_t got = 0;
    for (;;) {
        if (got >= cap) {
            /* a server that sent a length must not send more; one that did not must stay under max */
            char extra;
            if (esp_http_client_read(c, &extra, 1) > 0) { set_state(ST_FAILED, "download larger than %u bytes", (unsigned)cap); goto out; }
            break;
        }
        int n = esp_http_client_read(c, (char *)buf + got, (int)(cap - got < 8192 ? cap - got : 8192));
        if (n < 0) { set_state(ST_FAILED, "download failed after %u bytes", (unsigned)got); goto out; }
        if (n == 0) break;
        got += (size_t)n;
        LOCK();
        bool cancelled = st.state != ST_DOWNLOADING && st.state != ST_CHECKING;
        if (progress) st.done = (uint32_t)got;
        UNLOCK();
        if (cancelled) { e = ESP_ERR_INVALID_STATE; goto out; }
    }
    if (len > 0 && got != (size_t)len) { set_state(ST_FAILED, "short download: %u of %lld bytes", (unsigned)got, (long long)len); goto out; }
    buf[got] = 0;   /* manifests are parsed as text */
    *out = buf;
    *out_len = got;
    buf = NULL;
    e = ESP_OK;
out:
    free(buf);
    esp_http_client_close(c);
    esp_http_client_cleanup(c);
    return e;
}

static void install_task(void *arg)
{
    job_t *j = arg;
    char why[200];
    if (j->url[0]) {
        set_state(ST_DOWNLOADING, "downloading %.120s", j->url);
        const esp_partition_t *target = esp_ota_get_next_update_partition(NULL);
        if (download(j->url, &j->img, &j->len, target ? target->size : 0, true) != ESP_OK) {
            LOCK(); bool cancelled = st.state == ST_IDLE; UNLOCK();
            if (cancelled) log_ring_printf("firmware update: download cancelled");
            goto out;
        }
    }
    set_state(ST_VERIFYING, "checking %u bytes", (unsigned)j->len);
    if (!image_acceptable(j->img, j->len, j->sha, why, sizeof why)) { set_state(ST_FAILED, "%s", why); goto out; }
    if (sync_running()) { set_state(ST_FAILED, "a sync started; install again when it is done"); goto out; }
    char version[33];
    snprintf(version, sizeof version, "%.32s", ((const esp_app_desc_t *)(j->img + sizeof(esp_image_header_t) + sizeof(esp_image_segment_header_t)))->version);
    if (flash_image(j->img, j->len) != ESP_OK) goto out;
    set_state(ST_RESTARTING, "installed %s; restarting to try it (the current firmware comes back if it does not start cleanly)", version);
    free(j->img);
    free(j);
    const esp_timer_create_args_t ta = {.callback = restart_cb, .name = "ota_restart"};
    esp_timer_handle_t t;
    if (esp_timer_create(&ta, &t) != ESP_OK || esp_timer_start_once(t, 1500 * 1000) != ESP_OK) esp_restart();
    vTaskDelete(NULL);
    return;
out:
    free(j->img);
    free(j);
    vTaskDelete(NULL);
}

/* Takes ownership of j. */
static esp_err_t start_job(job_t *j, ota_state_t first)
{
    /* 8 KiB: the TLS session's own buffers are on the heap; flash writes need an
     * internal-RAM stack, which xTaskCreate gives. */
    LOCK(); st.state = first; st.msg[0] = 0; st.done = st.total = 0; UNLOCK();
    if (xTaskCreate(install_task, "ota", 8192, j, 4, NULL) != pdPASS) {
        free(j->img);
        free(j);
        set_state(ST_FAILED, "could not start the update task");
        return ESP_ERR_NO_MEM;
    }
    return ESP_OK;
}

static esp_err_t claim(ota_state_t s)
{
    if (!mtx) mtx = xSemaphoreCreateMutex();
    if (sync_running()) return ESP_ERR_NOT_ALLOWED;
    LOCK();
    bool b = busy_locked() || st.buf;
    if (!b) st.state = s;
    UNLOCK();
    return b ? ESP_ERR_INVALID_STATE : ESP_OK;
}

static bool hex64_ok(const char *s)
{
    if (!s || !*s) return true;
    if (strlen(s) != 64) return false;
    for (const char *p = s; *p; p++) if (!((*p >= '0' && *p <= '9') || (*p >= 'a' && *p <= 'f') || (*p >= 'A' && *p <= 'F'))) return false;
    return true;
}

esp_err_t ota_install_url(const char *url, const char *sha256_hex)
{
    if (!url || (strncmp(url, "http://", 7) && strncmp(url, "https://", 8)) || strlen(url) >= sizeof((job_t *)0)->url) return ESP_ERR_INVALID_ARG;
    if (!hex64_ok(sha256_hex)) return ESP_ERR_INVALID_ARG;
    esp_err_t e = claim(ST_DOWNLOADING);
    if (e != ESP_OK) return e;
    job_t *j = calloc(1, sizeof *j);
    if (!j) { LOCK(); st.state = ST_IDLE; UNLOCK(); return ESP_ERR_NO_MEM; }
    scpy(j->url, sizeof j->url, url);
    if (sha256_hex) scpy(j->sha, sizeof j->sha, sha256_hex);
    log_ring_printf("firmware update requested from %.120s", url);
    return start_job(j, ST_DOWNLOADING);
}

esp_err_t ota_install_available(void)
{
    if (!mtx) mtx = xSemaphoreCreateMutex();
    LOCK();
    bool have = st.avail;
    char url[300], sha[65];
    scpy(url, sizeof url, st.avail_url);
    scpy(sha, sizeof sha, st.avail_sha);
    UNLOCK();
    if (!have) return ESP_ERR_NOT_FOUND;
    return ota_install_url(url, sha);
}

/* ---- checking for a release -------------------------------------------------------- */
static void check_task(void *arg)
{
    (void)arg;
    bool dev = app_config_get()->ota_dev_channel;
    char base[200], url[260];
    /* Rolling releases, not GitHub's "latest": firmware releases are prereleases and
     * the repository's latest release is the app's. firmware-latest follows each
     * firmware release, firmware-dev each test build (.github/workflows/). */
    snprintf(base, sizeof base, "https://github.com/%s/releases/download/%s", CONFIG_RRC_OTA_GITHUB_REPO, dev ? "firmware-dev" : "firmware-latest");
    snprintf(url, sizeof url, "%s/rrcloud-ingest-ota.json", base);
    set_state(ST_CHECKING, "reading the %s manifest", dev ? "test-build" : "release");
    uint8_t *doc = NULL;
    size_t len = 0;
    if (download(url, &doc, &len, 16384, false) != ESP_OK) goto done;
    cJSON *m = cJSON_ParseWithLength((const char *)doc, len);
    free(doc);
    if (!m) { set_state(ST_FAILED, "the manifest is not JSON"); goto done; }
    const char *version = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(m, "version"));
    const char *tag = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(m, "tag"));
    const char *commit = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(m, "commit"));
    const cJSON *images = cJSON_GetObjectItemCaseSensitive(m, "images");
    const cJSON *pick = NULL, *it;
    uint32_t rev = efuse_hal_chip_revision();
    cJSON_ArrayForEach(it, images) {
        const char *variant = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(it, "variant"));
        const cJSON *lo = cJSON_GetObjectItemCaseSensitive(it, "chip_rev_min"), *hi = cJSON_GetObjectItemCaseSensitive(it, "chip_rev_max");
        if (!variant || strcmp(variant, rrc_fw_tag.variant)) continue;
        if (cJSON_IsNumber(lo) && rev < (uint32_t)lo->valuedouble) continue;
        if (cJSON_IsNumber(hi) && rev > (uint32_t)hi->valuedouble) continue;
        pick = it;
        break;
    }
    const char *file = pick ? cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(pick, "file")) : NULL;
    const char *sha = pick ? cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(pick, "sha256")) : NULL;
    const cJSON *size = pick ? cJSON_GetObjectItemCaseSensitive(pick, "size") : NULL;
    bool ok = version && file && !strchr(file, '/') && sha && hex64_ok(sha) && strlen(sha) == 64 && cJSON_IsNumber(size);
    LOCK();
    st.checked = true;
    st.avail = ok;
    scpy(st.checked_channel, sizeof st.checked_channel, dev ? "dev" : "release");
    if (ok) {
        scpy(st.avail_version, sizeof st.avail_version, version);
        scpy(st.avail_tag, sizeof st.avail_tag, tag ? tag : "");
        scpy(st.avail_commit, sizeof st.avail_commit, commit ? commit : "");
        /* The image comes from the versioned release the manifest names, not from the
         * rolling one: a release published between the check and the install must not
         * swap it (the SHA-256 would catch it, as a confusing failure). */
        if (!dev && tag && !strchr(tag, '/'))
            snprintf(st.avail_url, sizeof st.avail_url, "https://github.com/%s/releases/download/%s/%s", CONFIG_RRC_OTA_GITHUB_REPO, tag, file);
        else
            snprintf(st.avail_url, sizeof st.avail_url, "%s/%s", base, file);
        scpy(st.avail_sha, sizeof st.avail_sha, sha);
        st.avail_size = (uint32_t)size->valuedouble;
    }
    UNLOCK();
    if (ok) set_state(ST_IDLE, "%s %s is available (running %s)", dev ? "test build" : "release", version, esp_app_get_description()->version);
    else set_state(ST_FAILED, "the %s manifest has no image for %s on chip v%u.%u", dev ? "test-build" : "release", rrc_fw_tag.variant, (unsigned)(rev / 100), (unsigned)(rev % 100));
    cJSON_Delete(m);
done:
    vTaskDelete(NULL);
}

esp_err_t ota_check(void)
{
    if (!mtx) mtx = xSemaphoreCreateMutex();
    LOCK();
    bool b = busy_locked() || st.buf;
    if (!b) { st.state = ST_CHECKING; st.msg[0] = 0; }
    UNLOCK();
    if (b) return ESP_ERR_INVALID_STATE;
    if (xTaskCreate(check_task, "ota_check", 8192, NULL, 3, NULL) != pdPASS) { set_state(ST_FAILED, "could not start the check"); return ESP_ERR_NO_MEM; }
    return ESP_OK;
}

/* ---- uploads ----------------------------------------------------------------------- */
#define UPLOAD_IDLE_US (120 * 1000000LL)   /* an abandoned upload frees its buffer */

static void upload_drop_locked(void)
{
    free(st.buf);
    st.buf = NULL;
    st.size = st.got = 0;
}

esp_err_t ota_upload_begin(size_t size, const char *sha256_hex)
{
    if (!mtx) mtx = xSemaphoreCreateMutex();
    const esp_partition_t *target = esp_ota_get_next_update_partition(NULL);
    if (!target || size < FW_TAG_OFFSET + sizeof(fw_tag_t) || size > target->size || !hex64_ok(sha256_hex)) return ESP_ERR_INVALID_ARG;
    if (sync_running()) return ESP_ERR_NOT_ALLOWED;
    LOCK();
    /* a stale upload (client gone) gives way to a new one */
    if (st.buf && esp_timer_get_time() - st.last_chunk_us > UPLOAD_IDLE_US) upload_drop_locked();
    bool b = busy_locked() || st.buf;
    UNLOCK();
    if (b) return ESP_ERR_INVALID_STATE;
    uint8_t *buf = heap_caps_malloc(size, MALLOC_CAP_SPIRAM | MALLOC_CAP_8BIT);
    if (!buf) return ESP_ERR_NO_MEM;
    LOCK();
    st.buf = buf;
    st.size = size;
    st.got = 0;
    scpy(st.sha, sizeof st.sha, sha256_hex ? sha256_hex : "");
    st.last_chunk_us = esp_timer_get_time();
    st.state = ST_RECEIVING;
    st.done = 0;
    st.total = (uint32_t)size;
    snprintf(st.msg, sizeof st.msg, "receiving %u bytes", (unsigned)size);
    UNLOCK();
    log_ring_printf("firmware update: receiving a %u-byte image", (unsigned)size);
    return ESP_OK;
}

esp_err_t ota_upload_chunk(size_t offset, const uint8_t *data, size_t len, size_t *expected)
{
    if (!mtx) return ESP_ERR_INVALID_STATE;
    LOCK();
    esp_err_t e = ESP_OK;
    if (!st.buf || st.state != ST_RECEIVING) e = ESP_ERR_INVALID_STATE;
    else if (offset + len <= st.got) e = ESP_OK;                  /* a retried chunk already have */
    else if (offset != st.got || len > st.size - st.got) e = ESP_ERR_INVALID_ARG;
    else {
        memcpy(st.buf + st.got, data, len);
        st.got += len;
        st.done = (uint32_t)st.got;
    }
    if (st.buf) st.last_chunk_us = esp_timer_get_time();
    if (expected) *expected = st.got;
    UNLOCK();
    return e;
}

esp_err_t ota_upload_finish(void)
{
    if (!mtx) return ESP_ERR_INVALID_STATE;
    LOCK();
    if (!st.buf || st.state != ST_RECEIVING) { UNLOCK(); return ESP_ERR_INVALID_STATE; }
    if (st.got != st.size) { UNLOCK(); return ESP_ERR_INVALID_SIZE; }
    job_t *j = calloc(1, sizeof *j);
    if (!j) { UNLOCK(); return ESP_ERR_NO_MEM; }
    j->img = st.buf;
    j->len = st.size;
    scpy(j->sha, sizeof j->sha, st.sha);
    st.buf = NULL;   /* the job owns it now */
    st.size = st.got = 0;
    UNLOCK();
    return start_job(j, ST_VERIFYING);
}

void ota_cancel(void)
{
    if (!mtx) return;
    LOCK();
    bool was = st.buf || st.state == ST_DOWNLOADING || st.state == ST_CHECKING || st.state == ST_RECEIVING;
    if (st.buf) upload_drop_locked();
    /* a download notices on its next read; writing and restarting cannot be stopped */
    if (st.state == ST_DOWNLOADING || st.state == ST_CHECKING || st.state == ST_RECEIVING) { st.state = ST_IDLE; scpy(st.msg, sizeof st.msg, "cancelled"); }
    UNLOCK();
    if (was) log_ring_printf("firmware update cancelled");
    ESP_LOGI(TAG, "cancel");
}

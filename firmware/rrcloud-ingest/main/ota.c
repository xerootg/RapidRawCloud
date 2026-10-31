/*
 * ota — updates of the dock's own firmware; see ota.h for the design.
 *
 * One job at a time, identified by a generation number: starting a check, a download
 * or an upload takes a new generation, and cancelling bumps it. A job only changes the
 * shared state, and only reaches the flash, while its generation is still the current
 * one, so a cancelled download that is still reading cannot overwrite the next job's
 * state or flash its image. Once writing has begun the job can no longer be cancelled.
 *
 * A cancelled job's task can still be inside an HTTP connect or read for up to the
 * client timeout. No new job starts until it has exited (st.workers), so nothing
 * reaches the flash while a superseded download is still receiving.
 *
 * Other network work (sync runs and the sync tick, pairing, console diagnostics) holds
 * a reservation (ota_net_begin/end) taken under the same lock as an update's admission
 * and its switch to writing: no check, download, upload or flash write starts beside
 * it, and none of it starts beside any of them (nor before a cancelled one's task has
 * exited). The radio co-processor's updater (coproc.c) takes the update slot itself
 * through ota_external_begin/end.
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
#include "nvs.h"
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
#include "ble.h"
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

#define UPLOAD_IDLE_US (120 * 1000000LL)   /* an upload with no chunk for this long is dropped */

static struct {
    ota_state_t state;
    uint32_t gen;        /* the current job; see the file comment */
    int workers;         /* check/install tasks still running, current or cancelled */
    int net_users;       /* holders of the ota_net_begin reservation */
    bool external;       /* the co-processor updater holds the slot */
    char msg[160];
    uint32_t done, total;
    /* the last check */
    bool checked, avail;
    char avail_version[32], avail_tag[48], avail_commit[16], avail_url[300], avail_sha[65];
    uint32_t avail_size;
    char checked_channel[8];
    /* this boot */
    bool trial, rolled_back, no_rollback;
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

static void ensure_mutex(void)
{
    /* ota_init() runs first in app_main; the guard is for the console and API racing it */
    if (!mtx) mtx = xSemaphoreCreateMutex();
}

/* Sets the state for job `gen`; a stale job's update is dropped (false). */
static bool set_state(uint32_t gen, ota_state_t s, const char *fmt, ...) __attribute__((format(printf, 3, 4)));
static bool set_state(uint32_t gen, ota_state_t s, const char *fmt, ...)
{
    char m[160];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(m, sizeof m, fmt, ap);
    va_end(ap);
    LOCK();
    bool current = gen == st.gen;
    if (current) { st.state = s; scpy(st.msg, sizeof st.msg, m); }
    UNLOCK();
    if (current) log_ring_printf("firmware update%s: %s", s == ST_FAILED ? " failed" : "", m);
    return current;
}

static void upload_drop_locked(void)
{
    free(st.buf);
    st.buf = NULL;
    st.size = st.got = 0;
    if (st.state == ST_RECEIVING) { st.state = ST_IDLE; scpy(st.msg, sizeof st.msg, "upload abandoned"); }
}

/* An upload whose client went away gives way: it holds 3 MB of PSRAM and the busy flag. */
static void expire_upload_locked(void)
{
    if (st.buf && st.state == ST_RECEIVING && esp_timer_get_time() - st.last_chunk_us > UPLOAD_IDLE_US) {
        upload_drop_locked();
        log_ring_printf("firmware update: an upload had no data for %lld s and was dropped", UPLOAD_IDLE_US / 1000000);
    }
}

static bool busy_locked(void)
{
    expire_upload_locked();
    return (st.state >= ST_CHECKING && st.state <= ST_RESTARTING) || st.buf || st.workers > 0 || st.external;
}

bool ota_net_begin(void)
{
    ensure_mutex();
    LOCK();
    bool ok = !busy_locked();
    if (ok) st.net_users++;
    UNLOCK();
    return ok;
}

esp_err_t ota_external_begin(void)
{
    ensure_mutex();
    LOCK();
    esp_err_t e = ESP_OK;
    if (st.net_users > 0) e = ESP_ERR_NOT_ALLOWED;
    else if (busy_locked()) e = ESP_ERR_INVALID_STATE;
    else st.external = true;
    UNLOCK();
    return e;
}

void ota_external_end(void)
{
    ensure_mutex();
    LOCK(); st.external = false; UNLOCK();
}

void ota_net_end(void)
{
    ensure_mutex();
    LOCK(); if (st.net_users > 0) st.net_users--; UNLOCK();
}

static void worker_exit(void)
{
    LOCK(); st.workers--; UNLOCK();
}

/* Starts job `s` (with a task to run it) when nothing else runs; returns its generation in *gen. */
static esp_err_t claim(ota_state_t s, uint32_t *gen)
{
    ensure_mutex();
    LOCK();
    esp_err_t e = ESP_OK;
    /* Until the running image is kept, the other slot holds the one to roll back to:
     * installing over it would leave nothing to go back to. */
    if (st.trial) e = ESP_ERR_NOT_FINISHED;
    else if (st.net_users > 0) e = ESP_ERR_NOT_ALLOWED;
    else if (busy_locked()) e = ESP_ERR_INVALID_STATE;
    else { st.gen++; st.workers++; st.state = s; st.msg[0] = 0; st.done = st.total = 0; *gen = st.gen; }
    UNLOCK();
    return e;
}

/* ---- trial boot -------------------------------------------------------------------- */
#define HEALTH_MIN_UP_US   (60 * 1000000LL)
#define HEALTH_GIVE_UP_US  (5 * 60 * 1000000LL)

/* Healthy = up a minute and reachable for another update: on the network, or over
 * Bluetooth (a dock with no cable and no Wi-Fi is managed from the app). A build that
 * cannot be reached at all is the one that must not be kept. */
static void health_tick(void *arg)
{
    esp_timer_handle_t self = *(esp_timer_handle_t *)arg;
    int64_t up = esp_timer_get_time();
    bool ip = net_has_ip(), bt = ble_reachable();
    if (up >= HEALTH_MIN_UP_US && (ip || bt)) {
        esp_err_t e = esp_ota_mark_app_valid_cancel_rollback();
        if (e == ESP_OK) {
            LOCK(); st.trial = false; UNLOCK();
            log_ring_printf("firmware update: %s kept (up %lld s, reachable over %s)", esp_app_get_description()->version,
                            (long long)(up / 1000000), ip && bt ? "the network and Bluetooth" : ip ? "the network" : "Bluetooth");
            esp_timer_stop(self);
            return;
        }
        /* still pending verification for the bootloader: stay on trial and retry on the
         * next tick, up to the give-up time below */
        static bool logged;
        if (!logged) { logged = true; log_ring_printf("firmware update: could not mark %s as kept (%s); retrying", esp_app_get_description()->version, esp_err_to_name(e)); }
    }
    if (up >= HEALTH_GIVE_UP_US) {
        log_ring_printf("firmware update: %s is not reachable %lld s after starting (no network, no Bluetooth); going back to the previous firmware",
                        esp_app_get_description()->version, (long long)(up / 1000000));
        vTaskDelay(pdMS_TO_TICKS(500));   /* let the line reach the console */
        esp_ota_mark_app_invalid_rollback_and_reboot();
    }
}

/* Whether an update has shown this dock's bootloader cannot roll back, kept in NVS
 * until a trial boot shows that it can (flashing the bootloader over USB does). */
static void no_rollback_store(bool v)
{
    nvs_handle_t h;
    if (nvs_open("ota", NVS_READWRITE, &h) != ESP_OK) return;
    if (v) nvs_set_u8(h, "no_rollback", 1);
    else nvs_erase_key(h, "no_rollback");
    nvs_commit(h);
    nvs_close(h);
}

static bool no_rollback_load(void)
{
    nvs_handle_t h;
    uint8_t v = 0;
    if (nvs_open("ota", NVS_READONLY, &h) != ESP_OK) return false;
    nvs_get_u8(h, "no_rollback", &v);
    nvs_close(h);
    return v != 0;
}

void ota_init(void)
{
    ensure_mutex();
    const esp_partition_t *running = esp_ota_get_running_partition();
    esp_ota_img_states_t s;
    esp_err_t se = running ? esp_ota_get_state_partition(running, &s) : ESP_FAIL;
    if (se == ESP_OK && s == ESP_OTA_IMG_PENDING_VERIFY) {
        st.trial = true;
        log_ring_printf("firmware update: trying %s; it is kept after a minute if the dock can be reached, otherwise the previous firmware comes back",
                        esp_app_get_description()->version);
        static esp_timer_handle_t t;
        const esp_timer_create_args_t ta = {.callback = health_tick, .arg = &t, .name = "ota_health"};
        if (esp_timer_create(&ta, &t) != ESP_OK || esp_timer_start_periodic(t, 5 * 1000000) != ESP_OK) {
            /* Without the check the image cannot prove itself: fail closed, back to the
             * previous firmware, rather than keep an image nobody has checked. */
            log_ring_printf("firmware update: the health check could not start; going back to the previous firmware");
            vTaskDelay(pdMS_TO_TICKS(500));
            esp_ota_mark_app_invalid_rollback_and_reboot();
        }
        /* a trial boot proves the bootloader rolls back */
        no_rollback_store(false);
    } else if (se == ESP_OK && s == ESP_OTA_IMG_NEW) {
        /* Installed over the air, yet started without a trial: the bootloader predates
         * rollback (CONFIG_BOOTLOADER_APP_ROLLBACK_ENABLE lives there, and updates do not
         * replace it). Say so, and remember it: after this boot the image is VALID and
         * the next boot could not tell. */
        no_rollback_store(true);
        esp_ota_mark_app_valid_cancel_rollback();
    }
    st.no_rollback = no_rollback_load();
    if (st.no_rollback)
        log_ring_printf("firmware update: this dock's bootloader cannot roll back a bad update; flash the bootloader over USB once (README, \"Firmware updates\")");
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
    ensure_mutex();
    const esp_app_desc_t *app = esp_app_get_description();
    LOCK();
    expire_upload_locked();
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
    /* where this build takes its updates from, so a phone downloading for it uses the same */
    rrc_jsonw_key(&w, "repo", false); rrc_jsonw_str(&w, CONFIG_RRC_OTA_GITHUB_REPO);
    rrc_jsonw_key(&w, "state", false); rrc_jsonw_str(&w, state_names[st.state]);
    rrc_jsonw_key(&w, "message", false); rrc_jsonw_str(&w, st.msg);
    rrc_jsonw_key(&w, "done", false); rrc_jsonw_u64(&w, st.done);
    rrc_jsonw_key(&w, "total", false); rrc_jsonw_u64(&w, st.total);
    /* the image an interrupted upload is collecting, so a client resumes only its own */
    rrc_jsonw_key(&w, "upload_sha256", false); rrc_jsonw_str(&w, st.buf ? st.sha : "");
    rrc_jsonw_key(&w, "trial", false); rrc_jsonw_raw(&w, st.trial ? "true" : "false");
    rrc_jsonw_key(&w, "rollback", false); rrc_jsonw_raw(&w, st.no_rollback ? "false" : "true");
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
    uint32_t gen;
    uint8_t *img;        /* PSRAM, owned */
    size_t len;
    char sha[65];
    char url[300];       /* download source, or "" for an image that is already in img */
} job_t;

static void restart_cb(void *arg) { (void)arg; esp_restart(); }

/* Runs on an internal-RAM stack (flash writes cannot run from a PSRAM stack). The job
 * is already WRITING, which nothing can cancel. */
static esp_err_t flash_image(uint32_t gen, const uint8_t *img, size_t len)
{
    const esp_partition_t *target = esp_ota_get_next_update_partition(NULL);
    esp_ota_handle_t h = 0;
    esp_err_t e = esp_ota_begin(target, OTA_WITH_SEQUENTIAL_WRITES, &h);
    if (e != ESP_OK) { set_state(gen, ST_FAILED, "could not open %s: %s", target->label, esp_err_to_name(e)); return e; }
    for (size_t off = 0; off < len;) {
        size_t n = len - off < 16384 ? len - off : 16384;
        e = esp_ota_write(h, img + off, n);
        if (e != ESP_OK) { esp_ota_abort(h); set_state(gen, ST_FAILED, "write failed at %u: %s", (unsigned)off, esp_err_to_name(e)); return e; }
        off += n;
        LOCK(); st.done = (uint32_t)off; UNLOCK();
    }
    e = esp_ota_end(h);   /* verifies the written image (segments, checksum, SHA-256 appended by the build) */
    if (e != ESP_OK) { set_state(gen, ST_FAILED, "the written image does not verify: %s", esp_err_to_name(e)); return e; }
    e = esp_ota_set_boot_partition(target);
    if (e != ESP_OK) { set_state(gen, ST_FAILED, "could not select %s for the next boot: %s", target->label, esp_err_to_name(e)); return e; }
    return ESP_OK;
}

/* Fails with the reason in the state (job `gen`); ESP_ERR_INVALID_STATE when the job
 * was cancelled or superseded meanwhile. */
static esp_err_t download(uint32_t gen, const char *url, uint8_t **out, size_t *out_len, size_t max, bool progress)
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
    if (!c) { set_state(gen, ST_FAILED, "not enough memory for an HTTP client"); return ESP_ERR_NO_MEM; }
    esp_err_t e = ESP_FAIL;
    uint8_t *buf = NULL;
    int status = 0;
    int64_t len = -1;
    /* esp_http_client_open() does not follow redirects by itself: re-open on 3xx. */
    for (int hop = 0; hop <= 4; hop++) {
        if (esp_http_client_open(c, 0) != ESP_OK) { set_state(gen, ST_FAILED, "cannot connect to %.80s", url); goto out; }
        len = esp_http_client_fetch_headers(c);
        status = esp_http_client_get_status_code(c);
        if (status != 301 && status != 302 && status != 303 && status != 307 && status != 308) break;
        if (esp_http_client_set_redirection(c) != ESP_OK) break;
        esp_http_client_close(c);
    }
    if (status != 200) { set_state(gen, ST_FAILED, "HTTP %d from %.80s", status, url); goto out; }
    if (len > (int64_t)max) { set_state(gen, ST_FAILED, "download is %lld bytes, more than the %u allowed", (long long)len, (unsigned)max); goto out; }
    size_t cap = len > 0 ? (size_t)len : max;
    buf = heap_caps_malloc(cap + 1, MALLOC_CAP_SPIRAM | MALLOC_CAP_8BIT);
    if (!buf) { set_state(gen, ST_FAILED, "not enough free memory for %u bytes (restart the dock and try again)", (unsigned)cap); goto out; }
    if (progress) { LOCK(); if (gen == st.gen) { st.done = 0; st.total = len > 0 ? (uint32_t)len : 0; } UNLOCK(); }
    size_t got = 0;
    for (;;) {
        if (got >= cap) {
            /* a server that sent a length must not send more; one that did not must stay under max */
            char extra;
            if (esp_http_client_read(c, &extra, 1) > 0) { set_state(gen, ST_FAILED, "download larger than %u bytes", (unsigned)cap); goto out; }
            break;
        }
        int n = esp_http_client_read(c, (char *)buf + got, (int)(cap - got < 8192 ? cap - got : 8192));
        if (n < 0) { set_state(gen, ST_FAILED, "download failed after %u bytes", (unsigned)got); goto out; }
        if (n == 0) break;
        got += (size_t)n;
        LOCK();
        bool current = gen == st.gen;
        if (current && progress) st.done = (uint32_t)got;
        UNLOCK();
        if (!current) { e = ESP_ERR_INVALID_STATE; goto out; }
    }
    if (len > 0 && got != (size_t)len) { set_state(gen, ST_FAILED, "short download: %u of %lld bytes", (unsigned)got, (long long)len); goto out; }
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
        set_state(j->gen, ST_DOWNLOADING, "downloading %.120s", j->url);
        const esp_partition_t *target = esp_ota_get_next_update_partition(NULL);
        if (download(j->gen, j->url, &j->img, &j->len, target ? target->size : 0, true) != ESP_OK) goto out;
    }
    if (!set_state(j->gen, ST_VERIFYING, "checking %u bytes", (unsigned)j->len)) goto out;
    if (!image_acceptable(j->img, j->len, j->sha, why, sizeof why)) { set_state(j->gen, ST_FAILED, "%s", why); goto out; }
    char version[33];
    snprintf(version, sizeof version, "%.32s", ((const esp_app_desc_t *)(j->img + sizeof(esp_image_header_t) + sizeof(esp_image_segment_header_t)))->version);
    /* The point of no return: once WRITING, a cancel is refused (ota_cancel). Checked
     * under the lock ota_net_begin takes, so no sync can be starting meanwhile. */
    LOCK();
    bool current = j->gen == st.gen, sync = st.net_users > 0;
    if (current && !sync) { st.state = ST_WRITING; st.done = 0; st.total = (uint32_t)j->len; snprintf(st.msg, sizeof st.msg, "writing %u bytes", (unsigned)j->len); }
    UNLOCK();
    if (current && sync) { set_state(j->gen, ST_FAILED, "a sync or other network work started; install again when it is done"); goto out; }
    if (!current) goto out;
    log_ring_printf("firmware update: writing %s (%u bytes)", version, (unsigned)j->len);
    if (flash_image(j->gen, j->img, j->len) != ESP_OK) goto out;
    set_state(j->gen, ST_RESTARTING, "installed %s; restarting to try it (the current firmware comes back if it does not start cleanly)", version);
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
    worker_exit();
    vTaskDelete(NULL);
}

/* Takes ownership of j (and of j->img). */
static esp_err_t start_job(job_t *j)
{
    /* 8 KiB: the TLS session's own buffers are on the heap; flash writes need an
     * internal-RAM stack, which xTaskCreate gives. */
    uint32_t gen = j->gen;
    if (xTaskCreate(install_task, "ota", 8192, j, 4, NULL) != pdPASS) {
        free(j->img);
        free(j);
        worker_exit();
        set_state(gen, ST_FAILED, "could not start the update task");
        return ESP_ERR_NO_MEM;
    }
    return ESP_OK;
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
    job_t *j = calloc(1, sizeof *j);
    if (!j) return ESP_ERR_NO_MEM;
    esp_err_t e = claim(ST_DOWNLOADING, &j->gen);
    if (e != ESP_OK) { free(j); return e; }
    scpy(j->url, sizeof j->url, url);
    if (sha256_hex) scpy(j->sha, sizeof j->sha, sha256_hex);
    log_ring_printf("firmware update requested from %.120s", url);
    return start_job(j);
}

esp_err_t ota_install_available(void)
{
    ensure_mutex();
    LOCK();
    bool have = st.avail;
    /* the check's result belongs to the channel it ran on */
    bool same_channel = !strcmp(st.checked_channel, app_config_get()->ota_dev_channel ? "dev" : "release");
    char url[300], sha[65];
    scpy(url, sizeof url, st.avail_url);
    scpy(sha, sizeof sha, st.avail_sha);
    UNLOCK();
    if (!have || !same_channel) return ESP_ERR_NOT_FOUND;
    return ota_install_url(url, sha);
}

/* ---- checking for a release -------------------------------------------------------- */
static void check_task(void *arg)
{
    uint32_t gen = (uint32_t)(uintptr_t)arg;
    bool dev = app_config_get()->ota_dev_channel;
    char base[200], url[260];
    /* Rolling releases, not GitHub's "latest": firmware releases are prereleases and
     * the repository's latest release is the app's. firmware-latest follows each
     * firmware release, firmware-dev each test build (.github/workflows/). */
    snprintf(base, sizeof base, "https://github.com/%s/releases/download/%s", CONFIG_RRC_OTA_GITHUB_REPO, dev ? "firmware-dev" : "firmware-latest");
    snprintf(url, sizeof url, "%s/rrcloud-ingest-ota.json", base);
    set_state(gen, ST_CHECKING, "reading the %s manifest", dev ? "test-build" : "release");
    uint8_t *doc = NULL;
    size_t len = 0;
    if (download(gen, url, &doc, &len, 16384, false) != ESP_OK) goto done;
    cJSON *m = cJSON_ParseWithLength((const char *)doc, len);
    free(doc);
    if (!m) { set_state(gen, ST_FAILED, "the manifest is not JSON"); goto done; }
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
        if (cJSON_IsNumber(lo) && rev < lo->valuedouble) continue;
        if (cJSON_IsNumber(hi) && rev > hi->valuedouble) continue;
        pick = it;
        break;
    }
    const char *file = pick ? cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(pick, "file")) : NULL;
    const char *sha = pick ? cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(pick, "sha256")) : NULL;
    const cJSON *size = pick ? cJSON_GetObjectItemCaseSensitive(pick, "size") : NULL;
    bool ok = version && file && !strchr(file, '/') && sha && strlen(sha) == 64 && hex64_ok(sha)
              && cJSON_IsNumber(size) && size->valuedouble > 0 && size->valuedouble < 64.0 * 1024 * 1024;
    LOCK();
    bool current = gen == st.gen;
    if (current) {
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
    }
    UNLOCK();
    if (ok) set_state(gen, ST_IDLE, "%s %s is available (running %s)", dev ? "test build" : "release", version, esp_app_get_description()->version);
    else set_state(gen, ST_FAILED, "the %s manifest has no image for %s on chip v%u.%u", dev ? "test-build" : "release", rrc_fw_tag.variant, (unsigned)(rev / 100), (unsigned)(rev % 100));
    cJSON_Delete(m);
done:
    worker_exit();
    vTaskDelete(NULL);
}

esp_err_t ota_check(void)
{
    ensure_mutex();
    LOCK();
    esp_err_t e = ESP_OK;
    uint32_t gen = 0;
    /* a check is allowed on trial (it changes nothing), not beside a sync or other network work (it downloads) */
    if (st.net_users > 0) e = ESP_ERR_NOT_ALLOWED;
    else if (busy_locked()) e = ESP_ERR_INVALID_STATE;
    else { st.gen++; st.workers++; st.state = ST_CHECKING; st.msg[0] = 0; gen = st.gen; }
    UNLOCK();
    if (e != ESP_OK) return e;
    if (xTaskCreate(check_task, "ota_check", 8192, (void *)(uintptr_t)gen, 3, NULL) != pdPASS) {
        worker_exit();
        set_state(gen, ST_FAILED, "could not start the check");
        return ESP_ERR_NO_MEM;
    }
    return ESP_OK;
}

/* ---- uploads ----------------------------------------------------------------------- */
esp_err_t ota_upload_begin(size_t size, const char *sha256_hex)
{
    ensure_mutex();
    const esp_partition_t *target = esp_ota_get_next_update_partition(NULL);
    if (!target || size < FW_TAG_OFFSET + sizeof(fw_tag_t) || size > target->size || !hex64_ok(sha256_hex)) return ESP_ERR_INVALID_ARG;
    /* allocated before the lock (it can take a while), given back if another begin won */
    uint8_t *buf = heap_caps_malloc(size, MALLOC_CAP_SPIRAM | MALLOC_CAP_8BIT);
    if (!buf) return ESP_ERR_NO_MEM;
    LOCK();
    esp_err_t e = ESP_OK;
    if (st.trial) e = ESP_ERR_NOT_FINISHED;
    else if (st.net_users > 0) e = ESP_ERR_NOT_ALLOWED;
    else if (busy_locked()) e = ESP_ERR_INVALID_STATE;
    else {
        st.gen++;
        st.buf = buf;
        buf = NULL;
        st.size = size;
        st.got = 0;
        scpy(st.sha, sizeof st.sha, sha256_hex ? sha256_hex : "");
        st.last_chunk_us = esp_timer_get_time();
        st.state = ST_RECEIVING;
        st.done = 0;
        st.total = (uint32_t)size;
        snprintf(st.msg, sizeof st.msg, "receiving %u bytes", (unsigned)size);
    }
    UNLOCK();
    free(buf);
    if (e == ESP_OK) log_ring_printf("firmware update: receiving a %u-byte image", (unsigned)size);
    return e;
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
    job_t *j = calloc(1, sizeof *j);
    if (!j) return ESP_ERR_NO_MEM;
    LOCK();
    esp_err_t e = ESP_OK;
    if (!st.buf || st.state != ST_RECEIVING) e = ESP_ERR_INVALID_STATE;
    else if (st.got != st.size) e = ESP_ERR_INVALID_SIZE;
    else {
        j->gen = st.gen;
        st.workers++;
        j->img = st.buf;
        j->len = st.size;
        scpy(j->sha, sizeof j->sha, st.sha);
        st.buf = NULL;   /* the job owns it now */
        st.size = st.got = 0;
        st.state = ST_VERIFYING;
    }
    UNLOCK();
    if (e != ESP_OK) { free(j); return e; }
    return start_job(j);
}

esp_err_t ota_cancel(void)
{
    if (!mtx) return ESP_OK;
    LOCK();
    esp_err_t e = ESP_OK;
    if (st.state == ST_WRITING || st.state == ST_RESTARTING) e = ESP_ERR_INVALID_STATE;   /* past the point of no return */
    else {
        bool was = st.buf || (st.state >= ST_CHECKING && st.state <= ST_VERIFYING);
        if (st.buf) upload_drop_locked();
        if (was) {
            st.gen++;   /* the running job, if any, finds itself superseded and stops */
            st.state = ST_IDLE;
            scpy(st.msg, sizeof st.msg, "cancelled");
            log_ring_printf("firmware update cancelled");
        }
    }
    UNLOCK();
    ESP_LOGI(TAG, "cancel: %s", esp_err_to_name(e));
    return e;
}

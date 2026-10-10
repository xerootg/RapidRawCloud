/*
 * coproc — ESP32-C6 co-processor link, firmware identification and update.
 *
 * The Waveshare board ships the C6 with whatever ESP-Hosted slave build was
 * current at manufacture; the P4 host library (esp_hosted component) must match
 * it major.minor or Wi-Fi/BLE RPCs fail. ESP-Hosted keeps the transport up on a
 * version mismatch precisely so the host can OTA the co-processor, which is what
 * coproc_update_start() does: stream an image over HTTP(S) into the C6's OTA
 * partition in ≤1536-byte RPC chunks, verify, activate, then restart the dock.
 */
#include "coproc.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdarg.h>
#include "esp_log.h"
#include "esp_system.h"
#include "esp_http_client.h"
#include "esp_crt_bundle.h"
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/semphr.h"
#include "mbedtls/sha256.h"
#include "esp_hosted.h"
#include "eh_host_sys.h"
#include "eh_host_cp_ota.h"
#include "log_ring.h"
#include "rrc_proto.h"
#include "util.h"

static const char *TAG = "coproc";

#define OTA_CHUNK 1536u /* EH_RPC_OTA_CHUNK_MAX: one RPC blob per write */

typedef enum { UPD_IDLE, UPD_RUNNING, UPD_DONE, UPD_FAILED } upd_state_t;

static struct {
    bool linked, linking;
    char error[96];
    char version[32], project[32], idf[32];
    upd_state_t upd;
    uint32_t upd_done, upd_total;
    char upd_msg[128];
} st;
static SemaphoreHandle_t mtx;
#define LOCK() xSemaphoreTake(mtx, portMAX_DELAY)
#define UNLOCK() xSemaphoreGive(mtx)

static void set_upd(upd_state_t s, const char *fmt, ...) __attribute__((format(printf, 2, 3)));
static void set_upd(upd_state_t s, const char *fmt, ...)
{
    char buf[128];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(buf, sizeof buf, fmt, ap);
    va_end(ap);
    LOCK();
    st.upd = s;
    scpy(st.upd_msg, sizeof st.upd_msg, buf);
    UNLOCK();
    if (s == UPD_FAILED) log_ring_printf("co-processor update failed: %s", buf);
    else log_ring_printf("co-processor update: %s", buf);
}

static bool version_compatible(const char *v)
{
    /* major.minor must match the host library (ESP-Hosted-MCU rule). */
    unsigned cp_major, cp_minor, host_major, host_minor;
    if (sscanf(v, "%u.%u", &cp_major, &cp_minor) != 2) return false;
    if (sscanf(COPROC_HOST_LIB_VERSION, "%u.%u", &host_major, &host_minor) != 2) return false;
    return cp_major == host_major && cp_minor == host_minor;
}

static void read_identity(void)
{
    esp_hosted_app_desc_t d;
    memset(&d, 0, sizeof d);
    char version[32] = "", project[32] = "", idf[32] = "";
    if (eh_host_sys_get_cp_app_desc(&d) == ESP_OK && d.magic_word == ESP_HOSTED_APP_DESC_MAGIC_WORD) {
        snprintf(version, sizeof version, "%.*s", (int)sizeof d.version, d.version);
        snprintf(project, sizeof project, "%.*s", (int)sizeof d.project_name, d.project_name);
        snprintf(idf, sizeof idf, "%.*s", (int)sizeof d.idf_ver, d.idf_ver);
    }
    eh_host_coprocessor_fwver_t v;
    memset(&v, 0, sizeof v);
    if (eh_host_sys_get_cp_fw_version(&v) == ESP_OK && (v.major1 | v.minor1 | v.patch1))
        snprintf(version, sizeof version, "%u.%u.%u", (unsigned)v.major1, (unsigned)v.minor1, (unsigned)v.patch1);
    LOCK();
    scpy(st.version, sizeof st.version, version[0] ? version : "unknown");
    scpy(st.project, sizeof st.project, project);
    scpy(st.idf, sizeof st.idf, idf);
    UNLOCK();
}

static void link_task(void *arg)
{
    (void)arg;
    int rc = esp_hosted_init();
    if (rc != 0) {
        LOCK(); snprintf(st.error, sizeof st.error, "esp-hosted init failed (%d)", rc); st.linking = false; UNLOCK();
        log_ring_printf("co-processor: %s", st.error);
        vTaskDelete(NULL);
        return;
    }
    rc = esp_hosted_connect_to_slave();
    if (rc != 0) {
        LOCK(); snprintf(st.error, sizeof st.error, "the ESP32-C6 did not answer over SDIO (%d)", rc); st.linking = false; UNLOCK();
        log_ring_printf("co-processor: %s — Wi-Fi and Bluetooth are unavailable", st.error);
        vTaskDelete(NULL);
        return;
    }
    LOCK(); st.linked = true; st.linking = false; st.error[0] = 0; UNLOCK();
    read_identity();
    bool ok = version_compatible(st.version);
    log_ring_printf("co-processor: ESP32-C6 linked over SDIO, firmware %s%s%s (host library " COPROC_HOST_LIB_VERSION ")%s",
                    st.version, st.project[0] ? " " : "", st.project,
                    ok ? "" : " — version mismatch: update the co-processor firmware (Device → Radio) before using Wi-Fi or Bluetooth");
    vTaskDelete(NULL);
}

esp_err_t coproc_start(void)
{
    if (!mtx) mtx = xSemaphoreCreateMutex();
    LOCK();
    bool busy = st.linked || st.linking;
    if (!busy) st.linking = true;
    UNLOCK();
    if (busy) return ESP_OK;
    if (xTaskCreate(link_task, "coproc_link", 6144, NULL, 3, NULL) != pdPASS) {
        LOCK(); st.linking = false; UNLOCK();
        return ESP_ERR_NO_MEM;
    }
    return ESP_OK;
}

bool coproc_linked(void)
{
    if (!mtx) return false;
    LOCK(); bool l = st.linked; UNLOCK();
    return l;
}

esp_err_t coproc_ensure_link(uint32_t timeout_ms)
{
    coproc_start();
    for (uint32_t waited = 0;; waited += 100) {
        LOCK(); bool linked = st.linked, linking = st.linking; UNLOCK();
        if (linked) return ESP_OK;
        if (!linking || waited >= timeout_ms) return ESP_ERR_TIMEOUT;
        vTaskDelay(pdMS_TO_TICKS(100));
    }
}

int coproc_status_json(char *out, size_t cap)
{
    if (!mtx) { mtx = xSemaphoreCreateMutex(); }
    LOCK();
    rrc_jsonw w;
    rrc_jsonw_init(&w, out, cap);
    rrc_jsonw_raw(&w, "{");
    rrc_jsonw_key(&w, "linked", true); rrc_jsonw_raw(&w, st.linked ? "true" : "false");
    rrc_jsonw_key(&w, "linking", false); rrc_jsonw_raw(&w, st.linking ? "true" : "false");
    rrc_jsonw_key(&w, "version", false); rrc_jsonw_str(&w, st.version);
    rrc_jsonw_key(&w, "project", false); rrc_jsonw_str(&w, st.project);
    rrc_jsonw_key(&w, "idf", false); rrc_jsonw_str(&w, st.idf);
    rrc_jsonw_key(&w, "host_lib", false); rrc_jsonw_str(&w, COPROC_HOST_LIB_VERSION);
    rrc_jsonw_key(&w, "compatible", false); rrc_jsonw_raw(&w, st.linked && version_compatible(st.version) ? "true" : "false");
    rrc_jsonw_key(&w, "error", false); rrc_jsonw_str(&w, st.error);
    rrc_jsonw_key(&w, "update", false); rrc_jsonw_raw(&w, "{");
    static const char *const names[] = {"idle", "running", "done", "failed"};
    rrc_jsonw_key(&w, "state", true); rrc_jsonw_str(&w, names[st.upd]);
    rrc_jsonw_key(&w, "done", false); rrc_jsonw_u64(&w, st.upd_done);
    rrc_jsonw_key(&w, "total", false); rrc_jsonw_u64(&w, st.upd_total);
    rrc_jsonw_key(&w, "message", false); rrc_jsonw_str(&w, st.upd_msg);
    rrc_jsonw_raw(&w, "}}");
    UNLOCK();
    return rrc_jsonw_finish(&w);
}

typedef struct { char url[256]; char sha[65]; } upd_req_t;

static void update_task(void *arg)
{
    upd_req_t *r = arg;
    esp_http_client_handle_t c = NULL;
    uint8_t *buf = NULL;
    bool begun = false;
    mbedtls_sha256_context sha;
    mbedtls_sha256_init(&sha);

    set_upd(UPD_RUNNING, "waiting for the co-processor link");
    if (coproc_ensure_link(20000) != ESP_OK) { set_upd(UPD_FAILED, "co-processor link is down (%s)", st.error[0] ? st.error : "timeout"); goto out; }

    set_upd(UPD_RUNNING, "downloading %s", r->url);
    esp_http_client_config_t cfg = {
        .url = r->url,
        .timeout_ms = 30000,
        .crt_bundle_attach = esp_crt_bundle_attach,
        .buffer_size = 4096,
        .buffer_size_tx = 1024,
        .max_redirection_count = 3,
    };
    c = esp_http_client_init(&cfg);
    if (!c) { set_upd(UPD_FAILED, "http client init failed"); goto out; }
    if (esp_http_client_open(c, 0) != ESP_OK) { set_upd(UPD_FAILED, "cannot connect to %s", r->url); goto out; }
    int64_t len = esp_http_client_fetch_headers(c);
    int status = esp_http_client_get_status_code(c);
    if (status != 200) { set_upd(UPD_FAILED, "HTTP %d from %s", status, r->url); goto out; }
    LOCK(); st.upd_total = len > 0 ? (uint32_t)len : 0; st.upd_done = 0; UNLOCK();

    buf = malloc(OTA_CHUNK);
    if (!buf) { set_upd(UPD_FAILED, "out of memory"); goto out; }
    esp_err_t e = eh_host_cp_ota_begin();
    if (e != ESP_OK) { set_upd(UPD_FAILED, "the co-processor refused to start an update: %s", esp_err_to_name(e)); goto out; }
    begun = true;
    mbedtls_sha256_starts(&sha, 0);
    uint32_t done = 0, last_logged = 0;
    for (;;) {
        int n = esp_http_client_read(c, (char *)buf, OTA_CHUNK);
        if (n < 0) { set_upd(UPD_FAILED, "download failed after %u bytes", (unsigned)done); goto out; }
        if (n == 0) break;
        mbedtls_sha256_update(&sha, buf, (size_t)n);
        e = eh_host_cp_ota_write(buf, (uint32_t)n);
        if (e != ESP_OK) { set_upd(UPD_FAILED, "co-processor rejected data at %u bytes: %s", (unsigned)done, esp_err_to_name(e)); goto out; }
        done += (uint32_t)n;
        LOCK(); st.upd_done = done; UNLOCK();
        if (done - last_logged >= 256 * 1024) { last_logged = done; ESP_LOGI(TAG, "update: %u / %u bytes", (unsigned)done, (unsigned)st.upd_total); }
    }
    if (st.upd_total && done != st.upd_total) { set_upd(UPD_FAILED, "short download: %u of %u bytes", (unsigned)done, (unsigned)st.upd_total); goto out; }
    unsigned char dig[32];
    mbedtls_sha256_finish(&sha, dig);
    char hex[65];
    for (int i = 0; i < 32; i++) snprintf(hex + 2 * i, 3, "%02x", dig[i]);
    if (r->sha[0] && strcasecmp(hex, r->sha) != 0) { set_upd(UPD_FAILED, "image sha256 mismatch (got %s); not activated", hex); goto out; }
    set_upd(UPD_RUNNING, "verifying %u bytes on the co-processor", (unsigned)done);
    e = eh_host_cp_ota_end();
    if (e != ESP_OK) { set_upd(UPD_FAILED, "the co-processor rejected the image: %s", esp_err_to_name(e)); goto out; }
    e = eh_host_cp_ota_activate();
    if (e != ESP_OK) {
        /* Co-processors older than ESP-Hosted 2.6 switch the boot partition and
         * reboot inside `end` and do not know the separate activate RPC (it
         * times out while the C6 is already restarting). That is exactly the
         * factory firmware this update exists for, so only a co-processor that
         * reported a modern version makes a failed activation fatal. */
        LOCK(); bool legacy = !strcmp(st.version, "unknown"); UNLOCK();
        if (!legacy) { set_upd(UPD_FAILED, "activation failed: %s", esp_err_to_name(e)); goto out; }
        ESP_LOGW(TAG, "activate: %s — legacy co-processor, activation is implicit in end", esp_err_to_name(e));
    }
    set_upd(UPD_DONE, "installed %u bytes (sha256 %.8s…); the co-processor rebooted — restarting the dock to re-link", (unsigned)done, hex);
    begun = false;
    esp_http_client_close(c);
    esp_http_client_cleanup(c);
    c = NULL;
    vTaskDelay(pdMS_TO_TICKS(1500));
    esp_restart();
out:
    (void)begun;
    mbedtls_sha256_free(&sha);
    if (c) { esp_http_client_close(c); esp_http_client_cleanup(c); }
    free(buf);
    free(r);
    vTaskDelete(NULL);
}

esp_err_t coproc_update_start(const char *url, const char *sha256_hex)
{
    if (!mtx) mtx = xSemaphoreCreateMutex();
    if (!url || !*url) { url = COPROC_IMAGE_URL_DEFAULT; sha256_hex = COPROC_IMAGE_SHA256_DEFAULT; }
    if (strncmp(url, "http://", 7) && strncmp(url, "https://", 8)) return ESP_ERR_INVALID_ARG;
    if (sha256_hex && *sha256_hex && strlen(sha256_hex) != 64) return ESP_ERR_INVALID_ARG;
    LOCK();
    bool running = st.upd == UPD_RUNNING;
    if (!running) { st.upd = UPD_RUNNING; st.upd_done = st.upd_total = 0; st.upd_msg[0] = 0; }
    UNLOCK();
    if (running) return ESP_ERR_INVALID_STATE;
    upd_req_t *r = calloc(1, sizeof *r);
    if (!r) { LOCK(); st.upd = UPD_FAILED; UNLOCK(); return ESP_ERR_NO_MEM; }
    scpy(r->url, sizeof r->url, url);
    if (sha256_hex) scpy(r->sha, sizeof r->sha, sha256_hex);
    /* 12 KiB: TLS session for the download + RPC frames; the data buffer is on the heap. */
    if (xTaskCreate(update_task, "coproc_upd", 12288, r, 3, NULL) != pdPASS) { free(r); LOCK(); st.upd = UPD_FAILED; UNLOCK(); return ESP_ERR_NO_MEM; }
    return ESP_OK;
}

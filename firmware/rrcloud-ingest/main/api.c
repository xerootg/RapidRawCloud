/*
 * api — the dock's admin operations, independent of transport. web.c maps HTTP
 * requests onto api_dispatch(); ble.c maps BLE RPC messages onto it. Every
 * route answers with a JSON document and an HTTP-style status.
 */
#include "api.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>
#include "esp_log.h"
#include "esp_timer.h"
#include "esp_system.h"
#include "esp_app_desc.h"
#include "esp_heap_caps.h"
#include "cJSON.h"
#include "app_config.h"
#include "sync.h"
#include "net.h"
#include "pairing.h"
#include "usb_diag.h"
#include "usb_fault.h"
#include "log_ring.h"
#include "store.h"
#include "coproc.h"
#include "ota.h"
#include "ble.h"
#include "power.h"
#include "board.h"
#include "rrc_proto.h"
#include "util.h"
#include "mbedtls/base64.h"

static const char *TAG = "api";

static void set(api_resp_t *r, int status, char *json, int len)
{
    r->status = status;
    r->json = json;
    r->len = json ? (len < 0 ? strlen(json) : (size_t)len) : 0;
}

static void err(api_resp_t *r, int status, const char *msg)
{
    char q[260];
    rrc_json_quote(msg, q, sizeof q);
    char *b = malloc(300);
    if (b) snprintf(b, 300, "{\"ok\":false,\"error\":%s}", q);
    set(r, status, b, -1);
}

static void ok(api_resp_t *r) { set(r, 200, strdup("{\"ok\":true}"), -1); }

void api_resp_free(api_resp_t *r)
{
    free(r->json);
    r->json = NULL;
    r->len = 0;
}

const char *api_status_text(int status)
{
    switch (status) {
    case 200: return "OK";
    case 400: return "Bad Request";
    case 401: return "Unauthorized";
    case 404: return "Not Found";
    case 409: return "Conflict";
    case 413: return "Payload Too Large";
    default: return "Internal Server Error";
    }
}

bool api_password_ok(const char *password)
{
    const app_config_t *c = app_config_get();
    if (!c->admin_auth) return true;
    char pw[APP_SECRET_MAX + 1];
    if (app_config_get_admin_password(pw, sizeof pw) != ESP_OK) return false; /* set but unreadable: fail closed */
    if (!pw[0]) return true; /* auth enabled without a password: nothing to check against */
    return password && strcmp(password, pw) == 0;
}

/* ---- routes ---------------------------------------------------------------- */
static void r_status(api_resp_t *out)
{
    char *buf = malloc(6144);
    char *sync_json = malloc(1800);
    char coproc_json[640], ble_json[400], power_json[96];
    if (!buf || !sync_json) { free(buf); free(sync_json); set(out, 500, NULL, 0); return; }
    sync_status_json(sync_json, 1800);
    if (coproc_status_json(coproc_json, sizeof coproc_json) < 0) strcpy(coproc_json, "{}");
    if (ble_status_json(ble_json, sizeof ble_json) < 0) strcpy(ble_json, "{}");
    int pn = power_status_json(power_json, sizeof power_json);
    if (pn < 0 || pn >= (int)sizeof power_json) strcpy(power_json, "{}");
    char netd[64];
    net_describe(netd, sizeof netd);
    size_t total = 0, used = 0;
    store_usage(&total, &used);
    const esp_app_desc_t *app = esp_app_get_description();
    char q_net[80], q_ver[48], q_host[48], q_dev[48], q_time[24], q_ssid[200];
    rrc_json_quote(netd, q_net, sizeof q_net);
    rrc_json_quote(app->version, q_ver, sizeof q_ver);
    rrc_json_quote(app_config_get()->hostname, q_host, sizeof q_host);
    rrc_json_quote(app_device_id(), q_dev, sizeof q_dev);
    rrc_json_quote(app_config_get()->wifi_ssid, q_ssid, sizeof q_ssid);
    char iso[21] = "";
    if (net_time_synced()) rrc_format_iso8601((int64_t)time(NULL), iso);
    rrc_json_quote(iso, q_time, sizeof q_time);
    int n = snprintf(buf, 6144,
        "{\"sync\":%s,\"network\":%s,\"eth_link\":%s,\"wifi\":%s,\"time_synced\":%s,\"time\":%s,\"version\":%s,\"hostname\":%s,\"device_id\":%s,"
        "\"storage_total\":%u,\"storage_used\":%u,\"heap_free\":%u,\"psram_free\":%u,\"uptime_s\":%lld,\"usb_devices\":%d,\"usb_faults\":%u,\"usb_chan_irqs\":%u,\"eth_rx_frames\":%u,\"eth_rx_restarts\":%u,\"coproc\":%s,\"ble\":%s,"
        "\"board\":\"%s\",\"has_ethernet\":%s,\"wifi_enabled\":%s,\"wifi_ssid\":%s,\"wifi_rssi\":%d,\"power\":%s}",
        sync_json, q_net, net_eth_link() ? "true" : "false", net_wifi_connected() ? "true" : "false", net_time_synced() ? "true" : "false", q_time, q_ver, q_host, q_dev,
        (unsigned)total, (unsigned)used, (unsigned)heap_caps_get_free_size(MALLOC_CAP_INTERNAL), (unsigned)heap_caps_get_free_size(MALLOC_CAP_SPIRAM),
        (long long)(esp_timer_get_time() / 1000000), usb_diag_device_count(), (unsigned)usb_fault_count(), (unsigned)usb_fault_decode_calls(),
        (unsigned)net_eth_rx_frames(), (unsigned)net_eth_rx_restarts(), coproc_json, ble_json,
        BOARD_NAME, net_has_ethernet() ? "true" : "false", app_config_get()->wifi_enabled ? "true" : "false", q_ssid, net_wifi_rssi(), power_json);
    free(sync_json);
    if (n < 0 || n >= 6144) { free(buf); err(out, 500, "status too large"); return; }
    set(out, 200, buf, n);
}

static void r_config_get(api_resp_t *out)
{
    char *buf = malloc(4096);
    if (!buf) { set(out, 500, NULL, 0); return; }
    int n = app_config_to_json(buf, 4096);
    if (n < 0) { free(buf); err(out, 500, "encode"); return; }
    set(out, 200, buf, n);
}

static void r_config_post(const char *body, size_t len, api_resp_t *out)
{
    if (!body || !len) { err(out, 400, "missing body"); return; }
    char e[96];
    esp_err_t r = app_config_apply_json(body, len, e, sizeof e);
    if (r != ESP_OK) { err(out, 400, e[0] ? e : "invalid config"); return; }
    r = app_config_save(app_config_get());
    if (r != ESP_OK) { err(out, 500, "could not persist config"); return; }
    sync_config_changed();
    net_wifi_reconfigure();
    ble_reconfigure();
    usb_diag_set_verbose(app_config_get()->usb_debug);
    log_ring_printf("configuration saved");
    ok(out);
}

static char *json_string_field(const char *body, size_t len, const char *key, char *dst, size_t cap)
{
    dst[0] = 0;
    if (!body || !len) return dst;
    cJSON *o = cJSON_ParseWithLength(body, len);
    if (!o) return dst;
    cJSON *v = cJSON_GetObjectItemCaseSensitive(o, key);
    if (cJSON_IsString(v)) scpy(dst, cap, v->valuestring);
    cJSON_Delete(o);
    return dst;
}

static void r_pair_begin(const char *body, size_t len, api_resp_t *out)
{
    char url[CFG_STR];
    json_string_field(body, len, "url", url, sizeof url);
    if (!url[0]) { err(out, 400, "url required"); return; }
    esp_err_t e = pairing_begin(url);
    if (e == ESP_ERR_INVALID_STATE) { err(out, 409, "pairing already in progress"); return; }
    if (e != ESP_OK) { err(out, 500, esp_err_to_name(e)); return; }
    ok(out);
}

static void r_pair_status(api_resp_t *out)
{
    char *buf = malloc(1024);
    if (!buf) { set(out, 500, NULL, 0); return; }
    int n = pairing_status_json(buf, 1024);
    if (n < 0) { free(buf); err(out, 500, "encode"); return; }
    set(out, 200, buf, n);
}

static void r_log(api_resp_t *out)
{
    char *buf = malloc(24576);
    if (!buf) { set(out, 500, NULL, 0); return; }
    int n = log_ring_to_json(buf, 24576);
    if (n < 0) { free(buf); err(out, 500, "encode"); return; }
    set(out, 200, buf, n);
}

static void r_json(api_resp_t *out, size_t cap, int (*fn)(char *, size_t))
{
    char *buf = malloc(cap);
    if (!buf) { set(out, 500, NULL, 0); return; }
    int n = fn(buf, cap);
    if (n < 0 || (size_t)n >= cap) { free(buf); err(out, 500, "encode"); return; }
    set(out, 200, buf, n);
}

static void r_wifi_scan(api_resp_t *out)
{
    esp_err_t e = net_wifi_scan_start();
    if (e == ESP_ERR_NOT_SUPPORTED) { err(out, 409, "Wi-Fi is turned off"); return; }
    if (e == ESP_ERR_INVALID_STATE) { err(out, 409, "a scan is already running"); return; }
    if (e != ESP_OK) { err(out, 500, esp_err_to_name(e)); return; }
    ok(out);
}

static void restart_cb(void *arg) { (void)arg; esp_restart(); }

static void r_reboot(api_resp_t *out)
{
    log_ring_printf("reboot requested");
    const esp_timer_create_args_t ta = {.callback = restart_cb, .name = "reboot"};
    esp_timer_handle_t t;
    if (esp_timer_create(&ta, &t) == ESP_OK) esp_timer_start_once(t, 400 * 1000); /* after the reply left */
    ok(out);
}

static void r_coproc_update(const char *body, size_t len, api_resp_t *out)
{
    char url[256], sha[80];
    json_string_field(body, len, "url", url, sizeof url);
    json_string_field(body, len, "sha256", sha, sizeof sha);
    esp_err_t e = coproc_update_start(url, sha);
    if (e == ESP_ERR_INVALID_STATE) { err(out, 409, "a co-processor update is already running"); return; }
    if (e != ESP_OK) { err(out, 400, "need an http(s) url and an optional 64-hex sha256"); return; }
    log_ring_printf("co-processor update requested");
    ok(out);
}

/* ---- firmware update (main/ota.c) ---------------------------------------------- */
static void ota_err(api_resp_t *out, esp_err_t e)
{
    switch (e) {
    case ESP_ERR_INVALID_STATE: err(out, 409, "a firmware update is in progress, or a cancelled one is still stopping; try again shortly"); break;
    case ESP_ERR_NOT_ALLOWED: err(out, 409, "a sync is running; update when it is done"); break;
    case ESP_ERR_NOT_FOUND: err(out, 409, "no update known for this channel yet: check for one first"); break;
    case ESP_ERR_NOT_FINISHED: err(out, 409, "the running firmware is still on trial; install again once it is kept (about a minute after it started)"); break;
    case ESP_ERR_NO_MEM: err(out, 500, "not enough free memory for the image (restart the dock and try again)"); break;
    case ESP_ERR_INVALID_ARG: err(out, 400, "invalid request"); break;
    default: err(out, 500, esp_err_to_name(e)); break;
    }
}

static void r_ota_install(const char *body, size_t len, api_resp_t *out)
{
    char url[300], sha[80];
    json_string_field(body, len, "url", url, sizeof url);
    json_string_field(body, len, "sha256", sha, sizeof sha);
    /* json_string_field truncates: a URL that filled the buffer may have lost its end */
    if (strlen(url) >= sizeof url - 1 || strlen(sha) >= sizeof sha - 1) { err(out, 400, "url or sha256 too long"); return; }
    esp_err_t e = url[0] ? ota_install_url(url, sha) : ota_install_available();
    if (e == ESP_ERR_INVALID_ARG) { err(out, 400, "need an http(s) url and an optional 64-hex sha256"); return; }
    if (e != ESP_OK) { ota_err(out, e); return; }
    ok(out);
}

static void r_ota_begin(const char *body, size_t len, api_resp_t *out)
{
    cJSON *o = body && len ? cJSON_ParseWithLength(body, len) : NULL;
    const cJSON *size = cJSON_GetObjectItemCaseSensitive(o, "size");
    const char *sha = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(o, "sha256"));
    /* compared as a double before the cast: a JSON number can be anything */
    bool size_ok = cJSON_IsNumber(size) && size->valuedouble >= 1 && size->valuedouble <= 64.0 * 1024 * 1024;
    char shab[65] = "";
    if (sha && strlen(sha) < sizeof shab) scpy(shab, sizeof shab, sha);
    esp_err_t e = !size_ok || (sha && strlen(sha) >= sizeof shab) ? ESP_ERR_INVALID_ARG : ota_upload_begin((size_t)size->valuedouble, shab);
    cJSON_Delete(o);
    if (e == ESP_ERR_INVALID_ARG) { err(out, 400, "need the image size (it must fit an app slot) and an optional 64-hex sha256"); return; }
    if (e != ESP_OK) { ota_err(out, e); return; }
    char *b = malloc(64);
    if (b) snprintf(b, 64, "{\"ok\":true,\"chunk\":%d}", OTA_UPLOAD_CHUNK);
    set(out, 200, b, -1);
}

static void r_ota_chunk(const char *body, size_t len, api_resp_t *out)
{
    cJSON *o = body && len ? cJSON_ParseWithLength(body, len) : NULL;
    const cJSON *off = cJSON_GetObjectItemCaseSensitive(o, "off");
    const char *data = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(o, "data"));
    uint8_t *raw = NULL;
    size_t raw_len = 0, expected = 0;
    bool out_of_order = false;
    esp_err_t e = ESP_ERR_INVALID_ARG;
    if (cJSON_IsNumber(off) && off->valuedouble >= 0 && off->valuedouble <= 64.0 * 1024 * 1024 && data && strlen(data) <= (OTA_UPLOAD_CHUNK + 2) / 3 * 4) {
        raw = malloc(OTA_UPLOAD_CHUNK);
        if (!raw) e = ESP_ERR_NO_MEM;
        else if (mbedtls_base64_decode(raw, OTA_UPLOAD_CHUNK, &raw_len, (const unsigned char *)data, strlen(data)) == 0 && raw_len) {
            e = ota_upload_chunk((size_t)off->valuedouble, raw, raw_len, &expected);
            out_of_order = e == ESP_ERR_INVALID_ARG;   /* well-formed, at the wrong offset */
        }
    }
    free(raw);
    cJSON_Delete(o);
    if (out_of_order) {
        /* out of order: tell the client where to resume */
        char *b = malloc(96);
        if (b) snprintf(b, 96, "{\"ok\":false,\"error\":\"out of order\",\"expected\":%u}", (unsigned)expected);
        set(out, 409, b, -1);
        return;
    }
    if (e == ESP_ERR_INVALID_STATE) { err(out, 409, "no upload in progress (begin one first)"); return; }
    if (e == ESP_ERR_INVALID_ARG) { err(out, 400, "need \"off\" and up to 4096 bytes of base64 \"data\""); return; }
    if (e != ESP_OK) { ota_err(out, e); return; }
    char *b = malloc(48);
    if (b) snprintf(b, 48, "{\"ok\":true,\"expected\":%u}", (unsigned)expected);
    set(out, 200, b, -1);
}

static void r_ota_finish(api_resp_t *out)
{
    esp_err_t e = ota_upload_finish();
    if (e == ESP_ERR_INVALID_SIZE) { err(out, 409, "the image is not complete yet"); return; }
    if (e == ESP_ERR_INVALID_STATE) { err(out, 409, "no upload in progress"); return; }
    if (e != ESP_OK) { ota_err(out, e); return; }
    ok(out);   /* checking and flashing run on; progress in GET /api/ota */
}

void api_dispatch(const char *method, const char *path, const char *body, size_t body_len, api_resp_t *out)
{
    memset(out, 0, sizeof *out);
    bool get = method && !strcmp(method, "GET"), post = method && !strcmp(method, "POST");
    if (!path) path = "";
    /* upload chunks arrive by the hundred; one log line per chunk would flood the console */
    if (strcmp(path, "/api/ota/upload/chunk")) ESP_LOGI(TAG, "%s %s", method ? method : "?", path);
    if (get && !strcmp(path, "/api/status")) r_status(out);
    else if (get && !strcmp(path, "/api/config")) r_config_get(out);
    else if (post && !strcmp(path, "/api/config")) r_config_post(body, body_len, out);
    else if (post && !strcmp(path, "/api/sync/now")) { sync_request_now(); ok(out); }
    else if (post && !strcmp(path, "/api/sync/cancel")) { sync_cancel(); ok(out); }
    else if (post && !strcmp(path, "/api/usb/reset")) {
        log_ring_printf("usb: power-cycling the root port on request");
        if (usb_diag_power_cycle() == ESP_OK) ok(out); else err(out, 500, "root port power cycle failed");
    }
    else if (post && !strcmp(path, "/api/pair/begin")) r_pair_begin(body, body_len, out);
    else if (get && !strcmp(path, "/api/pair/status")) r_pair_status(out);
    else if (post && !strcmp(path, "/api/pair/cancel")) { pairing_cancel(); ok(out); }
    else if (get && !strcmp(path, "/api/log")) r_log(out);
    else if (post && !strcmp(path, "/api/reboot")) r_reboot(out);
    else if (post && !strcmp(path, "/api/coproc/update")) r_coproc_update(body, body_len, out);
    else if (post && !strcmp(path, "/api/ble/forget")) { ble_forget_bonds(); ok(out); }
    else if (get && !strcmp(path, "/api/wifi")) r_json(out, 1024, net_wifi_json);
    else if (post && !strcmp(path, "/api/wifi/scan")) r_wifi_scan(out);
    /* worst case: NET_SCAN_MAX SSIDs of 32 bytes, each escaped up to \u00XX (6x) */
    else if (get && !strcmp(path, "/api/wifi/scan")) r_json(out, 10240, net_wifi_scan_json);
    else if (get && !strcmp(path, "/api/ota")) r_json(out, 1024, ota_status_json);
    else if (post && !strcmp(path, "/api/ota/check")) { esp_err_t e = ota_check(); if (e == ESP_OK) ok(out); else ota_err(out, e); }
    else if (post && !strcmp(path, "/api/ota/install")) r_ota_install(body, body_len, out);
    else if (post && !strcmp(path, "/api/ota/upload/begin")) r_ota_begin(body, body_len, out);
    else if (post && !strcmp(path, "/api/ota/upload/chunk")) r_ota_chunk(body, body_len, out);
    else if (post && !strcmp(path, "/api/ota/upload/finish")) r_ota_finish(out);
    else if (post && !strcmp(path, "/api/ota/cancel")) { if (ota_cancel() == ESP_OK) ok(out); else err(out, 409, "the image is being written; it cannot be stopped now"); }
    else err(out, 404, "unknown endpoint");
    if (!out->json) set(out, 500, NULL, 0);
}

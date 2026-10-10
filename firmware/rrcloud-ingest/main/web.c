/*
 * web — the admin UI (embedded single page) and its JSON API on port 80.
 *
 *   GET  /                      page
 *   GET  /api/status            sync/network/camera/storage status
 *   GET  /api/config            config (secrets elided; has_* flags)
 *   POST /api/config            JSON patch (may include s3_secret_key / wifi_password / admin_password)
 *   POST /api/sync/now          start a sync of the attached camera
 *   POST /api/sync/cancel
 *   POST /api/pair/begin        {"url": "rrc.example.com"}
 *   GET  /api/pair/status
 *   POST /api/pair/cancel
 *   GET  /api/log
 *   POST /api/reboot
 *
 * Optional HTTP Basic auth (user "admin") when admin_auth is enabled and a
 * password is set. The UI is meant for the LAN; put it behind your own TLS
 * reverse proxy if you expose it further.
 */
#include "web.h"
#include <string.h>
#include <strings.h>
#include <stdio.h>
#include <stdlib.h>
#include "esp_log.h"
#include "esp_http_server.h"
#include "esp_system.h"
#include "esp_app_desc.h"
#include "esp_timer.h"
#include "esp_heap_caps.h"
#include "cJSON.h"
#include "rrc_hash.h"
#include "rrc_proto.h"
#include "app_config.h"
#include "sync.h"
#include "pairing.h"
#include "net.h"
#include "store.h"
#include "log_ring.h"
#include "usb_diag.h"
#include "lwip/sockets.h"

static const char *TAG = "web";
extern const char index_html_start[] asm("_binary_index_html_start");
extern const char index_html_end[] asm("_binary_index_html_end");

static bool authorized(httpd_req_t *req)
{
    const app_config_t *c = app_config_get();
    if (!c->admin_auth) return true;
    char pw[APP_SECRET_MAX + 1];
    if (app_config_get_admin_password(pw, sizeof pw) != ESP_OK) return false; /* set but unreadable: fail closed */
    if (!pw[0]) return true; /* auth enabled without a password: nothing to check against */
    char hdr[256];
    if (httpd_req_get_hdr_value_str(req, "Authorization", hdr, sizeof hdr) != ESP_OK) return false;
    if (strncmp(hdr, "Basic ", 6)) return false;
    char cred[APP_SECRET_MAX + 8];
    snprintf(cred, sizeof cred, "admin:%s", pw);
    char want[(APP_SECRET_MAX + 8) * 4 / 3 + 8];
    rrc_base64_encode((const uint8_t *)cred, strlen(cred), want);
    return strcmp(hdr + 6, want) == 0;
}

static esp_err_t deny(httpd_req_t *req)
{
    httpd_resp_set_status(req, "401 Unauthorized");
    httpd_resp_set_hdr(req, "WWW-Authenticate", "Basic realm=\"rrcloud-ingest\"");
    httpd_resp_set_type(req, "text/plain");
    return httpd_resp_send(req, "auth required", HTTPD_RESP_USE_STRLEN);
}

/* CSRF gate: every state-changing endpoint takes a JSON body, and an HTML form can
 * only send urlencoded/multipart/text bodies without a CORS preflight (which this
 * server never approves). Requiring application/json therefore closes the cross-site
 * form-POST vector without Origin/Host heuristics (proxies, DNS rebinding, long
 * headers). curl/scripts set the header anyway. */
static bool json_request(httpd_req_t *req)
{
    char ct[96];
    if (httpd_req_get_hdr_value_str(req, "Content-Type", ct, sizeof ct) != ESP_OK) return false;
    return strncasecmp(ct, "application/json", 16) == 0;
}

static esp_err_t forbid(httpd_req_t *req)
{
    httpd_resp_set_status(req, "415 Unsupported Media Type");
    httpd_resp_set_type(req, "text/plain");
    return httpd_resp_send(req, "state-changing requests must be application/json", HTTPD_RESP_USE_STRLEN);
}

#define AUTH() do { ESP_LOGI(TAG, "http: %s %s", http_method_str(req->method), req->uri); if (!authorized(req)) return deny(req); if (req->method != HTTP_GET && !json_request(req)) return forbid(req); } while (0)

static esp_err_t send_json(httpd_req_t *req, const char *json, int len)
{
    httpd_resp_set_type(req, "application/json");
    httpd_resp_set_hdr(req, "Cache-Control", "no-store");
    return httpd_resp_send(req, json, len < 0 ? HTTPD_RESP_USE_STRLEN : len);
}

static esp_err_t send_ok(httpd_req_t *req) { return send_json(req, "{\"ok\":true}", -1); }

static esp_err_t send_err(httpd_req_t *req, int code, const char *msg)
{
    char buf[300];
    char q[260];
    rrc_json_quote(msg, q, sizeof q);
    snprintf(buf, sizeof buf, "{\"ok\":false,\"error\":%s}", q);
    httpd_resp_set_status(req, code == 400 ? "400 Bad Request" : code == 409 ? "409 Conflict" : "500 Internal Server Error");
    return send_json(req, buf, -1);
}

static char *read_body(httpd_req_t *req, size_t max)
{
    if (req->content_len == 0 || req->content_len > max) return NULL;
    char *buf = malloc(req->content_len + 1);
    if (!buf) return NULL;
    size_t got = 0;
    while (got < req->content_len) {
        int r = httpd_req_recv(req, buf + got, req->content_len - got);
        if (r <= 0) { free(buf); return NULL; }
        got += (size_t)r;
    }
    buf[got] = 0;
    return buf;
}

static esp_err_t h_index(httpd_req_t *req)
{
    AUTH();
    httpd_resp_set_type(req, "text/html; charset=utf-8");
    return httpd_resp_send(req, index_html_start, index_html_end - index_html_start - 1);
}

static esp_err_t h_status(httpd_req_t *req)
{
    AUTH();
    char *buf = malloc(3072);
    if (!buf) return send_err(req, 500, "oom");
    char sync_json[1800];
    sync_status_json(sync_json, sizeof sync_json);
    char netd[64];
    net_describe(netd, sizeof netd);
    size_t total = 0, used = 0;
    store_usage(&total, &used);
    const esp_app_desc_t *app = esp_app_get_description();
    char q_net[80], q_ver[48], q_host[48], q_dev[48], q_time[24];
    rrc_json_quote(netd, q_net, sizeof q_net);
    rrc_json_quote(app->version, q_ver, sizeof q_ver);
    rrc_json_quote(app_config_get()->hostname, q_host, sizeof q_host);
    rrc_json_quote(app_device_id(), q_dev, sizeof q_dev);
    char iso[21] = "";
    if (net_time_synced()) rrc_format_iso8601((int64_t)time(NULL), iso);
    rrc_json_quote(iso, q_time, sizeof q_time);
    int n = snprintf(buf, 3072,
        "{\"sync\":%s,\"network\":%s,\"eth_link\":%s,\"wifi\":%s,\"time_synced\":%s,\"time\":%s,\"version\":%s,\"hostname\":%s,\"device_id\":%s,"
        "\"storage_total\":%u,\"storage_used\":%u,\"heap_free\":%u,\"psram_free\":%u,\"uptime_s\":%lld,\"usb_devices\":%d}",
        sync_json, q_net, net_eth_link() ? "true" : "false", net_wifi_connected() ? "true" : "false", net_time_synced() ? "true" : "false", q_time, q_ver, q_host, q_dev,
        (unsigned)total, (unsigned)used, (unsigned)heap_caps_get_free_size(MALLOC_CAP_INTERNAL), (unsigned)heap_caps_get_free_size(MALLOC_CAP_SPIRAM),
        (long long)(esp_timer_get_time() / 1000000), usb_diag_device_count());
    esp_err_t e = send_json(req, buf, n);
    free(buf);
    return e;
}

static esp_err_t h_config_get(httpd_req_t *req)
{
    AUTH();
    char *buf = malloc(4096);
    if (!buf) return send_err(req, 500, "oom");
    int n = app_config_to_json(buf, 4096);
    esp_err_t e = n < 0 ? send_err(req, 500, "encode") : send_json(req, buf, n);
    free(buf);
    return e;
}

static esp_err_t h_config_post(httpd_req_t *req)
{
    AUTH();
    char *body = read_body(req, 8192);
    if (!body) return send_err(req, 400, "missing or oversized body");
    char err[96];
    esp_err_t e = app_config_apply_json(body, strlen(body), err, sizeof err);
    free(body);
    if (e != ESP_OK) return send_err(req, 400, err[0] ? err : "invalid config");
    e = app_config_save(app_config_get());
    if (e != ESP_OK) return send_err(req, 500, "could not persist config");
    sync_config_changed();
    net_wifi_reconfigure();
    usb_diag_set_verbose(app_config_get()->usb_debug);
    log_ring_printf("configuration saved");
    return send_ok(req);
}

static esp_err_t h_sync_now(httpd_req_t *req) { AUTH(); sync_request_now(); return send_ok(req); }
static esp_err_t h_sync_cancel(httpd_req_t *req) { AUTH(); sync_cancel(); return send_ok(req); }
static esp_err_t h_usb_reset(httpd_req_t *req)
{
    AUTH();
    log_ring_printf("usb: power-cycling the root port on request");
    return usb_diag_power_cycle() == ESP_OK ? send_ok(req) : send_err(req, 500, "root port power cycle failed");
}

static esp_err_t h_pair_begin(httpd_req_t *req)
{
    AUTH();
    char *body = read_body(req, 1024);
    if (!body) return send_err(req, 400, "missing body");
    cJSON *o = cJSON_Parse(body);
    free(body);
    cJSON *u = o ? cJSON_GetObjectItemCaseSensitive(o, "url") : NULL;
    if (!cJSON_IsString(u) || !u->valuestring[0]) { if (o) cJSON_Delete(o); return send_err(req, 400, "url required"); }
    esp_err_t e = pairing_begin(u->valuestring);
    cJSON_Delete(o);
    if (e == ESP_ERR_INVALID_STATE) return send_err(req, 409, "pairing already in progress");
    if (e != ESP_OK) return send_err(req, 500, esp_err_to_name(e));
    return send_ok(req);
}

static esp_err_t h_pair_status(httpd_req_t *req)
{
    AUTH();
    char buf[1024];
    int n = pairing_status_json(buf, sizeof buf);
    return send_json(req, buf, n);
}

static esp_err_t h_pair_cancel(httpd_req_t *req) { AUTH(); pairing_cancel(); return send_ok(req); }

static esp_err_t h_log(httpd_req_t *req)
{
    AUTH();
    char *buf = malloc(24576);
    if (!buf) return send_err(req, 500, "oom");
    int n = log_ring_to_json(buf, 24576);
    esp_err_t e = n < 0 ? send_err(req, 500, "encode") : send_json(req, buf, n);
    free(buf);
    return e;
}

static esp_err_t h_reboot(httpd_req_t *req)
{
    AUTH();
    send_ok(req);
    log_ring_printf("reboot requested from UI");
    vTaskDelay(pdMS_TO_TICKS(300));
    esp_restart();
    return ESP_OK;
}

/* Every accepted TCP connection is logged with its peer address, so the serial
 * log answers "did the browser's packets reach the dock at all?" — the
 * question a silent timeout leaves open (e.g. a stateless inter-VLAN ACL
 * that drops the SYN-ACK on the way back). */
static esp_err_t on_open(httpd_handle_t hd, int sockfd)
{
    (void)hd;
    struct sockaddr_storage peer;
    socklen_t len = sizeof peer;
    char ip[INET6_ADDRSTRLEN] = "?";
    if (getpeername(sockfd, (struct sockaddr *)&peer, &len) == 0) {
        if (peer.ss_family == AF_INET) inet_ntop(AF_INET, &((struct sockaddr_in *)&peer)->sin_addr, ip, sizeof ip);
        else if (peer.ss_family == AF_INET6) inet_ntop(AF_INET6, &((struct sockaddr_in6 *)&peer)->sin6_addr, ip, sizeof ip);
    }
    ESP_LOGI(TAG, "http: connection from %s", ip);
    return ESP_OK;
}

esp_err_t web_start(void)
{
    httpd_config_t cfg = HTTPD_DEFAULT_CONFIG();
    cfg.server_port = 80;
    cfg.open_fn = on_open;
    cfg.max_uri_handlers = 16;
    cfg.stack_size = 8192;
    cfg.lru_purge_enable = true;
    cfg.max_open_sockets = 6;
    httpd_handle_t s = NULL;
    esp_err_t e = httpd_start(&s, &cfg);
    if (e != ESP_OK) { ESP_LOGE(TAG, "httpd_start: %s", esp_err_to_name(e)); return e; }
    const httpd_uri_t routes[] = {
        {.uri = "/", .method = HTTP_GET, .handler = h_index},
        {.uri = "/api/status", .method = HTTP_GET, .handler = h_status},
        {.uri = "/api/config", .method = HTTP_GET, .handler = h_config_get},
        {.uri = "/api/config", .method = HTTP_POST, .handler = h_config_post},
        {.uri = "/api/sync/now", .method = HTTP_POST, .handler = h_sync_now},
        {.uri = "/api/sync/cancel", .method = HTTP_POST, .handler = h_sync_cancel},
        {.uri = "/api/usb/reset", .method = HTTP_POST, .handler = h_usb_reset},
        {.uri = "/api/pair/begin", .method = HTTP_POST, .handler = h_pair_begin},
        {.uri = "/api/pair/status", .method = HTTP_GET, .handler = h_pair_status},
        {.uri = "/api/pair/cancel", .method = HTTP_POST, .handler = h_pair_cancel},
        {.uri = "/api/log", .method = HTTP_GET, .handler = h_log},
        {.uri = "/api/reboot", .method = HTTP_POST, .handler = h_reboot},
    };
    for (size_t i = 0; i < sizeof routes / sizeof routes[0]; i++) httpd_register_uri_handler(s, &routes[i]);
    log_ring_printf("web ui listening on port 80");
    return ESP_OK;
}

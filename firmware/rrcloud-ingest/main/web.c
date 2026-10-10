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
#include "coproc.h"
#include "api.h"
#include "util.h"
#include "lwip/sockets.h"
#include "lwip/stats.h"

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

static const char *status_line(int code)
{
    switch (code) {
    case 200: return "200 OK";
    case 400: return "400 Bad Request";
    case 401: return "401 Unauthorized";
    case 404: return "404 Not Found";
    case 409: return "409 Conflict";
    case 413: return "413 Payload Too Large";
    default: return "500 Internal Server Error";
    }
}

/* Every /api route: auth + CSRF gate here, the operation in api.c. */
static esp_err_t h_api(httpd_req_t *req)
{
    AUTH();
    char *body = NULL;
    if (req->method == HTTP_POST && req->content_len) {
        if (req->content_len > 8192) { httpd_resp_set_status(req, status_line(413)); return send_json(req, "{\"ok\":false,\"error\":\"body too large\"}", -1); }
        body = read_body(req, 8192);
        if (!body) { httpd_resp_set_status(req, status_line(400)); return send_json(req, "{\"ok\":false,\"error\":\"could not read body\"}", -1); }
    }
    api_resp_t r;
    api_dispatch(req->method == HTTP_POST ? "POST" : "GET", req->uri, body, body ? strlen(body) : 0, &r);
    free(body);
    httpd_resp_set_status(req, status_line(r.status));
    esp_err_t e = send_json(req, r.json ? r.json : "{\"ok\":false,\"error\":\"oom\"}", r.json ? (int)r.len : -1);
    api_resp_free(&r);
    return e;
}

static volatile unsigned external_connections;

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
    bool loopback = !strcmp(ip, "127.0.0.1") || !strcmp(ip, "::1") || !strcmp(ip, "::ffff:127.0.0.1");
    if (!loopback) external_connections++;
    ESP_LOGI(TAG, "http: connection from %s", ip);
    return ESP_OK;
}

/* Reachability self-test: once the server is up, open a TCP connection to it
 * over loopback and fetch /api/status. A pass proves listen/accept/handler
 * work end to end, so a browser timeout with a passing self-test is a network
 * path problem, not a server one. While no external client has connected the
 * lwIP packet counters are logged every minute: if `tcp rx` never moves after
 * the self-test, no SYN is reaching the dock. */
static void selftest_task(void *arg)
{
    (void)arg;
    vTaskDelay(pdMS_TO_TICKS(8000));
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    bool ok = false;
    if (fd >= 0) {
        struct sockaddr_in a = {.sin_family = AF_INET, .sin_port = htons(80)};
        a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        struct timeval tv = {.tv_sec = 5};
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
        if (connect(fd, (struct sockaddr *)&a, sizeof a) == 0) {
            const char req[] = "GET /api/status HTTP/1.0\r\nHost: 127.0.0.1\r\n\r\n";
            if (send(fd, req, sizeof req - 1, 0) == (int)(sizeof req - 1)) {
                char line[64] = "";
                int n = recv(fd, line, sizeof line - 1, 0);
                if (n > 0) { line[n] = 0; char *nl = strpbrk(line, "\r\n"); if (nl) *nl = 0; ok = strstr(line, " 200") != NULL || strstr(line, " 401") != NULL; log_ring_printf("web self-test over loopback: %s", line); }
            }
        }
        close(fd);
    }
    if (!ok) log_ring_printf("web self-test over loopback FAILED: the HTTP server is not accepting connections");
#if LWIP_STATS
    unsigned last_tcp = lwip_stats.tcp.recv;
    for (int minute = 1;; minute++) {
        vTaskDelay(pdMS_TO_TICKS(60000));
        if (external_connections) break;
        unsigned tcp = lwip_stats.tcp.recv;
        log_ring_printf("web: no client has connected yet (%d min); lwip tcp rx=%u (+%u) tx=%u, ip rx=%u, arp rx=%u — if tcp rx does not grow while you try, the packets are not reaching the dock",
                        minute, tcp, tcp - last_tcp, (unsigned)lwip_stats.tcp.xmit, (unsigned)lwip_stats.ip.recv, (unsigned)lwip_stats.etharp.recv);
        last_tcp = tcp;
    }
#endif
    vTaskDelete(NULL);
}

esp_err_t web_start(void)
{
    httpd_config_t cfg = HTTPD_DEFAULT_CONFIG();
    cfg.server_port = 80;
    cfg.open_fn = on_open;
    cfg.max_uri_handlers = 24;
    cfg.stack_size = 8192;
    cfg.lru_purge_enable = true;
    cfg.max_open_sockets = 6;
    httpd_handle_t s = NULL;
    esp_err_t e = httpd_start(&s, &cfg);
    if (e != ESP_OK) { ESP_LOGE(TAG, "httpd_start: %s", esp_err_to_name(e)); return e; }
    const httpd_uri_t routes[] = {
        {.uri = "/", .method = HTTP_GET, .handler = h_index},
        {.uri = "/api/status", .method = HTTP_GET, .handler = h_api},
        {.uri = "/api/config", .method = HTTP_GET, .handler = h_api},
        {.uri = "/api/config", .method = HTTP_POST, .handler = h_api},
        {.uri = "/api/sync/now", .method = HTTP_POST, .handler = h_api},
        {.uri = "/api/sync/cancel", .method = HTTP_POST, .handler = h_api},
        {.uri = "/api/usb/reset", .method = HTTP_POST, .handler = h_api},
        {.uri = "/api/pair/begin", .method = HTTP_POST, .handler = h_api},
        {.uri = "/api/pair/status", .method = HTTP_GET, .handler = h_api},
        {.uri = "/api/pair/cancel", .method = HTTP_POST, .handler = h_api},
        {.uri = "/api/log", .method = HTTP_GET, .handler = h_api},
        {.uri = "/api/reboot", .method = HTTP_POST, .handler = h_api},
        {.uri = "/api/coproc/update", .method = HTTP_POST, .handler = h_api},
        {.uri = "/api/ble/forget", .method = HTTP_POST, .handler = h_api},
    };
    for (size_t i = 0; i < sizeof routes / sizeof routes[0]; i++) httpd_register_uri_handler(s, &routes[i]);
    log_ring_printf("web ui listening on port 80");
    xTaskCreate(selftest_task, "web_selftest", 4096, NULL, 2, NULL);
    return ESP_OK;
}

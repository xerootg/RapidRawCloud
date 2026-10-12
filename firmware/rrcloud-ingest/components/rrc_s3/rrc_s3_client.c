#include "rrc_s3_client.h"
#include "rrc_hash.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <ctype.h>
#include "esp_log.h"
#include "esp_tls.h"
#include "lwip/sockets.h"
#include "lwip/netdb.h"
#include "esp_crt_bundle.h"
#include "mbedtls/ssl.h"
#include "mbedtls/net_sockets.h"
#include "esp_timer.h"
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"

static const char *TAG = "rrc_s3";

/* Forward-declared in rrc_proto, but rrc_s3 must not depend on rrc_proto (layering);
 * a tiny local HTTP-date parser is enough here. */
static int64_t parse_http_date(const char *s);

typedef struct {
    rrc_s3_result_t *res;
    rrc_s3_sink_t sink;
    void *sink_ctx;
    uint8_t *buf;
    size_t cap;
    size_t got;
    bool overflow;
    char errbody[1024];
    size_t errlen;
} req_ctx_t;

static void extract_error_code(req_ctx_t *c)
{
    c->errbody[c->errlen] = 0;
    const char *p = strstr(c->errbody, "<Code>");
    if (!p) return;
    p += 6;
    const char *e = strstr(p, "</Code>");
    if (!e) return;
    size_t n = (size_t)(e - p);
    if (n >= sizeof c->res->error_code) n = sizeof c->res->error_code - 1;
    memcpy(c->res->error_code, p, n);
    c->res->error_code[n] = 0;
}

/* Builds "<endpoint><encoded path>[?<canonical query>]" */
static char *build_url(const rrc_s3_t *s3, const char *raw_path, const rrc_kv *query, size_t n_query)
{
    size_t cap = strlen(s3->cfg.endpoint) + strlen(raw_path) * 3 + 4;
    for (size_t i = 0; i < n_query; i++) cap += (strlen(query[i].name) + strlen(query[i].value ? query[i].value : "")) * 3 + 2;
    char *url = malloc(cap);
    if (!url) return NULL;
    strcpy(url, s3->cfg.endpoint);
    size_t pos = strlen(url);
    int n = rrc_sigv4_encode_path(raw_path, url + pos, cap - pos);
    if (n < 0) { free(url); return NULL; }
    pos += (size_t)n;
    if (n_query) {
        url[pos++] = '?';
        n = rrc_sigv4_encode_query(query, n_query, url + pos, cap - pos);
        if (n < 0) { free(url); return NULL; }
    }
    return url;
}

/* ---- persistent connection ------------------------------------------------
 * esp_http_client opens a new TCP (and TLS) connection per request, leaves
 * Nagle on and caps TLS records at 4 KiB: on the P4 that pipeline moved a part
 * at ~0.2 MB/s. This layer keeps one connection per client, sets TCP_NODELAY
 * so the last partial segment of every write is not held back for a delayed
 * ACK, and hands whole 64 KiB slices of the PSRAM part buffer to mbedTLS. */
#define CONN_WRITE_SLICE (8u * 1024u)   /* one TLS record per call: the AES DMA path allocates a record-sized internal bounce buffer, and two uploaders must both find one */

static void conn_close(rrc_s3_t *s3)
{
    if (s3->conn_tls) { esp_tls_conn_destroy((esp_tls_t *)s3->conn_tls); s3->conn_tls = NULL; s3->conn_fd = -1; }
    else if (s3->conn_fd >= 0) { close(s3->conn_fd); s3->conn_fd = -1; }
}

/* mbedTLS I/O callbacks on the blocking socket. Same semantics as mbedTLS'
 * own net_sockets BIO, plus a running total of the time spent inside send()
 * so the speed diagnostics can tell crypto CPU from wire time. */
static int bio_send(void *ctx, const unsigned char *buf, size_t len)
{
    rrc_s3_t *s3 = ctx;
    int64_t t0 = esp_timer_get_time();
    int w = send(s3->conn_fd, buf, len, 0);
    s3->stat_send_us += esp_timer_get_time() - t0;
    if (w < 0) {
        if (errno == EWOULDBLOCK || errno == EAGAIN) return MBEDTLS_ERR_SSL_WANT_WRITE;
        if (errno == EPIPE || errno == ECONNRESET) return MBEDTLS_ERR_NET_CONN_RESET;
        return MBEDTLS_ERR_NET_SEND_FAILED;
    }
    return w;
}
static int bio_recv(void *ctx, unsigned char *buf, size_t len)
{
    rrc_s3_t *s3 = ctx;
    int r = recv(s3->conn_fd, buf, len, 0);
    if (r < 0) {
        if (errno == EWOULDBLOCK || errno == EAGAIN) return MBEDTLS_ERR_SSL_WANT_READ;
        if (errno == EPIPE || errno == ECONNRESET) return MBEDTLS_ERR_NET_CONN_RESET;
        return MBEDTLS_ERR_NET_RECV_FAILED;
    }
    return r;
}

/* Seconds on the monotonic timer; see have_server_offset in the header. */
static int64_t mono_s(void) { return esp_timer_get_time() / 1000000; }

static esp_err_t conn_open(rrc_s3_t *s3)
{
    if (s3->conn_fd >= 0) return ESP_OK;
    int timeout = s3->cfg.timeout_ms > 0 ? s3->cfg.timeout_ms : 30000;
    esp_tls_cfg_t cfg = {.timeout_ms = timeout, .non_block = false};
    if (s3->https) {
        if (s3->cfg.tls_insecure) cfg.skip_common_name = true;
        else cfg.crt_bundle_attach = esp_crt_bundle_attach;
        esp_tls_t *t = esp_tls_init();
        if (!t) return ESP_ERR_NO_MEM;
        if (esp_tls_conn_new_sync(s3->conn_host, (int)strlen(s3->conn_host), s3->conn_port, &cfg, t) != 1) {
            ESP_LOGW(TAG, "tls connect to %s:%d failed", s3->conn_host, s3->conn_port);
            esp_tls_conn_destroy(t);
            return ESP_FAIL;
        }
        int fd = -1;
        esp_tls_get_conn_sockfd(t, &fd);
        s3->conn_tls = t;
        s3->conn_fd = fd;
        mbedtls_ssl_set_bio((mbedtls_ssl_context *)esp_tls_get_ssl_context(t), s3, bio_send, bio_recv, NULL);
    } else {
        /* Plain TCP: resolve + connect ourselves (esp_tls_plain_tcp_connect
         * needs its error handle and failed here). */
        char port_str[8];
        snprintf(port_str, sizeof port_str, "%d", s3->conn_port);
        struct addrinfo hints = {.ai_family = AF_INET, .ai_socktype = SOCK_STREAM}, *ai = NULL;
        if (getaddrinfo(s3->conn_host, port_str, &hints, &ai) != 0 || !ai) { ESP_LOGW(TAG, "cannot resolve %s", s3->conn_host); return ESP_FAIL; }
        int fd = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
        if (fd < 0) { freeaddrinfo(ai); return ESP_FAIL; }
        struct timeval ctv = {.tv_sec = timeout / 1000, .tv_usec = (timeout % 1000) * 1000};
        setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &ctv, sizeof ctv);
        if (connect(fd, ai->ai_addr, ai->ai_addrlen) != 0) {
            ESP_LOGW(TAG, "tcp connect to %s:%d failed (errno %d)", s3->conn_host, s3->conn_port, errno);
            close(fd); freeaddrinfo(ai);
            return ESP_FAIL;
        }
        freeaddrinfo(ai);
        s3->conn_fd = fd;
    }
    int one = 1;
    setsockopt(s3->conn_fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
    setsockopt(s3->conn_fd, SOL_SOCKET, SO_KEEPALIVE, &one, sizeof one);
    struct timeval tv = {.tv_sec = timeout / 1000, .tv_usec = (timeout % 1000) * 1000};
    setsockopt(s3->conn_fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
    setsockopt(s3->conn_fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof tv);
    return ESP_OK;
}

void rrc_s3_disconnect(rrc_s3_t *s3) { conn_close(s3); }

/* Writes everything or returns -1. TLS records are MBEDTLS_SSL_OUT_CONTENT_LEN
 * each; the slice size only bounds one mbedTLS call. */
static int conn_write_all(rrc_s3_t *s3, const void *data, size_t len)
{
    const uint8_t *p = data;
    while (len) {
        size_t n = len > CONN_WRITE_SLICE ? CONN_WRITE_SLICE : len;
        int64_t t0 = esp_timer_get_time();
        ssize_t w = s3->conn_tls ? esp_tls_conn_write((esp_tls_t *)s3->conn_tls, p, n) : send(s3->conn_fd, p, n, 0);
        s3->stat_tx_us += esp_timer_get_time() - t0;
        if (w > 0) s3->stat_tx_bytes += (size_t)w;
        if (w == ESP_TLS_ERR_SSL_WANT_WRITE || w == ESP_TLS_ERR_SSL_WANT_READ || (w < 0 && !s3->conn_tls && errno == EWOULDBLOCK)) { vTaskDelay(1); continue; }
        if (w <= 0) return -1;
        p += w; len -= (size_t)w;
    }
    return 0;
}

/* One read; 0 = connection closed by the peer, -1 = error. */
static int conn_read(rrc_s3_t *s3, void *buf, size_t cap)
{
    int64_t deadline = esp_timer_get_time() + (int64_t)(s3->cfg.timeout_ms > 0 ? s3->cfg.timeout_ms : 30000) * 1000;
    for (;;) {
        ssize_t r = s3->conn_tls ? esp_tls_conn_read((esp_tls_t *)s3->conn_tls, buf, cap) : recv(s3->conn_fd, buf, cap, 0);
        if (r == ESP_TLS_ERR_SSL_WANT_READ || r == ESP_TLS_ERR_SSL_WANT_WRITE || (r < 0 && !s3->conn_tls && errno == EWOULDBLOCK)) {
            if (esp_timer_get_time() > deadline) { ESP_LOGW(TAG, "read timed out"); return -1; }
            vTaskDelay(1); continue;
        }
        return r < 0 ? -1 : (int)r;
    }
}

static void deliver_body(req_ctx_t *ctx, bool ok, const uint8_t *p, size_t n)
{
    if (!n) return;
    if (ok) {
        if (ctx->sink) { if (ctx->sink(ctx->sink_ctx, p, n)) ctx->overflow = true; }
        else if (ctx->buf) {
            if (ctx->got + n > ctx->cap) ctx->overflow = true;
            else { memcpy(ctx->buf + ctx->got, p, n); ctx->got += n; }
        } else ctx->got += n;
    } else {
        size_t room = sizeof ctx->errbody - 1 - ctx->errlen;
        size_t k = n < room ? n : room;
        memcpy(ctx->errbody + ctx->errlen, p, k); ctx->errlen += k;
    }
}

static void take_header(req_ctx_t *ctx, const char *name, const char *value, bool *chunked, bool *close_after, int64_t *clen)
{
    rrc_s3_result_t *res = ctx->res;
    if (!strcasecmp(name, "ETag")) {
        const char *v = value; size_t n = strlen(v);
        if (n >= 2 && v[0] == '"' && v[n - 1] == '"') { v++; n -= 2; }
        if (n >= sizeof res->etag) n = sizeof res->etag - 1;
        memcpy(res->etag, v, n); res->etag[n] = 0;
    } else if (!strcasecmp(name, "Date")) res->server_date = parse_http_date(value);
    else if (!strcasecmp(name, "Content-Length")) { *clen = strtoll(value, NULL, 10); res->content_length = *clen; }
    else if (!strcasecmp(name, "x-amz-meta-rrc-device")) snprintf(res->meta_device, sizeof res->meta_device, "%s", value);
    else if (!strcasecmp(name, "Transfer-Encoding") && strcasestr(value, "chunked")) *chunked = true;
    else if (!strcasecmp(name, "Connection") && strcasestr(value, "close")) *close_after = true;
}

/* Reads status line + headers + body off the persistent connection. Returns 0
 * when the response was consumed completely (so the connection may be reused),
 * -1 on a transport error. *got_any tells the caller whether any response byte
 * arrived, which decides whether a stale keep-alive retry is legitimate. */
static int read_response(rrc_s3_t *s3, req_ctx_t *ctx, bool is_head, bool *got_any)
{
    enum { HDR_CAP = 4096 };
    char *hdr = malloc(HDR_CAP);
    if (!hdr) return -1;
    size_t hlen = 0; char *body_start = NULL;
    while (!body_start) {
        if (hlen >= HDR_CAP - 1) { free(hdr); return -1; }
        int r = conn_read(s3, hdr + hlen, HDR_CAP - 1 - hlen);
        if (r <= 0) { free(hdr); return -1; }
        *got_any = true;
        hlen += (size_t)r; hdr[hlen] = 0;
        body_start = strstr(hdr, "\r\n\r\n");
    }
    *body_start = 0;
    size_t leftover_off = (size_t)(body_start + 4 - hdr), leftover = hlen - leftover_off;
    /* status line */
    int status = 0;
    if (sscanf(hdr, "HTTP/%*d.%*d %d", &status) != 1) { free(hdr); return -1; }
    ctx->res->status = status;
    bool chunked = false, close_after = false; int64_t clen = -1;
    char *line = strchr(hdr, '\n');
    if (line) line++;
    while (line && *line) {
        char *eol = strchr(line, '\n');
        char *next = eol ? eol + 1 : NULL;
        if (eol) *eol = 0;
        char *cr = strchr(line, '\r'); if (cr) *cr = 0;
        char *colon = strchr(line, ':');
        if (colon) { *colon = 0; char *v = colon + 1; while (*v == ' ' || *v == '\t') v++; take_header(ctx, line, v, &chunked, &close_after, &clen); }
        line = next;
    }
    bool ok = status >= 200 && status < 300;
    int rc = 0;
    if (!is_head && status != 204 && status != 304) {
        uint8_t *buf = malloc(16384);
        if (!buf) { free(hdr); return -1; }
        if (chunked) {
            /* chunk-size CRLF data CRLF ... 0 CRLF [trailers] CRLF — fed from the
             * leftover bytes first, then the wire. */
            size_t have = leftover; if (have) memmove(buf, hdr + leftover_off, have);
            for (;;) {
                /* need a full size line */
                uint8_t *nl;
                while (!(nl = memchr(buf, '\n', have))) {
                    if (have >= 16384 - 1) { rc = -1; goto done_body; }
                    int r = conn_read(s3, buf + have, 16384 - have); if (r <= 0) { rc = -1; goto done_body; }
                    have += (size_t)r;
                }
                size_t csz = strtoul((char *)buf, NULL, 16);
                size_t consumed = (size_t)(nl - buf) + 1;
                have -= consumed; memmove(buf, buf + consumed, have);
                if (csz == 0) break;
                while (csz) {
                    if (!have) { int r = conn_read(s3, buf, 16384); if (r <= 0) { rc = -1; goto done_body; } have = (size_t)r; }
                    size_t take = have < csz ? have : csz;
                    deliver_body(ctx, ok, buf, take);
                    have -= take; memmove(buf, buf + take, have); csz -= take;
                }
                /* CRLF after the chunk */
                while (have < 2) { int r = conn_read(s3, buf + have, 16384 - have); if (r <= 0) { rc = -1; goto done_body; } have += (size_t)r; }
                have -= 2; memmove(buf, buf + 2, have);
            }
            /* trailers until an empty line: already consumed the "0\r\n"; read to "\r\n" */
            for (;;) {
                while (!memchr(buf, '\n', have)) { int r = conn_read(s3, buf + have, 16384 - have); if (r <= 0) break; have += (size_t)r; }
                uint8_t *nl = memchr(buf, '\n', have); if (!nl) break;
                size_t consumed = (size_t)(nl - buf) + 1;
                bool empty = consumed <= 2;
                have -= consumed; memmove(buf, buf + consumed, have);
                if (empty) break;
            }
        } else if (clen >= 0) {
            size_t need = (size_t)clen;
            size_t first = leftover < need ? leftover : need;
            deliver_body(ctx, ok, (const uint8_t *)hdr + leftover_off, first);
            need -= first;
            while (need) {
                int r = conn_read(s3, buf, need < 16384 ? need : 16384);
                if (r <= 0) { rc = -1; break; }
                deliver_body(ctx, ok, buf, (size_t)r); need -= (size_t)r;
            }
        } else {
            /* no length: body runs to EOF, the connection is done afterwards */
            deliver_body(ctx, ok, (const uint8_t *)hdr + leftover_off, leftover);
            for (;;) { int r = conn_read(s3, buf, 16384); if (r < 0) { rc = -1; break; } if (r == 0) break; deliver_body(ctx, ok, buf, (size_t)r); }
            close_after = true;
        }
done_body:
        free(buf);
    }
    if (!ok && rc == 0) extract_error_code(ctx);
    free(hdr);
    if (rc == 0 && close_after) conn_close(s3);
    return rc;
}

/* The one request primitive. `body` may be NULL with len 0. Extra headers are
 * both sent and signed. On return res->status holds the HTTP status (0 on a
 * transport failure). Response bodies go to sink or buf; error bodies are
 * captured for <Code> extraction. */
static esp_err_t do_request(rrc_s3_t *s3, const char *method, const char *key, const rrc_kv *query, size_t n_query,
                            const rrc_kv *extra, size_t n_extra, const void *body, size_t body_len, const char *payload_hash,
                            req_ctx_t *ctx)
{
    rrc_s3_result_t *res = ctx->res;
    memset(res, 0, sizeof *res);
    res->content_length = -1;
    res->server_date = -1;

    char raw_path[RRC_RELKEY_PATH_MAX];
    if (snprintf(raw_path, sizeof raw_path, "/%s/%s", s3->cfg.bucket, key ? key : "") >= (int)sizeof raw_path) return ESP_ERR_INVALID_ARG;
    if (!key || !*key) snprintf(raw_path, sizeof raw_path, "/%s/", s3->cfg.bucket);
    char *url = build_url(s3, raw_path, query, n_query);
    if (!url) return ESP_ERR_NO_MEM;

    char amz_date[17];
    {
        time_t now = s3->have_server_offset ? (time_t)(s3->server_offset_s + mono_s()) : time(NULL);
        rrc_sigv4_amz_date(now, amz_date);
    }
    char payload_hex[65];
    if (!payload_hash) {
        if (body && body_len) rrc_sigv4_sha256_hex(body, body_len, payload_hex);
        else strcpy(payload_hex, RRC_SIGV4_EMPTY_SHA256);
        payload_hash = payload_hex;
    }
    /* Canonicalize extra header values once so signed == sent. */
    rrc_kv hdrs[RRC_SIGV4_MAX_HEADERS];
    char canon_vals[RRC_SIGV4_MAX_HEADERS][256];
    if (n_extra > RRC_SIGV4_MAX_HEADERS - 3) { free(url); return ESP_ERR_INVALID_ARG; }
    for (size_t i = 0; i < n_extra; i++) {
        hdrs[i].name = extra[i].name;
        if (rrc_sigv4_canon_header_value(extra[i].value ? extra[i].value : "", canon_vals[i], sizeof canon_vals[i]) < 0) { free(url); return ESP_ERR_INVALID_ARG; }
        hdrs[i].value = canon_vals[i];
    }
    rrc_sigv4_request sreq = {
        .method = method, .host = s3->host_header, .path = raw_path, .query = query, .n_query = n_query,
        .headers = hdrs, .n_headers = n_extra, .payload_sha256_hex = payload_hash, .amz_date = amz_date,
        .region = s3->cfg.region, .access_key = s3->cfg.access_key, .secret_key = s3->cfg.secret_key,
    };
    char auth[RRC_SIGV4_AUTH_MAX];
    if (rrc_sigv4_sign(&sreq, auth, sizeof auth) != 0) { free(url); return ESP_FAIL; }

    /* ---- request head: everything signed above goes out verbatim ---- */
    const char *path_q = url + strlen(s3->cfg.endpoint); /* "/bucket/key[?query]" */
    size_t head_cap = strlen(path_q) + strlen(s3->host_header) + strlen(auth) + 512;
    for (size_t i = 0; i < n_extra; i++) head_cap += strlen(hdrs[i].name) + strlen(hdrs[i].value) + 4;
    char *head = malloc(head_cap);
    if (!head) { free(url); return ESP_ERR_NO_MEM; }
    int hl = snprintf(head, head_cap,
                      "%s %s HTTP/1.1\r\nHost: %s\r\nx-amz-date: %s\r\nx-amz-content-sha256: %s\r\nAuthorization: %s\r\nContent-Length: %u\r\nConnection: keep-alive\r\n",
                      method, path_q, s3->host_header, amz_date, payload_hash, auth, (unsigned)body_len);
    for (size_t i = 0; i < n_extra && hl < (int)head_cap; i++) hl += snprintf(head + hl, head_cap - (size_t)hl, "%s: %s\r\n", hdrs[i].name, hdrs[i].value);
    if (hl + 2 >= (int)head_cap) { free(head); free(url); return ESP_ERR_NO_MEM; }
    head[hl++] = '\r'; head[hl++] = '\n'; head[hl] = 0;
    free(url);

    esp_err_t err = ESP_FAIL;
    bool is_head = !strcmp(method, "HEAD");
    /* A keep-alive connection the server has meanwhile closed fails on the first
     * write or read with no response bytes; that case is retried once on a fresh
     * connection. A failure after response bytes arrived is a real failure. */
    for (int attempt = 0; attempt < 2; attempt++) {
        bool reused = s3->conn_fd >= 0;
        if (conn_open(s3) != ESP_OK) { err = ESP_FAIL; break; }
        bool got_any = false;
        s3->stat_tx_us = 0; s3->stat_tx_bytes = 0; s3->stat_wait_us = 0; s3->stat_send_us = 0;
        if (conn_write_all(s3, head, (size_t)hl) != 0 || (body_len && conn_write_all(s3, body, body_len) != 0)) {
            ESP_LOGW(TAG, "%s %s: send failed (%s)", method, key, reused ? "stale keep-alive, retrying" : "fresh connection");
            conn_close(s3);
            if (reused) continue;
            err = ESP_FAIL; break;
        }
        int64_t t_sent = esp_timer_get_time();
        int rc = read_response(s3, ctx, is_head, &got_any);
        s3->stat_wait_us = esp_timer_get_time() - t_sent;
        if (rc != 0) {
            ESP_LOGW(TAG, "%s %s: response failed (%s)", method, key, reused && !got_any ? "stale keep-alive, retrying" : "transport");
            conn_close(s3);
            if (reused && !got_any) continue;
            err = ESP_FAIL; break;
        }
        err = ctx->overflow ? ESP_ERR_NO_MEM : ESP_OK;
        break;
    }
    free(head);
    if (err == ESP_OK && res->server_date > 0) {
        s3->server_offset_s = res->server_date - mono_s();
        s3->have_server_offset = true;
    }
    return err;
}

/* Retry wrapper: transport failures and 5xx/503 get up to 3 attempts with backoff. */
static esp_err_t with_retries(rrc_s3_t *s3, const char *method, const char *key, const rrc_kv *query, size_t n_query,
                              const rrc_kv *extra, size_t n_extra, const void *body, size_t body_len, const char *payload_hash,
                              req_ctx_t *ctx, bool allow_retry)
{
    int delay_ms = 1000;
    esp_err_t err = ESP_FAIL;
    for (int attempt = 0; attempt < (allow_retry ? 3 : 1); attempt++) {
        ctx->got = 0; ctx->errlen = 0; ctx->overflow = false;
        err = do_request(s3, method, key, query, n_query, extra, n_extra, body, body_len, payload_hash, ctx);
        if (err == ESP_OK && ctx->res->status != 0 && (ctx->res->status < 500 || ctx->res->status == 501)) return ESP_OK;
        if (err == ESP_ERR_NO_MEM || err == ESP_ERR_INVALID_ARG) return err;
        ESP_LOGW(TAG, "%s %s attempt %d failed (err=%s status=%d code=%s)", method, key, attempt + 1, esp_err_to_name(err),
                 ctx->res->status, ctx->res->error_code);
        vTaskDelay(pdMS_TO_TICKS(delay_ms));
        delay_ms *= 2;
    }
    return err == ESP_OK ? ESP_FAIL : err;
}

esp_err_t rrc_s3_init(rrc_s3_t *s3, const rrc_s3_config_t *cfg)
{
    memset(s3, 0, sizeof *s3);
    s3->conn_tls = NULL;
    s3->conn_fd = -1;
    s3->cfg = *cfg;
    /* trim trailing slash */
    size_t n = strlen(s3->cfg.endpoint);
    while (n > 0 && s3->cfg.endpoint[n - 1] == '/') s3->cfg.endpoint[--n] = 0;
    const char *h;
    if (!strncmp(s3->cfg.endpoint, "https://", 8)) { s3->https = true; h = s3->cfg.endpoint + 8; }
    else if (!strncmp(s3->cfg.endpoint, "http://", 7)) { s3->https = false; h = s3->cfg.endpoint + 7; }
    else return ESP_ERR_INVALID_ARG;
    if (!*h || strchr(h, '/')) return ESP_ERR_INVALID_ARG; /* no path component (keys.rs: bare host[:port]) */
    /* Mirror esp_http_client's _get_host_header(): ":port" is kept only when the
     * port is neither 80 nor 443, regardless of scheme. SigV4 signs this exact value. */
    const char *colon = strrchr(h, ':');
    if (colon && !strchr(colon, ']')) {
        int port = atoi(colon + 1);
        if (port == 80 || port == 443) {
            size_t hl = (size_t)(colon - h);
            if (hl >= sizeof s3->host_header) return ESP_ERR_INVALID_ARG;
            memcpy(s3->host_header, h, hl); s3->host_header[hl] = 0;
        } else {
            strncpy(s3->host_header, h, sizeof s3->host_header - 1);
        }
    } else {
        strncpy(s3->host_header, h, sizeof s3->host_header - 1);
    }
    /* Connection target: host without the port, default port by scheme. */
    {
        const char *pc = strrchr(h, ':');
        if (pc && !strchr(pc, ']')) {
            size_t hl = (size_t)(pc - h);
            if (hl >= sizeof s3->conn_host) return ESP_ERR_INVALID_ARG;
            memcpy(s3->conn_host, h, hl); s3->conn_host[hl] = 0;
            s3->conn_port = atoi(pc + 1);
        } else {
            strncpy(s3->conn_host, h, sizeof s3->conn_host - 1);
            s3->conn_port = s3->https ? 443 : 80;
        }
        if (s3->conn_port <= 0) return ESP_ERR_INVALID_ARG;
    }
    if (!s3->cfg.bucket[0] || !s3->cfg.access_key[0] || !s3->cfg.secret_key[0]) return ESP_ERR_INVALID_ARG;
    if (!s3->cfg.region[0]) strcpy(s3->cfg.region, "garage");
    if (s3->cfg.timeout_ms <= 0) s3->cfg.timeout_ms = 30000;
    return ESP_OK;
}

int64_t rrc_s3_server_now(const rrc_s3_t *s3)
{
    return s3->have_server_offset ? s3->server_offset_s + mono_s() : (int64_t)time(NULL);
}

static size_t meta_headers(const rrc_kv *meta, size_t n_meta, rrc_kv *out, char names[][64], size_t start)
{
    for (size_t i = 0; i < n_meta && start < RRC_SIGV4_MAX_HEADERS - 3; i++) {
        snprintf(names[start], 64, "x-amz-meta-%s", meta[i].name);
        out[start].name = names[start];
        out[start].value = meta[i].value;
        start++;
    }
    return start;
}

/* Content-MD5 header value; `digest` may be precomputed by the caller (the sync
 * pipeline hashes parts on another core) or NULL to hash `body` here. */
static void md5_b64(const void *body, size_t len, const uint8_t *digest, char out[32])
{
    uint8_t d[16];
    if (!digest) { rrc_md5(body, len, d); digest = d; }
    rrc_base64_encode(digest, 16, out);
}

esp_err_t rrc_s3_put(rrc_s3_t *s3, const char *key, const void *body, size_t len, const char *content_type,
                     const rrc_kv *meta, size_t n_meta, rrc_s3_result_t *res)
{
    return rrc_s3_put_md5(s3, key, body, len, content_type, meta, n_meta, NULL, res);
}

esp_err_t rrc_s3_put_md5(rrc_s3_t *s3, const char *key, const void *body, size_t len, const char *content_type,
                         const rrc_kv *meta, size_t n_meta, const uint8_t *md5_digest, rrc_s3_result_t *res)
{
    char md5[32];
    md5_b64(body, len, md5_digest, md5);
    rrc_kv h[RRC_SIGV4_MAX_HEADERS];
    char names[RRC_SIGV4_MAX_HEADERS][64];
    size_t n = 0;
    h[n++] = (rrc_kv){"content-md5", md5};
    h[n++] = (rrc_kv){"content-type", content_type ? content_type : "application/octet-stream"};
    n = meta_headers(meta, n_meta, h, names, n);
    req_ctx_t ctx = {.res = res};
    esp_err_t err = with_retries(s3, "PUT", key, NULL, 0, h, n, body, len, NULL, &ctx, true);
    if (err != ESP_OK) return err;
    return (res->status >= 200 && res->status < 300) ? ESP_OK : ESP_FAIL;
}

esp_err_t rrc_s3_head(rrc_s3_t *s3, const char *key, rrc_s3_result_t *res)
{
    req_ctx_t ctx = {.res = res};
    esp_err_t err = with_retries(s3, "HEAD", key, NULL, 0, NULL, 0, NULL, 0, NULL, &ctx, true);
    if (err != ESP_OK) return err;
    return (res->status == 200 || res->status == 404) ? ESP_OK : ESP_FAIL;
}

esp_err_t rrc_s3_get(rrc_s3_t *s3, const char *key, void *buf, size_t cap, size_t *got, rrc_s3_result_t *res)
{
    req_ctx_t ctx = {.res = res, .buf = buf, .cap = cap};
    esp_err_t err = with_retries(s3, "GET", key, NULL, 0, NULL, 0, NULL, 0, NULL, &ctx, true);
    if (got) *got = ctx.got;
    if (err != ESP_OK) return err;
    return res->status == 200 ? ESP_OK : ESP_FAIL;
}

esp_err_t rrc_s3_get_stream(rrc_s3_t *s3, const char *key, rrc_s3_sink_t sink, void *sctx, rrc_s3_result_t *res)
{
    req_ctx_t ctx = {.res = res, .sink = sink, .sink_ctx = sctx};
    /* No retry: a sink has already consumed partial data. */
    esp_err_t err = with_retries(s3, "GET", key, NULL, 0, NULL, 0, NULL, 0, NULL, &ctx, false);
    if (err != ESP_OK) return err;
    return res->status == 200 ? ESP_OK : ESP_FAIL;
}

esp_err_t rrc_s3_delete(rrc_s3_t *s3, const char *key, rrc_s3_result_t *res)
{
    req_ctx_t ctx = {.res = res};
    esp_err_t err = with_retries(s3, "DELETE", key, NULL, 0, NULL, 0, NULL, 0, NULL, &ctx, true);
    if (err != ESP_OK) return err;
    return (res->status == 204 || res->status == 200 || res->status == 404) ? ESP_OK : ESP_FAIL;
}

static bool xml_tag(const char *body, const char *tag, char *out, size_t cap)
{
    char open[48], close[48];
    snprintf(open, sizeof open, "<%s>", tag);
    snprintf(close, sizeof close, "</%s>", tag);
    const char *p = strstr(body, open);
    if (!p) return false;
    p += strlen(open);
    const char *e = strstr(p, close);
    if (!e) return false;
    size_t n = (size_t)(e - p);
    if (n >= cap) return false;
    memcpy(out, p, n); out[n] = 0;
    return true;
}

esp_err_t rrc_s3_multipart_create(rrc_s3_t *s3, const char *key, const char *content_type, const rrc_kv *meta, size_t n_meta,
                                  char *upload_id, size_t upload_id_cap, rrc_s3_result_t *res)
{
    rrc_kv q[1] = {{"uploads", ""}};
    rrc_kv h[RRC_SIGV4_MAX_HEADERS];
    char names[RRC_SIGV4_MAX_HEADERS][64];
    size_t n = 0;
    h[n++] = (rrc_kv){"content-type", content_type ? content_type : "application/octet-stream"};
    if (s3->sha256_check_known && s3->backend_checks_sha256) h[n++] = (rrc_kv){"x-amz-checksum-algorithm", "SHA256"};
    n = meta_headers(meta, n_meta, h, names, n);
    char body[2048];
    req_ctx_t ctx = {.res = res, .buf = (uint8_t *)body, .cap = sizeof body - 1};
    esp_err_t err = with_retries(s3, "POST", key, q, 1, h, n, NULL, 0, NULL, &ctx, true);
    if (err != ESP_OK) return err;
    if (res->status != 200) return ESP_FAIL;
    body[ctx.got] = 0;
    /* UploadId may contain XML-escaped chars on some backends; Garage/MinIO/AWS emit plain base64-ish ids. */
    if (!xml_tag(body, "UploadId", upload_id, upload_id_cap)) return ESP_FAIL;
    return ESP_OK;
}

esp_err_t rrc_s3_upload_part(rrc_s3_t *s3, const char *key, const char *upload_id, int part_number, const void *data, size_t len,
                             const uint8_t *md5_digest, const uint8_t *sha256_digest, char *etag, size_t etag_cap, rrc_s3_result_t *res)
{
    char pn[8];
    snprintf(pn, sizeof pn, "%d", part_number);
    rrc_kv q[2] = {{"partNumber", pn}, {"uploadId", upload_id}};
    char md5[48];
    rrc_kv h[1];
    if (sha256_digest) {
        rrc_base64_encode(sha256_digest, 32, md5);
        h[0] = (rrc_kv){"x-amz-checksum-sha256", md5};
    } else {
        md5_b64(data, len, md5_digest, md5);
        h[0] = (rrc_kv){"content-md5", md5};
    }
    req_ctx_t ctx = {.res = res};
    /* UNSIGNED-PAYLOAD like the Rust transfer lane (§3.9): Content-MD5 carries integrity. */
    esp_err_t err = with_retries(s3, "PUT", key, q, 2, h, 1, data, len, RRC_SIGV4_UNSIGNED_PAYLOAD, &ctx, true);
    if (err != ESP_OK) return err;
    if (res->status < 200 || res->status >= 300) return ESP_FAIL;
    if (!res->etag[0]) return ESP_FAIL;
    strncpy(etag, res->etag, etag_cap - 1);
    etag[etag_cap - 1] = 0;
    return ESP_OK;
}

esp_err_t rrc_s3_multipart_complete(rrc_s3_t *s3, const char *key, const char *upload_id, const char *const *etags, int n_parts,
                                    rrc_s3_result_t *res)
{
    size_t cap = 128 + (size_t)n_parts * 128;
    char *xml = malloc(cap);
    if (!xml) return ESP_ERR_NO_MEM;
    size_t pos = (size_t)snprintf(xml, cap, "<CompleteMultipartUpload>");
    for (int i = 0; i < n_parts; i++) {
        int n = snprintf(xml + pos, cap - pos, "<Part><PartNumber>%d</PartNumber><ETag>\"%s\"</ETag></Part>", i + 1, etags[i]);
        if (n < 0 || (size_t)n >= cap - pos) { free(xml); return ESP_ERR_NO_MEM; }
        pos += (size_t)n;
    }
    pos += (size_t)snprintf(xml + pos, cap - pos, "</CompleteMultipartUpload>");
    rrc_kv q[1] = {{"uploadId", upload_id}};
    rrc_kv h[1] = {{"content-type", "application/xml"}};
    char body[2048];
    req_ctx_t ctx = {.res = res, .buf = (uint8_t *)body, .cap = sizeof body - 1};
    esp_err_t err = with_retries(s3, "POST", key, q, 1, h, 1, xml, pos, NULL, &ctx, true);
    free(xml);
    if (err != ESP_OK) return err;
    if (res->status != 200) return ESP_FAIL;
    body[ctx.got] = 0;
    /* A 200 with an <Error> body is how S3 reports a failed completion. */
    if (strstr(body, "<Error>")) { extract_error_code(&ctx); char code[48]; if (xml_tag(body, "Code", code, sizeof code)) snprintf(res->error_code, sizeof res->error_code, "%s", code); return ESP_FAIL; }
    char et[80];
    if (xml_tag(body, "ETag", et, sizeof et)) {
        size_t n = strlen(et);
        const char *v = et;
        if (n >= 2 && v[0] == '"' && v[n - 1] == '"') { v++; n -= 2; }
        /* XML-escaped quotes: &quot; */
        if (!strncmp(v, "&quot;", 6)) { v += 6; n -= 6; if (n >= 6) n -= 6; }
        if (n >= sizeof res->etag) n = sizeof res->etag - 1;
        memcpy(res->etag, v, n); res->etag[n] = 0;
    }
    return ESP_OK;
}

esp_err_t rrc_s3_multipart_abort(rrc_s3_t *s3, const char *key, const char *upload_id, rrc_s3_result_t *res)
{
    rrc_kv q[1] = {{"uploadId", upload_id}};
    req_ctx_t ctx = {.res = res};
    esp_err_t err = with_retries(s3, "DELETE", key, q, 1, NULL, 0, NULL, 0, NULL, &ctx, true);
    if (err != ESP_OK) return err;
    return (res->status == 204 || res->status == 404) ? ESP_OK : ESP_FAIL;
}

esp_err_t rrc_s3_probe_digest_check(rrc_s3_t *s3, const char *device_id)
{
    char key[160];
    snprintf(key, sizeof key, ".rrcloud/v1/probe/%s.bin", device_id);
    static const char payload[] = "rrcloud-ingest digest probe";
    /* Deliberately wrong MD5 (of a different string). */
    char bad_md5[32];
    md5_b64("not the payload", 15, NULL, bad_md5);
    rrc_kv h[2] = {{"content-md5", bad_md5}, {"content-type", "application/octet-stream"}};
    rrc_s3_result_t res;
    req_ctx_t ctx = {.res = &res};
    esp_err_t err = with_retries(s3, "PUT", key, NULL, 0, h, 2, payload, sizeof payload - 1, NULL, &ctx, true);
    if (err != ESP_OK) return err;
    if (res.status == 400 || !strcmp(res.error_code, "BadDigest") || !strcmp(res.error_code, "InvalidDigest")) {
        s3->backend_rejects_bad_md5 = true;
    } else if (res.status >= 200 && res.status < 300) {
        s3->backend_rejects_bad_md5 = false;
        ESP_LOGW(TAG, "backend accepted a wrong Content-MD5 — uploads will be read back and re-hashed (§2.4)");
        rrc_s3_result_t d;
        rrc_s3_delete(s3, key, &d);
    } else {
        ESP_LOGW(TAG, "digest probe inconclusive: status %d code %s", res.status, res.error_code);
        return ESP_FAIL;
    }
    s3->digest_check_known = true;
    return ESP_OK;
}

esp_err_t rrc_s3_probe_sha256_check(rrc_s3_t *s3, const char *device_id)
{
    char key[160];
    snprintf(key, sizeof key, ".rrcloud/v1/probe/%s.sha", device_id);
    static const char payload[] = "rrcloud-ingest sha256 probe";
    uint8_t wrong[32] = {0};                       /* a digest of nothing in particular */
    char b64[48];
    rrc_base64_encode(wrong, 32, b64);
    rrc_kv h[2] = {{"x-amz-checksum-sha256", b64}, {"content-type", "application/octet-stream"}};
    rrc_s3_result_t res;
    req_ctx_t ctx = {.res = &res};
    esp_err_t err = with_retries(s3, "PUT", key, NULL, 0, h, 2, payload, sizeof payload - 1, NULL, &ctx, true);
    if (err != ESP_OK) return err;
    if (res.status == 400 || !strcmp(res.error_code, "InvalidDigest") || !strcmp(res.error_code, "BadDigest")) {
        s3->backend_checks_sha256 = true;
    } else if (res.status >= 200 && res.status < 300) {
        s3->backend_checks_sha256 = false;
        rrc_s3_result_t d;
        rrc_s3_delete(s3, key, &d);
    } else {
        ESP_LOGW(TAG, "sha256 probe inconclusive: status %d code %s", res.status, res.error_code);
        return ESP_FAIL;
    }
    s3->sha256_check_known = true;
    return ESP_OK;
}

const char *rrc_s3_result_str(const rrc_s3_result_t *res, char *buf, size_t cap)
{
    snprintf(buf, cap, "status=%d%s%s etag=%s", res->status, res->error_code[0] ? " code=" : "", res->error_code, res->etag);
    return buf;
}

/* ---- local HTTP-date parser (RFC 1123) ---------------------------------- */
static int64_t days_from_civil(int64_t y, int m, int d)
{
    y -= m <= 2;
    int64_t era = (y >= 0 ? y : y - 399) / 400;
    int64_t yoe = y - era * 400;
    int64_t doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
    int64_t doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return era * 146097 + doe - 719468;
}

static int64_t parse_http_date(const char *s)
{
    if (!s) return -1;
    const char *p = strchr(s, ',');
    p = p ? p + 1 : s;
    while (*p == ' ') p++;
    int day, year, hh, mm, ss;
    char mon[4] = {0};
    if (sscanf(p, "%2d %3s %4d %2d:%2d:%2d", &day, mon, &year, &hh, &mm, &ss) != 6) return -1;
    static const char *m[] = {"Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};
    int mo = -1;
    for (int i = 0; i < 12; i++) if (!strncasecmp(mon, m[i], 3)) { mo = i + 1; break; }
    if (mo < 0) return -1;
    return days_from_civil(year, mo, day) * 86400 + hh * 3600 + mm * 60 + ss;
}

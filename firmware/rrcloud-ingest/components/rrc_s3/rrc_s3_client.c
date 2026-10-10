#include "rrc_s3_client.h"
#include "rrc_hash.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <ctype.h>
#include "esp_log.h"
#include "esp_http_client.h"
#include "esp_crt_bundle.h"
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

static esp_err_t http_evt(esp_http_client_event_t *evt)
{
    req_ctx_t *c = evt->user_data;
    if (!c) return ESP_OK;
    switch (evt->event_id) {
    case HTTP_EVENT_ON_HEADER:
        if (!strcasecmp(evt->header_key, "ETag")) {
            const char *v = evt->header_value;
            size_t n = strlen(v);
            if (n >= 2 && v[0] == '"' && v[n - 1] == '"') { v++; n -= 2; }
            if (n >= sizeof c->res->etag) n = sizeof c->res->etag - 1;
            memcpy(c->res->etag, v, n); c->res->etag[n] = 0;
        } else if (!strcasecmp(evt->header_key, "Date")) {
            c->res->server_date = parse_http_date(evt->header_value);
        } else if (!strcasecmp(evt->header_key, "Content-Length")) {
            c->res->content_length = strtoll(evt->header_value, NULL, 10);
        } else if (!strcasecmp(evt->header_key, "x-amz-meta-rrc-device")) {
            snprintf(c->res->meta_device, sizeof c->res->meta_device, "%s", evt->header_value);
        }
        break;
    default:
        break;
    }
    return ESP_OK;
}

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

static esp_http_client_method_t method_enum(const char *m)
{
    if (!strcmp(m, "GET")) return HTTP_METHOD_GET;
    if (!strcmp(m, "PUT")) return HTTP_METHOD_PUT;
    if (!strcmp(m, "POST")) return HTTP_METHOD_POST;
    if (!strcmp(m, "HEAD")) return HTTP_METHOD_HEAD;
    return HTTP_METHOD_DELETE;
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
        time_t now = time(NULL) + (s3->have_server_offset ? (time_t)s3->server_offset_s : 0);
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

    esp_http_client_config_t hc = {
        .url = url,
        .method = method_enum(method),
        .timeout_ms = s3->cfg.timeout_ms > 0 ? s3->cfg.timeout_ms : 30000,
        .event_handler = http_evt,
        .user_data = ctx,
        .disable_auto_redirect = true,  /* SigV4 signs host+path; never follow redirects */
        .buffer_size = 4096,
        .buffer_size_tx = 2048,
        .keep_alive_enable = false,
    };
    if (s3->https) {
        if (s3->cfg.tls_insecure) hc.skip_cert_common_name_check = true;
        else hc.crt_bundle_attach = esp_crt_bundle_attach;
    }
    esp_http_client_handle_t cl = esp_http_client_init(&hc);
    free(url);
    if (!cl) return ESP_ERR_NO_MEM;

    esp_err_t err = ESP_OK;
    esp_http_client_set_header(cl, "x-amz-date", amz_date);
    esp_http_client_set_header(cl, "x-amz-content-sha256", payload_hash);
    esp_http_client_set_header(cl, "Authorization", auth);
    for (size_t i = 0; i < n_extra; i++) esp_http_client_set_header(cl, hdrs[i].name, hdrs[i].value);
    if (body_len == 0 && (hc.method == HTTP_METHOD_PUT || hc.method == HTTP_METHOD_POST)) esp_http_client_set_header(cl, "Content-Length", "0");

    err = esp_http_client_open(cl, (int)body_len);
    if (err != ESP_OK) { ESP_LOGW(TAG, "%s %s: open failed: %s", method, key, esp_err_to_name(err)); goto out; }
    if (body_len) {
        const char *p = body;
        size_t left = body_len;
        while (left) {
            int w = esp_http_client_write(cl, p, left > 16384 ? 16384 : (int)left);
            if (w <= 0) { err = ESP_FAIL; ESP_LOGW(TAG, "%s %s: write failed", method, key); goto out; }
            p += w; left -= (size_t)w;
        }
    }
    int64_t clen = esp_http_client_fetch_headers(cl);
    if (clen < 0) { err = ESP_FAIL; ESP_LOGW(TAG, "%s %s: fetch_headers failed", method, key); goto out; }
    res->status = esp_http_client_get_status_code(cl);
    if (res->content_length < 0) res->content_length = clen;
    if (res->server_date > 0) {
        s3->server_offset_s = res->server_date - (int64_t)time(NULL);
        s3->have_server_offset = true;
    }
    bool ok = res->status >= 200 && res->status < 300;
    if (hc.method != HTTP_METHOD_HEAD) {
        uint8_t chunk[2048];
        for (;;) {
            int r = esp_http_client_read(cl, (char *)chunk, sizeof chunk);
            if (r < 0) { err = ESP_FAIL; break; }
            if (r == 0) break;
            if (ok) {
                if (ctx->sink) { if (ctx->sink(ctx->sink_ctx, chunk, (size_t)r)) { err = ESP_FAIL; break; } }
                else if (ctx->buf) {
                    if (ctx->got + (size_t)r > ctx->cap) { ctx->overflow = true; }
                    else { memcpy(ctx->buf + ctx->got, chunk, (size_t)r); ctx->got += (size_t)r; }
                } else { ctx->got += (size_t)r; }
            } else {
                size_t room = sizeof ctx->errbody - 1 - ctx->errlen;
                size_t n = (size_t)r < room ? (size_t)r : room;
                memcpy(ctx->errbody + ctx->errlen, chunk, n); ctx->errlen += n;
            }
        }
        if (!ok) extract_error_code(ctx);
    }
    if (ctx->overflow) err = ESP_ERR_NO_MEM;
out:
    esp_http_client_close(cl);
    esp_http_client_cleanup(cl);
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
    if (!s3->cfg.bucket[0] || !s3->cfg.access_key[0] || !s3->cfg.secret_key[0]) return ESP_ERR_INVALID_ARG;
    if (!s3->cfg.region[0]) strcpy(s3->cfg.region, "garage");
    if (s3->cfg.timeout_ms <= 0) s3->cfg.timeout_ms = 30000;
    return ESP_OK;
}

int64_t rrc_s3_server_now(const rrc_s3_t *s3)
{
    return (int64_t)time(NULL) + (s3->have_server_offset ? s3->server_offset_s : 0);
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

static void md5_b64(const void *body, size_t len, char out[32])
{
    uint8_t d[16];
    rrc_md5(body, len, d);
    rrc_base64_encode(d, 16, out);
}

esp_err_t rrc_s3_put(rrc_s3_t *s3, const char *key, const void *body, size_t len, const char *content_type,
                     const rrc_kv *meta, size_t n_meta, rrc_s3_result_t *res)
{
    char md5[32];
    md5_b64(body, len, md5);
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
                             char *etag, size_t etag_cap, rrc_s3_result_t *res)
{
    char pn[8];
    snprintf(pn, sizeof pn, "%d", part_number);
    rrc_kv q[2] = {{"partNumber", pn}, {"uploadId", upload_id}};
    char md5[32];
    md5_b64(data, len, md5);
    rrc_kv h[1] = {{"content-md5", md5}};
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
    md5_b64("not the payload", 15, bad_md5);
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

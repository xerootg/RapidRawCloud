/*
 * rrc_s3_client — the firmware's S3 client: SigV4 over esp_http_client,
 * path-style addressing, Content-MD5 on every PUT, server-time capture from
 * the `Date` header (architecture §2.1.4/§2.10), single PUT + multipart.
 *
 * All calls are synchronous and must be made from a task with a generous
 * stack (TLS handshakes are done inside). One `rrc_s3_t` may be shared by
 * tasks that serialize their use of it (the sync task owns it in practice).
 */
#pragma once
#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>
#include "esp_err.h"
#include "rrc_sigv4.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
    char endpoint[160];     /* "https://garage.example.com" or "http://10.0.0.5:3900" */
    char region[32];        /* "garage" */
    char bucket[64];
    char access_key[80];
    char secret_key[128];
    bool tls_insecure;      /* skip certificate verification (self-signed homelab) */
    int timeout_ms;         /* per-request network timeout (default 30000) */
} rrc_s3_config_t;

typedef struct {
    rrc_s3_config_t cfg;
    char host_header[96];   /* exactly what esp_http_client puts in Host: */
    bool https;
    /* server minus local clock, seconds; learned from every response's Date header */
    bool have_server_offset;
    int64_t server_offset_s;
    /* set at configure time by rrc_s3_probe_digest_check (§2.4 backend probe) */
    bool digest_check_known;
    bool backend_rejects_bad_md5;
} rrc_s3_t;

typedef struct {
    int status;             /* HTTP status; 0 on transport failure */
    char etag[80];          /* quotes stripped */
    int64_t content_length; /* -1 if absent */
    int64_t server_date;    /* unix seconds from Date header, -1 if absent */
    char error_code[48];    /* S3 <Code> from an error body, "" otherwise */
    char meta_device[40];   /* x-amz-meta-rrc-device on HEAD/GET, "" otherwise */
} rrc_s3_result_t;

typedef int (*rrc_s3_sink_t)(void *ctx, const uint8_t *data, size_t len);

esp_err_t rrc_s3_init(rrc_s3_t *s3, const rrc_s3_config_t *cfg);

/* Best server-time estimate: local clock + learned offset. */
int64_t rrc_s3_server_now(const rrc_s3_t *s3);

/* PUT a buffered body. meta (optional) are bare x-amz-meta- suffixes. 2xx -> ESP_OK. */
esp_err_t rrc_s3_put(rrc_s3_t *s3, const char *key, const void *body, size_t len, const char *content_type,
                     const rrc_kv *meta, size_t n_meta, rrc_s3_result_t *res);

/* HEAD. res->status is 200 or 404 (ESP_OK for both); other statuses -> ESP_FAIL. */
esp_err_t rrc_s3_head(rrc_s3_t *s3, const char *key, rrc_s3_result_t *res);

/* GET into a caller buffer (small objects). *got receives the length. */
esp_err_t rrc_s3_get(rrc_s3_t *s3, const char *key, void *buf, size_t cap, size_t *got, rrc_s3_result_t *res);

/* Streaming GET: body chunks go to sink. */
esp_err_t rrc_s3_get_stream(rrc_s3_t *s3, const char *key, rrc_s3_sink_t sink, void *ctx, rrc_s3_result_t *res);

esp_err_t rrc_s3_delete(rrc_s3_t *s3, const char *key, rrc_s3_result_t *res);

/* Multipart (§2.4). Parts are buffered (one part in PSRAM at a time). */
esp_err_t rrc_s3_multipart_create(rrc_s3_t *s3, const char *key, const char *content_type, const rrc_kv *meta, size_t n_meta,
                                  char *upload_id, size_t upload_id_cap, rrc_s3_result_t *res);
esp_err_t rrc_s3_upload_part(rrc_s3_t *s3, const char *key, const char *upload_id, int part_number, const void *data, size_t len,
                             char *etag, size_t etag_cap, rrc_s3_result_t *res);
esp_err_t rrc_s3_multipart_complete(rrc_s3_t *s3, const char *key, const char *upload_id, const char *const *etags, int n_parts,
                                    rrc_s3_result_t *res);
esp_err_t rrc_s3_multipart_abort(rrc_s3_t *s3, const char *key, const char *upload_id, rrc_s3_result_t *res);

/* §2.4 backend probe: PUT a tiny object with a deliberately wrong Content-MD5 and
 * record whether the backend rejects it (400 BadDigest). Probe object key is
 * ".rrcloud/v1/probe/<device>.bin" and is deleted afterwards. */
esp_err_t rrc_s3_probe_digest_check(rrc_s3_t *s3, const char *device_id);

/* Human-readable summary of a result for logs. */
const char *rrc_s3_result_str(const rrc_s3_result_t *res, char *buf, size_t cap);

#ifdef __cplusplus
}
#endif

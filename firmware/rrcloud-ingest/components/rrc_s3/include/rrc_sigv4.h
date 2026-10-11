/*
 * rrc_sigv4 — AWS Signature Version 4 for S3 (path-style), pure C99.
 *
 * Mirrors rrcloud-core's `s3::sigv4` (architecture §3.9): buffered bodies are
 * signed with their real SHA-256; streamed multipart parts use
 * `UNSIGNED-PAYLOAD` (integrity rides on `Content-MD5`, §2.4). Header values
 * are trimmed and whitespace-collapsed before signing AND before sending, so
 * signed == sent. Verified against botocore's S3SigV4Auth in host_tests/.
 */
#pragma once
#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>
#include <time.h>

#ifdef __cplusplus
extern "C" {
#endif

#define RRC_SIGV4_UNSIGNED_PAYLOAD "UNSIGNED-PAYLOAD"
#define RRC_SIGV4_EMPTY_SHA256 "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
#define RRC_SIGV4_MAX_HEADERS 16
#define RRC_SIGV4_MAX_QUERY 8
#define RRC_SIGV4_AUTH_MAX 512
#define RRC_RELKEY_PATH_MAX 1024

typedef struct {
    const char *name;
    const char *value;
} rrc_kv;

typedef struct {
    const char *method;      /* "PUT", "GET", ... */
    const char *host;        /* exact Host header value, e.g. "garage.example.com" or "10.0.0.5:3900" */
    const char *path;        /* RAW absolute path (unencoded), e.g. "/bucket/library/a b.NEF" */
    const rrc_kv *query;     /* RAW query parameters (unencoded); value "" for valueless (e.g. uploads) */
    size_t n_query;
    const rrc_kv *headers;   /* extra headers to send+sign (content-md5, content-type, range, x-amz-meta-*);
                                host / x-amz-date / x-amz-content-sha256 are added by the signer */
    size_t n_headers;
    const char *payload_sha256_hex; /* 64 hex chars, or RRC_SIGV4_UNSIGNED_PAYLOAD */
    const char *amz_date;    /* "YYYYMMDDTHHMMSSZ" */
    const char *region;
    const char *access_key;
    const char *secret_key;
} rrc_sigv4_request;

/* Writes the Authorization header value. Returns 0 on success, -1 on overflow / bad input. */
int rrc_sigv4_sign(const rrc_sigv4_request *req, char *authorization, size_t cap);

/* AWS URI-encoding (unreserved kept, everything else %XX uppercase, UTF-8 bytewise).
 * `/` kept literal only when encode_slash is false. Returns bytes written (excl. NUL) or -1 on overflow. */
int rrc_sigv4_uri_encode(const char *in, bool encode_slash, char *out, size_t cap);

/* Encodes a raw absolute path segment-by-segment (what goes on the wire AND in the canonical request). */
int rrc_sigv4_encode_path(const char *raw_path, char *out, size_t cap);

/* Canonical (sorted, encoded) query string: also what goes on the wire. Returns length or -1. */
int rrc_sigv4_encode_query(const rrc_kv *query, size_t n, char *out, size_t cap);

/* Canonicalizes a header value in place semantics: trims, collapses whitespace runs to one space.
 * Returns length written (excl. NUL) or -1 on overflow. */
int rrc_sigv4_canon_header_value(const char *in, char *out, size_t cap);

/* "YYYYMMDDTHHMMSSZ" from unix time (UTC). out must hold 17 bytes. */
void rrc_sigv4_amz_date(time_t t, char out[17]);

/* Hex SHA-256 of a buffer (65-byte out). */
void rrc_sigv4_sha256_hex(const void *data, size_t len, char out[65]);

#ifdef __cplusplus
}
#endif

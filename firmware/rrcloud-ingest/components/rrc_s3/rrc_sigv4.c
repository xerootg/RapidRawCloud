#include "rrc_sigv4.h"
#include "rrc_hash.h"
#include <string.h>
#include <stdio.h>
#include <ctype.h>
#include <stdlib.h>

static int put(char *out, size_t cap, size_t *pos, const char *s, size_t n)
{
    if (*pos + n + 1 > cap) return -1;
    memcpy(out + *pos, s, n);
    *pos += n;
    out[*pos] = 0;
    return 0;
}
#define PUTS(s) do { if (put(out, cap, &pos, (s), strlen(s))) return -1; } while (0)
#define PUTN(s, n) do { if (put(out, cap, &pos, (s), (n))) return -1; } while (0)

int rrc_sigv4_uri_encode(const char *in, bool encode_slash, char *out, size_t cap)
{
    static const char HEX[] = "0123456789ABCDEF";
    size_t pos = 0;
    for (const unsigned char *p = (const unsigned char *)in; *p; p++) {
        unsigned char b = *p;
        if ((b >= 'A' && b <= 'Z') || (b >= 'a' && b <= 'z') || (b >= '0' && b <= '9') || b == '-' || b == '_' || b == '.' || b == '~' ||
            (b == '/' && !encode_slash)) {
            char c = (char)b;
            PUTN(&c, 1);
        } else {
            char e[3] = {'%', HEX[b >> 4], HEX[b & 15]};
            PUTN(e, 3);
        }
    }
    if (pos + 1 > cap) return -1;
    out[pos] = 0;
    return (int)pos;
}

int rrc_sigv4_encode_path(const char *raw_path, char *out, size_t cap)
{
    size_t pos = 0;
    if (!raw_path || !*raw_path) { PUTS("/"); return (int)pos; }
    const char *p = raw_path;
    if (*p != '/') PUTS("/");
    while (*p) {
        const char *seg_end = strchr(p, '/');
        size_t seg_len = seg_end ? (size_t)(seg_end - p) : strlen(p);
        char seg[seg_len + 1];
        memcpy(seg, p, seg_len);
        seg[seg_len] = 0;
        char enc[seg_len * 3 + 1];
        if (rrc_sigv4_uri_encode(seg, true, enc, sizeof enc) < 0) return -1;
        PUTS(enc);
        if (seg_end) { PUTS("/"); p = seg_end + 1; } else break;
    }
    return (int)pos;
}

typedef struct { char *k; char *v; } enc_kv;

static int cmp_kv(const void *a, const void *b)
{
    const enc_kv *x = a, *y = b;
    int c = strcmp(x->k, y->k);
    return c ? c : strcmp(x->v, y->v);
}

int rrc_sigv4_encode_query(const rrc_kv *query, size_t n, char *out, size_t cap)
{
    size_t pos = 0;
    out[0] = 0;
    if (n == 0) return 0;
    if (n > RRC_SIGV4_MAX_QUERY) return -1;
    enc_kv enc[RRC_SIGV4_MAX_QUERY];
    int rc = 0;
    memset(enc, 0, sizeof enc);
    for (size_t i = 0; i < n; i++) {
        size_t kl = strlen(query[i].name) * 3 + 1, vl = strlen(query[i].value ? query[i].value : "") * 3 + 1;
        enc[i].k = malloc(kl); enc[i].v = malloc(vl);
        if (!enc[i].k || !enc[i].v) { rc = -1; goto done; }
        if (rrc_sigv4_uri_encode(query[i].name, true, enc[i].k, kl) < 0 ||
            rrc_sigv4_uri_encode(query[i].value ? query[i].value : "", true, enc[i].v, vl) < 0) { rc = -1; goto done; }
    }
    qsort(enc, n, sizeof enc[0], cmp_kv);
    for (size_t i = 0; i < n; i++) {
        if (i) { if (put(out, cap, &pos, "&", 1)) { rc = -1; goto done; } }
        if (put(out, cap, &pos, enc[i].k, strlen(enc[i].k)) || put(out, cap, &pos, "=", 1) ||
            put(out, cap, &pos, enc[i].v, strlen(enc[i].v))) { rc = -1; goto done; }
    }
    rc = (int)pos;
done:
    for (size_t i = 0; i < n; i++) { free(enc[i].k); free(enc[i].v); }
    return rc;
}

int rrc_sigv4_canon_header_value(const char *in, char *out, size_t cap)
{
    size_t pos = 0;
    const char *p = in;
    while (*p && isspace((unsigned char)*p)) p++;
    bool pending_space = false;
    for (; *p; p++) {
        if (isspace((unsigned char)*p)) { pending_space = true; continue; }
        if (pending_space) { if (pos + 1 >= cap) return -1; out[pos++] = ' '; pending_space = false; }
        if (pos + 1 >= cap) return -1;
        out[pos++] = *p;
    }
    out[pos] = 0;
    return (int)pos;
}

typedef struct { char name[64]; char value[512]; } canon_hdr;

static int cmp_hdr(const void *a, const void *b) { return strcmp(((const canon_hdr *)a)->name, ((const canon_hdr *)b)->name); }

void rrc_sigv4_sha256_hex(const void *data, size_t len, char out[65])
{
    uint8_t d[32];
    rrc_sha256(data, len, d);
    rrc_hex_lower(d, 32, out);
}

void rrc_sigv4_amz_date(time_t t, char out[17])
{
    struct tm tm;
    gmtime_r(&t, &tm);
    strftime(out, 17, "%Y%m%dT%H%M%SZ", &tm);
}

int rrc_sigv4_sign(const rrc_sigv4_request *req, char *authorization, size_t cap)
{
    if (!req || !req->method || !req->host || !req->amz_date || strlen(req->amz_date) != 16 || !req->region ||
        !req->access_key || !req->secret_key || !req->payload_sha256_hex) return -1;
    if (req->n_headers + 3 > RRC_SIGV4_MAX_HEADERS) return -1;

    /* 1. Canonical headers (lowercase names, canonical values, sorted). */
    canon_hdr hdrs[RRC_SIGV4_MAX_HEADERS];
    size_t nh = 0;
    for (size_t i = 0; i < req->n_headers; i++) {
        const char *n = req->headers[i].name;
        if (strlen(n) >= sizeof hdrs[0].name) return -1;
        for (size_t j = 0; j <= strlen(n); j++) hdrs[nh].name[j] = (char)tolower((unsigned char)n[j]);
        if (rrc_sigv4_canon_header_value(req->headers[i].value ? req->headers[i].value : "", hdrs[nh].value, sizeof hdrs[nh].value) < 0) return -1;
        nh++;
    }
    strcpy(hdrs[nh].name, "host"); if (rrc_sigv4_canon_header_value(req->host, hdrs[nh].value, sizeof hdrs[nh].value) < 0) return -1; nh++;
    strcpy(hdrs[nh].name, "x-amz-content-sha256"); strncpy(hdrs[nh].value, req->payload_sha256_hex, sizeof hdrs[nh].value - 1); hdrs[nh].value[sizeof hdrs[nh].value - 1] = 0; nh++;
    strcpy(hdrs[nh].name, "x-amz-date"); strcpy(hdrs[nh].value, req->amz_date); nh++;
    qsort(hdrs, nh, sizeof hdrs[0], cmp_hdr);

    /* 2. Canonical URI + query. */
    size_t plen = strlen(req->path) * 3 + 2;
    char *cpath = malloc(plen);
    size_t qlen = 1;
    for (size_t i = 0; i < req->n_query; i++) qlen += (strlen(req->query[i].name) + strlen(req->query[i].value ? req->query[i].value : "")) * 3 + 2;
    char *cquery = malloc(qlen);
    if (!cpath || !cquery) { free(cpath); free(cquery); return -1; }
    int rc = -1;
    if (rrc_sigv4_encode_path(req->path, cpath, plen) < 0) goto done;
    if (rrc_sigv4_encode_query(req->query, req->n_query, cquery, qlen) < 0) goto done;

    /* 3. Canonical request. */
    size_t creq_cap = strlen(req->method) + strlen(cpath) + strlen(cquery) + 16 + 64;
    for (size_t i = 0; i < nh; i++) creq_cap += strlen(hdrs[i].name) * 2 + strlen(hdrs[i].value) + 4;
    creq_cap += strlen(req->payload_sha256_hex);
    char *creq = malloc(creq_cap);
    char signed_headers[RRC_SIGV4_MAX_HEADERS * 64];
    if (!creq) goto done;
    {
        char *out = creq; size_t cap2 = creq_cap; size_t pos = 0;
#define CP(s) do { if (put(out, cap2, &pos, (s), strlen(s))) { free(creq); goto done; } } while (0)
        CP(req->method); CP("\n"); CP(cpath); CP("\n"); CP(cquery); CP("\n");
        for (size_t i = 0; i < nh; i++) { CP(hdrs[i].name); CP(":"); CP(hdrs[i].value); CP("\n"); }
        CP("\n");
        signed_headers[0] = 0;
        for (size_t i = 0; i < nh; i++) { if (i) strcat(signed_headers, ";"); strcat(signed_headers, hdrs[i].name); }
        CP(signed_headers); CP("\n"); CP(req->payload_sha256_hex);
#undef CP
    }
    char creq_hash[65];
    rrc_sigv4_sha256_hex(creq, strlen(creq), creq_hash);
    free(creq);

    /* 4. String to sign + signing key. */
    char date[9]; memcpy(date, req->amz_date, 8); date[8] = 0;
    char scope[160];
    snprintf(scope, sizeof scope, "%s/%s/s3/aws4_request", date, req->region);
    char sts[512];
    snprintf(sts, sizeof sts, "AWS4-HMAC-SHA256\n%s\n%s\n%s", req->amz_date, scope, creq_hash);
    uint8_t k[32];
    {
        size_t sl = strlen(req->secret_key);
        char *kseed = malloc(sl + 5);
        if (!kseed) goto done;
        memcpy(kseed, "AWS4", 4); memcpy(kseed + 4, req->secret_key, sl + 1);
        rrc_hmac_sha256(kseed, sl + 4, date, 8, k);
        free(kseed);
    }
    rrc_hmac_sha256(k, 32, req->region, strlen(req->region), k);
    rrc_hmac_sha256(k, 32, "s3", 2, k);
    rrc_hmac_sha256(k, 32, "aws4_request", 12, k);
    uint8_t sig[32];
    rrc_hmac_sha256(k, 32, sts, strlen(sts), sig);
    char sig_hex[65];
    rrc_hex_lower(sig, 32, sig_hex);

    int n = snprintf(authorization, cap, "AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s",
                     req->access_key, scope, signed_headers, sig_hex);
    rc = (n > 0 && (size_t)n < cap) ? 0 : -1;
done:
    free(cpath); free(cquery);
    return rc;
}

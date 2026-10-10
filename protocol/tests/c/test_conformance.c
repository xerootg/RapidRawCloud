/* Conformance of the generated C SDK against protocol/fixtures: decode → re-encode must be
 * byte-identical to the canonical fixture, and the fail-closed rules must hold. */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "rrcloud_proto.h"
#include "rrc_blake3.h"

void rrcp_blake3_hook(const uint8_t *data, size_t len, uint8_t out[32]) { rrc_blake3(data, len, out); }

static int fails;
#define CHECK(c, ...) do { if (!(c)) { fails++; printf("FAIL %s:%d ", __FILE__, __LINE__); printf(__VA_ARGS__); printf("\n"); } } while (0)
static const char *dir;

static char *read_file(const char *name)
{
    char path[512];
    snprintf(path, sizeof path, "%s/%s", dir, name);
    FILE *f = fopen(path, "rb");
    if (!f) { printf("cannot open %s\n", path); exit(2); }
    fseek(f, 0, SEEK_END); long n = ftell(f); fseek(f, 0, SEEK_SET);
    char *buf = malloc((size_t)n + 1);
    size_t got = fread(buf, 1, (size_t)n, f); buf[got] = 0; fclose(f);
    return buf;
}

static void trim_nl(char *s) { size_t n = strlen(s); while (n && (s[n - 1] == '\n' || s[n - 1] == '\r')) s[--n] = 0; }

#define D "0f6b2a1e-1111-4222-8333-944444444444"

int main(int argc, char **argv)
{
    dir = argc > 1 ? argv[1] : "../../fixtures";
    char out[4096];

    /* journal segment: each line round-trips */
    char *seg = read_file("journal_segment.v1.ndjson");
    int lines = 0;
    for (char *line = strtok(seg, "\n"); line; line = strtok(NULL, "\n")) {
        rrcp_journal_entry_t e;
        rrcp_err_t rc = rrcp_journal_entry_decode(line, strlen(line), &e);
        CHECK(rc == RRCP_OK, "line %d decode: %s", lines + 1, rrcp_err_str(rc));
        if (rc == RRCP_OK) {
            int n = rrcp_journal_entry_encode(&e, out, sizeof out);
            CHECK(n > 0 && !strcmp(out, line), "line %d re-encode\n  got  %s\n  want %s", lines + 1, out, line);
        }
        if (lines == 1) {
            CHECK(e.op == RRCP_OP_PUT && e.kind == RRCP_KIND_SIDECAR && e.has_rating && e.rating == 3 && e.vv.len == 2, "entry 2 fields");
            CHECK(rrcp_version_vector_get(&e.vv, D) == 9, "vv lookup");
        }
        if (lines == 3) CHECK(e.op == RRCP_OP_MOVE && e.has_from_key, "move entry");
        lines++;
    }
    CHECK(lines == 5, "5 entries, got %d", lines);
    free(seg);

    /* fail-closed rules */
    rrcp_journal_entry_t e;
    const char *v2 = "{\"v\":2,\"seq\":1,\"ts\":0,\"device\":\"" D "\",\"op\":\"put\",\"kind\":\"original\",\"key\":\"library/x\",\"vv\":{}}";
    CHECK(rrcp_journal_entry_decode(v2, strlen(v2), &e) == RRCP_E_UNSUPPORTED_VERSION, "version gate");
    const char *nov = "{\"seq\":1,\"ts\":0,\"device\":\"" D "\",\"op\":\"put\",\"kind\":\"original\",\"key\":\"library/x\",\"vv\":{}}";
    CHECK(rrcp_journal_entry_decode(nov, strlen(nov), &e) == RRCP_E_MISSING_FIELD, "missing version");
    const char *extra = "{\"v\":1,\"seq\":1,\"ts\":0,\"device\":\"" D "\",\"op\":\"put\",\"kind\":\"original\",\"key\":\"library/x\",\"vv\":{\"" D "\":1,\"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa\":0},\"future\":[1,{\"a\":null}],\"more\":\"\\u00e9\"}";
    rrcp_err_t rc = rrcp_journal_entry_decode(extra, strlen(extra), &e);
    CHECK(rc == RRCP_OK, "unknown fields ignored: %s", rrcp_err_str(rc));
    CHECK(e.vv.len == 1, "zero vv component dropped (len %u)", (unsigned)e.vv.len);
    const char *badhex = "{\"v\":1,\"seq\":1,\"ts\":0,\"device\":\"" D "\",\"op\":\"put\",\"kind\":\"original\",\"key\":\"library/x\",\"vv\":{},\"blake3\":\"6A0F\"}";
    CHECK(rrcp_journal_entry_decode(badhex, strlen(badhex), &e) == RRCP_E_INVALID, "bad hex rejected");
    const char *badop = "{\"v\":1,\"seq\":1,\"ts\":0,\"device\":\"" D "\",\"op\":\"zap\",\"kind\":\"original\",\"key\":\"library/x\",\"vv\":{}}";
    CHECK(rrcp_journal_entry_decode(badop, strlen(badop), &e) == RRCP_E_BAD_VALUE, "unknown enum rejected");
    const char *big = "{\"v\":1,\"seq\":18446744073709551615,\"ts\":-5,\"device\":\"" D "\",\"op\":\"put\",\"kind\":\"original\",\"key\":\"library/x\",\"vv\":{}}";
    CHECK(rrcp_journal_entry_decode(big, strlen(big), &e) == RRCP_OK && e.seq == 18446744073709551615ull && e.ts == -5, "exact 64-bit integers");
    const char *frac = "{\"v\":1,\"seq\":1.5,\"ts\":0,\"device\":\"" D "\",\"op\":\"put\",\"kind\":\"original\",\"key\":\"library/x\",\"vv\":{}}";
    CHECK(rrcp_journal_entry_decode(frac, strlen(frac), &e) == RRCP_E_BAD_VALUE, "fractional integer rejected");
    const char *trailing = "{\"v\":1,\"seq\":1,\"ts\":0,\"device\":\"" D "\",\"op\":\"put\",\"kind\":\"original\",\"key\":\"library/x\",\"vv\":{}} x";
    CHECK(rrcp_journal_entry_decode(trailing, strlen(trailing), &e) == RRCP_E_SYNTAX, "trailing garbage rejected");

    /* tombstone / device entry / pairing docs round-trip */
    char *t = read_file("tombstone.json"); trim_nl(t);
    rrcp_tombstone_t ts;
    CHECK(rrcp_tombstone_decode(t, strlen(t), &ts) == RRCP_OK && ts.kinds_len == 3 && ts.kinds[2] == RRCP_KIND_XMP, "tombstone decode");
    CHECK(rrcp_tombstone_encode(&ts, out, sizeof out) > 0 && !strcmp(out, t), "tombstone re-encode\n  got  %s\n  want %s", out, t);
    const char *nfd = "{\"relkey\":\"cafe\\u0301\",\"vv\":{},\"device\":\"" D "\",\"server_ts\":1,\"kinds\":[]}";
    CHECK(rrcp_tombstone_decode(nfd, strlen(nfd), &ts) == RRCP_E_INVALID, "NFD relkey fails closed");
    free(t);
    char *d = read_file("device_entry.json"); trim_nl(d);
    rrcp_device_entry_t de;
    CHECK(rrcp_device_entry_decode(d, strlen(d), &de) == RRCP_OK && de.proto.read_len == 1 && de.proto.write == 1 && de.applied.len == 1, "device entry decode");
    CHECK(rrcp_device_entry_encode(&de, out, sizeof out) > 0 && !strcmp(out, d), "device entry re-encode\n  got  %s\n  want %s", out, d);
    free(d);
    char *pi = read_file("pairing_info.json"); trim_nl(pi);
    rrcp_pairing_info_t info;
    CHECK(rrcp_pairing_info_decode(pi, strlen(pi), &info) == RRCP_OK && !strcmp(info.config_endpoint, "/api/config"), "pairing info decode");
    CHECK(rrcp_pairing_info_encode(&info, out, sizeof out) > 0 && !strcmp(out, pi), "pairing info re-encode\n  got  %s\n  want %s", out, pi);
    const char *mininfo = "{\"version\":1,\"issuer\":\"https://i\",\"clientId\":\"c\"}";
    CHECK(rrcp_pairing_info_decode(mininfo, strlen(mininfo), &info) == RRCP_OK && !strcmp(info.config_endpoint, "/api/config") && !info.has_redirect_uri, "pairing info defaults");
    free(pi);
    char *pc = read_file("pairing_config.json"); trim_nl(pc);
    rrcp_pairing_config_doc_t cfg;
    CHECK(rrcp_pairing_config_doc_decode(pc, strlen(pc), &cfg) == RRCP_OK && cfg.sync.worker_backfill && !strcmp(cfg.credentials.access_key_id, "GK1"), "pairing config decode");
    CHECK(rrcp_pairing_config_doc_encode(&cfg, out, sizeof out) > 0 && !strcmp(out, pc), "pairing config re-encode\n  got  %s\n  want %s", out, pc);
    const char *minimal = "{\"sync\":{\"endpoint\":\"https://e\",\"bucket\":\"b\"},\"credentials\":{\"accessKeyId\":\"k\",\"secretAccessKey\":\"s\"}}";
    CHECK(rrcp_pairing_config_doc_decode(minimal, strlen(minimal), &cfg) == RRCP_OK && cfg.sync.cache_size_gb == 8 && cfg.sync.force_path_style && !cfg.sync.worker_backfill && cfg.version == 1, "pairing config defaults");
    free(pc);

    /* manifest lines */
    char *mf = read_file("manifest.ndjson");
    int ml = 0;
    for (char *line = strtok(mf, "\n"); line; line = strtok(NULL, "\n"), ml++) {
        int n = -1;
        if (ml == 0) { rrcp_manifest_header_t h; CHECK(rrcp_manifest_header_decode(line, strlen(line), &h) == RRCP_OK, "header"); n = rrcp_manifest_header_encode(&h, out, sizeof out); }
        else if (!strncmp(line, "{\"key\"", 6)) { rrcp_manifest_row_t r; CHECK(rrcp_manifest_row_decode(line, strlen(line), &r) == RRCP_OK, "row %d", ml); n = rrcp_manifest_row_encode(&r, out, sizeof out); }
        else { rrcp_deleted_row_t r; CHECK(rrcp_deleted_row_decode(line, strlen(line), &r) == RRCP_OK, "deleted row"); n = rrcp_deleted_row_encode(&r, out, sizeof out); }
        CHECK(n > 0 && !strcmp(out, line), "manifest line %d re-encode\n  got  %s\n  want %s", ml + 1, out, line);
    }
    CHECK(ml == 4, "manifest lines");
    free(mf);

    /* keys */
    char *ke = read_file("keys.expected");
    for (char *line = strtok(ke, "\n"); line; line = strtok(NULL, "\n")) {
        char *tab1 = strchr(line, '\t'); *tab1 = 0; char *input = tab1 + 1; char *tab2 = strchr(input, '\t'); *tab2 = 0; const char *want = tab2 + 1;
        char got[512]; int n = -1;
        if (!strcmp(line, "library_original")) n = rrcp_key_library_original(input, got, sizeof got);
        else if (!strcmp(line, "sidecar")) n = rrcp_key_sidecar(input, got, sizeof got);
        else if (!strcmp(line, "vc_sidecar")) { char *bar = strchr(input, '|'); *bar = 0; n = rrcp_key_vc_sidecar(input, bar + 1, got, sizeof got); }
        else if (!strcmp(line, "journal_segment")) { char *bar = strchr(input, '|'); *bar = 0; n = rrcp_key_journal_segment(input, strtoull(bar + 1, NULL, 10), got, sizeof got); }
        else if (!strcmp(line, "manifest")) n = rrcp_key_manifest(input, got, sizeof got);
        else if (!strcmp(line, "device_registry")) n = rrcp_key_device_registry(input, got, sizeof got);
        else if (!strcmp(line, "device_retired")) n = rrcp_key_device_retired(input, got, sizeof got);
        else if (!strcmp(line, "tombstone")) n = rrcp_key_tombstone(input, got, sizeof got);
        else if (!strcmp(line, "preview")) n = rrcp_key_preview(input, got, sizeof got);
        else if (!strcmp(line, "thumb")) { char *bar = strchr(input, '|'); *bar = 0; rrcp_thumb_size_t sz; rrcp_thumb_size_parse(bar + 1, &sz); n = rrcp_key_thumb(input, sz, got, sizeof got); }
        else if (!strcmp(line, "thumbpack")) n = rrcp_key_thumbpack(input, got, sizeof got);
        else if (!strcmp(line, "pairing_user_config")) n = rrcp_key_pairing_user_config(input, got, sizeof got);
        CHECK(n > 0 && !strcmp(got, want), "key %s\n  got  %s\n  want %s", line, n > 0 ? got : "(err)", want);
    }
    free(ke);
    char small[8];
    CHECK(rrcp_key_manifest(D, small, sizeof small) == RRCP_E_OVERFLOW, "key overflow reported");
    CHECK(rrcp_key_manifest("not-a-uuid", out, sizeof out) == RRCP_E_INVALID, "key param validated");
    CHECK(rrcp_key_pairing_user_config("../x", out, sizeof out) == RRCP_E_INVALID, "username validated");

    /* encode validation + overflow */
    rrcp_journal_entry_t bad; rrcp_journal_entry_init(&bad);
    bad.v = 1; strcpy(bad.device, "nope"); strcpy(bad.key, "library/x");
    CHECK(rrcp_journal_entry_encode(&bad, out, sizeof out) == RRCP_E_INVALID, "encode validates scalars");
    strcpy(bad.device, D);
    CHECK(rrcp_journal_entry_encode(&bad, out, 16) == RRCP_E_OVERFLOW, "encode overflow");

    printf(fails ? "FAILED (%d)\n" : "ALL OK\n", fails);
    return fails ? 1 : 0;
}

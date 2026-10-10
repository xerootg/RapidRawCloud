#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "rrc_proto.h"
static int fails = 0;
#define CHECK(c, ...) do { if (!(c)) { fails++; printf("FAIL %s:%d ", __FILE__, __LINE__); printf(__VA_ARGS__); printf("\n"); } } while (0)
#define STREQ(a, b) CHECK(!strcmp((a), (b)), "\n  got  %s\n  want %s", (a), (b))
static int sink(void *ctx, const uint8_t *d, size_t n) { FILE *f = ctx; return fwrite(d, 1, n, f) == n ? 0 : -1; }
int main(int argc, char **argv) {
    char out[2048];
    /* identity */
    uint8_t rnd[16] = {0x0f,0x6b,0x2a,0x1e,0x11,0x11,0xf2,0x22,0x03,0x33,0x94,0x44,0x44,0x44,0x44,0x44};
    char id[37]; rrc_uuid4_format(rnd, id);
    STREQ(id, "0f6b2a1e-1111-4222-8333-944444444444");
    CHECK(rrc_device_id_valid(id), "uuid valid");
    CHECK(!rrc_device_id_valid("0F6B2A1E-1111-4222-8333-944444444444"), "uppercase rejected");
    CHECK(!rrc_device_id_valid("0f6b2a1e-1111-1222-8333-944444444444"), "version nibble");
    CHECK(!rrc_device_id_valid("0f6b2a1e-1111-4222-7333-944444444444"), "variant nibble");
    /* relkey validation (mirrors keys.rs tests) */
    CHECK(rrc_relkey_validate("2026/10/IMG_0042.NEF") == RRC_RELKEY_OK, "ok key");
    CHECK(rrc_relkey_validate("") == RRC_RELKEY_EMPTY, "empty");
    CHECK(rrc_relkey_validate("/a") == RRC_RELKEY_LEADING_SLASH, "leading slash");
    CHECK(rrc_relkey_validate("a\\b") == RRC_RELKEY_BACKSLASH, "backslash");
    CHECK(rrc_relkey_validate("C:/x") == RRC_RELKEY_COLON, "colon");
    CHECK(rrc_relkey_validate("a/../b") == RRC_RELKEY_BAD_SEGMENT, "dotdot");
    CHECK(rrc_relkey_validate("a/./b") == RRC_RELKEY_BAD_SEGMENT, "dot");
    CHECK(rrc_relkey_validate("a//b") == RRC_RELKEY_BAD_SEGMENT, "empty seg");
    CHECK(rrc_relkey_validate("a/b/") == RRC_RELKEY_BAD_SEGMENT, "trailing slash");
    CHECK(rrc_relkey_validate("a./b") == RRC_RELKEY_TRAILING_DOT_SPACE, "trailing dot");
    CHECK(rrc_relkey_validate("a /b") == RRC_RELKEY_TRAILING_DOT_SPACE, "trailing space");
    CHECK(rrc_relkey_validate("x/CON") == RRC_RELKEY_WINDOWS_RESERVED, "CON");
    CHECK(rrc_relkey_validate("x/Com1.txt") == RRC_RELKEY_WINDOWS_RESERVED, "COM1.txt");
    CHECK(rrc_relkey_validate("x/lpt9") == RRC_RELKEY_WINDOWS_RESERVED, "LPT9");
    CHECK(rrc_relkey_validate("x/COM\xc2\xb9.jpg") == RRC_RELKEY_WINDOWS_RESERVED, "COM superscript 1");
    CHECK(rrc_relkey_validate("x/com0") == RRC_RELKEY_OK, "COM0 fine");
    CHECK(rrc_relkey_validate("x/com10") == RRC_RELKEY_OK, "COM10 fine");
    CHECK(rrc_relkey_validate("x/console.log") == RRC_RELKEY_OK, "console fine");
    CHECK(rrc_relkey_validate("a/.rr.part-foo.NEF") == RRC_RELKEY_ENGINE_RESERVED, "engine reserved");
    CHECK(rrc_relkey_validate("a/x.rr.y") == RRC_RELKEY_OK, ".rr. mid-segment fine");
    CHECK(rrc_relkey_validate("caf\xc3\xa9/x.jpg") == RRC_RELKEY_OK, "NFC e-acute ok");
    CHECK(rrc_relkey_validate("cafe\xcc\x81/x.jpg") == RRC_RELKEY_NOT_NFC, "NFD rejected");
    CHECK(rrc_relkey_validate("a\xff/x") == RRC_RELKEY_INVALID_UTF8, "bad utf8");
    CHECK(rrc_relkey_validate("a\tb") == RRC_RELKEY_CONTROL, "control");
    CHECK(rrc_relkey_validate("\xe5\x86\x99\xe7\x9c\x9f/x.NEF") == RRC_RELKEY_OK, "CJK ok");
    /* sanitize */
    rrc_relkey_err e = rrc_relkey_sanitize("/DCIM//100NIKON/DSC_0001.NEF. ", out, sizeof out); CHECK(e == RRC_RELKEY_OK, "san1 %s", rrc_relkey_err_str(e)); STREQ(out, "DCIM/100NIKON/DSC_0001.NEF");
    rrc_relkey_sanitize("a:b\\c\x01" "d/x", out, sizeof out); STREQ(out, "a_b_c_d/x");
    rrc_relkey_sanitize("CON/aux.txt/x", out, sizeof out); STREQ(out, "CON_/aux.txt_/x");
    rrc_relkey_sanitize(".rr.part/./../x", out, sizeof out); STREQ(out, "_.rr.part/x");
    e = rrc_relkey_sanitize("...", out, sizeof out); CHECK(e == RRC_RELKEY_EMPTY, "all dots -> empty: %s", rrc_relkey_err_str(e));
    e = rrc_relkey_sanitize("cafe\xcc\x81", out, sizeof out); CHECK(e == RRC_RELKEY_NOT_NFC, "nfd stays rejected");
    /* key schema */
    rrc_key_library("a/b.NEF", out, sizeof out); STREQ(out, "library/a/b.NEF");
    char fn[32]; rrc_segment_filename(412, fn); STREQ(fn, "000000000000019c.v1.ndjson");
    rrc_key_journal_segment(id, 1, out, sizeof out); STREQ(out, ".rrcloud/v1/journal/0f6b2a1e-1111-4222-8333-944444444444/0000000000000001.v1.ndjson");
    rrc_key_manifest(id, out, sizeof out); STREQ(out, ".rrcloud/v1/manifests/0f6b2a1e-1111-4222-8333-944444444444.json.gz");
    rrc_key_device_registry(id, out, sizeof out); STREQ(out, ".rrcloud/v1/devices/0f6b2a1e-1111-4222-8333-944444444444.json");
    /* json quoting */
    rrc_json_quote("a\"b\\c\n\t\x01 \xc3\xa9", out, sizeof out); STREQ(out, "\"a\\\"b\\\\c\\n\\t\\u0001 \xc3\xa9\"");
    CHECK(rrc_json_quote("0123456789", out, 5) == -1, "quote overflow");
    /* journal entry */
    const char *b3 = "6a0f8e2d3c4b5a69788796a5b4c3d2e1f0f1e2d3c4b5a69788796a5b4c3d2e1f";
    rrc_journal_put_original je = { .seq = 412, .ts = 1769900000, .device = id, .bucket_key = "library/2026/10/IMG_0042.NEF", .vv_self = 1,
        .size = 48213, .blake3_hex = b3, .content_id_hex = b3, .has_mtime = true, .mtime = 1769899000 };
    int n = rrc_journal_encode_put_original(&je, out, sizeof out); CHECK(n > 0, "journal encode");
    STREQ(out, "{\"v\":1,\"seq\":412,\"ts\":1769900000,\"device\":\"0f6b2a1e-1111-4222-8333-944444444444\",\"op\":\"put\",\"kind\":\"original\","
               "\"key\":\"library/2026/10/IMG_0042.NEF\",\"vv\":{\"0f6b2a1e-1111-4222-8333-944444444444\":1},\"size\":48213,"
               "\"blake3\":\"6a0f8e2d3c4b5a69788796a5b4c3d2e1f0f1e2d3c4b5a69788796a5b4c3d2e1f\",\"content_id\":\"6a0f8e2d3c4b5a69788796a5b4c3d2e1f0f1e2d3c4b5a69788796a5b4c3d2e1f\",\"mtime\":1769899000}");
    je.vv_self = 0; CHECK(rrc_journal_encode_put_original(&je, out, sizeof out) == -1, "vv zero rejected"); je.vv_self = 1;
    CHECK(rrc_journal_encode_put_original(&je, out, 100) == -1, "journal overflow");
    /* device entry */
    rrc_device_entry de = { .name = "Camera dock", .platform = "esp32", .created = 1769000000, .last_seen_server_ts = 1769900000 };
    rrc_device_entry_encode(&de, out, sizeof out);
    STREQ(out, "{\"name\":\"Camera dock\",\"platform\":\"esp32\",\"created\":1769000000,\"last_seen_server_ts\":1769900000,\"applied\":{},\"proto\":{\"read\":[1],\"write\":1}}");
    const char *devs[1] = {"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"}; uint64_t seqs[1] = {7};
    de.applied_devices = devs; de.applied_seqs = seqs; de.n_applied = 1; rrc_device_entry_encode(&de, out, sizeof out);
    CHECK(strstr(out, "\"applied\":{\"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa\":7}") != NULL, "applied map %s", out);
    /* manifest */
    rrc_manifest_header_encode(1769900000, id, 412, out, sizeof out);
    STREQ(out, "{\"written_server_ts\":1769900000,\"cursors\":{\"0f6b2a1e-1111-4222-8333-944444444444\":412},\"proto\":1}");
    rrc_manifest_header_encode(1769900000, id, 0, out, sizeof out);
    STREQ(out, "{\"written_server_ts\":1769900000,\"cursors\":{},\"proto\":1}");
    rrc_manifest_row_original row = { .relkey = "2026/10/IMG_0042.NEF", .size = 48213, .blake3_hex = b3, .device = id, .vv_self = 1, .content_id_hex = b3, .has_mtime = true, .mtime = 1769899000, .ts = 1769900000 };
    rrc_manifest_row_encode(&row, out, sizeof out);
    STREQ(out, "{\"key\":\"2026/10/IMG_0042.NEF\",\"kind\":\"original\",\"size\":48213,\"blake3\":\"6a0f8e2d3c4b5a69788796a5b4c3d2e1f0f1e2d3c4b5a69788796a5b4c3d2e1f\","
               "\"vv\":{\"0f6b2a1e-1111-4222-8333-944444444444\":1},\"device\":\"0f6b2a1e-1111-4222-8333-944444444444\","
               "\"content_id\":\"6a0f8e2d3c4b5a69788796a5b4c3d2e1f0f1e2d3c4b5a69788796a5b4c3d2e1f\",\"mtime\":1769899000,\"ts\":1769900000}");
    /* gzip (verified externally by tools/check_gzip.py) */
    if (argc > 1) {
        FILE *f = fopen(argv[1], "wb"); CHECK(f != NULL, "open gz out");
        rrc_gzip_store g; rrc_gzip_store_begin(&g, sink, f);
        const char *hdr = "{\"written_server_ts\":1,\"cursors\":{},\"proto\":1}\n"; rrc_gzip_store_write(&g, hdr, strlen(hdr));
        char big[70000]; for (size_t i = 0; i < sizeof big; i++) big[i] = (char)('a' + (i % 26)); big[sizeof big - 1] = '\n';
        rrc_gzip_store_write(&g, big, sizeof big);
        rrc_gzip_store_write(&g, "", 0);
        CHECK(rrc_gzip_store_end(&g) == 0, "gzip end"); fclose(f);
    }
    /* time */
    CHECK(rrc_parse_http_date("Fri, 10 Oct 2026 12:34:56 GMT") == 1791635696LL, "http date %lld", (long long)rrc_parse_http_date("Fri, 10 Oct 2026 12:34:56 GMT"));
    CHECK(rrc_parse_http_date("Thu, 01 Jan 1970 00:00:00 GMT") == 0, "epoch");
    CHECK(rrc_parse_http_date("garbage") == -1, "bad date");
    CHECK(rrc_parse_ptp_datetime("20261010T123456") == 1791635696LL, "ptp dt %lld", (long long)rrc_parse_ptp_datetime("20261010T123456"));
    CHECK(rrc_parse_ptp_datetime("20261010T123456.5Z") == 1791635696LL, "ptp dt frac z");
    CHECK(rrc_parse_ptp_datetime("20261010T143456+0200") == 1791635696LL, "ptp dt tz");
    CHECK(rrc_parse_ptp_datetime("2026-10-10") == -1, "ptp bad");
    char iso[21]; rrc_format_iso8601(1791635696LL, iso); STREQ(iso, "2026-10-10T12:34:56Z");
    /* template */
    rrc_template_vars v = { .name = "DSC_0001.NEF", .path = "DCIM/100NIKON", .model = "NIKON Z f", .serial = "3012345", .when = 1791635696LL };
    e = rrc_template_expand("Camera Import/{model}/{yyyy}/{mm}/{dd}/{name}", &v, out, sizeof out); CHECK(e == RRC_RELKEY_OK, "tpl %s", rrc_relkey_err_str(e));
    STREQ(out, "Camera Import/NIKON Z f/2026/10/10/DSC_0001.NEF");
    rrc_template_expand("{serial}/{path}/{stem}-{hh}.{ext}", &v, out, sizeof out); STREQ(out, "3012345/DCIM/100NIKON/DSC_0001-12.NEF");
    v.when = 0; rrc_template_expand("{yyyy}/{mm}/{name}", &v, out, sizeof out); STREQ(out, "0000/00/DSC_0001.NEF");
    v.model = "Sigma: fp\\L"; rrc_template_expand("{model}/{name}", &v, out, sizeof out); STREQ(out, "Sigma_ fp_L/DSC_0001.NEF");
    rrc_template_expand("{bogus}/{name}", &v, out, sizeof out); STREQ(out, "{bogus}/DSC_0001.NEF");
    /* ledger */
    rrc_ledger_rec lr = { .status = 'U', .source_id = "ptp:NIKON Z f:3012345", .source_path = "DCIM/100NIKON/DSC_0001.NEF", .size = 123, .mtime = 456, .relkey = "a/b.NEF", .blake3_hex = b3, .seq = 9, .ts = 789 };
    n = rrc_ledger_format(&lr, out, sizeof out); CHECK(n > 0, "ledger fmt");
    STREQ(out, "L1\tU\tptp:NIKON Z f:3012345\tDCIM/100NIKON/DSC_0001.NEF\t123\t456\ta/b.NEF\t6a0f8e2d3c4b5a69788796a5b4c3d2e1f0f1e2d3c4b5a69788796a5b4c3d2e1f\t9\t789\n");
    rrc_ledger_rec pr; CHECK(rrc_ledger_parse(out, &pr) == 0, "ledger parse");
    CHECK(pr.status == 'U' && !strcmp(pr.source_path, "DCIM/100NIKON/DSC_0001.NEF") && pr.size == 123 && pr.mtime == 456 && !strcmp(pr.relkey, "a/b.NEF") && pr.seq == 9 && pr.ts == 789, "ledger fields");
    strcpy(out, "L1\tR\tmsc:1234:5678:SN\tx.jpg\t1\t2\t\t\t0\t0\n"); CHECK(rrc_ledger_parse(out, &pr) == 0 && pr.relkey[0] == 0 && pr.status == 'R', "ledger empty fields");
    strcpy(out, "L2\tR\n"); CHECK(rrc_ledger_parse(out, &pr) == -1, "ledger bad version");
    lr.source_path = "a\tb"; CHECK(rrc_ledger_format(&lr, out, sizeof out) == -1, "tab rejected");
    /* Fixtures for the Rust interop test (rrcloud-core/tests/firmware_interop.rs). */
    if (argc > 2) {
        char path[512];
        snprintf(path, sizeof path, "%s/0000000000000001.v1.ndjson", argv[2]);
        FILE *f = fopen(path, "wb"); CHECK(f != NULL, "open fixture %s", path);
        if (f) {
            rrc_journal_put_original e1 = je; e1.seq = 1; e1.has_mtime = true;
            rrc_journal_put_original e2 = je; e2.seq = 2; e2.has_mtime = false; e2.bucket_key = "library/Camera Import/NIKON Z f/2026/10/10/DSC_0002.NEF"; e2.size = 1;
            rrc_journal_encode_put_original(&e1, out, sizeof out); fprintf(f, "%s\n", out);
            rrc_journal_encode_put_original(&e2, out, sizeof out); fprintf(f, "%s\n", out);
            fclose(f);
        }
        snprintf(path, sizeof path, "%s/device.json", argv[2]);
        f = fopen(path, "wb");
        if (f) { rrc_device_entry d2 = de; d2.n_applied = 0; rrc_device_entry_encode(&d2, out, sizeof out); fputs(out, f); fclose(f); }
        snprintf(path, sizeof path, "%s/manifest.json.gz", argv[2]);
        f = fopen(path, "wb");
        if (f) {
            rrc_gzip_store g; rrc_gzip_store_begin(&g, sink, f);
            rrc_manifest_header_encode(1769900000, id, 2, out, sizeof out); rrc_gzip_store_write(&g, out, strlen(out)); rrc_gzip_store_write(&g, "\n", 1);
            rrc_manifest_row_encode(&row, out, sizeof out); rrc_gzip_store_write(&g, out, strlen(out)); rrc_gzip_store_write(&g, "\n", 1);
            rrc_manifest_row_original r2 = row; r2.relkey = "Camera Import/NIKON Z f/2026/10/10/DSC_0002.NEF"; r2.has_mtime = false; r2.size = 1;
            rrc_manifest_row_encode(&r2, out, sizeof out); rrc_gzip_store_write(&g, out, strlen(out)); rrc_gzip_store_write(&g, "\n", 1);
            rrc_gzip_store_end(&g); fclose(f);
        }
    }
    printf(fails ? "FAILED (%d)\n" : "ALL OK\n", fails);
    return fails ? 1 : 0;
}

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "rrc_sigv4.h"
static int fails = 0, cases = 0;
#define CHECK(c, ...) do { if (!(c)) { fails++; printf("FAIL %s:%d ", __FILE__, __LINE__); printf(__VA_ARGS__); printf("\n"); } } while (0)
static char *dup_after(const char *line, const char *prefix) { return strdup(line + strlen(prefix)); }
int main(int argc, char **argv) {
    FILE *f = fopen(argc > 1 ? argv[1] : "sigv4_vectors.txt", "r");
    if (!f) { printf("cannot open vectors\n"); return 1; }
    char line[2048];
    char *date = NULL, *name = NULL, *method = NULL, *region = NULL, *host = NULL, *path = NULL, *payload = NULL, *auth = NULL;
    rrc_kv q[8], h[16]; char *qbuf[16], *hbuf[32]; size_t nq = 0, nh = 0, nqb = 0, nhb = 0;
    while (fgets(line, sizeof line, f)) {
        line[strcspn(line, "\n")] = 0;
        if (!strncmp(line, "CASE ", 5)) { name = dup_after(line, "CASE "); nq = nh = 0; }
        else if (!strncmp(line, "METHOD ", 7)) method = dup_after(line, "METHOD ");
        else if (!strncmp(line, "REGION ", 7)) region = dup_after(line, "REGION ");
        else if (!strncmp(line, "HOST ", 5)) host = dup_after(line, "HOST ");
        else if (!strncmp(line, "PATH ", 5)) path = dup_after(line, "PATH ");
        else if (!strncmp(line, "QUERY ", 6)) { char *s = dup_after(line, "QUERY "); char *t = strchr(s, '\t'); *t = 0; q[nq].name = s; q[nq].value = t + 1; qbuf[nqb++] = s; nq++; }
        else if (!strncmp(line, "HDR ", 4)) { char *s = dup_after(line, "HDR "); char *t = strchr(s, '\t'); *t = 0; h[nh].name = s; h[nh].value = t + 1; hbuf[nhb++] = s; nh++; }
        else if (!strncmp(line, "DATE ", 5)) date = dup_after(line, "DATE ");
        else if (!strncmp(line, "PAYLOAD ", 8)) payload = dup_after(line, "PAYLOAD ");
        else if (!strncmp(line, "AUTH ", 5)) auth = dup_after(line, "AUTH ");
        else if (!strcmp(line, "END")) {
            rrc_sigv4_request r = { .method = method, .host = host, .path = path, .query = q, .n_query = nq, .headers = h, .n_headers = nh,
                .payload_sha256_hex = payload, .amz_date = date, .region = region,
                .access_key = "GKexampleaccesskey", .secret_key = "examplesecretkey0123456789abcdef" };
            char out[512];
            int rc = rrc_sigv4_sign(&r, out, sizeof out);
            CHECK(rc == 0, "%s: sign rc=%d", name, rc);
            CHECK(!strcmp(out, auth), "%s:\n  got  %s\n  want %s", name, out, auth);
            cases++;
            free(date); free(name); free(method); free(region); free(host); free(path); free(payload); free(auth);
            for (size_t i = 0; i < nqb; i++) free(qbuf[i]);
            for (size_t i = 0; i < nhb; i++) free(hbuf[i]);
            nqb = nhb = 0;
        }
    }
    fclose(f);
    /* encoder edge cases */
    char e[256];
    rrc_sigv4_uri_encode("a b+c/d~e_f.g-h", false, e, sizeof e); CHECK(!strcmp(e, "a%20b%2Bc/d~e_f.g-h"), "enc %s", e);
    rrc_sigv4_uri_encode("a/b", true, e, sizeof e); CHECK(!strcmp(e, "a%2Fb"), "enc slash %s", e);
    rrc_sigv4_encode_path("", e, sizeof e); CHECK(!strcmp(e, "/"), "empty path %s", e);
    rrc_sigv4_encode_path("/bucket/", e, sizeof e); CHECK(!strcmp(e, "/bucket/"), "trailing slash %s", e);
    rrc_sigv4_encode_path("/b/café", e, sizeof e); CHECK(!strcmp(e, "/b/caf%C3%A9"), "utf8 %s", e);
    { rrc_kv qq[3] = {{"uploads", ""}, {"partNumber", "3"}, {"Zeta", "a/b"}}; rrc_sigv4_encode_query(qq, 3, e, sizeof e); CHECK(!strcmp(e, "Zeta=a%2Fb&partNumber=3&uploads="), "query %s", e); }
    rrc_sigv4_canon_header_value("  a   b\t c  ", e, sizeof e); CHECK(!strcmp(e, "a b c"), "canon hdr '%s'", e);
    { char d[17]; rrc_sigv4_amz_date(1791000000, d); CHECK(!strcmp(d, "20261003T040000Z"), "amz date %s", d); }
    CHECK(rrc_sigv4_sign(NULL, e, sizeof e) == -1, "null req");
    printf("sigv4 cases: %d\n", cases);
    printf(fails ? "FAILED (%d)\n" : "ALL OK\n", fails);
    return fails ? 1 : 0;
}

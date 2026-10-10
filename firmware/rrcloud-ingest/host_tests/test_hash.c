#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "rrc_hash.h"
#include "rrc_blake3.h"
static int fails = 0;
#define CHECK(c, ...) do { if (!(c)) { fails++; printf("FAIL %s:%d ", __FILE__, __LINE__); printf(__VA_ARGS__); printf("\n"); } } while (0)
static void hexs(const uint8_t *d, size_t n, char *o) { rrc_hex_lower(d, n, o); }
int main(int argc, char **argv) {
    uint8_t d[32]; char hx[65];
    rrc_sha256((const uint8_t*)"", 0, d); hexs(d, 32, hx);
    CHECK(!strcmp(hx, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"), "sha256 empty %s", hx);
    rrc_sha256("abc", 3, d); hexs(d, 32, hx);
    CHECK(!strcmp(hx, "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"), "sha256 abc %s", hx);
    rrc_sha256("abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq", 56, d); hexs(d, 32, hx);
    CHECK(!strcmp(hx, "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1"), "sha256 448bit %s", hx);
    rrc_md5("", 0, d); hexs(d, 16, hx);
    CHECK(!strcmp(hx, "d41d8cd98f00b204e9800998ecf8427e"), "md5 empty %s", hx);
    rrc_md5("abc", 3, d); hexs(d, 16, hx);
    CHECK(!strcmp(hx, "900150983cd24fb0d6963f7d28e17f72"), "md5 abc %s", hx);
    rrc_md5("The quick brown fox jumps over the lazy dog", 43, d); hexs(d, 16, hx);
    CHECK(!strcmp(hx, "9e107d9d372bb6826bd81d3542a419d6"), "md5 fox %s", hx);
    rrc_hmac_sha256("Jefe", 4, "what do ya want for nothing?", 28, d); hexs(d, 32, hx);
    CHECK(!strcmp(hx, "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"), "hmac rfc4231#2 %s", hx);
    { uint8_t k[131]; memset(k, 0xaa, sizeof k);
      rrc_hmac_sha256(k, 131, "Test Using Larger Than Block-Size Key - Hash Key First", 54, d); hexs(d, 32, hx);
      CHECK(!strcmp(hx, "60e431591ee0b67f0d8a26aacbf5b77f8e0bc6213728c5140546040f0ee37f54"), "hmac rfc4231#6 %s", hx); }
    CHECK(rrc_crc32("123456789", 9) == 0xcbf43926u, "crc32 %08x", rrc_crc32("123456789", 9));
    { uint32_t c = rrc_crc32_update(0, "1234", 4); c = rrc_crc32_update(c, "56789", 5); CHECK(c == 0xcbf43926u, "crc32 incremental"); }
    { char b[64]; rrc_base64_encode((const uint8_t*)"", 0, b); CHECK(!strcmp(b, ""), "b64 empty");
      rrc_base64_encode((const uint8_t*)"f", 1, b); CHECK(!strcmp(b, "Zg=="), "b64 f %s", b);
      rrc_base64_encode((const uint8_t*)"fo", 2, b); CHECK(!strcmp(b, "Zm8="), "b64 fo %s", b);
      rrc_base64_encode((const uint8_t*)"foobar", 6, b); CHECK(!strcmp(b, "Zm9vYmFy"), "b64 foobar %s", b);
      uint8_t raw[3] = {0xfb, 0xff, 0xbf}; rrc_base64url_encode_nopad(raw, 3, b); CHECK(!strcmp(b, "-_-_"), "b64url %s", b);
      rrc_base64url_encode_nopad(raw, 2, b); CHECK(!strcmp(b, "-_8"), "b64url nopad %s", b); }
    /* Streaming equivalence of sha256/md5 across odd chunking. */
    { size_t n = 100000; uint8_t *buf = malloc(n); for (size_t i = 0; i < n; i++) buf[i] = (uint8_t)(i * 7 + 3);
      uint8_t one[32], two[32]; rrc_sha256(buf, n, one);
      rrc_sha256_ctx c; rrc_sha256_init(&c); size_t off = 0, step = 1; while (off < n) { size_t t = step; if (off + t > n) t = n - off; rrc_sha256_update(&c, buf + off, t); off += t; step = (step * 3 + 1) % 200 + 1; } rrc_sha256_final(&c, two);
      CHECK(!memcmp(one, two, 32), "sha256 streaming");
      uint8_t m1[16], m2[16]; rrc_md5(buf, n, m1); rrc_md5_ctx mc; rrc_md5_init(&mc); off = 0; step = 5; while (off < n) { size_t t = step; if (off + t > n) t = n - off; rrc_md5_update(&mc, buf + off, t); off += t; step = (step * 7 + 1) % 300 + 1; } rrc_md5_final(&mc, m2);
      CHECK(!memcmp(m1, m2, 16), "md5 streaming"); free(buf); }
    /* BLAKE3 vectors from the python reference (input byte i = i % 251). */
    const char *vec = argc > 1 ? argv[1] : "blake3_vectors.txt";
    FILE *f = fopen(vec, "r"); CHECK(f != NULL, "open %s", vec);
    if (f) { unsigned long n; char want[65]; int count = 0;
      while (fscanf(f, "%lu %64s", &n, want) == 2) {
        uint8_t *buf = malloc(n ? n : 1); for (unsigned long i = 0; i < n; i++) buf[i] = (uint8_t)(i % 251);
        rrc_blake3_ctx h; rrc_blake3_init(&h);
        /* odd chunking to exercise block/chunk boundaries */
        size_t off = 0, step = 1; while (off < n) { size_t t = step; if (off + t > n) t = n - off; rrc_blake3_update(&h, buf + off, t); off += t; step = (step * 5 + 3) % 1500 + 1; }
        char got[65]; rrc_blake3_final_hex(&h, got);
        CHECK(!strcmp(got, want), "blake3 len=%lu got %s want %s", n, got, want);
        uint8_t one[32]; rrc_blake3(buf, n, one); char g2[65]; rrc_hex_lower(one, 32, g2);
        CHECK(!strcmp(g2, want), "blake3 oneshot len=%lu", n);
        free(buf); count++; }
      fclose(f); printf("blake3 vectors checked: %d\n", count); }
    printf(fails ? "FAILED (%d)\n" : "ALL OK\n", fails);
    return fails ? 1 : 0;
}

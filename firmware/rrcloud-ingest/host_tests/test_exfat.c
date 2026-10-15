/* Walk an exFAT image with rrc_exfat and compare every file against the source
 * tree it was populated from. usage: test_exfat <image> <tree> */
#define _GNU_SOURCE
#include "rrc_exfat.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static FILE *g_img;
static int g_fail, g_files;
static const char *g_tree;
static uint64_t g_reads;

static int rd(void *ctx, uint64_t lba, uint32_t count, void *buf)
{
    (void)ctx; g_reads++;
    if (fseeko(g_img, (off_t)(lba * 512), SEEK_SET) != 0) return -1;
    return fread(buf, 512, count, g_img) == count ? 0 : -1;
}

typedef struct { rrc_exfat_t *fs; char rel[1024]; int depth; } walk_t;
static void walk(rrc_exfat_t *fs, const rrc_exfat_file_t *dir, const char *rel);

typedef struct { rrc_exfat_t *fs; const char *rel; } cbctx_t;

static int cb(void *vctx, const rrc_exfat_entry_t *e)
{
    cbctx_t *c = vctx;
    char path[2048];
    snprintf(path, sizeof path, "%s%s%s", c->rel, c->rel[0] ? "/" : "", e->name);
    rrc_exfat_file_t f;
    rrc_exfat_entry_file(e, &f);
    if (e->is_dir) { char copy[2048]; strcpy(copy, path); walk(c->fs, &f, copy); return 0; }
    char host[4096];
    snprintf(host, sizeof host, "%s/%s", g_tree, path);
    FILE *h = fopen(host, "rb");
    if (!h) { printf("FAIL %s: not in source tree\n", path); g_fail++; return 0; }
    fseeko(h, 0, SEEK_END);
    uint64_t hs = (uint64_t)ftello(h);
    if (hs != e->size) { printf("FAIL %s: size %llu != %llu\n", path, (unsigned long long)e->size, (unsigned long long)hs); g_fail++; fclose(h); return 0; }
    unsigned char *want = malloc(hs ? hs : 1), *got = malloc(hs ? hs : 1);
    rewind(h);
    if (hs && fread(want, 1, hs, h) != hs) { g_fail++; }
    fclose(h);
    /* sequential odd-sized reads */
    uint64_t off = 0; size_t chunk = 1000;
    while (off < hs) {
        size_t n, want_n = chunk;
        if (rrc_exfat_read(c->fs, &f, off, got + off, want_n, &n) != 0) { printf("FAIL %s: read error at %llu\n", path, (unsigned long long)off); g_fail++; break; }
        if (!n) break;
        off += n; chunk = chunk * 3 + 7; if (chunk > 300000) chunk = 777;
    }
    if (off != hs || memcmp(want, got, hs) != 0) { printf("FAIL %s: sequential content mismatch\n", path); g_fail++; }
    /* random-offset reads */
    for (int i = 0; i < 20 && hs > 10; i++) {
        uint64_t o = (uint64_t)rand() % hs; size_t l = (size_t)(rand() % 70000) + 1, n;
        if (rrc_exfat_read(c->fs, &f, o, got, l, &n) != 0 || n != (l > hs - o ? hs - o : l) || memcmp(got, want + o, n) != 0) {
            printf("FAIL %s: random read at %llu len %zu\n", path, (unsigned long long)o, l); g_fail++; break;
        }
    }
    g_files++;
    free(want); free(got);
    return 0;
}

static void walk(rrc_exfat_t *fs, const rrc_exfat_file_t *dir, const char *rel)
{
    cbctx_t c = {fs, rel};
    if (rrc_exfat_readdir(fs, dir, cb, &c) < 0) { printf("FAIL readdir %s\n", rel); g_fail++; }
}

int main(int argc, char **argv)
{
    if (argc < 3) { fprintf(stderr, "usage: %s image tree\n", argv[0]); return 2; }
    g_img = fopen(argv[1], "rb"); g_tree = argv[2];
    if (!g_img) { perror("image"); return 2; }
    uint64_t base;
    if (rrc_exfat_find_volume(rd, NULL, &base) != 0) { printf("FAIL find_volume\n"); return 1; }
    rrc_exfat_t *fs = malloc(sizeof *fs);
    int r = rrc_exfat_mount(fs, rd, NULL, base);
    if (r) { printf("FAIL mount %d\n", r); return 1; }
    rrc_exfat_file_t root; rrc_exfat_root(fs, &root);
    walk(fs, &root, "");
    rrc_exfat_entry_t e;
    if (rrc_exfat_lookup(fs, "dcim/100sigma/../nope", &e) != 1) { printf("FAIL lookup of missing path\n"); g_fail++; }
    printf("%d files checked, %d failures, %llu device reads, base lba %llu\n", g_files, g_fail, (unsigned long long)g_reads, (unsigned long long)base);
    return g_fail || !g_files;
}

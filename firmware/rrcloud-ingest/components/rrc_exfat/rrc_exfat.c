#include "rrc_exfat.h"
#include <string.h>

#define EOC 0xFFFFFFF8u
#define MAX_READ_RUN_SECTORS 32u    /* 16 KiB per device read: the MSC driver reallocates its DMA buffer to fit */

static uint16_t rd16(const uint8_t *p) { return (uint16_t)(p[0] | p[1] << 8); }
static uint32_t rd32(const uint8_t *p) { return (uint32_t)p[0] | (uint32_t)p[1] << 8 | (uint32_t)p[2] << 16 | (uint32_t)p[3] << 24; }
static uint64_t rd64(const uint8_t *p) { return (uint64_t)rd32(p) | (uint64_t)rd32(p + 4) << 32; }

static uint32_t cluster_sectors(const rrc_exfat_t *fs) { return 1u << fs->spc_shift; }
static bool valid_cluster(const rrc_exfat_t *fs, uint32_t c) { return c >= 2 && c - 2 < fs->cluster_count; }
static uint64_t cluster_lba(const rrc_exfat_t *fs, uint32_t c)
{
    return fs->base + fs->heap_off + ((uint64_t)(c - 2) << fs->spc_shift);
}

int rrc_exfat_find_volume(rrc_exfat_read_fn read, void *ctx, uint64_t *base)
{
    uint8_t s[RRC_EXFAT_SECTOR];
    if (read(ctx, 0, 1, s) != 0) return -1;
    if (memcmp(s + 3, "EXFAT   ", 8) == 0) { *base = 0; return 0; }
    if (s[510] != 0x55 || s[511] != 0xaa) return 1;
    for (int i = 0; i < 4; i++) {
        const uint8_t *pe = s + 446 + 16 * i;
        uint32_t start = rd32(pe + 8);
        if (pe[4] != 0x07 || start == 0) continue;
        uint8_t b[RRC_EXFAT_SECTOR];
        if (read(ctx, start, 1, b) != 0) return -1;
        if (memcmp(b + 3, "EXFAT   ", 8) == 0) { *base = start; return 0; }
    }
    return 1;
}

int rrc_exfat_mount(rrc_exfat_t *fs, rrc_exfat_read_fn read, void *ctx, uint64_t base)
{
    uint8_t b[RRC_EXFAT_SECTOR];
    memset(fs, 0, sizeof *fs);
    fs->read = read; fs->ctx = ctx; fs->base = base;
    fs->fat_cache_sec = UINT32_MAX;
    if (read(ctx, base, 1, b) != 0) return -1;
    if (memcmp(b + 3, "EXFAT   ", 8) != 0 || b[510] != 0x55 || b[511] != 0xaa) return -2;
    if (b[108] != 9) return -3;                    /* 512-byte sectors only */
    if (b[109] > 25 - 9) return -3;                /* clusters up to 32 MiB */
    fs->spc_shift = b[109];
    fs->fat_off = rd32(b + 80);
    fs->heap_off = rd32(b + 88);
    fs->cluster_count = rd32(b + 92);
    fs->root_cluster = rd32(b + 96);
    if (fs->cluster_count == 0 || fs->cluster_count > 0xFFFFFFF5u || fs->fat_off == 0) return -4;
    if (!valid_cluster(fs, fs->root_cluster)) return -4;
    return 0;
}

void rrc_exfat_root(const rrc_exfat_t *fs, rrc_exfat_file_t *dir)
{
    memset(dir, 0, sizeof *dir);
    dir->first_cluster = fs->root_cluster;
    dir->no_fat_chain = false;
    dir->size = UINT64_MAX;     /* the root directory has no stored length: follow its chain */
}

void rrc_exfat_entry_file(const rrc_exfat_entry_t *e, rrc_exfat_file_t *f)
{
    memset(f, 0, sizeof *f);
    f->first_cluster = e->first_cluster;
    f->no_fat_chain = e->no_fat_chain;
    f->size = e->size;
}

/* Next cluster in the chain, or 0 at the end / on error (*err set for I/O errors). */
static uint32_t fat_next(rrc_exfat_t *fs, uint32_t c, int *err)
{
    uint32_t off = c * 4u;
    uint32_t sec = fs->fat_off + off / RRC_EXFAT_SECTOR;
    if (fs->fat_cache_sec != sec) {
        if (fs->read(fs->ctx, fs->base + sec, 1, fs->fat_cache) != 0) { *err = -1; fs->fat_cache_sec = UINT32_MAX; return 0; }
        fs->fat_cache_sec = sec;
    }
    uint32_t n = rd32(fs->fat_cache + off % RRC_EXFAT_SECTOR);
    return valid_cluster(fs, n) ? n : 0;
}

/* Cluster number of the idx-th cluster of the file, 0 past the end. */
static uint32_t file_cluster(rrc_exfat_t *fs, rrc_exfat_file_t *f, uint32_t idx, int *err)
{
    if (idx >= fs->cluster_count) return 0;
    if (f->no_fat_chain) {
        uint32_t c = f->first_cluster + idx;
        return valid_cluster(fs, c) ? c : 0;
    }
    uint32_t c, i;
    if (f->cache_cluster && f->cache_idx <= idx) { c = f->cache_cluster; i = f->cache_idx; }
    else { c = f->first_cluster; i = 0; }
    while (i < idx) {
        c = fat_next(fs, c, err);
        if (!c) return 0;
        i++;
    }
    if (!valid_cluster(fs, c)) return 0;
    f->cache_idx = idx; f->cache_cluster = c;
    return c;
}

int rrc_exfat_read(rrc_exfat_t *fs, rrc_exfat_file_t *f, uint64_t offset, void *vbuf, size_t want, size_t *got)
{
    uint8_t *buf = vbuf;
    *got = 0;
    if (offset >= f->size) return 0;
    if (want > f->size - offset) want = (size_t)(f->size - offset);
    const uint32_t csec = cluster_sectors(fs);
    const uint64_t cbytes = (uint64_t)csec * RRC_EXFAT_SECTOR;
    uint8_t bounce[RRC_EXFAT_SECTOR];
    int err = 0;
    while (want) {
        uint32_t idx = (uint32_t)(offset / cbytes);
        uint64_t in_cluster = offset % cbytes;
        uint32_t c = file_cluster(fs, f, idx, &err);
        if (!c) return err ? err : -5;
        uint32_t sec_in = (uint32_t)(in_cluster / RRC_EXFAT_SECTOR);
        uint32_t sec_off = (uint32_t)(in_cluster % RRC_EXFAT_SECTOR);
        uint64_t lba = cluster_lba(fs, c) + sec_in;
        if (sec_off) {                           /* unaligned head */
            if (fs->read(fs->ctx, lba, 1, bounce) != 0) return -1;
            size_t n = RRC_EXFAT_SECTOR - sec_off;
            if (n > want) n = want;
            memcpy(buf, bounce + sec_off, n);
            buf += n; offset += n; want -= n; *got += n;
            continue;
        }
        if (want < RRC_EXFAT_SECTOR) {           /* tail */
            if (fs->read(fs->ctx, lba, 1, bounce) != 0) return -1;
            memcpy(buf, bounce, want);
            *got += want;
            return 0;
        }
        /* whole sectors: extend the run across physically contiguous clusters */
        uint32_t nsec = (uint32_t)(want / RRC_EXFAT_SECTOR);
        uint32_t avail = csec - sec_in;
        uint32_t last = c, extra = idx;
        const uint32_t keep_idx = f->cache_idx, keep_cluster = f->cache_cluster;
        while (avail < nsec && avail < MAX_READ_RUN_SECTORS) {
            uint32_t nc = file_cluster(fs, f, extra + 1, &err);
            if (!nc || nc != last + 1) break;
            last = nc; extra++; avail += csec;
        }
        f->cache_idx = keep_idx; f->cache_cluster = keep_cluster;
        if (nsec > avail) nsec = avail;
        if (nsec > MAX_READ_RUN_SECTORS) nsec = MAX_READ_RUN_SECTORS;
        if (fs->read(fs->ctx, lba, nsec, buf) != 0) return -1;
        size_t n = (size_t)nsec * RRC_EXFAT_SECTOR;
        buf += n; offset += n; want -= n; *got += n;
    }
    return 0;
}

int64_t rrc_exfat_timestamp(uint32_t ts)
{
    int year = 1980 + (int)(ts >> 25), mon = (int)((ts >> 21) & 15), day = (int)((ts >> 16) & 31);
    int hh = (int)((ts >> 11) & 31), mm = (int)((ts >> 5) & 63), ss = (int)(ts & 31) * 2;
    if (mon < 1 || mon > 12 || day < 1 || hh > 23 || mm > 59 || ss > 59) return 0;
    /* days from civil (Howard Hinnant) */
    int y = year - (mon <= 2);
    int era = y / 400;
    unsigned yoe = (unsigned)(y - era * 400);
    unsigned doy = (unsigned)((153 * (mon + (mon > 2 ? -3 : 9)) + 2) / 5 + day - 1);
    unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    int64_t days = (int64_t)era * 146097 + (int64_t)doe - 719468;
    return days * 86400 + hh * 3600 + mm * 60 + ss;
}

/* ---- directory iteration ---- */

typedef struct {
    rrc_exfat_t *fs;
    rrc_exfat_file_t file;
    uint32_t cluster;        /* current cluster, 0 = exhausted */
    uint32_t sec_in;         /* sector within the cluster */
    uint32_t ent;            /* entry within the sector */
    uint64_t consumed;       /* bytes of directory consumed (bounded by file.size unless root) */
    bool loaded;
    int cluster_idx;
    uint8_t sec[RRC_EXFAT_SECTOR];
} dir_it_t;

static void it_init(dir_it_t *it, rrc_exfat_t *fs, const rrc_exfat_file_t *dir)
{
    memset(it, 0, sizeof *it);
    it->fs = fs; it->file = *dir;
    it->cluster = dir->first_cluster;
    if (!valid_cluster(fs, it->cluster)) it->cluster = 0;
}

/* 1: entry in out, 0: end of directory, <0: error */
static int it_next(dir_it_t *it, uint8_t out[32])
{
    rrc_exfat_t *fs = it->fs;
    for (;;) {
        if (!it->cluster) return 0;
        if (it->file.size != UINT64_MAX && it->consumed >= it->file.size) return 0;
        if (!it->loaded) {
            if (fs->read(fs->ctx, cluster_lba(fs, it->cluster) + it->sec_in, 1, it->sec) != 0) return -1;
            it->loaded = true;
        }
        if (it->ent < RRC_EXFAT_SECTOR / 32) {
            memcpy(out, it->sec + 32 * it->ent, 32);
            it->ent++; it->consumed += 32;
            return 1;
        }
        it->ent = 0; it->loaded = false;
        if (++it->sec_in >= cluster_sectors(fs)) {
            it->sec_in = 0;
            int err = 0;
            it->cluster_idx++;
            if (it->file.no_fat_chain) {
                uint32_t c = it->file.first_cluster + (uint32_t)it->cluster_idx;
                it->cluster = valid_cluster(fs, c) ? c : 0;
            } else {
                it->cluster = fat_next(fs, it->cluster, &err);
                if (err) return err;
            }
        }
    }
}

static void put_utf8(char *o, size_t *n, size_t cap, uint32_t cp)
{
    char t[4]; size_t l;
    if (cp < 0x80) { t[0] = (char)cp; l = 1; }
    else if (cp < 0x800) { t[0] = (char)(0xc0 | cp >> 6); t[1] = (char)(0x80 | (cp & 0x3f)); l = 2; }
    else if (cp < 0x10000) { t[0] = (char)(0xe0 | cp >> 12); t[1] = (char)(0x80 | ((cp >> 6) & 0x3f)); t[2] = (char)(0x80 | (cp & 0x3f)); l = 3; }
    else { t[0] = (char)(0xf0 | cp >> 18); t[1] = (char)(0x80 | ((cp >> 12) & 0x3f)); t[2] = (char)(0x80 | ((cp >> 6) & 0x3f)); t[3] = (char)(0x80 | (cp & 0x3f)); l = 4; }
    if (*n + l >= cap) return;
    memcpy(o + *n, t, l); *n += l;
}

int rrc_exfat_readdir(rrc_exfat_t *fs, const rrc_exfat_file_t *dir, rrc_exfat_dir_cb cb, void *ctx)
{
    dir_it_t it;
    it_init(&it, fs, dir);
    uint8_t e[32];
    int r;
    while ((r = it_next(&it, e)) == 1) {
        if (e[0] == 0x00) return 0;                  /* end of directory */
        if (e[0] != 0x85) continue;                  /* not a file entry set (bitmap, upcase, label, deleted ...) */
        unsigned secondary = e[1];
        uint16_t attr = rd16(e + 4);
        uint32_t modts = rd32(e + 12);
        if (secondary < 2) continue;
        uint8_t se[32];
        if ((r = it_next(&it, se)) != 1) return r < 0 ? r : -6;
        if (se[0] != 0xC0) continue;
        rrc_exfat_entry_t ent;
        memset(&ent, 0, sizeof ent);
        ent.is_dir = (attr & 0x10) != 0;
        ent.no_fat_chain = (se[1] & 2) != 0;
        unsigned name_len = se[3];
        ent.first_cluster = rd32(se + 20);
        ent.size = rd64(se + 24);
        ent.mtime = rrc_exfat_timestamp(modts);
        size_t n = 0;
        unsigned done = 0;
        uint32_t hi = 0;
        bool ok = true;
        for (unsigned i = 2; i <= secondary; i++) {
            if ((r = it_next(&it, se)) != 1) return r < 0 ? r : -6;
            if (se[0] != 0xC1) { ok = false; break; }
            for (int k = 0; k < 15 && done < name_len; k++, done++) {
                uint32_t u = rd16(se + 2 + 2 * k);
                if (u >= 0xD800 && u < 0xDC00) { hi = u; continue; }
                if (u >= 0xDC00 && u < 0xE000) { if (!hi) continue; u = 0x10000 + ((hi - 0xD800) << 10) + (u - 0xDC00); }
                hi = 0;
                put_utf8(ent.name, &n, sizeof ent.name, u);
            }
        }
        if (!ok || !n) continue;
        ent.name[n] = 0;
        if (cb(ctx, &ent)) return 0;
    }
    return r < 0 ? r : 0;
}

typedef struct { const char *name; size_t len; rrc_exfat_entry_t *out; bool found; } find_t;

static int ieq(const char *a, const char *b, size_t n)
{
    for (size_t i = 0; i < n; i++) {
        char x = a[i], y = b[i];
        if (x >= 'A' && x <= 'Z') x = (char)(x + 32);
        if (y >= 'A' && y <= 'Z') y = (char)(y + 32);
        if (x != y) return 0;
    }
    return 1;
}

static int find_cb(void *ctx, const rrc_exfat_entry_t *e)
{
    find_t *f = ctx;
    if (strlen(e->name) == f->len && ieq(e->name, f->name, f->len)) { *f->out = *e; f->found = true; return 1; }
    return 0;
}

int rrc_exfat_lookup(rrc_exfat_t *fs, const char *path, rrc_exfat_entry_t *out)
{
    rrc_exfat_file_t dir;
    rrc_exfat_root(fs, &dir);
    memset(out, 0, sizeof *out);
    out->is_dir = true;
    out->first_cluster = fs->root_cluster;
    out->size = UINT64_MAX;
    while (*path) {
        while (*path == '/') path++;
        if (!*path) break;
        const char *end = strchr(path, '/');
        size_t len = end ? (size_t)(end - path) : strlen(path);
        if (!out->is_dir) return 1;
        find_t f = {path, len, out, false};
        int r = rrc_exfat_readdir(fs, &dir, find_cb, &f);
        if (r < 0) return r;
        if (!f.found) return 1;
        rrc_exfat_entry_file(out, &dir);
        path += len;
    }
    return 0;
}

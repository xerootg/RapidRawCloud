/*
 * rrc_exfat — minimal read-only exFAT reader over a 512-byte-sector block
 * device. ESP-IDF's FatFS is built without exFAT, and SD/CF cards above 32 GB
 * (Sigma fp) are exFAT. Pure C, no ESP-IDF calls: host-testable.
 *
 * Not thread safe: one volume is used by one task at a time.
 */
#pragma once
#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

#define RRC_EXFAT_SECTOR 512u
#define RRC_EXFAT_NAME_MAX 256   /* UTF-8 bytes incl. NUL */

/* Read `count` 512-byte sectors starting at absolute LBA `lba` into buf. Return 0 on success. */
typedef int (*rrc_exfat_read_fn)(void *ctx, uint64_t lba, uint32_t count, void *buf);

typedef struct {
    rrc_exfat_read_fn read;
    void *ctx;
    uint64_t base;            /* LBA of the volume's boot sector */
    uint32_t spc_shift;       /* sectors per cluster = 1 << spc_shift */
    uint32_t fat_off;         /* sectors from base */
    uint32_t heap_off;        /* sectors from base */
    uint32_t cluster_count;
    uint32_t root_cluster;
    uint32_t fat_cache_sec;   /* FAT sector currently cached, UINT32_MAX = none */
    uint8_t fat_cache[RRC_EXFAT_SECTOR];
} rrc_exfat_t;

typedef struct {
    char name[RRC_EXFAT_NAME_MAX];
    bool is_dir;
    uint64_t size;
    int64_t mtime;            /* unix seconds, the FAT local time read as if UTC; 0 if invalid */
    uint32_t first_cluster;
    bool no_fat_chain;
} rrc_exfat_entry_t;

/* A file/dir location, as returned by readdir. */
typedef struct {
    uint32_t first_cluster;
    bool no_fat_chain;
    uint64_t size;
    /* sequential-read cache */
    uint32_t cache_idx;
    uint32_t cache_cluster;
} rrc_exfat_file_t;

/* Look at sector 0 of the device: an MBR with an exFAT (type 07) partition, or a
 * bare exFAT volume. On success *base is the volume's boot sector LBA. */
int rrc_exfat_find_volume(rrc_exfat_read_fn read, void *ctx, uint64_t *base);

int rrc_exfat_mount(rrc_exfat_t *fs, rrc_exfat_read_fn read, void *ctx, uint64_t base);

void rrc_exfat_root(const rrc_exfat_t *fs, rrc_exfat_file_t *dir);
void rrc_exfat_entry_file(const rrc_exfat_entry_t *e, rrc_exfat_file_t *f);

/* Walk the entries of a directory. The callback returns non-zero to stop; the
 * entry is only valid during the call. Returns 0 on success (or early stop), <0 on I/O or format error. */
typedef int (*rrc_exfat_dir_cb)(void *ctx, const rrc_exfat_entry_t *e);
int rrc_exfat_readdir(rrc_exfat_t *fs, const rrc_exfat_file_t *dir, rrc_exfat_dir_cb cb, void *ctx);

/* Look a path up (no leading slash, '/'-separated, case-insensitive ASCII). 0 on success, 1 not found, <0 error. */
int rrc_exfat_lookup(rrc_exfat_t *fs, const char *path, rrc_exfat_entry_t *out);

/* Read up to `want` bytes at `offset`; *got = bytes read (short only at EOF). */
int rrc_exfat_read(rrc_exfat_t *fs, rrc_exfat_file_t *f, uint64_t offset, void *buf, size_t want, size_t *got);

/* Helpers exposed for tests. */
int64_t rrc_exfat_timestamp(uint32_t ts);

#include "camera_source.h"
#include "util.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <dirent.h>
#include <sys/stat.h>
#include <wchar.h>
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "esp_log.h"
#include "esp_heap_caps.h"
#include "usb/usb_host.h"
#include "usb/msc_host.h"
#include "usb/msc_host_vfs.h"
#include "app_config.h"
#include "rrc_proto.h"
#include "rrc_exfat.h"
#include "freertos/semphr.h"

static const char *TAG = "src_msc";
#define MOUNT "/usb"

/* msc_host has no multi-sector public read; this is the driver's own READ(10). */
extern esp_err_t scsi_cmd_read10(msc_host_device_handle_t dev, uint8_t *data, uint32_t sector_address,
                                 uint32_t num_sectors, uint32_t sector_size);

typedef struct {
    msc_host_device_handle_t dev;
    msc_host_vfs_handle_t vfs;
    bool connected;
    bool exfat;                 /* mounted with rrc_exfat instead of FatFS */
    rrc_exfat_t xfs;
    SemaphoreHandle_t xlock;    /* the exFAT sector cache is shared */
} msc_impl_t;

static struct {
    cam_event_cb_t cb;
    void *arg;
    cam_source_t src;
    msc_impl_t impl;
    bool in_use;              /* g.src handed to the sync task and not yet released */
    bool teardown_pending;    /* device gone; VFS/device still registered until release */
    bool has_pending_addr;    /* a disk appeared while in_use */
    uint8_t pending_addr;
} g;

static void attach_address(uint8_t address);

static void wcs_to_utf8(const wchar_t *w, char *out, size_t cap)
{
    size_t o = 0;
    for (; *w && o + 4 < cap; w++) {
        uint32_t c = (uint32_t)*w;
        if (c < 0x80) out[o++] = (char)c;
        else if (c < 0x800) { out[o++] = (char)(0xc0 | (c >> 6)); out[o++] = (char)(0x80 | (c & 0x3f)); }
        else { out[o++] = (char)(0xe0 | (c >> 12)); out[o++] = (char)(0x80 | ((c >> 6) & 0x3f)); out[o++] = (char)(0x80 | (c & 0x3f)); }
    }
    out[o] = 0;
}

static void trim(char *s)
{
    size_t n = strlen(s);
    while (n && (s[n - 1] == ' ' || s[n - 1] == '\t')) s[--n] = 0;
    char *p = s;
    while (*p == ' ') p++;
    if (p != s) memmove(s, p, strlen(p) + 1);
}

/* recursive walk; `rel` is the camera-relative path of `dir` ("" for the root we scan) */
static int walk(const char *dir, const char *rel, cam_enum_cb_t cb, void *ctx, int depth)
{
    if (depth > 8) return 0;
    DIR *d = opendir(dir);
    if (!d) return 0;
    struct dirent *de;
    int stop = 0;
    while (!stop && (de = readdir(d)) != NULL) {
        if (de->d_name[0] == '.') continue;
        char full[640];
        if (snprintf(full, sizeof full, "%s/%s", dir, de->d_name) >= (int)sizeof full) continue;
        struct stat st;
        if (stat(full, &st) != 0) continue;
        char nrel[512];
        if (snprintf(nrel, sizeof nrel, "%s%s%s", rel, rel[0] ? "/" : "", de->d_name) >= (int)sizeof nrel) continue;
        if (S_ISDIR(st.st_mode)) {
            stop = walk(full, nrel, cb, ctx, depth + 1);
        } else if (S_ISREG(st.st_mode)) {
            cam_object_t o = {0};
            scpy(o.name, sizeof o.name, de->d_name);
            scpy(o.path, sizeof o.path, nrel);
            o.size = (uint64_t)st.st_size;
            o.mtime = (int64_t)st.st_mtime; /* FAT local time, reported as if UTC */
            stop = cb(ctx, &o);
        }
    }
    closedir(d);
    return stop;
}

static int xread(void *ctx, uint64_t lba, uint32_t count, void *buf)
{
    msc_impl_t *m = ctx;
    if (!m->connected || lba > UINT32_MAX) return -1;
    return scsi_cmd_read10(m->dev, buf, (uint32_t)lba, count, RRC_EXFAT_SECTOR) == ESP_OK ? 0 : -1;
}

typedef struct { cam_enum_cb_t cb; void *ctx; rrc_exfat_t *fs; char rel[512]; int depth; } xwalk_t;

static int xwalk_dir(rrc_exfat_t *fs, const rrc_exfat_file_t *dir, const char *rel, int depth, cam_enum_cb_t cb, void *ctx);

typedef struct { xwalk_t w; int stop; } xcb_t;

static int xentry(void *vctx, const rrc_exfat_entry_t *e)
{
    xcb_t *x = vctx;
    if (e->name[0] == '.') return 0;
    char nrel[512];
    if (snprintf(nrel, sizeof nrel, "%s%s%s", x->w.rel, x->w.rel[0] ? "/" : "", e->name) >= (int)sizeof nrel) return 0;
    if (e->is_dir) {
        rrc_exfat_file_t d;
        rrc_exfat_entry_file(e, &d);
        x->stop = xwalk_dir(x->w.fs, &d, nrel, x->w.depth + 1, x->w.cb, x->w.ctx);
        return x->stop;
    }
    cam_object_t o = {0};
    scpy(o.name, sizeof o.name, e->name);
    scpy(o.path, sizeof o.path, nrel);
    o.size = e->size;
    o.mtime = e->mtime;
    x->stop = x->w.cb(x->w.ctx, &o);
    return x->stop;
}

static int xwalk_dir(rrc_exfat_t *fs, const rrc_exfat_file_t *dir, const char *rel, int depth, cam_enum_cb_t cb, void *ctx)
{
    if (depth > 8) return 0;
    xcb_t x = {.w = {.cb = cb, .ctx = ctx, .fs = fs, .depth = depth}, .stop = 0};
    scpy(x.w.rel, sizeof x.w.rel, rel);
    if (rrc_exfat_readdir(fs, dir, xentry, &x) < 0) ESP_LOGW(TAG, "exfat: directory read failed under /%s", rel);
    return x.stop;
}

static esp_err_t msc_enumerate_exfat(msc_impl_t *m, cam_enum_cb_t cb, void *ctx)
{
    const char *root = app_config_get()->msc_root;
    rrc_exfat_file_t dir;
    char rel[128] = "";
    rrc_exfat_entry_t e;
    xSemaphoreTake(m->xlock, portMAX_DELAY);
    if (root && *root && rrc_exfat_lookup(&m->xfs, root, &e) == 0 && e.is_dir) {
        rrc_exfat_entry_file(&e, &dir);
        scpy(rel, sizeof rel, root);
    } else {
        if (root && *root) ESP_LOGW(TAG, "root folder %s not found on card; scanning whole card", root);
        rrc_exfat_root(&m->xfs, &dir);
    }
    xwalk_dir(&m->xfs, &dir, rel, 0, cb, ctx);
    xSemaphoreGive(m->xlock);
    return ESP_OK;
}

static esp_err_t msc_enumerate(cam_source_t *s, cam_enum_cb_t cb, void *ctx)
{
    msc_impl_t *m = s->impl;
    if (!m->connected) return ESP_ERR_NOT_FOUND;
    if (m->exfat) return msc_enumerate_exfat(m, cb, ctx);
    const char *root = app_config_get()->msc_root;
    char dir[128];
    char rel[128] = "";
    if (root && *root) {
        snprintf(dir, sizeof dir, MOUNT "/%s", root);
        scpy(rel, sizeof rel, root);
        struct stat st;
        if (stat(dir, &st) != 0 || !S_ISDIR(st.st_mode)) {
            ESP_LOGW(TAG, "root folder %s not found on card; scanning whole card", root);
            strcpy(dir, MOUNT); rel[0] = 0;
        }
    } else {
        strcpy(dir, MOUNT);
    }
    walk(dir, rel, cb, ctx, 0);
    return ESP_OK;
}

static esp_err_t msc_open(cam_source_t *s, const cam_object_t *obj, void **fh)
{
    msc_impl_t *m = s->impl;
    if (m->exfat) {
        rrc_exfat_entry_t e;
        xSemaphoreTake(m->xlock, portMAX_DELAY);
        int r = rrc_exfat_lookup(&m->xfs, obj->path, &e);
        xSemaphoreGive(m->xlock);
        if (r != 0 || e.is_dir) return ESP_ERR_NOT_FOUND;
        rrc_exfat_file_t *f = malloc(sizeof *f);
        if (!f) return ESP_ERR_NO_MEM;
        rrc_exfat_entry_file(&e, f);
        *fh = f;
        return ESP_OK;
    }
    char full[640];
    snprintf(full, sizeof full, MOUNT "/%s", obj->path);
    FILE *f = fopen(full, "rb");
    if (!f) return ESP_ERR_NOT_FOUND;
    setvbuf(f, NULL, _IONBF, 0);
    *fh = f;
    return ESP_OK;
}

static esp_err_t msc_read(cam_source_t *s, void *fh, uint64_t offset, uint8_t *buf, size_t want, size_t *got)
{
    msc_impl_t *m = s->impl;
    if (!m->connected) return ESP_ERR_NOT_FOUND;
    if (m->exfat) {
        xSemaphoreTake(m->xlock, portMAX_DELAY);
        int r = rrc_exfat_read(&m->xfs, fh, offset, buf, want, got);
        xSemaphoreGive(m->xlock);
        return r == 0 ? ESP_OK : ESP_FAIL;
    }
    FILE *f = fh;
    if (fseeko(f, (off_t)offset, SEEK_SET) != 0) return ESP_FAIL;
    size_t n = fread(buf, 1, want, f);
    *got = n;
    if (n < want && ferror(f)) return ESP_FAIL;
    return ESP_OK;
}

static void msc_close(cam_source_t *s, void *fh)
{
    if (((msc_impl_t *)s->impl)->exfat) free(fh); else fclose((FILE *)fh);
}
static bool msc_connected(cam_source_t *s) { return ((msc_impl_t *)s->impl)->connected; }
/* Runs on the sync task once it has closed every file: only now is it safe to pull
 * the VFS and the device out from under /usb. */
static void msc_release(cam_source_t *s)
{
    (void)s;
    if (g.teardown_pending) {
        if (!g.impl.exfat) msc_host_vfs_unregister(g.impl.vfs);
        else if (g.impl.xlock) { vSemaphoreDelete(g.impl.xlock); g.impl.xlock = NULL; }
        msc_host_uninstall_device(g.impl.dev);
        g.teardown_pending = false;
        ESP_LOGI(TAG, "disk removed");
    }
    g.in_use = false;
    if (g.has_pending_addr) { g.has_pending_addr = false; attach_address(g.pending_addr); }
}

static void attach_address(uint8_t address)
{
    memset(&g.impl, 0, sizeof g.impl);
    esp_err_t e = msc_host_install_device(address, &g.impl.dev);
    if (e != ESP_OK) { ESP_LOGE(TAG, "install_device: %s", esp_err_to_name(e)); return; }
    const esp_vfs_fat_mount_config_t mc = {.format_if_mount_failed = false, .max_files = 4, .allocation_unit_size = 0};
    msc_host_device_info_t di0;
    uint64_t xbase;
    g.impl.connected = true;   /* xread refuses I/O until the device is marked connected */
    if (msc_host_get_device_info(g.impl.dev, &di0) == ESP_OK && di0.sector_size == RRC_EXFAT_SECTOR &&
        rrc_exfat_find_volume(xread, &g.impl, &xbase) == 0) {
        int r = rrc_exfat_mount(&g.impl.xfs, xread, &g.impl, xbase);
        if (r != 0) { ESP_LOGE(TAG, "exfat mount failed (%d)", r); g.impl.connected = false; msc_host_uninstall_device(g.impl.dev); return; }
        g.impl.exfat = true;
        g.impl.xlock = xSemaphoreCreateMutex();
        if (!g.impl.xlock) { g.impl.connected = false; msc_host_uninstall_device(g.impl.dev); return; }
        ESP_LOGI(TAG, "exFAT volume at LBA %llu", (unsigned long long)xbase);
        e = ESP_OK;
    } else {
        g.impl.connected = false;
        e = msc_host_vfs_register(g.impl.dev, MOUNT, &mc, &g.impl.vfs);
    }
    if (e != ESP_OK) {
        ESP_LOGE(TAG, "vfs_register: %s (0x%x)", esp_err_to_name(e), (unsigned)e);
        msc_host_uninstall_device(g.impl.dev);
        return;
    }
    msc_host_device_info_t info;
    memset(&g.src, 0, sizeof g.src);
    if (msc_host_get_device_info(g.impl.dev, &info) == ESP_OK) {
        char prod[64], ser[64];
        wcs_to_utf8(info.iProduct, prod, sizeof prod); trim(prod);
        wcs_to_utf8(info.iSerialNumber, ser, sizeof ser); trim(ser);
        scpy(g.src.model, sizeof g.src.model, prod[0] ? prod : "USB disk");
        scpy(g.src.serial, sizeof g.src.serial, ser);
        snprintf(g.src.source_id, sizeof g.src.source_id, "msc:%04x:%04x:%s", info.idVendor, info.idProduct, ser);
        ESP_LOGI(TAG, "disk: %s sn %s, %u MiB", g.src.model, ser, (unsigned)((uint64_t)info.sector_count * info.sector_size / (1024 * 1024)));
    } else {
        strcpy(g.src.model, "USB disk");
        strcpy(g.src.source_id, "msc:unknown");
    }
    g.src.kind = "msc";
    g.src.impl = &g.impl;
    g.src.enumerate = msc_enumerate; g.src.open = msc_open; g.src.read = msc_read; g.src.close = msc_close;
    g.src.connected = msc_connected; g.src.release = msc_release;
    g.impl.connected = true;
    g.in_use = true;
    if (g.cb) g.cb(CAM_EV_ATTACHED, &g.src, g.arg);
}

/* msc_host_install_device() does control transfers that the driver's own event
 * task must service, so it cannot run from that task's callback. */
static void attach_task(void *arg)
{
    attach_address((uint8_t)(uintptr_t)arg);
    vTaskDelete(NULL);
}

static void msc_event(const msc_host_event_t *ev, void *arg)
{
    (void)arg;
    if (ev->event == MSC_DEVICE_CONNECTED) {
        if (g.in_use) { ESP_LOGI(TAG, "disk attached before the previous one was released; queued"); g.has_pending_addr = true; g.pending_addr = ev->device.address; return; }
        if (xTaskCreate(attach_task, "msc_attach", 6144, (void *)(uintptr_t)ev->device.address, 5, NULL) != pdPASS)
            ESP_LOGE(TAG, "attach task: out of memory");
    } else if (ev->event == MSC_DEVICE_DISCONNECTED) {
        if (!g.in_use || ev->device.handle != g.impl.dev) return;
        g.impl.connected = false;
        g.teardown_pending = true; /* the sync task still owns open files on /usb */
        if (g.cb) g.cb(CAM_EV_DETACHED, &g.src, g.arg);
    }
}

esp_err_t source_msc_install(cam_event_cb_t cb, void *arg)
{
    g.cb = cb; g.arg = arg;
    const msc_host_driver_config_t cfg = {
        .create_backround_task = true,
        .task_priority = 5,
        .stack_size = 6144,
        .core_id = tskNO_AFFINITY,
        .callback = msc_event,
        .callback_arg = NULL,
    };
    return msc_host_install(&cfg);
}

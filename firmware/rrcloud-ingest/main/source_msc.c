#include "camera_source.h"
#include "util.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <dirent.h>
#include <sys/stat.h>
#include <wchar.h>
#include "esp_log.h"
#include "esp_heap_caps.h"
#include "usb/usb_host.h"
#include "usb/msc_host.h"
#include "usb/msc_host_vfs.h"
#include "app_config.h"
#include "rrc_proto.h"

static const char *TAG = "src_msc";
#define MOUNT "/usb"

typedef struct {
    msc_host_device_handle_t dev;
    msc_host_vfs_handle_t vfs;
    bool connected;
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

static esp_err_t msc_enumerate(cam_source_t *s, cam_enum_cb_t cb, void *ctx)
{
    msc_impl_t *m = s->impl;
    if (!m->connected) return ESP_ERR_NOT_FOUND;
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
    (void)s;
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
    FILE *f = fh;
    if (fseeko(f, (off_t)offset, SEEK_SET) != 0) return ESP_FAIL;
    size_t n = fread(buf, 1, want, f);
    *got = n;
    if (n < want && ferror(f)) return ESP_FAIL;
    return ESP_OK;
}

static void msc_close(cam_source_t *s, void *fh) { (void)s; fclose((FILE *)fh); }
static bool msc_connected(cam_source_t *s) { return ((msc_impl_t *)s->impl)->connected; }
/* Runs on the sync task once it has closed every file: only now is it safe to pull
 * the VFS and the device out from under /usb. */
static void msc_release(cam_source_t *s)
{
    (void)s;
    if (g.teardown_pending) {
        msc_host_vfs_unregister(g.impl.vfs);
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
    e = msc_host_vfs_register(g.impl.dev, MOUNT, &mc, &g.impl.vfs);
    if (e != ESP_OK) { ESP_LOGE(TAG, "vfs_register: %s", esp_err_to_name(e)); msc_host_uninstall_device(g.impl.dev); return; }
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

static void msc_event(const msc_host_event_t *ev, void *arg)
{
    (void)arg;
    if (ev->event == MSC_DEVICE_CONNECTED) {
        if (g.in_use) { ESP_LOGI(TAG, "disk attached before the previous one was released; queued"); g.has_pending_addr = true; g.pending_addr = ev->device.address; return; }
        attach_address(ev->device.address);
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

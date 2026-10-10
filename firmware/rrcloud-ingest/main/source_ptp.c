#include "camera_source.h"
#include "util.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include "esp_log.h"
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "esp_heap_caps.h"
#include "rrc_ptp.h"
#include "rrc_proto.h"

static const char *TAG = "src_ptp";

typedef struct {
    rrc_ptp_dev_t *dev;
    bool session;
} ptp_impl_t;

static struct {
    cam_event_cb_t cb;
    void *arg;
    cam_source_t src;
    ptp_impl_t impl;
} g;

typedef struct { uint32_t handle, parent; uint16_t fmt; bool is_dir; char name[128]; } node_t;

static const node_t *find_node(const node_t *nodes, size_t n, uint32_t handle)
{
    for (size_t i = 0; i < n; i++) if (nodes[i].handle == handle) return &nodes[i];
    return NULL;
}

/* Builds "A/B/name" by walking parents (bounded depth). */
static void build_path(const node_t *nodes, size_t n, const node_t *leaf, char *out, size_t cap)
{
    const char *parts[16];
    int np = 0;
    const node_t *cur = leaf;
    while (cur && np < 16) {
        parts[np++] = cur->name;
        if (cur->parent == 0 || cur->parent == 0xFFFFFFFFu) break;
        cur = find_node(nodes, n, cur->parent);
    }
    size_t o = 0;
    out[0] = 0;
    for (int i = np - 1; i >= 0; i--) {
        int w = snprintf(out + o, cap - o, "%s%s", parts[i], i ? "/" : "");
        if (w < 0 || (size_t)w >= cap - o) break;
        o += (size_t)w;
    }
}

static esp_err_t ensure_session(ptp_impl_t *p)
{
    if (!rrc_ptp_is_connected(p->dev)) return ESP_ERR_NOT_FOUND;
    if (p->session) return ESP_OK;
    esp_err_t e = rrc_ptp_open(p->dev);
    if (e == ESP_OK) p->session = true;
    return e;
}

static esp_err_t ptp_enumerate(cam_source_t *s, cam_enum_cb_t cb, void *ctx)
{
    ptp_impl_t *p = s->impl;
    esp_err_t e = ensure_session(p);
    if (e != ESP_OK) return e;
    uint32_t stores[8];
    size_t ns = 0;
    e = rrc_ptp_get_storage_ids(p->dev, stores, 8, &ns);
    if (e != ESP_OK) return e;
    for (size_t si = 0; si < ns; si++) {
        /* Skip logical-storage-less physical ids (low 16 bits == 0 means no media). */
        if ((stores[si] & 0xFFFF) == 0) continue;
        uint32_t *handles = NULL;
        size_t nh = 0;
        e = rrc_ptp_get_object_handles(p->dev, stores[si], &handles, &nh);
        if (e != ESP_OK) { ESP_LOGW(TAG, "GetObjectHandles(0x%08x) failed rc=0x%04x", stores[si], rrc_ptp_last_response(p->dev)); continue; }
        node_t *nodes = heap_caps_calloc(nh ? nh : 1, sizeof *nodes, MALLOC_CAP_SPIRAM | MALLOC_CAP_8BIT);
        if (!nodes) nodes = calloc(nh ? nh : 1, sizeof *nodes);
        if (!nodes) { free(handles); return ESP_ERR_NO_MEM; }
        /* pass 1: object infos (names, parents, sizes) */
        uint64_t *sizes = heap_caps_calloc(nh ? nh : 1, sizeof *sizes, MALLOC_CAP_SPIRAM | MALLOC_CAP_8BIT);
        int64_t *mtimes = heap_caps_calloc(nh ? nh : 1, sizeof *mtimes, MALLOC_CAP_SPIRAM | MALLOC_CAP_8BIT);
        if (!sizes || !mtimes) { free(sizes); free(mtimes); free(nodes); free(handles); return ESP_ERR_NO_MEM; }
        size_t good = 0;
        for (size_t i = 0; i < nh; i++) {
            ptp_object_info oi;
            if (rrc_ptp_get_object_info(p->dev, handles[i], &oi) != ESP_OK) {
                if (!rrc_ptp_is_connected(p->dev)) { free(sizes); free(mtimes); free(nodes); free(handles); return ESP_ERR_NOT_FOUND; }
                continue;
            }
            node_t *n = &nodes[good];
            n->handle = handles[i]; n->parent = oi.parent_object; n->fmt = oi.object_format;
            n->is_dir = oi.object_format == PTP_OFC_Association;
            scpy(n->name, sizeof n->name, oi.filename);
            sizes[good] = oi.compressed_size;
            int64_t t = rrc_parse_ptp_datetime(oi.capture_date);
            if (t <= 0) t = rrc_parse_ptp_datetime(oi.modification_date);
            mtimes[good] = t > 0 ? t : 0;
            good++;
        }
        ESP_LOGI(TAG, "storage 0x%08x: %u objects", stores[si], (unsigned)good);
        /* pass 2: emit files with reconstructed paths */
        int stop = 0;
        for (size_t i = 0; i < good && !stop; i++) {
            if (nodes[i].is_dir) continue;
            cam_object_t o = {0};
            scpy(o.name, sizeof o.name, nodes[i].name);
            build_path(nodes, good, &nodes[i], o.path, sizeof o.path);
            o.size = sizes[i];
            o.mtime = mtimes[i];
            o.handle = nodes[i].handle;
            stop = cb(ctx, &o);
        }
        free(sizes); free(mtimes); free(nodes); free(handles);
        if (stop) break;
    }
    return ESP_OK;
}

typedef struct { uint32_t handle; } ptp_fh_t;

static esp_err_t ptp_open(cam_source_t *s, const cam_object_t *obj, void **fh)
{
    ptp_impl_t *p = s->impl;
    esp_err_t e = ensure_session(p);
    if (e != ESP_OK) return e;
    ptp_fh_t *h = calloc(1, sizeof *h);
    if (!h) return ESP_ERR_NO_MEM;
    h->handle = obj->handle;
    *fh = h;
    return ESP_OK;
}

static esp_err_t ptp_read(cam_source_t *s, void *fh, uint64_t offset, uint8_t *buf, size_t want, size_t *got)
{
    ptp_impl_t *p = s->impl;
    ptp_fh_t *h = fh;
    if (offset > 0xFFFFFFFFull) return ESP_ERR_INVALID_SIZE; /* GetPartialObject is 32-bit */
    size_t total = 0;
    while (total < want) {
        size_t chunk = want - total;
        if (chunk > 0x00FF0000) chunk = 0x00FF0000;
        size_t g2 = 0;
        esp_err_t e = ESP_OK;
        for (int attempt = 0; attempt < 3; attempt++) {
            e = rrc_ptp_read_partial(p->dev, h->handle, (uint32_t)(offset + total), buf + total, chunk, &g2);
            if (e == ESP_OK) break;
            if (!rrc_ptp_is_connected(p->dev)) return ESP_ERR_NOT_FOUND;
            if (rrc_ptp_last_response(p->dev) == PTP_RC_DeviceBusy) { vTaskDelay(pdMS_TO_TICKS(300)); continue; }
            break;
        }
        if (e != ESP_OK) return e;
        total += g2;
        if (g2 == 0 || g2 < chunk) break; /* EOF */
    }
    *got = total;
    return ESP_OK;
}

static void ptp_close(cam_source_t *s, void *fh) { (void)s; free(fh); }
static bool ptp_connected(cam_source_t *s) { return rrc_ptp_is_connected(((ptp_impl_t *)s->impl)->dev); }
static void ptp_release(cam_source_t *s) { ptp_impl_t *p = s->impl; p->session = false; p->dev = NULL; }

static void ptp_event(rrc_ptp_event_t ev, rrc_ptp_dev_t *dev, void *arg)
{
    (void)arg;
    if (ev == RRC_PTP_EV_CONNECTED) {
        memset(&g.src, 0, sizeof g.src);
        g.impl.dev = dev; g.impl.session = false;
        g.src.kind = "ptp";
        g.src.impl = &g.impl;
        /* Model/serial are known only after OpenSession+GetDeviceInfo, which must
         * not run inside this callback; the sync task fills them via source_ptp_identify. */
        snprintf(g.src.source_id, sizeof g.src.source_id, "ptp:%04x:%04x", rrc_ptp_vid(dev), rrc_ptp_pid(dev));
        strcpy(g.src.model, "PTP camera");
        g.src.enumerate = ptp_enumerate; g.src.open = ptp_open; g.src.read = ptp_read; g.src.close = ptp_close;
        g.src.connected = ptp_connected; g.src.release = ptp_release;
        if (g.cb) g.cb(CAM_EV_ATTACHED, &g.src, g.arg);
    } else {
        g.impl.session = false;
        if (g.cb) g.cb(CAM_EV_DETACHED, &g.src, g.arg);
    }
}

/* Called by the sync task after ATTACHED: opens the session and fills model/serial/source_id. */
esp_err_t source_ptp_identify(cam_source_t *s)
{
    if (!s || s->kind == NULL || strcmp(s->kind, "ptp")) return ESP_ERR_INVALID_ARG;
    ptp_impl_t *p = s->impl;
    esp_err_t e = ensure_session(p);
    if (e != ESP_OK) return e;
    const ptp_device_info *di = rrc_ptp_device_info(p->dev);
    char model[64], serial[64];
    scpy(model, sizeof model, di->model[0] ? di->model : "PTP camera"); model[sizeof model - 1] = 0;
    scpy(serial, sizeof serial, di->serial_number); serial[sizeof serial - 1] = 0;
    /* Nikon pads serials with leading zeros / spaces; keep as-is but trim spaces. */
    for (char *q = serial + strlen(serial); q > serial && q[-1] == ' '; q--) q[-1] = 0;
    scpy(s->model, sizeof s->model, model);
    scpy(s->serial, sizeof s->serial, serial);
    snprintf(s->source_id, sizeof s->source_id, "ptp:%s:%s", model, serial);
    return ESP_OK;
}

esp_err_t source_ptp_install(cam_event_cb_t cb, void *arg)
{
    g.cb = cb; g.arg = arg;
    return rrc_ptp_host_install(ptp_event, NULL);
}

#include "camera_source.h"
#include "usb_diag.h"
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
    bool attached;            /* g.src handed to the sync task and not yet released */
    rrc_ptp_dev_t *pending;   /* a device that appeared while `attached` */
} g;

static void attach(rrc_ptp_dev_t *dev);

/* One storage's object table for enumeration. Names live in a string pool
 * and both grow in modest chunks, so a card with tens of thousands of objects
 * never asks for one large contiguous PSRAM block (a 140-byte node times
 * 6665 objects used to, and failed once the heap was fragmented). */
typedef struct { uint64_t size; int64_t mtime; uint32_t handle, parent, name /* pool << 16 | offset */; uint16_t fmt; bool is_dir; } node_t;
#define NODE_CHUNK 2048u
#define NAME_POOL  (32u * 1024u)
#define NODE_MAX_CHUNKS 64
typedef struct {
    node_t *chunks[NODE_MAX_CHUNKS];
    char *pools[NODE_MAX_CHUNKS];
    size_t n, nchunks, npools, pool_used;
} ntab_t;

static void ntab_free(ntab_t *t)
{
    for (size_t i = 0; i < t->nchunks; i++) free(t->chunks[i]);
    for (size_t i = 0; i < t->npools; i++) free(t->pools[i]);
    memset(t, 0, sizeof *t);
}

static inline node_t *node_at(const ntab_t *t, size_t i) { return &t->chunks[i / NODE_CHUNK][i % NODE_CHUNK]; }
static inline const char *node_name(const ntab_t *t, const node_t *n) { return t->pools[n->name >> 16] + (n->name & 0xFFFF); }

/* Appends a node; NULL when memory runs out. */
static node_t *ntab_add(ntab_t *t, const char *name)
{
    size_t len = strlen(name) + 1;
    if (len > NAME_POOL) return NULL;
    if (t->npools == 0 || t->pool_used + len > NAME_POOL) {
        if (t->npools == NODE_MAX_CHUNKS) return NULL;
        char *pool = heap_caps_malloc(NAME_POOL, MALLOC_CAP_SPIRAM | MALLOC_CAP_8BIT);
        if (!pool) return NULL;
        t->pools[t->npools++] = pool; t->pool_used = 0;
    }
    if (t->n % NODE_CHUNK == 0) {
        if (t->nchunks == NODE_MAX_CHUNKS) return NULL;
        node_t *ch = heap_caps_calloc(NODE_CHUNK, sizeof *ch, MALLOC_CAP_SPIRAM | MALLOC_CAP_8BIT);
        if (!ch) return NULL;
        t->chunks[t->nchunks++] = ch;
    }
    memcpy(t->pools[t->npools - 1] + t->pool_used, name, len);
    node_t *n = node_at(t, t->n++);
    memset(n, 0, sizeof *n);
    n->name = (uint32_t)((t->npools - 1) << 16 | t->pool_used);
    t->pool_used += len;
    return n;
}

static const node_t *find_node(const ntab_t *t, uint32_t handle)
{
    for (size_t i = 0; i < t->n; i++) { const node_t *n = node_at(t, i); if (n->handle == handle) return n; }
    return NULL;
}

/* Builds "A/B/name" by walking parents (bounded depth). */
static void build_path(const ntab_t *t, const node_t *leaf, char *out, size_t cap)
{
    const char *parts[16];
    int np = 0;
    const node_t *cur = leaf;
    while (cur && np < 16) {
        parts[np++] = node_name(t, cur);
        if (cur->parent == 0 || cur->parent == 0xFFFFFFFFu) break;
        cur = find_node(t, cur->parent);
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

/* Fallback for bodies that reject GetObjectHandles(storage, all, parent=0): walk
 * the association tree from the root, one GetObjectHandles per folder. Costs one
 * extra GetObjectInfo per object (to tell folders from files); bounded. */
#define RECURSIVE_MAX_OBJECTS 20000
static esp_err_t collect_handles_recursive(ptp_impl_t *p, uint32_t storage, uint32_t **out, size_t *out_n)
{
    size_t cap = 1024, n = 0, scan = 0;
    uint32_t *all = heap_caps_malloc(cap * 4, MALLOC_CAP_SPIRAM | MALLOC_CAP_8BIT);
    if (!all) return ESP_ERR_NO_MEM;
    uint32_t *kids = NULL; size_t nk = 0;
    esp_err_t e = rrc_ptp_get_object_handles_in(p->dev, storage, 0xFFFFFFFFu, &kids, &nk);
    if (e != ESP_OK) { free(all); return e; }
    for (;;) {
        if (n + nk > cap) {
            while (n + nk > cap) cap *= 2;
            uint32_t *na = heap_caps_realloc(all, cap * 4, MALLOC_CAP_SPIRAM | MALLOC_CAP_8BIT);
            if (!na) { free(kids); free(all); return ESP_ERR_NO_MEM; }
            all = na;
        }
        memcpy(all + n, kids, nk * 4); n += nk;
        free(kids); kids = NULL; nk = 0;
        if (n > RECURSIVE_MAX_OBJECTS) { ESP_LOGW(TAG, "recursive enumeration capped at %u objects", (unsigned)n); break; }
        /* find the next unexpanded association */
        bool found = false;
        for (; scan < n; scan++) {
            ptp_object_info oi;
            if (rrc_ptp_get_object_info(p->dev, all[scan], &oi) != ESP_OK) {
                if (!rrc_ptp_is_connected(p->dev)) { free(all); return ESP_ERR_NOT_FOUND; }
                continue;
            }
            if (oi.object_format != PTP_OFC_Association) continue;
            e = rrc_ptp_get_object_handles_in(p->dev, storage, all[scan], &kids, &nk);
            scan++;
            if (e != ESP_OK) { ESP_LOGW(TAG, "GetObjectHandles(parent 0x%08x) rc=0x%04x; folder skipped", all[scan - 1], rrc_ptp_last_response(p->dev)); kids = NULL; nk = 0; continue; }
            found = true;
            break;
        }
        if (!found) break;
    }
    *out = all; *out_n = n;
    return ESP_OK;
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
        if (e != ESP_OK) {
            if (!rrc_ptp_is_connected(p->dev)) return ESP_ERR_NOT_FOUND;
            ESP_LOGW(TAG, "GetObjectHandles(0x%08x, all) rc=0x%04x; walking folders from the root instead", stores[si], rrc_ptp_last_response(p->dev));
            e = collect_handles_recursive(p, stores[si], &handles, &nh);
            if (e != ESP_OK) { ESP_LOGW(TAG, "recursive enumeration of 0x%08x failed: %s", stores[si], esp_err_to_name(e)); continue; }
        }
        ntab_t tab = {0};
        /* pass 1: object infos (names, parents, sizes) */
        for (size_t i = 0; i < nh; i++) {
            ptp_object_info oi;
            if (rrc_ptp_get_object_info(p->dev, handles[i], &oi) != ESP_OK) {
                if (!rrc_ptp_is_connected(p->dev)) { ntab_free(&tab); free(handles); return ESP_ERR_NOT_FOUND; }
                continue;
            }
            node_t *n = ntab_add(&tab, oi.filename);
            if (!n) { ntab_free(&tab); free(handles); return ESP_ERR_NO_MEM; }
            n->handle = handles[i]; n->parent = oi.parent_object; n->fmt = oi.object_format;
            n->is_dir = oi.object_format == PTP_OFC_Association;
            n->size = oi.compressed_size;
            int64_t t = rrc_parse_ptp_datetime(oi.capture_date);
            if (t <= 0) t = rrc_parse_ptp_datetime(oi.modification_date);
            n->mtime = t > 0 ? t : 0;
        }
        ESP_LOGI(TAG, "storage 0x%08x: %u objects", stores[si], (unsigned)tab.n);
        /* pass 2: emit files with reconstructed paths */
        int stop = 0;
        for (size_t i = 0; i < tab.n && !stop; i++) {
            const node_t *n = node_at(&tab, i);
            if (n->is_dir) continue;
            cam_object_t o = {0};
            scpy(o.name, sizeof o.name, node_name(&tab, n));
            build_path(&tab, n, o.path, sizeof o.path);
            o.size = n->size;
            o.mtime = n->mtime;
            o.handle = n->handle;
            stop = cb(ctx, &o);
        }
        ntab_free(&tab); free(handles);
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
            e = rrc_ptp_read_partial_direct(p->dev, h->handle, (uint32_t)(offset + total), buf + total, chunk, &g2);
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
static void ptp_release(cam_source_t *s)
{
    ptp_impl_t *p = s->impl;
    p->session = false; p->dev = NULL;
    g.attached = false;
    if (g.pending && rrc_ptp_is_connected(g.pending)) { rrc_ptp_dev_t *d = g.pending; g.pending = NULL; attach(d); }
    else g.pending = NULL;
}

static void attach(rrc_ptp_dev_t *dev)
{
    memset(&g.src, 0, sizeof g.src);
    g.impl.dev = dev; g.impl.session = false;
    g.src.kind = "ptp";
    g.src.impl = &g.impl;
    /* Model/serial are known only after OpenSession+GetDeviceInfo, which must not
     * run inside a USB callback; the sync task fills them via source_ptp_identify. */
    snprintf(g.src.source_id, sizeof g.src.source_id, "ptp:%04x:%04x", rrc_ptp_vid(dev), rrc_ptp_pid(dev));
    strcpy(g.src.model, "PTP camera");
    g.src.enumerate = ptp_enumerate; g.src.open = ptp_open; g.src.read = ptp_read; g.src.close = ptp_close;
    g.src.connected = ptp_connected; g.src.release = ptp_release;
    g.attached = true;
    if (g.cb) g.cb(CAM_EV_ATTACHED, &g.src, g.arg);
}

static void ptp_event(rrc_ptp_event_t ev, rrc_ptp_dev_t *dev, void *arg)
{
    (void)arg;
    if (ev == RRC_PTP_EV_CONNECTED) {
        if (g.attached) { ESP_LOGI(TAG, "camera re-attached before the previous source was released; queued"); g.pending = dev; return; }
        attach(dev);
    } else {
        if (dev == g.pending) { g.pending = NULL; return; }
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
    rrc_ptp_set_describe_cb(usb_diag_log_device);
    return rrc_ptp_host_install(ptp_event, NULL);
}

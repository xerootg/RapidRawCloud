#include "rrc_ptp.h"
#include <string.h>
#include <stdlib.h>
#include "esp_log.h"
#include "esp_heap_caps.h"
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/semphr.h"
#include "usb/usb_host.h"

static const char *TAG = "rrc_ptp";

#define PTP_IN_BUF   (64 * 1024)   /* bulk IN transfer buffer (internal DMA RAM) */
#define PTP_OUT_BUF  64
#define PTP_CMD_TIMEOUT_MS   8000
#define PTP_DATA_TIMEOUT_MS  20000

struct rrc_ptp_dev {
    usb_device_handle_t hdl;
    uint8_t dev_addr;
    uint8_t intf_num;
    uint8_t ep_in, ep_out, ep_int;
    uint16_t mps_in;
    uint16_t vid, pid;
    bool connected;
    bool session_open;
    uint32_t session_id;
    uint32_t tid;
    uint16_t last_rc;
    ptp_device_info info;
    usb_transfer_t *xfer_in;
    usb_transfer_t *xfer_out;
    SemaphoreHandle_t done;      /* transfer completion */
    SemaphoreHandle_t lock;      /* one transaction at a time */
};

static struct {
    usb_host_client_handle_t client;
    rrc_ptp_event_cb_t cb;
    void *cb_arg;
    rrc_ptp_dev_t dev;           /* single camera */
    bool in_use;
} g;

static void xfer_cb(usb_transfer_t *t)
{
    rrc_ptp_dev_t *d = t->context;
    xSemaphoreGive(d->done);
}

/* Submit one transfer and wait; on timeout, halt+flush+clear the endpoint so the
 * controller returns the transfer to us (CANCELED) before we reuse the buffer. */
static esp_err_t submit_wait(rrc_ptp_dev_t *d, usb_transfer_t *t, uint32_t timeout_ms)
{
    t->callback = xfer_cb;
    t->context = d;
    t->device_handle = d->hdl;
    xSemaphoreTake(d->done, 0); /* drain stale give */
    esp_err_t err = usb_host_transfer_submit(t);
    if (err != ESP_OK) return err;
    if (xSemaphoreTake(d->done, pdMS_TO_TICKS(timeout_ms)) != pdTRUE) {
        ESP_LOGW(TAG, "transfer timeout on ep 0x%02x", t->bEndpointAddress);
        usb_host_endpoint_halt(d->hdl, t->bEndpointAddress);
        usb_host_endpoint_flush(d->hdl, t->bEndpointAddress);
        xSemaphoreTake(d->done, pdMS_TO_TICKS(1000));
        usb_host_endpoint_clear(d->hdl, t->bEndpointAddress);
        return ESP_ERR_TIMEOUT;
    }
    switch (t->status) {
    case USB_TRANSFER_STATUS_COMPLETED: return ESP_OK;
    case USB_TRANSFER_STATUS_STALL:
        ESP_LOGW(TAG, "stall on ep 0x%02x", t->bEndpointAddress);
        usb_host_endpoint_clear(d->hdl, t->bEndpointAddress);
        return ESP_ERR_INVALID_RESPONSE;
    case USB_TRANSFER_STATUS_NO_DEVICE: d->connected = false; return ESP_ERR_NOT_FOUND;
    default:
        ESP_LOGW(TAG, "transfer status %d on ep 0x%02x", t->status, t->bEndpointAddress);
        return ESP_FAIL;
    }
}

static esp_err_t bulk_out(rrc_ptp_dev_t *d, const uint8_t *data, size_t len)
{
    if (len > d->xfer_out->data_buffer_size) return ESP_ERR_INVALID_SIZE;
    memcpy(d->xfer_out->data_buffer, data, len);
    d->xfer_out->num_bytes = (int)len;
    d->xfer_out->bEndpointAddress = d->ep_out;
    return submit_wait(d, d->xfer_out, PTP_CMD_TIMEOUT_MS);
}

/* Reads one bulk IN transfer of up to `max` bytes (rounded down to a multiple of MPS). */
static esp_err_t bulk_in(rrc_ptp_dev_t *d, size_t max, size_t *got, uint32_t timeout_ms)
{
    size_t n = max > d->xfer_in->data_buffer_size ? d->xfer_in->data_buffer_size : max;
    n -= n % d->mps_in;
    if (n == 0) n = d->mps_in;
    d->xfer_in->num_bytes = (int)n;
    d->xfer_in->bEndpointAddress = d->ep_in;
    esp_err_t err = submit_wait(d, d->xfer_in, timeout_ms);
    *got = err == ESP_OK ? (size_t)d->xfer_in->actual_num_bytes : 0;
    return err;
}

typedef int (*data_sink_t)(void *ctx, const uint8_t *p, size_t n);

/* Full PTP transaction: command → [data-in phase] → response.
 * Data payload chunks go to `sink`. Response params are returned (up to 5). */
static esp_err_t transact(rrc_ptp_dev_t *d, uint16_t code, const uint32_t *params, size_t np, data_sink_t sink, void *ctx,
                          uint32_t *resp_params, size_t *n_resp)
{
    if (!d->connected) return ESP_ERR_NOT_FOUND;
    xSemaphoreTake(d->lock, portMAX_DELAY);
    uint32_t tid = d->tid++;
    uint8_t cmd[PTP_HDR_LEN + 20];
    size_t clen = ptp_encode_command(cmd, code, tid, params, np);
    esp_err_t err = bulk_out(d, cmd, clen);
    if (err != ESP_OK) goto out;

    bool have_response = false;
    uint32_t data_total = 0, data_recv = 0;
    bool in_data = false;
    uint32_t timeout = PTP_DATA_TIMEOUT_MS;
    for (int guard = 0; guard < 1000000 && !have_response; guard++) {
        size_t want = in_data ? (size_t)(data_total - data_recv) : 1024;
        if (in_data && data_total == 0xFFFFFFFFu) want = PTP_IN_BUF;
        if (in_data) want = (want + d->mps_in - 1) / d->mps_in * d->mps_in;
        size_t got = 0;
        err = bulk_in(d, want, &got, timeout);
        if (err != ESP_OK) goto out;
        if (got == 0) continue; /* ZLP terminating an MPS-aligned data phase */
        const uint8_t *p = d->xfer_in->data_buffer;
        if (!in_data) {
            ptp_container_hdr h;
            if (!ptp_decode_hdr(p, got, &h) || h.transaction_id != tid) {
                ESP_LOGW(TAG, "unexpected container (len=%u type=%u tid=%u want %u)", got ? (unsigned)h.length : 0, got ? h.type : 0, got ? h.transaction_id : 0, tid);
                err = ESP_ERR_INVALID_RESPONSE;
                goto out;
            }
            if (h.type == PTP_CT_DATA) {
                in_data = true;
                data_total = h.length;
                size_t payload = got - PTP_HDR_LEN;
                if (sink && payload && sink(ctx, p + PTP_HDR_LEN, payload)) { err = ESP_ERR_NO_MEM; goto out; }
                data_recv = (uint32_t)got;
                if (data_total != 0xFFFFFFFFu && data_recv >= data_total) in_data = false;
                /* A short packet already ended the data phase when the whole container fit. */
                continue;
            }
            if (h.type == PTP_CT_RESPONSE) {
                d->last_rc = h.code;
                size_t n = (got - PTP_HDR_LEN) / 4;
                if (n > 5) n = 5;
                if (n_resp) *n_resp = n;
                if (resp_params) for (size_t i = 0; i < n; i++) resp_params[i] = (uint32_t)p[12 + 4 * i] | ((uint32_t)p[13 + 4 * i] << 8) | ((uint32_t)p[14 + 4 * i] << 16) | ((uint32_t)p[15 + 4 * i] << 24);
                have_response = true;
                err = (h.code == PTP_RC_OK) ? ESP_OK : ESP_ERR_INVALID_RESPONSE;
                break;
            }
            /* Event containers on the bulk pipe are not expected; ignore. */
            continue;
        }
        /* continuation of the data phase */
        if (sink && sink(ctx, p, got)) { err = ESP_ERR_NO_MEM; goto out; }
        data_recv += (uint32_t)got;
        if (data_total != 0xFFFFFFFFu) {
            if (data_recv >= data_total) in_data = false;
        } else if (got < want) {
            in_data = false; /* short packet ends an unknown-length phase */
        }
    }
    if (!have_response && err == ESP_OK) err = ESP_ERR_TIMEOUT;
out:
    xSemaphoreGive(d->lock);
    return err;
}

/* ---- growable collector for small data phases ---------------------------- */
typedef struct { uint8_t *buf; size_t len, cap; } collector_t;
static int collect(void *ctx, const uint8_t *p, size_t n)
{
    collector_t *c = ctx;
    if (c->len + n > c->cap) {
        size_t ncap = c->cap ? c->cap * 2 : 4096;
        while (ncap < c->len + n) ncap *= 2;
        uint8_t *nb = heap_caps_realloc(c->buf, ncap, MALLOC_CAP_SPIRAM | MALLOC_CAP_8BIT);
        if (!nb) nb = realloc(c->buf, ncap);
        if (!nb) return -1;
        c->buf = nb; c->cap = ncap;
    }
    memcpy(c->buf + c->len, p, n);
    c->len += n;
    return 0;
}

/* ---- public ops --------------------------------------------------------- */

esp_err_t rrc_ptp_open(rrc_ptp_dev_t *d)
{
    if (!d->connected) return ESP_ERR_NOT_FOUND;
    uint32_t p[1] = {1};
    d->tid = 0;
    esp_err_t err = transact(d, PTP_OC_OpenSession, p, 1, NULL, NULL, NULL, NULL);
    if (err != ESP_OK && d->last_rc == PTP_RC_SessionAlreadyOpen) {
        ESP_LOGW(TAG, "session already open — closing and reopening");
        transact(d, PTP_OC_CloseSession, NULL, 0, NULL, NULL, NULL, NULL);
        d->tid = 0;
        err = transact(d, PTP_OC_OpenSession, p, 1, NULL, NULL, NULL, NULL);
    }
    if (err != ESP_OK) { ESP_LOGE(TAG, "OpenSession failed rc=0x%04x", d->last_rc); return err; }
    d->session_open = true;
    d->session_id = 1;
    collector_t c = {0};
    err = transact(d, PTP_OC_GetDeviceInfo, NULL, 0, collect, &c, NULL, NULL);
    if (err == ESP_OK && !ptp_decode_device_info(c.buf, c.len, &d->info)) err = ESP_ERR_INVALID_RESPONSE;
    free(c.buf);
    if (err == ESP_OK) {
        ESP_LOGI(TAG, "camera: %s %s fw %s sn %s (partial=%d)", d->info.manufacturer, d->info.model, d->info.device_version,
                 d->info.serial_number, d->info.supports_partial_object);
    }
    return err;
}

esp_err_t rrc_ptp_close(rrc_ptp_dev_t *d)
{
    if (!d->connected || !d->session_open) return ESP_OK;
    esp_err_t err = transact(d, PTP_OC_CloseSession, NULL, 0, NULL, NULL, NULL, NULL);
    d->session_open = false;
    return err;
}

bool rrc_ptp_is_connected(const rrc_ptp_dev_t *d) { return d && d->connected; }
const ptp_device_info *rrc_ptp_device_info(const rrc_ptp_dev_t *d) { return &d->info; }
uint16_t rrc_ptp_vid(const rrc_ptp_dev_t *d) { return d->vid; }
uint16_t rrc_ptp_pid(const rrc_ptp_dev_t *d) { return d->pid; }
uint16_t rrc_ptp_last_response(const rrc_ptp_dev_t *d) { return d->last_rc; }

esp_err_t rrc_ptp_get_storage_ids(rrc_ptp_dev_t *d, uint32_t *ids, size_t cap, size_t *count)
{
    collector_t c = {0};
    esp_err_t err = transact(d, PTP_OC_GetStorageIDs, NULL, 0, collect, &c, NULL, NULL);
    if (err == ESP_OK) {
        int n = ptp_decode_u32_array(c.buf, c.len, ids, cap);
        if (n < 0) err = ESP_ERR_INVALID_RESPONSE;
        else *count = (size_t)n < cap ? (size_t)n : cap;
    }
    free(c.buf);
    return err;
}

esp_err_t rrc_ptp_get_object_handles(rrc_ptp_dev_t *d, uint32_t storage_id, uint32_t **handles, size_t *count)
{
    uint32_t p[3] = {storage_id, 0x00000000, 0x00000000}; /* all formats, all parents */
    collector_t c = {0};
    esp_err_t err = transact(d, PTP_OC_GetObjectHandles, p, 3, collect, &c, NULL, NULL);
    if (err != ESP_OK) { free(c.buf); return err; }
    int n = ptp_decode_u32_array(c.buf, c.len, NULL, 0);
    if (n < 0) { free(c.buf); return ESP_ERR_INVALID_RESPONSE; }
    uint32_t *out = heap_caps_malloc((size_t)(n ? n : 1) * 4, MALLOC_CAP_SPIRAM | MALLOC_CAP_8BIT);
    if (!out) out = malloc((size_t)(n ? n : 1) * 4);
    if (!out) { free(c.buf); return ESP_ERR_NO_MEM; }
    ptp_decode_u32_array(c.buf, c.len, out, (size_t)n);
    free(c.buf);
    *handles = out;
    *count = (size_t)n;
    return ESP_OK;
}

esp_err_t rrc_ptp_get_object_info(rrc_ptp_dev_t *d, uint32_t handle, ptp_object_info *oi)
{
    uint32_t p[1] = {handle};
    collector_t c = {0};
    esp_err_t err = transact(d, PTP_OC_GetObjectInfo, p, 1, collect, &c, NULL, NULL);
    if (err == ESP_OK && !ptp_decode_object_info(c.buf, c.len, oi)) err = ESP_ERR_INVALID_RESPONSE;
    free(c.buf);
    return err;
}

typedef struct { uint8_t *out; size_t cap, len; } span_t;
static int span_sink(void *ctx, const uint8_t *p, size_t n)
{
    span_t *s = ctx;
    if (s->len + n > s->cap) n = s->cap - s->len; /* tolerate a device returning more than asked */
    memcpy(s->out + s->len, p, n);
    s->len += n;
    return 0;
}

esp_err_t rrc_ptp_read_partial(rrc_ptp_dev_t *d, uint32_t handle, uint32_t offset, uint8_t *out, size_t want, size_t *got)
{
    uint32_t p[3] = {handle, offset, (uint32_t)want};
    span_t s = {.out = out, .cap = want};
    uint32_t rp[5]; size_t nrp = 0;
    esp_err_t err = transact(d, PTP_OC_GetPartialObject, p, 3, span_sink, &s, rp, &nrp);
    *got = s.len;
    return err;
}

/* ---- device matching / lifecycle ----------------------------------------- */

static bool match_interface(const usb_config_desc_t *cfg, rrc_ptp_dev_t *d)
{
    int off = 0;
    const usb_standard_desc_t *cur = (const usb_standard_desc_t *)cfg;
    while ((cur = usb_parse_next_descriptor_of_type(cur, cfg->wTotalLength, USB_B_DESCRIPTOR_TYPE_INTERFACE, &off)) != NULL) {
        const usb_intf_desc_t *intf = (const usb_intf_desc_t *)cur;
        bool still_image = intf->bInterfaceClass == 0x06 && intf->bInterfaceSubClass == 0x01 && intf->bInterfaceProtocol == 0x01;
        /* Some bodies expose MTP as vendor-specific (0xFF) with the same 2 bulk + 1 interrupt endpoint shape. */
        bool vendor_mtp_shape = intf->bInterfaceClass == 0xFF && intf->bNumEndpoints == 3;
        if (!still_image && !vendor_mtp_shape) continue;
        uint8_t ep_in = 0, ep_out = 0, ep_int = 0; uint16_t mps_in = 512;
        for (int i = 0; i < intf->bNumEndpoints; i++) {
            int eoff = off;
            const usb_ep_desc_t *ep = usb_parse_endpoint_descriptor_by_index(intf, i, cfg->wTotalLength, &eoff);
            if (!ep) break;
            bool in = USB_EP_DESC_GET_EP_DIR(ep);
            int type = ep->bmAttributes & USB_BM_ATTRIBUTES_XFERTYPE_MASK;
            if (type == USB_BM_ATTRIBUTES_XFER_BULK) {
                if (in) { ep_in = ep->bEndpointAddress; mps_in = USB_EP_DESC_GET_MPS(ep); }
                else ep_out = ep->bEndpointAddress;
            } else if (type == USB_BM_ATTRIBUTES_XFER_INT && in) {
                ep_int = ep->bEndpointAddress;
            }
        }
        if (ep_in && ep_out) {
            d->intf_num = intf->bInterfaceNumber;
            d->ep_in = ep_in; d->ep_out = ep_out; d->ep_int = ep_int; d->mps_in = mps_in;
            return true;
        }
    }
    return false;
}

static void handle_new_device(uint8_t addr)
{
    if (g.in_use) { ESP_LOGW(TAG, "second device (addr %u) ignored: one camera at a time", addr); return; }
    rrc_ptp_dev_t *d = &g.dev;
    memset(d, 0, sizeof *d);
    d->done = xSemaphoreCreateBinary();
    d->lock = xSemaphoreCreateMutex();
    if (usb_host_device_open(g.client, addr, &d->hdl) != ESP_OK) { ESP_LOGE(TAG, "device_open failed"); goto fail; }
    const usb_device_desc_t *dd;
    const usb_config_desc_t *cfg;
    if (usb_host_get_device_descriptor(d->hdl, &dd) != ESP_OK || usb_host_get_active_config_descriptor(d->hdl, &cfg) != ESP_OK) goto close;
    d->vid = dd->idVendor; d->pid = dd->idProduct;
    if (dd->bDeviceClass == 0x08) goto close; /* mass storage: the MSC driver's */
    if (!match_interface(cfg, d)) { ESP_LOGD(TAG, "addr %u (%04x:%04x) is not a PTP device", addr, d->vid, d->pid); goto close; }
    if (usb_host_interface_claim(g.client, d->hdl, d->intf_num, 0) != ESP_OK) { ESP_LOGE(TAG, "interface_claim failed"); goto close; }
    if (usb_host_transfer_alloc(PTP_IN_BUF, 0, &d->xfer_in) != ESP_OK || usb_host_transfer_alloc(PTP_OUT_BUF, 0, &d->xfer_out) != ESP_OK) {
        ESP_LOGE(TAG, "transfer_alloc failed"); goto release;
    }
    d->dev_addr = addr;
    d->connected = true;
    g.in_use = true;
    ESP_LOGI(TAG, "PTP camera attached: %04x:%04x intf %u ep_in 0x%02x ep_out 0x%02x mps %u", d->vid, d->pid, d->intf_num, d->ep_in, d->ep_out, d->mps_in);
    if (g.cb) g.cb(RRC_PTP_EV_CONNECTED, d, g.cb_arg);
    return;
release:
    usb_host_interface_release(g.client, d->hdl, d->intf_num);
close:
    usb_host_device_close(g.client, d->hdl);
fail:
    if (d->xfer_in) usb_host_transfer_free(d->xfer_in);
    if (d->xfer_out) usb_host_transfer_free(d->xfer_out);
    if (d->done) vSemaphoreDelete(d->done);
    if (d->lock) vSemaphoreDelete(d->lock);
    memset(d, 0, sizeof *d);
}

static void handle_device_gone(usb_device_handle_t hdl)
{
    rrc_ptp_dev_t *d = &g.dev;
    if (!g.in_use || d->hdl != hdl) return;
    ESP_LOGI(TAG, "PTP camera detached");
    d->connected = false;
    d->session_open = false;
    if (g.cb) g.cb(RRC_PTP_EV_DISCONNECTED, d, g.cb_arg);
    /* Wait for any in-flight transaction to observe NO_DEVICE and release the lock. */
    xSemaphoreTake(d->lock, pdMS_TO_TICKS(5000));
    usb_host_interface_release(g.client, d->hdl, d->intf_num);
    usb_host_device_close(g.client, d->hdl);
    usb_host_transfer_free(d->xfer_in);
    usb_host_transfer_free(d->xfer_out);
    xSemaphoreGive(d->lock);
    vSemaphoreDelete(d->lock);
    vSemaphoreDelete(d->done);
    memset(d, 0, sizeof *d);
    g.in_use = false;
}

static void client_event_cb(const usb_host_client_event_msg_t *msg, void *arg)
{
    (void)arg;
    switch (msg->event) {
    case USB_HOST_CLIENT_EVENT_NEW_DEV: handle_new_device(msg->new_dev.address); break;
    case USB_HOST_CLIENT_EVENT_DEV_GONE: handle_device_gone(msg->dev_gone.dev_hdl); break;
    default: break;
    }
}

static void client_task(void *arg)
{
    (void)arg;
    for (;;) usb_host_client_handle_events(g.client, portMAX_DELAY);
}

esp_err_t rrc_ptp_host_install(rrc_ptp_event_cb_t cb, void *arg)
{
    g.cb = cb; g.cb_arg = arg;
    const usb_host_client_config_t cc = {
        .is_synchronous = false,
        .max_num_event_msg = 8,
        .async = {.client_event_callback = client_event_cb, .callback_arg = NULL},
    };
    esp_err_t err = usb_host_client_register(&cc, &g.client);
    if (err != ESP_OK) return err;
    if (xTaskCreate(client_task, "ptp_client", 4096, NULL, 6, NULL) != pdPASS) return ESP_ERR_NO_MEM;
    return ESP_OK;
}

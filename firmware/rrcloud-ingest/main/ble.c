/*
 * ble — NimBLE peripheral exposing the admin API over one GATT service.
 *
 * Characteristics (UUIDs from the protocol definition, rrcloud_proto.h):
 *   RX   write (+ without response), encrypted link required: request fragments
 *   TX   notify: reply fragments
 *   INFO read, no pairing: DockBleInfo so a scanner can identify the dock
 *
 * Framing (DOCK_RPC_FLAG_*): byte 0 = flags | tag<<4; a FIRST fragment carries
 * the total length (u16 LE) before its payload. Requests are reassembled and
 * handed to a worker task so the NimBLE host task never blocks on the API; the
 * reply is fragmented to ATT_MTU-3 and notified. Security is LE Secure
 * Connections "Just Works" bonding (the dock has no display) plus the same
 * admin-password gate as the web UI when `admin_auth` is on.
 */
#include "ble.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "esp_log.h"
#include "esp_app_desc.h"
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/queue.h"
#include "freertos/semphr.h"
#include "cJSON.h"
#include "esp_hosted_bt_host_stack.h"
#include "nimble/nimble_port.h"
#include "nimble/nimble_port_freertos.h"
#include "host/ble_hs.h"
#include "host/ble_uuid.h"
#include "host/util/util.h"
#include "services/gap/ble_svc_gap.h"
#include "services/gatt/ble_svc_gatt.h"
#include "store/config/ble_store_config.h"

void ble_store_config_init(void); /* NimBLE store glue; not exported by a header in IDF */
#include "rrcloud_proto.h"
#include "api.h"
#include "app_config.h"
#include "coproc.h"
#include "log_ring.h"
#include "rrc_proto.h"
#include "util.h"

static const char *TAG = "ble";

#define RPC_QUEUE_DEPTH 4
#define FRAG_HDR_FIRST 3
#define FRAG_HDR_CONT 1
#define TX_FRAG_MAX 244u

static struct {
    bool up, advertising, connected, encrypted, starting;
    uint16_t conn;
    uint16_t mtu;
    uint8_t own_addr_type;
    char addr[18];
    char name[48];
    char error[96];
} st;
static SemaphoreHandle_t mtx;
#define LOCK() xSemaphoreTake(mtx, portMAX_DELAY)
#define UNLOCK() xSemaphoreGive(mtx)

static ble_uuid_any_t svc_uuid, rx_uuid, tx_uuid, info_uuid;
static uint16_t tx_handle;

/* request reassembly (one connection) */
static struct { uint8_t *buf; size_t len, total; uint8_t tag; bool open; } asm_;
typedef struct { uint16_t conn; char *data; size_t len; } rpc_msg_t;
static QueueHandle_t rpc_q;

static void set_error(const char *msg) { LOCK(); scpy(st.error, sizeof st.error, msg); UNLOCK(); }

/* ---- identity ---------------------------------------------------------------- */
static int info_json(char *out, size_t cap)
{
    rrcp_dock_ble_info_t d;
    memset(&d, 0, sizeof d);
    d.proto = 1;
    scpy(d.device_id, sizeof d.device_id, app_device_id());
    scpy(d.name, sizeof d.name, app_config_get()->device_name);
    scpy(d.hostname, sizeof d.hostname, app_config_get()->hostname);
    scpy(d.version, sizeof d.version, esp_app_get_description()->version);
    return rrcp_dock_ble_info_encode(&d, out, cap);
}

/* ---- reply framing ----------------------------------------------------------- */
static int notify_fragment(uint16_t conn, const uint8_t *hdr, size_t hl, const uint8_t *payload, size_t pl)
{
    for (int attempt = 0; attempt < 50; attempt++) {
        struct os_mbuf *om = ble_hs_mbuf_from_flat(hdr, (uint16_t)hl);
        if (!om) { vTaskDelay(pdMS_TO_TICKS(10)); continue; }
        if (pl && os_mbuf_append(om, payload, (uint16_t)pl) != 0) { os_mbuf_free_chain(om); vTaskDelay(pdMS_TO_TICKS(10)); continue; }
        int rc = ble_gatts_notify_custom(conn, tx_handle, om); /* consumes om */
        if (rc == 0) return 0;
        if (rc != BLE_HS_ENOMEM && rc != BLE_HS_EBUSY) return rc;
        vTaskDelay(pdMS_TO_TICKS(10));
    }
    return BLE_HS_ETIMEOUT;
}

static int send_message(uint16_t conn, uint8_t tag, const uint8_t *msg, size_t len)
{
    uint16_t mtu = ble_att_mtu(conn);
    size_t max = mtu > 3 ? (size_t)mtu - 3 : 20;
    /* Keep every notification inside one LE ACL packet (251-byte LL payload
     * minus the L2CAP and ATT headers): notifications that NimBLE has to
     * split into several HCI ACL fragments never left the ESP32-C6 over the
     * hosted HCI pipe (observed with ESP-Hosted 3.0.9 / IDF 5.5). */
    if (max > TX_FRAG_MAX) max = TX_FRAG_MAX;
    size_t off = 0;
    bool first = true;
    do {
        uint8_t hdr[3];
        size_t hl = first ? FRAG_HDR_FIRST : FRAG_HDR_CONT;
        size_t room = max > hl ? max - hl : 1;
        size_t chunk = len - off < room ? len - off : room;
        bool last = off + chunk >= len;
        hdr[0] = (uint8_t)((first ? RRCP_DOCK_RPC_FLAG_FIRST : 0) | (last ? RRCP_DOCK_RPC_FLAG_LAST : 0) | ((tag & 0xF) << 4));
        if (first) { hdr[1] = (uint8_t)(len & 0xFF); hdr[2] = (uint8_t)((len >> 8) & 0xFF); }
        int rc = notify_fragment(conn, hdr, hl, msg + off, chunk);
        if (rc != 0) return rc;
        off += chunk;
        first = false;
    } while (off < len);
    return 0;
}

/* ---- RPC worker ---------------------------------------------------------------- */
static void rpc_task(void *arg)
{
    (void)arg;
    rpc_msg_t m;
    for (;;) {
        if (xQueueReceive(rpc_q, &m, portMAX_DELAY) != pdTRUE) continue;
        uint32_t id = 0;
        int status = 400;
        char *reply_body = NULL;
        api_resp_t r = {0};
        cJSON *o = cJSON_ParseWithLength(m.data, m.len);
        if (!o) {
            r.json = strdup("{\"ok\":false,\"error\":\"request is not JSON\"}");
        } else {
            cJSON *jid = cJSON_GetObjectItemCaseSensitive(o, "id");
            cJSON *jm = cJSON_GetObjectItemCaseSensitive(o, "m");
            cJSON *jp = cJSON_GetObjectItemCaseSensitive(o, "p");
            cJSON *jb = cJSON_GetObjectItemCaseSensitive(o, "b");
            cJSON *ja = cJSON_GetObjectItemCaseSensitive(o, "auth");
            if (cJSON_IsNumber(jid)) id = (uint32_t)jid->valuedouble;
            char *body = jb ? cJSON_PrintUnformatted(jb) : NULL;
            if (!api_password_ok(cJSON_IsString(ja) ? ja->valuestring : NULL)) {
                r.status = 401;
                r.json = strdup("{\"ok\":false,\"error\":\"auth required: send the admin password as \\\"auth\\\"\"}");
            } else {
                api_dispatch(cJSON_IsString(jm) ? jm->valuestring : "", cJSON_IsString(jp) ? jp->valuestring : "", body, body ? strlen(body) : 0, &r);
            }
            free(body);
            cJSON_Delete(o);
        }
        status = r.status ? r.status : 500;
        const char *inner = r.json ? r.json : "{\"ok\":false,\"error\":\"oom\"}";
        size_t inner_len = r.json ? r.len : strlen(inner);
        size_t cap = inner_len + 48;
        reply_body = malloc(cap);
        if (reply_body) {
            int n = snprintf(reply_body, cap, "{\"id\":%lu,\"s\":%d,\"b\":", (unsigned long)id, status);
            memcpy(reply_body + n, inner, inner_len);
            reply_body[n + inner_len] = '}';
            size_t total = (size_t)n + inner_len + 1;
            int rc = send_message(m.conn, (uint8_t)id, (const uint8_t *)reply_body, total);
            if (rc != 0) ESP_LOGW(TAG, "notify failed rc=%d (id %lu, %u bytes)", rc, (unsigned long)id, (unsigned)total);
            free(reply_body);
        }
        api_resp_free(&r);
        free(m.data);
    }
}

/* ---- GATT ------------------------------------------------------------------------ */
static int rx_write(uint16_t conn, struct os_mbuf *om)
{
    uint16_t len = OS_MBUF_PKTLEN(om);
    if (len < 1 || len > 520) return BLE_ATT_ERR_INVALID_ATTR_VALUE_LEN;
    uint8_t frag[520];
    if (ble_hs_mbuf_to_flat(om, frag, sizeof frag, &len) != 0) return BLE_ATT_ERR_UNLIKELY;
    uint8_t flags = frag[0];
    uint8_t tag = flags >> 4;
    const uint8_t *payload = frag + 1;
    size_t plen = len - 1;
    if (flags & RRCP_DOCK_RPC_FLAG_FIRST) {
        if (len < FRAG_HDR_FIRST) return BLE_ATT_ERR_INVALID_ATTR_VALUE_LEN;
        size_t total = (size_t)frag[1] | ((size_t)frag[2] << 8);
        if (total == 0 || total > RRCP_DOCK_RPC_MAX_REQUEST_BYTES) return BLE_ATT_ERR_INVALID_ATTR_VALUE_LEN;
        free(asm_.buf);
        asm_.buf = malloc(total + 1);
        if (!asm_.buf) return BLE_ATT_ERR_INSUFFICIENT_RES;
        asm_.len = 0; asm_.total = total; asm_.tag = tag; asm_.open = true;
        payload = frag + FRAG_HDR_FIRST;
        plen = len - FRAG_HDR_FIRST;
    } else if (!asm_.open || asm_.tag != tag) {
        return BLE_ATT_ERR_UNLIKELY; /* stray continuation */
    }
    if (asm_.len + plen > asm_.total) { asm_.open = false; return BLE_ATT_ERR_INVALID_ATTR_VALUE_LEN; }
    memcpy(asm_.buf + asm_.len, payload, plen);
    asm_.len += plen;
    if (flags & RRCP_DOCK_RPC_FLAG_LAST) {
        asm_.open = false;
        if (asm_.len != asm_.total) { ESP_LOGW(TAG, "short request: %u of %u bytes", (unsigned)asm_.len, (unsigned)asm_.total); return BLE_ATT_ERR_INVALID_ATTR_VALUE_LEN; }
        asm_.buf[asm_.len] = 0;
        rpc_msg_t m = {.conn = conn, .data = (char *)asm_.buf, .len = asm_.len};
        asm_.buf = NULL;
        if (xQueueSend(rpc_q, &m, 0) != pdTRUE) { free(m.data); return BLE_ATT_ERR_INSUFFICIENT_RES; }
    }
    return 0;
}

static int chr_access(uint16_t conn, uint16_t attr, struct ble_gatt_access_ctxt *ctxt, void *arg)
{
    (void)attr;
    char which = (char)(uintptr_t)arg;
    if (ctxt->op == BLE_GATT_ACCESS_OP_READ_CHR && which == 'I') {
        char info[320];
        int n = info_json(info, sizeof info);
        if (n < 0) return BLE_ATT_ERR_UNLIKELY;
        return os_mbuf_append(ctxt->om, info, (uint16_t)n) == 0 ? 0 : BLE_ATT_ERR_INSUFFICIENT_RES;
    }
    if (ctxt->op == BLE_GATT_ACCESS_OP_WRITE_CHR && which == 'R') return rx_write(conn, ctxt->om);
    return BLE_ATT_ERR_UNLIKELY;
}

static const struct ble_gatt_svc_def gatt_svcs[] = {
    {
        .type = BLE_GATT_SVC_TYPE_PRIMARY,
        .uuid = &svc_uuid.u,
        .characteristics = (struct ble_gatt_chr_def[]) {
            {.uuid = &rx_uuid.u, .access_cb = chr_access, .arg = (void *)'R',
             .flags = BLE_GATT_CHR_F_WRITE | BLE_GATT_CHR_F_WRITE_NO_RSP | BLE_GATT_CHR_F_WRITE_ENC},
            {.uuid = &tx_uuid.u, .access_cb = chr_access, .arg = (void *)'T', .val_handle = &tx_handle,
             .flags = BLE_GATT_CHR_F_NOTIFY},
            {.uuid = &info_uuid.u, .access_cb = chr_access, .arg = (void *)'I',
             .flags = BLE_GATT_CHR_F_READ},
            {0},
        },
    },
    {0},
};

/* ---- GAP ---------------------------------------------------------------------- */
static int gap_event(struct ble_gap_event *ev, void *arg);

static void advertise(void)
{
    if (!app_config_get()->ble_enabled) return;
    struct ble_hs_adv_fields f;
    memset(&f, 0, sizeof f);
    f.flags = BLE_HS_ADV_F_DISC_GEN | BLE_HS_ADV_F_BREDR_UNSUP;
    f.uuids128 = &svc_uuid.u128;
    f.num_uuids128 = 1;
    f.uuids128_is_complete = 1;
    int rc = ble_gap_adv_set_fields(&f);
    if (rc != 0) { ESP_LOGE(TAG, "adv fields rc=%d", rc); return; }
    struct ble_hs_adv_fields sr;
    memset(&sr, 0, sizeof sr);
    sr.name = (const uint8_t *)st.name;
    sr.name_len = (uint8_t)strlen(st.name);
    sr.name_is_complete = 1;
    rc = ble_gap_adv_rsp_set_fields(&sr);
    if (rc != 0) { ESP_LOGE(TAG, "scan response rc=%d", rc); return; }
    struct ble_gap_adv_params p;
    memset(&p, 0, sizeof p);
    p.conn_mode = BLE_GAP_CONN_MODE_UND;
    p.disc_mode = BLE_GAP_DISC_MODE_GEN;
    rc = ble_gap_adv_start(st.own_addr_type, NULL, BLE_HS_FOREVER, &p, gap_event, NULL);
    if (rc != 0 && rc != BLE_HS_EALREADY) { ESP_LOGE(TAG, "adv start rc=%d", rc); return; }
    LOCK(); st.advertising = true; UNLOCK();
}

static int gap_event(struct ble_gap_event *ev, void *arg)
{
    (void)arg;
    switch (ev->type) {
    case BLE_GAP_EVENT_CONNECT:
        if (ev->connect.status == 0) {
            LOCK(); st.connected = true; st.encrypted = false; st.conn = ev->connect.conn_handle; st.advertising = false; st.mtu = ble_att_mtu(ev->connect.conn_handle); UNLOCK();
            log_ring_printf("ble: central connected");
            /* The central (phone) initiates the ATT MTU exchange; ATT_PREFERRED_MTU answers it. */
        } else {
            advertise();
        }
        return 0;
    case BLE_GAP_EVENT_DISCONNECT:
        LOCK(); st.connected = false; st.encrypted = false; st.conn = BLE_HS_CONN_HANDLE_NONE; UNLOCK();
        free(asm_.buf); asm_.buf = NULL; asm_.open = false;
        log_ring_printf("ble: central disconnected (reason %d)", ev->disconnect.reason);
        advertise();
        return 0;
    case BLE_GAP_EVENT_ADV_COMPLETE:
        LOCK(); st.advertising = false; UNLOCK();
        advertise();
        return 0;
    case BLE_GAP_EVENT_ENC_CHANGE:
        LOCK(); st.encrypted = ev->enc_change.status == 0; UNLOCK();
        log_ring_printf("ble: link %s", ev->enc_change.status == 0 ? "encrypted (bonded)" : "encryption failed");
        return 0;
    case BLE_GAP_EVENT_MTU:
        LOCK(); st.mtu = ev->mtu.value; UNLOCK();
        ESP_LOGI(TAG, "mtu %u", ev->mtu.value);
        return 0;
    case BLE_GAP_EVENT_REPEAT_PAIRING: {
        /* The phone forgot the bond: drop ours and pair again. */
        struct ble_gap_conn_desc d;
        if (ble_gap_conn_find(ev->repeat_pairing.conn_handle, &d) == 0) ble_store_util_delete_peer(&d.peer_id_addr);
        return BLE_GAP_REPEAT_PAIRING_RETRY;
    }
    case BLE_GAP_EVENT_SUBSCRIBE:
        ESP_LOGI(TAG, "subscribe attr %u notify=%d", ev->subscribe.attr_handle, ev->subscribe.cur_notify);
        return 0;
    default:
        return 0;
    }
}

static void on_reset(int reason) { ESP_LOGW(TAG, "host reset, reason %d", reason); }

static void on_sync(void)
{
    int rc = ble_hs_util_ensure_addr(0);
    if (rc != 0) { ESP_LOGE(TAG, "ensure_addr rc=%d", rc); return; }
    rc = ble_hs_id_infer_auto(0, &st.own_addr_type);
    if (rc != 0) { ESP_LOGE(TAG, "infer_auto rc=%d", rc); return; }
    uint8_t a[6] = {0};
    ble_hs_id_copy_addr(st.own_addr_type, a, NULL);
    LOCK();
    snprintf(st.addr, sizeof st.addr, "%02x:%02x:%02x:%02x:%02x:%02x", a[5], a[4], a[3], a[2], a[1], a[0]);
    st.up = true;
    UNLOCK();
    log_ring_printf("ble: ready as \"%s\" (%s)%s", st.name, st.addr, app_config_get()->ble_enabled ? "" : " — disabled in settings");
    advertise();
}

/* IDF 5.5.0 NimBLE: a Client Supported Features write on a bonded link first
 * deletes the previous CSFC record and turns the store's "no such record"
 * (BLE_HS_ENOENT == 5) into ATT error 0x05 "Insufficient Authentication".
 * BlueZ and Android then escalate to authenticated (MITM) pairing, which a
 * display-less dock cannot satisfy, and every bonded reconnect fails. Nothing to
 * delete is not an error. */
static int store_delete_tolerant(int obj_type, const union ble_store_key *key)
{
    int rc = ble_store_config_delete(obj_type, key);
    if (rc == BLE_HS_ENOENT && obj_type == BLE_STORE_OBJ_TYPE_CSFC) return 0;
    return rc;
}

static void host_task(void *arg)
{
    (void)arg;
    nimble_port_run();
    nimble_port_freertos_deinit();
}

static void refresh_name(void)
{
    LOCK();
    const char *host = app_config_get()->hostname;
    /* "rrcloud-ingest" already carries the prefix: advertise it as-is. */
    if (!strncmp(host, RRCP_DOCK_BLE_NAME_PREFIX, strlen(RRCP_DOCK_BLE_NAME_PREFIX))) snprintf(st.name, sizeof st.name, "%s", host);
    else snprintf(st.name, sizeof st.name, "%s%s", RRCP_DOCK_BLE_NAME_PREFIX, host);
    UNLOCK();
}

static void start_task(void *arg)
{
    (void)arg;
    if (coproc_ensure_link(20000) != ESP_OK) { set_error("co-processor link is down"); log_ring_printf("ble: unavailable, the ESP32-C6 is not linked"); goto out; }
    esp_hosted_bt_host_stack_cfg_t bt = ESP_HOSTED_BT_HOST_STACK_CONFIG_DEFAULT();
    esp_err_t e = esp_hosted_bt_host_stack_setup(&bt);
    if (e != ESP_OK) { set_error(esp_err_to_name(e)); log_ring_printf("ble: controller on the ESP32-C6 did not come up (%s) — is its firmware current?", esp_err_to_name(e)); goto out; }
    e = nimble_port_init();
    if (e != ESP_OK) { set_error("nimble init failed"); log_ring_printf("ble: host init failed (%s)", esp_err_to_name(e)); goto out; }
    ble_hs_cfg.sync_cb = on_sync;
    ble_hs_cfg.reset_cb = on_reset;
    ble_hs_cfg.store_status_cb = ble_store_util_status_rr;
    ble_hs_cfg.sm_io_cap = BLE_SM_IO_CAP_NO_IO;
    ble_hs_cfg.sm_bonding = 1;
    ble_hs_cfg.sm_mitm = 0;
    ble_hs_cfg.sm_sc = 1;
    ble_hs_cfg.sm_our_key_dist = BLE_SM_PAIR_KEY_DIST_ENC | BLE_SM_PAIR_KEY_DIST_ID;
    ble_hs_cfg.sm_their_key_dist = BLE_SM_PAIR_KEY_DIST_ENC | BLE_SM_PAIR_KEY_DIST_ID;
    ble_svc_gap_init();
    ble_svc_gatt_init();
    int rc = ble_gatts_count_cfg(gatt_svcs);
    if (rc == 0) rc = ble_gatts_add_svcs(gatt_svcs);
    if (rc != 0) { set_error("gatt registration failed"); log_ring_printf("ble: gatt registration failed (%d)", rc); goto out; }
    refresh_name();
    ble_svc_gap_device_name_set(st.name);
    ble_store_config_init();
    ble_hs_cfg.store_delete_cb = store_delete_tolerant;
    nimble_port_freertos_init(host_task);
out:
    LOCK(); st.starting = false; UNLOCK();
    vTaskDelete(NULL);
}

esp_err_t ble_start(void)
{
    if (!mtx) mtx = xSemaphoreCreateMutex();
    if (!rpc_q) rpc_q = xQueueCreate(RPC_QUEUE_DEPTH, sizeof(rpc_msg_t));
    if (ble_uuid_from_str(&svc_uuid, RRCP_DOCK_BLE_SERVICE_UUID) != 0 || ble_uuid_from_str(&rx_uuid, RRCP_DOCK_BLE_RX_UUID) != 0 ||
        ble_uuid_from_str(&tx_uuid, RRCP_DOCK_BLE_TX_UUID) != 0 || ble_uuid_from_str(&info_uuid, RRCP_DOCK_BLE_INFO_UUID) != 0)
        return ESP_ERR_INVALID_ARG;
    st.conn = BLE_HS_CONN_HANDLE_NONE;
    refresh_name();
    LOCK(); st.starting = true; UNLOCK();
    /* API calls (config save, log encode, status) run here, not on the NimBLE host task. */
    if (xTaskCreate(rpc_task, "ble_rpc", 12288, NULL, 3, NULL) != pdPASS) return ESP_ERR_NO_MEM;
    if (xTaskCreate(start_task, "ble_start", 6144, NULL, 3, NULL) != pdPASS) return ESP_ERR_NO_MEM;
    return ESP_OK;
}

void ble_reconfigure(void)
{
    if (!mtx) return;
    LOCK(); bool up = st.up, adv = st.advertising, connected = st.connected; uint16_t conn = st.conn; UNLOCK();
    if (!up) return;
    bool want = app_config_get()->ble_enabled;
    refresh_name();
    ble_svc_gap_device_name_set(st.name);
    if (want && !adv && !connected) advertise();
    if (!want) {
        if (adv) { ble_gap_adv_stop(); LOCK(); st.advertising = false; UNLOCK(); }
        if (connected) ble_gap_terminate(conn, BLE_ERR_REM_USER_CONN_TERM);
        log_ring_printf("ble: disabled");
    }
}

void ble_forget_bonds(void)
{
    if (!mtx) return;
    LOCK(); bool up = st.up, connected = st.connected; uint16_t conn = st.conn; UNLOCK();
    if (!up) return;
    if (connected) ble_gap_terminate(conn, BLE_ERR_REM_USER_CONN_TERM);
    ble_store_clear();
    log_ring_printf("ble: forgot all paired phones");
}

int ble_status_json(char *out, size_t cap)
{
    rrc_jsonw w;
    rrc_jsonw_init(&w, out, cap);
    if (mtx) LOCK();
    rrc_jsonw_raw(&w, "{");
    rrc_jsonw_key(&w, "enabled", true); rrc_jsonw_raw(&w, app_config_get()->ble_enabled ? "true" : "false");
    rrc_jsonw_key(&w, "up", false); rrc_jsonw_raw(&w, st.up ? "true" : "false");
    rrc_jsonw_key(&w, "starting", false); rrc_jsonw_raw(&w, st.starting ? "true" : "false");
    rrc_jsonw_key(&w, "advertising", false); rrc_jsonw_raw(&w, st.advertising ? "true" : "false");
    rrc_jsonw_key(&w, "connected", false); rrc_jsonw_raw(&w, st.connected ? "true" : "false");
    rrc_jsonw_key(&w, "encrypted", false); rrc_jsonw_raw(&w, st.encrypted ? "true" : "false");
    rrc_jsonw_key(&w, "name", false); rrc_jsonw_str(&w, st.name);
    rrc_jsonw_key(&w, "address", false); rrc_jsonw_str(&w, st.addr);
    rrc_jsonw_key(&w, "mtu", false); rrc_jsonw_u64(&w, st.mtu);
    rrc_jsonw_key(&w, "error", false); rrc_jsonw_str(&w, st.error);
    rrc_jsonw_raw(&w, "}");
    if (mtx) UNLOCK();
    return rrc_jsonw_finish(&w);
}

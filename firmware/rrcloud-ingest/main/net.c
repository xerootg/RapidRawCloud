#include "net.h"
#include "esp_timer.h"
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/queue.h"
#include "freertos/semphr.h"
#include "util.h"
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <time.h>
#include "esp_log.h"
#include "esp_netif.h"
#include "esp_event.h"
#include "esp_netif_sntp.h"
#include "esp_netif_net_stack.h"
#include "esp_attr.h"
#include "esp_system.h"
#include "lwip/etharp.h"
#include "esp_wifi.h"
#include "mdns.h"
#include "sdkconfig.h"
#include "board.h"
#include "app_config.h"
#include "log_ring.h"
#include "rrc_proto.h"
#if CONFIG_RRC_HAS_ETHERNET
#include "esp_eth.h"
#include "esp_eth_mac_esp.h"
#include "esp_eth_phy.h"
#endif
#include "sync.h"
#if CONFIG_ESP_WIFI_REMOTE_ENABLED
#include "esp_hosted.h"
#include "coproc.h"
#endif

static esp_netif_t *wifi_netif;
static bool eth_link, eth_ip, wifi_up, wifi_ip, wifi_started, wifi_driver;
static char applied_ssid[33], applied_pw[65];
/* Policy changes run on their own task: starting Wi-Fi waits for the
 * co-processor link and every esp_wifi_* call is an RPC over SDIO, so none of
 * that may run in an event handler or an esp_timer callback.
 *
 * On the Ethernet board Wi-Fi is failover only: it carries traffic when the
 * cable is out. Both links sit in the same subnet, and lwIP routes on-link
 * destinations to whichever netif it finds first, so an associated Wi-Fi
 * station would silently take uploads off the wire. On a board without
 * Ethernet Wi-Fi is the only path and comes up at boot. */
typedef enum { WIFI_POLICY_UP, WIFI_POLICY_DOWN, WIFI_POLICY_REAPPLY, WIFI_POLICY_CONNECT, WIFI_POLICY_SCAN } wifi_policy_t;
static QueueHandle_t policy_q;
static void policy_post(wifi_policy_t p) { if (policy_q) xQueueSend(policy_q, &p, 0); }
static char eth_ip_str[16], wifi_ip_str[16];

/* Association bookkeeping for the UI (/api/wifi). */
static volatile uint16_t last_reason;          /* last WIFI_EVENT_STA_DISCONNECTED reason, 0 = none */
static volatile uint32_t connect_failures;     /* consecutive failed attempts since the last association */
static volatile int64_t assoc_since_us;        /* esp_timer time of the current association */
static volatile bool scanning;
static esp_timer_handle_t retry_timer;
static SemaphoreHandle_t scan_done_sem;

#if CONFIG_RRC_HAS_ETHERNET
static const char *TAG = "net";
static esp_netif_t *eth_netif;
static esp_eth_handle_t eth_handle;

/* ---- Ethernet receive watchdog ---------------------------------------------
 * The ESP32-P4 EMAC can stop delivering received frames for good while the
 * MAC keeps transmitting: after an RX FIFO overflow (receive descriptors
 * exhausted while the RX task is starved, e.g. through flash-cache-disabled
 * windows during littlefs writes) the MTL FIFO latches and no further receive
 * interrupt ever fires (espressif/esp-idf#18800; reproduces on IDF master).
 * Upstream found no recovery short of a full DMA software reset with complete
 * MAC/DMA reconfiguration; esp_eth_stop()/start() and receive poll demand do
 * not help. On the dock it looked like a hang: DNS and the web UI dead, the
 * serial console fine, a sync run aborting on timeouts (issue #39).
 *
 * Every received frame is counted at the driver-to-netif boundary. When the
 * link is up and nothing has arrived for a while, the gateway is probed with
 * ARP, which it must answer; when even that draws nothing, the dock restarts,
 * marking the restart so an interrupted sync run resumes on its own. Three
 * such restarts in a row without ten minutes of healthy receive stop it from
 * looping. */
#define RX_WD_PERIOD_S      10
#define RX_WD_PROBE_AFTER_S 20
#define RX_WD_STALL_AFTER_S 60
#define RX_WD_MAX_RESTARTS  3
#define RX_WD_HEALTHY_S     600
#define RX_WD_MAGIC         0x45525831u   /* "ERX1" */

static volatile uint32_t eth_rx_frames;
static volatile bool eth_rx_drop_test;
static RTC_NOINIT_ATTR uint32_t rtc_rx_magic, rtc_rx_restarts, rtc_rx_restart_pending;

/* Replaces the netif glue's input callback (esp_eth_netif_glue.c,
 * eth_input_to_netif) after esp_netif_attach installed it: the same
 * esp_netif_receive call, plus the frame count. The glue's only other work
 * there is the L2 TAP filter, which this build does not enable. */
static esp_err_t eth_input_counted(esp_eth_handle_t h, uint8_t *buffer, uint32_t length, void *priv, void *info)
{
    (void)h; (void)info;
    if (eth_rx_drop_test) { free(buffer); return ESP_OK; }   /* emulate the lockup: nothing reaches the stack */
    eth_rx_frames++;
    return esp_netif_receive((esp_netif_t *)priv, buffer, length, NULL);
}

static esp_err_t arp_probe(void *ctx)
{
    struct netif *n = esp_netif_get_netif_impl(eth_netif);
    if (n) etharp_request(n, (const ip4_addr_t *)ctx);
    return ESP_OK;
}

static void eth_rx_watchdog_task(void *arg)
{
    (void)arg;
    uint32_t last = eth_rx_frames;
    int quiet_s = 0, healthy_s = 0;
    bool gave_up = false;
    for (;;) {
        vTaskDelay(pdMS_TO_TICKS(RX_WD_PERIOD_S * 1000));
        uint32_t now = eth_rx_frames;
        if (!eth_link || !eth_ip) { last = now; quiet_s = 0; continue; }
        if (now != last) {
            last = now;
            quiet_s = 0;
            if (rtc_rx_restarts && (healthy_s += RX_WD_PERIOD_S) >= RX_WD_HEALTHY_S) { rtc_rx_restarts = 0; gave_up = false; }
            continue;
        }
        quiet_s += RX_WD_PERIOD_S;
        healthy_s = 0;
        if (quiet_s < RX_WD_STALL_AFTER_S) {
            if (quiet_s >= RX_WD_PROBE_AFTER_S) {
                esp_netif_ip_info_t ip;
                if (esp_netif_get_ip_info(eth_netif, &ip) == ESP_OK && ip.gw.addr) esp_netif_tcpip_exec(arp_probe, &ip.gw);
            }
            continue;
        }
        if (rtc_rx_restarts >= RX_WD_MAX_RESTARTS) {
            if (!gave_up) log_ring_printf("net: ethernet still receives nothing after %u restarts; not restarting again (power-cycle the dock)", (unsigned)rtc_rx_restarts);
            gave_up = true;
            continue;
        }
        rtc_rx_restarts++;
        rtc_rx_restart_pending = 1;
        log_ring_printf("net: ethernet has received nothing for %d s with the link up, gateway ARP unanswered; restarting to recover (%u of %u)",
                        quiet_s, (unsigned)rtc_rx_restarts, (unsigned)RX_WD_MAX_RESTARTS);
        sync_mark_recovery_restart();
        vTaskDelay(pdMS_TO_TICKS(300));   /* let the line reach the console */
        esp_restart();
    }
}

static void eth_rx_watchdog_start(void)
{
    if (rtc_rx_magic != RX_WD_MAGIC) { rtc_rx_magic = RX_WD_MAGIC; rtc_rx_restarts = 0; rtc_rx_restart_pending = 0; }
    if (rtc_rx_restart_pending) {
        rtc_rx_restart_pending = 0;
        if (esp_reset_reason() == ESP_RST_SW)
            log_ring_printf("net: restarted to recover from an ethernet receive lockup (%u in a row)", (unsigned)rtc_rx_restarts);
    }
    xTaskCreate(eth_rx_watchdog_task, "eth_rx_wd", 3072, NULL, 3, NULL);
}

uint32_t net_eth_rx_frames(void) { return eth_rx_frames; }
uint32_t net_eth_rx_restarts(void) { return rtc_rx_magic == RX_WD_MAGIC ? rtc_rx_restarts : 0; }
void net_eth_rx_drop_test(bool on) { eth_rx_drop_test = on; }

static void eth_event(void *arg, esp_event_base_t base, int32_t id, void *data)
{
    (void)arg; (void)base; (void)data;
    switch (id) {
    case ETHERNET_EVENT_CONNECTED: eth_link = true; log_ring_printf("ethernet link up"); policy_post(WIFI_POLICY_DOWN); break;
    case ETHERNET_EVENT_DISCONNECTED: eth_link = false; eth_ip = false; eth_ip_str[0] = 0; log_ring_printf("ethernet link down"); policy_post(WIFI_POLICY_UP); break;
    default: break;
    }
}
#endif
#if !CONFIG_RRC_HAS_ETHERNET
/* No Ethernet on this board: nothing to count, nothing to watch. */
uint32_t net_eth_rx_frames(void) { return 0; }
uint32_t net_eth_rx_restarts(void) { return 0; }
void net_eth_rx_drop_test(bool on) { (void)on; }
#endif

static void ip_event(void *arg, esp_event_base_t base, int32_t id, void *data)
{
    (void)arg; (void)base;
    if (id == IP_EVENT_ETH_GOT_IP) {
        ip_event_got_ip_t *e = data;
        snprintf(eth_ip_str, sizeof eth_ip_str, IPSTR, IP2STR(&e->ip_info.ip));
        eth_ip = true;
        log_ring_printf("ethernet ip %s — web ui: http://%s/ (http://%s.local/)", eth_ip_str, eth_ip_str, app_config_get()->hostname);
    } else if (id == IP_EVENT_STA_GOT_IP) {
        ip_event_got_ip_t *e = data;
        snprintf(wifi_ip_str, sizeof wifi_ip_str, IPSTR, IP2STR(&e->ip_info.ip));
        wifi_ip = true;
        log_ring_printf("wifi ip %s — web ui: http://%s/ (http://%s.local/)", wifi_ip_str, wifi_ip_str, app_config_get()->hostname);
    } else if (id == IP_EVENT_ETH_LOST_IP) {
        eth_ip = false;
    } else if (id == IP_EVENT_STA_LOST_IP) {
        wifi_ip = false;
    }
}

static void retry_cb(void *arg) { (void)arg; policy_post(WIFI_POLICY_CONNECT); }

/* Reconnect backoff: immediately for the first three attempts, then 2, 4, 8 … 30 s.
 * A station retrying an absent network back to back keeps the radio busy (and, on
 * the battery board, awake) for nothing; a field device walks in and out of range. */
static void schedule_retry(void)
{
    uint32_t n = connect_failures;
    if (n < 3) { policy_post(WIFI_POLICY_CONNECT); return; }
    uint32_t s = 1u << (n - 2 > 4 ? 4 : n - 2);
    if (s > 30) s = 30;
    if (retry_timer) { esp_timer_stop(retry_timer); esp_timer_start_once(retry_timer, (uint64_t)s * 1000000); }
}

static void wifi_event(void *arg, esp_event_base_t base, int32_t id, void *data)
{
    (void)arg; (void)base;
    if (id == WIFI_EVENT_STA_CONNECTED) {
        wifi_up = true;
        connect_failures = 0;
        last_reason = 0;
        assoc_since_us = esp_timer_get_time();
        log_ring_printf("wifi associated");
    } else if (id == WIFI_EVENT_STA_DISCONNECTED) {
        const wifi_event_sta_disconnected_t *d = data;
        bool was_up = wifi_up;
        wifi_up = false; wifi_ip = false; wifi_ip_str[0] = 0;
        if (d) last_reason = d->reason;
        if (was_up) log_ring_printf("wifi disconnected: %s (%u)", net_wifi_reason_str(last_reason), (unsigned)last_reason);
        else connect_failures++;
        /* keep retrying; Ethernet (where fitted) carries traffic meanwhile */
        if (wifi_started && applied_ssid[0] && !scanning) schedule_retry();
    } else if (id == WIFI_EVENT_SCAN_DONE) {
        if (scan_done_sem) xSemaphoreGive(scan_done_sem);
    }
}

#if CONFIG_RRC_HAS_ETHERNET
static esp_err_t eth_init(void)
{
    eth_mac_config_t mac_config = ETH_MAC_DEFAULT_CONFIG();
    eth_phy_config_t phy_config = ETH_PHY_DEFAULT_CONFIG();
    phy_config.phy_addr = BOARD_ETH_PHY_ADDR;
    phy_config.reset_gpio_num = BOARD_ETH_PHY_RST_GPIO;
    eth_esp32_emac_config_t emac = ETH_ESP32_EMAC_DEFAULT_CONFIG();
    emac.smi_gpio.mdc_num = BOARD_ETH_MDC_GPIO;
    emac.smi_gpio.mdio_num = BOARD_ETH_MDIO_GPIO;
    emac.interface = EMAC_DATA_INTERFACE_RMII;
    emac.clock_config.rmii.clock_mode = EMAC_CLK_EXT_IN;
    emac.clock_config.rmii.clock_gpio = (emac_rmii_clock_gpio_t)BOARD_ETH_REFCLK_GPIO;
    emac.emac_dataif_gpio.rmii.tx_en_num = BOARD_ETH_TX_EN_GPIO;
    emac.emac_dataif_gpio.rmii.txd0_num = BOARD_ETH_TXD0_GPIO;
    emac.emac_dataif_gpio.rmii.txd1_num = BOARD_ETH_TXD1_GPIO;
    emac.emac_dataif_gpio.rmii.crs_dv_num = BOARD_ETH_CRS_DV_GPIO;
    emac.emac_dataif_gpio.rmii.rxd0_num = BOARD_ETH_RXD0_GPIO;
    emac.emac_dataif_gpio.rmii.rxd1_num = BOARD_ETH_RXD1_GPIO;
    esp_eth_mac_t *mac = esp_eth_mac_new_esp32(&emac, &mac_config);
    esp_eth_phy_t *phy = esp_eth_phy_new_ip101(&phy_config);
    if (!mac || !phy) return ESP_FAIL;
    esp_eth_config_t cfg = ETH_DEFAULT_CONFIG(mac, phy);
    esp_eth_handle_t eth = NULL;
    esp_err_t e = esp_eth_driver_install(&cfg, &eth);
    eth_handle = eth;
    if (e != ESP_OK) { ESP_LOGE(TAG, "eth driver install: %s", esp_err_to_name(e)); return e; }
    /* IDF ranks a Wi-Fi station netif (route_prio 100) above Ethernet (50), so once
     * Wi-Fi joins the same subnet every upload would leave over the C6's SDIO link
     * at a fraction of the wired rate. Wired is the default route whenever it has
     * link; Wi-Fi only carries traffic when the cable is out. */
    esp_netif_inherent_config_t eth_base = ESP_NETIF_INHERENT_DEFAULT_ETH();
    eth_base.route_prio = 200;
    esp_netif_config_t ncfg = ESP_NETIF_DEFAULT_ETH();
    ncfg.base = &eth_base;
    eth_netif = esp_netif_new(&ncfg);
    esp_netif_set_hostname(eth_netif, app_config_get()->hostname);
    ESP_ERROR_CHECK(esp_netif_attach(eth_netif, esp_eth_new_netif_glue(eth)));
    ESP_ERROR_CHECK(esp_eth_update_input_path_info(eth, eth_input_counted, eth_netif));
    ESP_ERROR_CHECK(esp_event_handler_register(ETH_EVENT, ESP_EVENT_ANY_ID, eth_event, NULL));
    eth_rx_watchdog_start();
    return esp_eth_start(eth);
}
#endif

/* Brings the Wi-Fi driver up (netif, esp_wifi_init, STA mode, started) without
 * joining anything: the scan needs this much even with no network configured. */
static esp_err_t wifi_driver_up(void)
{
    if (wifi_driver && wifi_started) return ESP_OK;
#if CONFIG_ESP_WIFI_REMOTE_ENABLED
    /* The radio lives on the ESP32-C6; its ESP-Hosted link is owned by coproc.c. */
    if (coproc_ensure_link(15000) != ESP_OK) { log_ring_printf("wifi: the ESP32-C6 co-processor is not linked — Wi-Fi stays off"); return ESP_FAIL; }
#endif
    if (!wifi_driver) {
        if (!wifi_netif) {
            wifi_netif = esp_netif_create_default_wifi_sta();
            esp_netif_set_hostname(wifi_netif, app_config_get()->hostname);
        }
        wifi_init_config_t wcfg = WIFI_INIT_CONFIG_DEFAULT();
        esp_err_t e = esp_wifi_init(&wcfg);
        if (e != ESP_OK) { log_ring_printf("wifi init failed: %s", esp_err_to_name(e)); return e; }
        ESP_ERROR_CHECK(esp_event_handler_register(WIFI_EVENT, ESP_EVENT_ANY_ID, wifi_event, NULL));
        wifi_driver = true;
    }
    if (!wifi_started) {
        esp_err_t e = esp_wifi_set_mode(WIFI_MODE_STA);
        if (e == ESP_OK) e = esp_wifi_start();
        if (e != ESP_OK) { log_ring_printf("wifi: start failed: %s", esp_err_to_name(e)); return e; }
        wifi_started = true;
    }
    return ESP_OK;
}

static esp_err_t wifi_start(void)
{
    const app_config_t *c = app_config_get();
    char pw[65];
    app_config_get_wifi_password(pw, sizeof pw);
    if (!c->wifi_ssid[0] || !c->wifi_enabled) return ESP_OK;
    /* Re-applying identical credentials would bounce a live association (and,
     * on the Wi-Fi-only board, cut off the very web session that saved them). */
    if (wifi_started && !strcmp(applied_ssid, c->wifi_ssid) && !strcmp(applied_pw, pw)) return ESP_OK;
    bool was_started = wifi_started;
    esp_err_t e = wifi_driver_up();
    if (e != ESP_OK) return e;
    wifi_config_t wc = {0};
    /* SSIDs are 1..32 octets and need no terminator in wifi_config_t. */
    size_t sl = strlen(c->wifi_ssid);
    if (sl > sizeof wc.sta.ssid) sl = sizeof wc.sta.ssid;
    memcpy(wc.sta.ssid, c->wifi_ssid, sl);
    size_t pl = strlen(pw);
    if (pl > sizeof wc.sta.password) pl = sizeof wc.sta.password;
    memcpy(wc.sta.password, pw, pl);
    wc.sta.threshold.authmode = pw[0] ? WIFI_AUTH_WPA2_PSK : WIFI_AUTH_OPEN;
    /* Strongest AP of the SSID rather than the first one heard on the lowest channel:
     * the difference between a usable and a marginal link in a multi-AP site. */
    wc.sta.scan_method = WIFI_ALL_CHANNEL_SCAN;
    wc.sta.sort_method = WIFI_CONNECT_AP_BY_SIGNAL;
    /* User-supplied settings must never take the device down: an invalid
     * password (1–7 chars) is a logged configuration error, not a panic. */
    e = esp_wifi_set_config(WIFI_IF_STA, &wc);
    if (e != ESP_OK) { log_ring_printf("wifi: rejected credentials for %s: %s", c->wifi_ssid, esp_err_to_name(e)); return e; }
    scpy(applied_ssid, sizeof applied_ssid, c->wifi_ssid);
    scpy(applied_pw, sizeof applied_pw, pw);
    connect_failures = 0;
    last_reason = 0;
    if (retry_timer) esp_timer_stop(retry_timer);
    if (was_started) esp_wifi_disconnect();
    policy_post(WIFI_POLICY_CONNECT); /* runs after this, on the same task */
    log_ring_printf("wifi: connecting to %s", c->wifi_ssid);
    return ESP_OK;
}

static void wifi_stop(const char *why)
{
    if (retry_timer) esp_timer_stop(retry_timer);
    if (!wifi_started) return;
    applied_ssid[0] = applied_pw[0] = 0;
    esp_wifi_disconnect();
    esp_wifi_stop();
    wifi_started = false; wifi_up = wifi_ip = false; wifi_ip_str[0] = 0;
    log_ring_printf("wifi: off (%s)", why);
}

/* ---- scan ------------------------------------------------------------------ */
#define SCAN_MAX_RECORDS 32
static net_ap_t scan_aps[NET_SCAN_MAX];
static size_t scan_n;
static volatile net_scan_state_t scan_state;
static char scan_err[64];
static SemaphoreHandle_t scan_mtx;

static void do_scan(void)
{
    scan_err[0] = 0;
    if (wifi_driver_up() != ESP_OK) { scpy(scan_err, sizeof scan_err, "radio co-processor not available"); scan_state = NET_SCAN_FAILED; return; }
    /* A station between attempts holds the radio in "connecting", and the driver
     * refuses to scan then; park reconnects until the scan is over. */
    scanning = true;
    if (retry_timer) esp_timer_stop(retry_timer);
    if (!wifi_up) esp_wifi_disconnect();
    xSemaphoreTake(scan_done_sem, 0);
    wifi_scan_config_t sc = {.show_hidden = false, .scan_type = WIFI_SCAN_TYPE_ACTIVE};
    esp_err_t e = esp_wifi_scan_start(&sc, false);
    if (e == ESP_OK && xSemaphoreTake(scan_done_sem, pdMS_TO_TICKS(12000)) != pdTRUE) { esp_wifi_scan_stop(); e = ESP_ERR_TIMEOUT; }
    wifi_ap_record_t *recs = NULL;
    uint16_t n = SCAN_MAX_RECORDS;
    if (e == ESP_OK) {
        recs = calloc(SCAN_MAX_RECORDS, sizeof *recs);
        e = recs ? esp_wifi_scan_get_ap_records(&n, recs) : ESP_ERR_NO_MEM;
    } else {
        esp_wifi_clear_ap_list();
    }
    scanning = false;
    if (wifi_started && applied_ssid[0] && !wifi_up) policy_post(WIFI_POLICY_CONNECT);
    if (e != ESP_OK) {
        free(recs);
        snprintf(scan_err, sizeof scan_err, "scan failed: %s", esp_err_to_name(e));
        scan_state = NET_SCAN_FAILED;
        return;
    }
    /* One row per SSID (strongest BSSID wins; `aps` counts the access points that
     * share it, which is what roaming between them would choose from). The driver
     * returns records sorted by RSSI, strongest first. */
    xSemaphoreTake(scan_mtx, portMAX_DELAY);
    scan_n = 0;
    for (uint16_t i = 0; i < n; i++) {
        const char *ssid = (const char *)recs[i].ssid;
        if (!ssid[0]) continue;
        size_t j = 0;
        for (; j < scan_n; j++) if (!strcmp(scan_aps[j].ssid, ssid)) break;
        if (j < scan_n) { scan_aps[j].aps++; continue; }
        if (scan_n == NET_SCAN_MAX) continue;
        net_ap_t *a = &scan_aps[scan_n++];
        memset(a, 0, sizeof *a);
        scpy(a->ssid, sizeof a->ssid, ssid);
        memcpy(a->bssid, recs[i].bssid, 6);
        a->rssi = recs[i].rssi;
        a->channel = recs[i].primary;
        a->authmode = recs[i].authmode;
        a->wifi6 = recs[i].phy_11ax;
        a->aps = 1;
    }
    xSemaphoreGive(scan_mtx);
    free(recs);
    scan_state = NET_SCAN_DONE;
}

static void policy_task(void *arg)
{
    (void)arg;
    wifi_policy_t p;
    for (;;) {
        if (xQueueReceive(policy_q, &p, portMAX_DELAY) != pdTRUE) continue;
        const app_config_t *c = app_config_get();
        bool want = c->wifi_enabled && c->wifi_ssid[0] && !eth_link;
        switch (p) {
        case WIFI_POLICY_DOWN: wifi_stop("ethernet carries traffic"); break;
        case WIFI_POLICY_UP: if (want) wifi_start(); break;
        case WIFI_POLICY_REAPPLY:
            if (!c->wifi_enabled) wifi_stop("disabled in settings");
            else if (!c->wifi_ssid[0]) wifi_stop("no network configured");
            else if (eth_link) wifi_stop("ethernet carries traffic");
            else wifi_start();
            break;
        case WIFI_POLICY_CONNECT:
            if (wifi_started && applied_ssid[0] && !wifi_up && !scanning) esp_wifi_connect();
            break;
        case WIFI_POLICY_SCAN: do_scan(); break;
        }
    }
}

static void boot_policy_cb(void *arg) { (void)arg; policy_post(WIFI_POLICY_UP); }

esp_err_t net_wifi_reconfigure(void)
{
    policy_post(WIFI_POLICY_REAPPLY);
    return ESP_OK;
}

esp_err_t net_wifi_scan_start(void)
{
    if (!app_config_get()->wifi_enabled) return ESP_ERR_NOT_SUPPORTED;
    if (scan_state == NET_SCAN_RUNNING) return ESP_ERR_INVALID_STATE;
    scan_state = NET_SCAN_RUNNING;
    policy_post(WIFI_POLICY_SCAN);
    return ESP_OK;
}

const char *net_wifi_authmode_str(int authmode)
{
    switch ((wifi_auth_mode_t)authmode) {
    case WIFI_AUTH_OPEN: return "open";
    case WIFI_AUTH_WEP: return "WEP";
    case WIFI_AUTH_WPA_PSK: return "WPA-PSK";
    case WIFI_AUTH_WPA2_PSK: return "WPA2-PSK";
    case WIFI_AUTH_WPA_WPA2_PSK: return "WPA/WPA2-PSK";
    case WIFI_AUTH_WPA2_ENTERPRISE: return "WPA2-Enterprise";
    case WIFI_AUTH_WPA3_PSK: return "WPA3-SAE";
    case WIFI_AUTH_WPA2_WPA3_PSK: return "WPA2/WPA3";
    case WIFI_AUTH_OWE: return "OWE";
    default: return "other";
    }
}

const char *net_wifi_reason_str(int reason)
{
    switch (reason) {
    case 0: return "";
    case WIFI_REASON_4WAY_HANDSHAKE_TIMEOUT: case WIFI_REASON_HANDSHAKE_TIMEOUT: return "handshake timed out (wrong password?)";
    case WIFI_REASON_AUTH_FAIL: return "authentication failed (wrong password?)";
    case WIFI_REASON_NO_AP_FOUND: return "network not found";
    case WIFI_REASON_NO_AP_FOUND_W_COMPATIBLE_SECURITY: case WIFI_REASON_NO_AP_FOUND_IN_AUTHMODE_THRESHOLD: return "network found, but its security mode does not match";
    case WIFI_REASON_NO_AP_FOUND_IN_RSSI_THRESHOLD: return "signal too weak";
    case WIFI_REASON_BEACON_TIMEOUT: return "lost the access point (out of range)";
    case WIFI_REASON_ASSOC_FAIL: case WIFI_REASON_CONNECTION_FAIL: return "association failed";
    case WIFI_REASON_ASSOC_TOOMANY: return "access point is full";
    case WIFI_REASON_AUTH_EXPIRE: case WIFI_REASON_AUTH_LEAVE: case WIFI_REASON_ASSOC_LEAVE: return "disconnected by the access point";
    case WIFI_REASON_ROAMING: return "roaming";
    default: return "disconnected";
    }
}

int net_wifi_scan_json(char *out, size_t cap)
{
    static const char *const st[] = {"idle", "running", "done", "failed"};
    rrc_jsonw w;
    rrc_jsonw_init(&w, out, cap);
    rrc_jsonw_raw(&w, "{");
    rrc_jsonw_key(&w, "state", true); rrc_jsonw_str(&w, st[scan_state]);
    rrc_jsonw_key(&w, "error", false); rrc_jsonw_str(&w, scan_err);
    rrc_jsonw_key(&w, "networks", false); rrc_jsonw_raw(&w, "[");
    xSemaphoreTake(scan_mtx, portMAX_DELAY);
    for (size_t i = 0; i < scan_n && scan_state == NET_SCAN_DONE; i++) {
        const net_ap_t *a = &scan_aps[i];
        char bssid[18];
        snprintf(bssid, sizeof bssid, "%02x:%02x:%02x:%02x:%02x:%02x", a->bssid[0], a->bssid[1], a->bssid[2], a->bssid[3], a->bssid[4], a->bssid[5]);
        if (i) rrc_jsonw_raw(&w, ",");
        rrc_jsonw_raw(&w, "{");
        rrc_jsonw_key(&w, "ssid", true); rrc_jsonw_str(&w, a->ssid);
        rrc_jsonw_key(&w, "bssid", false); rrc_jsonw_str(&w, bssid);
        rrc_jsonw_key(&w, "rssi", false); rrc_jsonw_i64(&w, a->rssi);
        rrc_jsonw_key(&w, "channel", false); rrc_jsonw_u64(&w, a->channel);
        rrc_jsonw_key(&w, "security", false); rrc_jsonw_str(&w, net_wifi_authmode_str(a->authmode));
        rrc_jsonw_key(&w, "open", false); rrc_jsonw_raw(&w, a->authmode == WIFI_AUTH_OPEN || a->authmode == WIFI_AUTH_OWE ? "true" : "false");
        rrc_jsonw_key(&w, "wifi6", false); rrc_jsonw_raw(&w, a->wifi6 ? "true" : "false");
        rrc_jsonw_key(&w, "aps", false); rrc_jsonw_u64(&w, a->aps);
        rrc_jsonw_raw(&w, "}");
    }
    xSemaphoreGive(scan_mtx);
    rrc_jsonw_raw(&w, "]}");
    return rrc_jsonw_finish(&w);
}

/* esp_wifi_sta_get_ap_info is an SDIO round trip to the C6; the status bar polls
 * once a second, so the answer is cached for a few seconds. */
static wifi_ap_record_t ap_cache;
static int64_t ap_cache_us;
static bool ap_cache_ok;

static bool ap_info(wifi_ap_record_t *out)
{
    if (!wifi_up) return false;
    int64_t now = esp_timer_get_time();
    xSemaphoreTake(scan_mtx, portMAX_DELAY);
    if (!ap_cache_us || now - ap_cache_us > 3000000) {
        ap_cache_ok = esp_wifi_sta_get_ap_info(&ap_cache) == ESP_OK;
        ap_cache_us = now;
    }
    bool ok = ap_cache_ok;
    if (ok) *out = ap_cache;
    xSemaphoreGive(scan_mtx);
    return ok;
}

int net_wifi_rssi(void)
{
    wifi_ap_record_t ap;
    return ap_info(&ap) ? ap.rssi : 0;
}

static void ip4_str(const esp_ip4_addr_t *a, char *out, size_t cap) { snprintf(out, cap, IPSTR, IP2STR(a)); }

int net_wifi_json(char *out, size_t cap)
{
    const app_config_t *c = app_config_get();
    wifi_ap_record_t ap;
    bool have_ap = ap_info(&ap);
    rrc_jsonw w;
    rrc_jsonw_init(&w, out, cap);
    rrc_jsonw_raw(&w, "{");
    rrc_jsonw_key(&w, "enabled", true); rrc_jsonw_raw(&w, c->wifi_enabled ? "true" : "false");
    rrc_jsonw_key(&w, "ssid", false); rrc_jsonw_str(&w, c->wifi_ssid);
    rrc_jsonw_key(&w, "standby", false); rrc_jsonw_raw(&w, eth_link && c->wifi_enabled && c->wifi_ssid[0] ? "true" : "false");
    rrc_jsonw_key(&w, "started", false); rrc_jsonw_raw(&w, wifi_started ? "true" : "false");
    rrc_jsonw_key(&w, "associated", false); rrc_jsonw_raw(&w, wifi_up ? "true" : "false");
    rrc_jsonw_key(&w, "has_ip", false); rrc_jsonw_raw(&w, wifi_ip ? "true" : "false");
    rrc_jsonw_key(&w, "attempts", false); rrc_jsonw_u64(&w, connect_failures);
    rrc_jsonw_key(&w, "last_reason", false); rrc_jsonw_u64(&w, last_reason);
    rrc_jsonw_key(&w, "last_reason_text", false); rrc_jsonw_str(&w, net_wifi_reason_str(last_reason));
    if (wifi_up) { rrc_jsonw_key(&w, "connected_s", false); rrc_jsonw_u64(&w, (uint64_t)((esp_timer_get_time() - assoc_since_us) / 1000000)); }
    if (have_ap) {
        char bssid[18];
        snprintf(bssid, sizeof bssid, "%02x:%02x:%02x:%02x:%02x:%02x", ap.bssid[0], ap.bssid[1], ap.bssid[2], ap.bssid[3], ap.bssid[4], ap.bssid[5]);
        rrc_jsonw_key(&w, "bssid", false); rrc_jsonw_str(&w, bssid);
        rrc_jsonw_key(&w, "rssi", false); rrc_jsonw_i64(&w, ap.rssi);
        rrc_jsonw_key(&w, "channel", false); rrc_jsonw_u64(&w, ap.primary);
        rrc_jsonw_key(&w, "security", false); rrc_jsonw_str(&w, net_wifi_authmode_str(ap.authmode));
        rrc_jsonw_key(&w, "phy", false); rrc_jsonw_str(&w, ap.phy_11ax ? "802.11ax (Wi-Fi 6)" : ap.phy_11n ? "802.11n (Wi-Fi 4)" : ap.phy_11g ? "802.11g" : "802.11b");
    }
    if (wifi_netif) {
        uint8_t mac[6];
        if (esp_netif_get_mac(wifi_netif, mac) == ESP_OK) {
            char m[18];
            snprintf(m, sizeof m, "%02x:%02x:%02x:%02x:%02x:%02x", mac[0], mac[1], mac[2], mac[3], mac[4], mac[5]);
            rrc_jsonw_key(&w, "mac", false); rrc_jsonw_str(&w, m);
        }
        esp_netif_ip_info_t ip;
        if (wifi_ip && esp_netif_get_ip_info(wifi_netif, &ip) == ESP_OK) {
            char s[16];
            ip4_str(&ip.ip, s, sizeof s); rrc_jsonw_key(&w, "ip", false); rrc_jsonw_str(&w, s);
            ip4_str(&ip.netmask, s, sizeof s); rrc_jsonw_key(&w, "netmask", false); rrc_jsonw_str(&w, s);
            ip4_str(&ip.gw, s, sizeof s); rrc_jsonw_key(&w, "gateway", false); rrc_jsonw_str(&w, s);
            esp_netif_dns_info_t dns;
            if (esp_netif_get_dns_info(wifi_netif, ESP_NETIF_DNS_MAIN, &dns) == ESP_OK && dns.ip.type == ESP_IPADDR_TYPE_V4) {
                ip4_str(&dns.ip.u_addr.ip4, s, sizeof s); rrc_jsonw_key(&w, "dns", false); rrc_jsonw_str(&w, s);
            }
        }
    }
    rrc_jsonw_raw(&w, "}");
    return rrc_jsonw_finish(&w);
}

const char *net_eth_ip(void) { return eth_ip ? eth_ip_str : ""; }

static void sntp_synced(struct timeval *tv)
{
    (void)tv;
    log_ring_printf("time synchronized (sntp)");
}

esp_err_t net_init(void)
{
    ESP_ERROR_CHECK(esp_netif_init());
    ESP_ERROR_CHECK(esp_event_loop_create_default());
    ESP_ERROR_CHECK(esp_event_handler_register(IP_EVENT, ESP_EVENT_ANY_ID, ip_event, NULL));
    scan_mtx = xSemaphoreCreateMutex();
    scan_done_sem = xSemaphoreCreateBinary();
    const esp_timer_create_args_t rta = {.callback = retry_cb, .name = "wifi_retry"};
    esp_timer_create(&rta, &retry_timer);
#if CONFIG_RRC_HAS_ETHERNET
    esp_err_t e = eth_init();
    if (e != ESP_OK) log_ring_printf("ethernet init failed: %s", esp_err_to_name(e));
#endif
    policy_q = xQueueCreate(8, sizeof(wifi_policy_t));
    xTaskCreate(policy_task, "net_policy", 4096, NULL, 3, NULL);
#if CONFIG_RRC_HAS_ETHERNET
    /* No cable within ten seconds of boot: bring Wi-Fi up (link detection takes ~2 s). */
    const esp_timer_create_args_t ta = {.callback = boot_policy_cb, .name = "wifi_boot"};
    esp_timer_handle_t t;
    if (esp_timer_create(&ta, &t) == ESP_OK) esp_timer_start_once(t, 10 * 1000 * 1000);
#else
    boot_policy_cb(NULL); /* Wi-Fi is the only network */
#endif
    esp_sntp_config_t sc = ESP_NETIF_SNTP_DEFAULT_CONFIG_MULTIPLE(2, ESP_SNTP_SERVER_LIST("pool.ntp.org", "time.cloudflare.com"));
    sc.start = true;
    sc.sync_cb = sntp_synced;
    esp_netif_sntp_init(&sc);
    if (mdns_init() == ESP_OK) {
        mdns_hostname_set(app_config_get()->hostname);
        mdns_instance_name_set("RapidRawCloud camera dock");
        mdns_service_add(NULL, "_http", "_tcp", 80, NULL, 0);
    }
    return ESP_OK;
}

bool net_has_ip(void) { return eth_ip || wifi_ip; }
#if CONFIG_RRC_HAS_ETHERNET
bool net_has_ethernet(void) { return true; }
#else
bool net_has_ethernet(void) { return false; }
#endif
bool net_eth_link(void) { return eth_link; }

void net_eth_link_info(int *speed_mbps, bool *full_duplex)
{
    *speed_mbps = 0; *full_duplex = false;
#if CONFIG_RRC_HAS_ETHERNET
    if (!eth_handle) return;
    eth_speed_t sp = ETH_SPEED_10M; eth_duplex_t dp = ETH_DUPLEX_HALF;
    if (esp_eth_ioctl(eth_handle, ETH_CMD_G_SPEED, &sp) == ESP_OK) *speed_mbps = sp == ETH_SPEED_100M ? 100 : 10;
    if (esp_eth_ioctl(eth_handle, ETH_CMD_G_DUPLEX_MODE, &dp) == ESP_OK) *full_duplex = dp == ETH_DUPLEX_FULL;
#endif
}
bool net_wifi_connected(void) { return wifi_up; }
bool net_time_synced(void) { return time(NULL) > 1600000000; }

void net_describe(char *out, size_t cap)
{
    if (eth_ip && wifi_ip) snprintf(out, cap, "eth %s, wifi %s", eth_ip_str, wifi_ip_str);
    else if (eth_ip) snprintf(out, cap, "eth %s", eth_ip_str);
    else if (wifi_ip) snprintf(out, cap, "wifi %s", wifi_ip_str);
    else if (eth_link) snprintf(out, cap, "eth link, no ip");
    else snprintf(out, cap, "%s", wifi_up ? "wifi associated, no ip" : "offline");
}

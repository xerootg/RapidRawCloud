#include "net.h"
#include "esp_timer.h"
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/queue.h"
#include "util.h"
#include <string.h>
#include <stdio.h>
#include <time.h>
#include "esp_log.h"
#include "esp_netif.h"
#include "esp_event.h"
#include "esp_eth.h"
#include "esp_eth_mac_esp.h"
#include "esp_eth_phy.h"
#include "esp_netif_sntp.h"
#include "esp_wifi.h"
#include "mdns.h"
#include "sdkconfig.h"
#include "board.h"
#include "app_config.h"
#include "log_ring.h"
#if CONFIG_ESP_WIFI_REMOTE_ENABLED
#include "esp_hosted.h"
#include "coproc.h"
#endif

static const char *TAG = "net";
static esp_netif_t *eth_netif, *wifi_netif;
static bool eth_link, eth_ip, wifi_up, wifi_ip, wifi_started;
static esp_eth_handle_t eth_handle;
static char applied_ssid[33], applied_pw[65];
/* Wi-Fi is failover only: it carries traffic when the cable is out. Both links
 * sit in the same subnet, and lwIP routes on-link destinations to whichever
 * netif it finds first, so an associated Wi-Fi station would silently take
 * uploads off the wire. Policy changes run on their own task because starting
 * Wi-Fi waits for the co-processor link. */
typedef enum { WIFI_POLICY_UP, WIFI_POLICY_DOWN, WIFI_POLICY_REAPPLY } wifi_policy_t;
static QueueHandle_t policy_q;
static void policy_post(wifi_policy_t p) { if (policy_q) xQueueSend(policy_q, &p, 0); }
static char eth_ip_str[16], wifi_ip_str[16];

static void eth_event(void *arg, esp_event_base_t base, int32_t id, void *data)
{
    (void)arg; (void)base; (void)data;
    switch (id) {
    case ETHERNET_EVENT_CONNECTED: eth_link = true; log_ring_printf("ethernet link up"); policy_post(WIFI_POLICY_DOWN); break;
    case ETHERNET_EVENT_DISCONNECTED: eth_link = false; eth_ip = false; eth_ip_str[0] = 0; log_ring_printf("ethernet link down"); policy_post(WIFI_POLICY_UP); break;
    default: break;
    }
}

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

static void wifi_event(void *arg, esp_event_base_t base, int32_t id, void *data)
{
    (void)arg; (void)base; (void)data;
    if (id == WIFI_EVENT_STA_START) { esp_wifi_connect(); }
    else if (id == WIFI_EVENT_STA_CONNECTED) { wifi_up = true; log_ring_printf("wifi associated"); }
    else if (id == WIFI_EVENT_STA_DISCONNECTED) {
        wifi_up = false; wifi_ip = false; wifi_ip_str[0] = 0;
        if (wifi_started) esp_wifi_connect(); /* keep retrying; Ethernet carries traffic meanwhile */
    }
}

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
    ESP_ERROR_CHECK(esp_event_handler_register(ETH_EVENT, ESP_EVENT_ANY_ID, eth_event, NULL));
    return esp_eth_start(eth);
}

static esp_err_t wifi_start(void)
{
    const app_config_t *c = app_config_get();
    char pw[65];
    app_config_get_wifi_password(pw, sizeof pw);
    if (!c->wifi_ssid[0]) return ESP_OK;
#if CONFIG_ESP_WIFI_REMOTE_ENABLED
    /* The radio lives on the ESP32-C6; its ESP-Hosted link is owned by coproc.c. */
    if (coproc_ensure_link(15000) != ESP_OK) { log_ring_printf("wifi: the ESP32-C6 co-processor is not linked — Wi-Fi stays off"); return ESP_FAIL; }
#endif
    if (!wifi_netif) {
        wifi_netif = esp_netif_create_default_wifi_sta();
        esp_netif_set_hostname(wifi_netif, c->hostname);
        wifi_init_config_t wcfg = WIFI_INIT_CONFIG_DEFAULT();
        esp_err_t e = esp_wifi_init(&wcfg);
        if (e != ESP_OK) { log_ring_printf("wifi init failed: %s", esp_err_to_name(e)); return e; }
        ESP_ERROR_CHECK(esp_event_handler_register(WIFI_EVENT, ESP_EVENT_ANY_ID, wifi_event, NULL));
    }
    /* Re-applying identical credentials would bounce a live association. */
    if (wifi_started && !strcmp(applied_ssid, c->wifi_ssid) && !strcmp(applied_pw, pw)) return ESP_OK;
    wifi_config_t wc = {0};
    /* SSIDs are 1..32 octets and need no terminator in wifi_config_t. */
    size_t sl = strlen(c->wifi_ssid);
    if (sl > sizeof wc.sta.ssid) sl = sizeof wc.sta.ssid;
    memcpy(wc.sta.ssid, c->wifi_ssid, sl);
    size_t pl = strlen(pw);
    if (pl > sizeof wc.sta.password) pl = sizeof wc.sta.password;
    memcpy(wc.sta.password, pw, pl);
    wc.sta.threshold.authmode = pw[0] ? WIFI_AUTH_WPA2_PSK : WIFI_AUTH_OPEN;
    /* User-supplied settings must never take the device down: an invalid
     * password (1–7 chars) is a logged configuration error, not a panic. */
    esp_err_t e = esp_wifi_set_mode(WIFI_MODE_STA);
    if (e == ESP_OK) e = esp_wifi_set_config(WIFI_IF_STA, &wc);
    if (e != ESP_OK) { log_ring_printf("wifi: rejected credentials for %s: %s", c->wifi_ssid, esp_err_to_name(e)); return e; }
    if (!wifi_started) {
        e = esp_wifi_start();
        if (e != ESP_OK) { log_ring_printf("wifi: start failed: %s", esp_err_to_name(e)); return e; }
        wifi_started = true;
    } else {
        esp_wifi_disconnect();
        esp_wifi_connect();
    }
    scpy(applied_ssid, sizeof applied_ssid, c->wifi_ssid);
    scpy(applied_pw, sizeof applied_pw, pw);
    log_ring_printf("wifi: connecting to %s", c->wifi_ssid);
    return ESP_OK;
}

static void wifi_stop(void)
{
    if (!wifi_started) return;
    esp_wifi_disconnect();
    esp_wifi_stop();
    wifi_started = false; wifi_up = wifi_ip = false; wifi_ip_str[0] = 0;
    applied_ssid[0] = applied_pw[0] = 0;
    log_ring_printf("wifi: off (ethernet carries traffic)");
}

static void policy_task(void *arg)
{
    (void)arg;
    wifi_policy_t p;
    for (;;) {
        if (xQueueReceive(policy_q, &p, portMAX_DELAY) != pdTRUE) continue;
        const app_config_t *c = app_config_get();
        switch (p) {
        case WIFI_POLICY_DOWN: wifi_stop(); break;
        case WIFI_POLICY_UP: if (!eth_link && c->wifi_ssid[0]) wifi_start(); break;
        case WIFI_POLICY_REAPPLY:
            if (!c->wifi_ssid[0] || eth_link) wifi_stop();
            else { applied_ssid[0] = 0; wifi_start(); }
            break;
        }
    }
}

static void boot_policy_cb(void *arg) { (void)arg; policy_post(WIFI_POLICY_UP); }

esp_err_t net_wifi_reconfigure(void)
{
    policy_post(WIFI_POLICY_REAPPLY);
    return ESP_OK;
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
    esp_err_t e = eth_init();
    if (e != ESP_OK) log_ring_printf("ethernet init failed: %s", esp_err_to_name(e));
    policy_q = xQueueCreate(8, sizeof(wifi_policy_t));
    xTaskCreate(policy_task, "net_policy", 6144, NULL, 3, NULL);
    /* No cable within ten seconds of boot: bring Wi-Fi up (link detection takes ~2 s). */
    const esp_timer_create_args_t ta = {.callback = boot_policy_cb, .name = "wifi_boot"};
    esp_timer_handle_t t;
    if (esp_timer_create(&ta, &t) == ESP_OK) esp_timer_start_once(t, 10 * 1000 * 1000);
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
bool net_eth_link(void) { return eth_link; }

void net_eth_link_info(int *speed_mbps, bool *full_duplex)
{
    *speed_mbps = 0; *full_duplex = false;
    if (!eth_handle) return;
    eth_speed_t sp = ETH_SPEED_10M; eth_duplex_t dp = ETH_DUPLEX_HALF;
    if (esp_eth_ioctl(eth_handle, ETH_CMD_G_SPEED, &sp) == ESP_OK) *speed_mbps = sp == ETH_SPEED_100M ? 100 : 10;
    if (esp_eth_ioctl(eth_handle, ETH_CMD_G_DUPLEX_MODE, &dp) == ESP_OK) *full_duplex = dp == ETH_DUPLEX_FULL;
}
bool net_wifi_connected(void) { return wifi_up; }
bool net_time_synced(void) { return time(NULL) > 1600000000; }

void net_describe(char *out, size_t cap)
{
    if (eth_ip && wifi_ip) snprintf(out, cap, "eth %s, wifi %s", eth_ip_str, wifi_ip_str);
    else if (eth_ip) snprintf(out, cap, "eth %s", eth_ip_str);
    else if (wifi_ip) snprintf(out, cap, "wifi %s", wifi_ip_str);
    else snprintf(out, cap, "%s", eth_link ? "eth link, no ip" : "offline");
}

#pragma once
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include "esp_err.h"

esp_err_t net_init(void);                 /* Ethernet (where fitted) + optional Wi-Fi STA + SNTP + mDNS */
bool net_has_ip(void);
/* "eth 192.168.1.20" / "wifi 10.0.0.5" / "" — for the UI */
void net_describe(char *out, size_t cap);
/* The board has an Ethernet port (CONFIG_RRC_HAS_ETHERNET). */
bool net_has_ethernet(void);
bool net_eth_link(void);
/* Ethernet IPv4 as text ("" when none). */
const char *net_eth_ip(void);
/* Negotiated Ethernet speed (10/100, 0 = unknown) and duplex. */
void net_eth_link_info(int *speed_mbps, bool *full_duplex);
bool net_wifi_connected(void);
bool net_time_synced(void);
/* Re-applies Wi-Fi settings from config (after the UI changed them). Identical
 * credentials leave a live association alone. */
esp_err_t net_wifi_reconfigure(void);

/* Station details for /api/wifi:
 * {"enabled","ssid","standby","started","associated","has_ip","attempts","last_reason","last_reason_text",
 *  "connected_s","bssid","rssi","channel","security","phy","mac","ip","netmask","gateway","dns"}
 * (association and address fields only while they exist). */
int net_wifi_json(char *out, size_t cap);
/* RSSI of the current association in dBm, 0 when not associated (cached ~3 s). */
int net_wifi_rssi(void);

/* Asynchronous scan (runs on the network task; poll net_wifi_scan_json).
 * ESP_ERR_NOT_SUPPORTED while Wi-Fi is disabled, ESP_ERR_INVALID_STATE while a scan runs. */
esp_err_t net_wifi_scan_start(void);
typedef enum { NET_SCAN_IDLE, NET_SCAN_RUNNING, NET_SCAN_DONE, NET_SCAN_FAILED } net_scan_state_t;
#define NET_SCAN_MAX 24
typedef struct {
    char ssid[33];
    uint8_t bssid[6];
    int8_t rssi;
    uint8_t channel;
    uint8_t authmode;   /* wifi_auth_mode_t */
    bool wifi6;
    uint8_t aps;        /* access points advertising this SSID */
} net_ap_t;
/* {"state":"idle|running|done|failed","error":"…","networks":[{"ssid","bssid","rssi","channel","security","open","wifi6","aps"}]}
 * One entry per SSID (strongest access point), strongest first. */
int net_wifi_scan_json(char *out, size_t cap);
const char *net_wifi_authmode_str(int authmode);
const char *net_wifi_reason_str(int reason);

/* Frames the Ethernet driver has handed to the IP stack since boot, and how
 * many times in a row the receive watchdog has restarted the dock (see net.c). */
uint32_t net_eth_rx_frames(void);
uint32_t net_eth_rx_restarts(void);
/* Test hook: drop every received frame before the stack sees it, which is what
 * the EMAC lockup looks like from above. The watchdog then restarts the dock. */
void net_eth_rx_drop_test(bool on);
/* Diagnostics for a receive path that passes nothing: the IP101 PHY's registers
 * (page 0, 0..31) into regs[32]; ESP_ERR_INVALID_STATE without Ethernet. */
esp_err_t net_eth_phy_regs(uint16_t regs[32]);
/* Stops Ethernet, pulses the PHY's hardware reset, and starts it again (link,
 * DHCP and all), without restarting the dock. */
esp_err_t net_eth_restart_phy(void);

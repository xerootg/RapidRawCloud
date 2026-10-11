#pragma once
#include <stdbool.h>
#include <stddef.h>
#include "esp_err.h"

esp_err_t net_init(void);                 /* Ethernet + optional Wi-Fi STA + SNTP + mDNS */
bool net_has_ip(void);
/* "eth 192.168.1.20" / "wifi 10.0.0.5" / "" — for the UI */
void net_describe(char *out, size_t cap);
bool net_eth_link(void);
/* Ethernet IPv4 as text ("" when none). */
const char *net_eth_ip(void);
/* Negotiated Ethernet speed (10/100, 0 = unknown) and duplex. */
void net_eth_link_info(int *speed_mbps, bool *full_duplex);
bool net_wifi_connected(void);
bool net_time_synced(void);
/* Re-applies Wi-Fi credentials from config (after the UI changed them). */
esp_err_t net_wifi_reconfigure(void);

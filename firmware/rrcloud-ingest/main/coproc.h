#pragma once
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include "esp_err.h"

/* ESP32-C6 radio co-processor (Wi-Fi 6 / BLE 5) reached over ESP-Hosted on SDIO.
 *
 * The link is brought up in the background at boot so the radio is available
 * whether or not Wi-Fi is configured (BLE provisioning needs it). Wi-Fi start
 * and the co-processor update wait for it with coproc_ensure_link(). */

/* Spawns the link task; never blocks. */
esp_err_t coproc_start(void);
/* Blocks until the SDIO link is up or `timeout_ms` elapsed. */
esp_err_t coproc_ensure_link(uint32_t timeout_ms);
bool coproc_linked(void);

/* {"linked":bool,"linking":bool,"version":"3.0.9","project":"…","idf":"…",
 *  "host_lib":"3.0.9","compatible":bool,"error":"…",
 *  "update":{"state":"idle|running|done|failed","done":n,"total":n,"message":"…"}} */
int coproc_status_json(char *out, size_t cap);

/* Streams a co-processor image from `url` (http/https) into the C6's OTA slot
 * and activates it. When `sha256_hex` (64 hex chars) is given the image is
 * verified before activation. Runs in its own task; progress is reported by
 * coproc_status_json(). On success the dock restarts so the host re-links to
 * the freshly booted co-processor. Returns ESP_ERR_INVALID_STATE while an
 * update is already running. */
esp_err_t coproc_update_start(const char *url, const char *sha256_hex);

/* The ESP-Hosted host library this firmware is built against; the co-processor
 * must run the same major.minor (ESP-Hosted-MCU wire compatibility rule). */
#define COPROC_HOST_LIB_VERSION "3.0.9"
/* Stock ESP-Hosted co-processor image (Wi-Fi + Bluetooth over Hosted-HCI, SDIO)
 * for the ESP32-C6, as published by the esp-hosted-firmware project. */
#define COPROC_IMAGE_URL_DEFAULT "https://esphome.github.io/esp-hosted-firmware/v3.0.9/network_adapter_esp32c6.bin"
#define COPROC_IMAGE_SHA256_DEFAULT "80e85881783840ff7c533878b679b9a6d72c426d0696e2f15ee064ca8b6c12db"

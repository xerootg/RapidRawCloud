#pragma once
/*
 * ota — updates of the dock's own firmware (the ESP32-P4 application image).
 *
 * Three ways in, one way to flash:
 *   - ota_check() reads the manifest (rrcloud-ingest-ota.json) of the rolling
 *     `firmware-latest` release, or of `firmware-dev` (test builds) when the test
 *     channel is on, and remembers the image built for this board;
 *     ota_install_available() then installs it;
 *   - ota_install_url() downloads any http(s) URL (optional SHA-256);
 *   - ota_upload_begin/chunk/finish() take an image in pieces through the admin
 *     API: the web page's file upload and the RapidRAW app over Bluetooth.
 * Every path collects the whole image in PSRAM first and checks it (SHA-256 when
 * given, the ESP image header's chip and revision range, the project name, and
 * the board this image was built for) before a byte is written to flash: flash
 * writes stall the caches, which is what trips the ESP32-P4 Ethernet receive
 * lockup, so they never overlap a download.
 *
 * The new image boots on trial (CONFIG_BOOTLOADER_APP_ROLLBACK_ENABLE): it is
 * kept once it has stayed up for a minute and can be reached (an IP address, or
 * the Bluetooth admin service up); a crash or reset before that, or nothing
 * reachable within five minutes, returns the dock to the previous image. No
 * install starts while the running image is still on trial (the other slot holds
 * the image to go back to). Installs only happen on request, never during a
 * sync, and a sync does not start while an install is in progress.
 *
 * Installs are as open as the rest of the admin API: with no admin password set,
 * anyone on the network or with a bonded phone can install any image (the owner's
 * choice, 2026-10-26; the web and Bluetooth gates in web.c / ble.c apply when a
 * password is set).
 */
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include "esp_err.h"

/* Call early in app_main: notes whether this boot is a trial of a new image and
 * whether the previous update was rolled back, and starts the health check. */
void ota_init(void);

/* True from the start of a download/upload until it fails or the dock restarts. */
bool ota_busy(void);

/* /api/ota document. */
int ota_status_json(char *out, size_t cap);

/* Asynchronous; progress and result in the status document. ESP_ERR_INVALID_STATE:
 * another job runs; ESP_ERR_NOT_ALLOWED: a sync runs; ESP_ERR_NOT_FINISHED: the
 * running image is still on trial; ESP_ERR_NOT_FOUND (install_available): no check
 * result for the current channel. */
esp_err_t ota_check(void);
esp_err_t ota_install_available(void);
esp_err_t ota_install_url(const char *url, const char *sha256_hex);

/* Synchronous pieces of an upload. `sha256_hex` may be empty (no digest check).
 * Chunks must arrive in order; a repeated chunk (offset below what was received)
 * is accepted and ignored so a retried request is harmless. *expected receives the
 * next offset the dock wants. */
esp_err_t ota_upload_begin(size_t size, const char *sha256_hex);
esp_err_t ota_upload_chunk(size_t offset, const uint8_t *data, size_t len, size_t *expected);
esp_err_t ota_upload_finish(void);

/* Drops a check, download or upload; ESP_ERR_INVALID_STATE once flashing has begun. */
esp_err_t ota_cancel(void);

/* Upload chunk size the clients use (raw bytes; base64 in the JSON body keeps a
 * request under the API's 8 KiB body limit, web and Bluetooth alike). */
#define OTA_UPLOAD_CHUNK 4096

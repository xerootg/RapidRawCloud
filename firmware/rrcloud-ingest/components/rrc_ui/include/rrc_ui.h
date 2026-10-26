/*
 * rrc_ui — the dock's on-screen Material 3 interface (LVGL 9; 480x800 portrait with a
 * bottom navigation bar, or 800x480 landscape with a navigation rail, chosen from the
 * display's size at start and after each rotation).
 *
 * The UI is a third transport for the admin API, next to the web page (web.c)
 * and BLE (ble.c): every read and every change goes through the same
 * `/api/...` routes and JSON documents that api_dispatch() serves, so the screen
 * can do exactly what the web page does and validation lives in one place
 * (app_config_apply_json). This component makes no ESP-IDF calls; the firmware
 * glue (main/display.c) and the host simulator (ui_sim/) each provide the host
 * hooks below.
 *
 * Threading: every rrc_ui_* function runs with the LVGL lock held (on the LVGL
 * task, or wrapped in lv_lock()/lv_unlock()). host->request() must not block:
 * the firmware queues it to a worker that calls api_dispatch() and later calls
 * rrc_ui_complete() under the LVGL lock.
 */
#pragma once
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include "lvgl.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
    /* Admin API call. method "GET"/"POST", path "/api/…", body JSON or NULL.
     * Pseudo-method "AUTH" (path "", body = candidate admin password) checks the
     * on-screen unlock: 200 when api_password_ok(), 401 otherwise. Complete it
     * with rrc_ui_complete(token, …). */
    void (*request)(const char *method, const char *path, const char *body, uint32_t token);
    /* Backlight level 0..100 (0 = off). Applied immediately; persistence goes through
     * /api/config like every other setting. */
    void (*set_backlight)(int percent);
    /* Optional. Turns the panel: 0, 90, 180 or 270 degrees (the config's display_rotation).
     * Called with the LVGL lock held; afterwards the display reports the new resolution
     * and the UI lays itself out again. NULL hides the Orientation setting. */
    void (*set_rotation)(int degrees);
} rrc_ui_host_t;

/* Builds the interface on `disp`'s active screen. `host` must outlive the UI. */
void rrc_ui_start(const rrc_ui_host_t *host, lv_display_t *disp);
/* Delivers the answer to request `token` (status is HTTP-style; json may be NULL). */
void rrc_ui_complete(uint32_t token, int status, const char *json, size_t len);
/* The board's user key (BOOT on the Touch-LCD-4.3): screen off ↔ on. */
void rrc_ui_user_key(void);
/* True while the screen is asleep (the host may poll the touch controller slower). */
bool rrc_ui_asleep(void);

#ifdef __cplusplus
}
#endif

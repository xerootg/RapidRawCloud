#pragma once
#include "esp_err.h"

/* On-screen UI for boards with CONFIG_RRC_HAS_DISPLAY (main/board.h): brings up
 * the MIPI-DSI panel, touch, backlight and user key, starts LVGL and the
 * components/rrc_ui interface. Failures are logged and leave the dock running
 * headless (the web page, BLE and console still work). */
esp_err_t display_start(void);

/* Call once the rest of the dock is up (sync engine, camera sources, web
 * server). The panel comes up first so the screen shows progress, but the UI's
 * API requests touch state those create, so they wait for this. */
void display_set_api_ready(void);

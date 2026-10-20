#pragma once
#include "esp_err.h"

/* On-screen UI for boards with CONFIG_RRC_HAS_DISPLAY (main/board.h): brings up
 * the MIPI-DSI panel, touch, backlight and user key, starts LVGL and the
 * components/rrc_ui interface. Failures are logged and leave the dock running
 * headless (the web page, BLE and console still work). */
esp_err_t display_start(void);

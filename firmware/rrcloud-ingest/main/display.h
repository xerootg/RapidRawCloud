#pragma once
#include <stdbool.h>
#include "sdkconfig.h"
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

#if CONFIG_RRC_DISPLAY_RPI_DSI
/* Panel bring-up tool (console `dsi-timing`): stop the DSI link and bring it back
 * with another lane rate, pixel clock, porches and video mode, keeping LVGL and
 * the UI. Every clone of the Pi display has its own idea of the timing it wants. */
typedef struct {
    float lane_mbps, dpi_mhz;
    int hsw, hbp, hfp, vsw, vbp, vfp;
    bool non_burst, disable_lp;
} display_dsi_timing_t;
esp_err_t display_dsi_retime(const display_dsi_timing_t *t);
void display_dsi_get_timing(display_dsi_timing_t *out);
esp_err_t display_dsi_bridge_write(uint16_t reg, uint32_t val);
void display_dsi_test_pattern(bool on);
void display_dsi_dump_framebuffer(void);
#endif

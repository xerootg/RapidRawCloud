# Vendored from https://github.com/kurozz/esp_lcd_rpi_dsi at commit 4a3f3b6 (v0.1.0),
# GPL-2.0-only (LICENSE). Sources are the upstream files; the two local changes are
# marked "rrcloud:" in this file and in esp_lcd_rpi_dsi.c, and both exist only because
# upstream targets ESP-IDF 6 while the dock builds on 5.5.5.
#
# rrcloud: the DSI HAL headers the panel driver uses (hal/mipi_dsi_hal.h, hal/mipi_dsi_host_ll.h)
# live in the `hal` component before IDF 6.0 and in `esp_hal_lcd` from 6.0.
if(IDF_VERSION_MAJOR GREATER_EQUAL 6)
    set(dsi_hal_component esp_hal_lcd)
else()
    set(dsi_hal_component hal)
endif()

idf_component_register(SRCS "esp_lcd_rpi_dsi.c" "esp_lcd_rpi_dsi_panel.c"
                       INCLUDE_DIRS "include"
                       REQUIRES esp_lcd esp_driver_i2c esp_hw_support esp_lcd_touch_ft5x06
                       PRIV_REQUIRES ${dsi_hal_component})

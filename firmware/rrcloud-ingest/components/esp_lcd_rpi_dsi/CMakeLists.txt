# Vendored from https://github.com/kurozz/esp_lcd_rpi_dsi at commit 4a3f3b6 (v0.1.0),
# GPL-2.0-only (LICENSE). Sources are the upstream files; every local change is marked
# "rrcloud:" where it sits (this file, idf_component.yml, esp_lcd_rpi_dsi.c and .h,
# esp_lcd_rpi_dsi_panel.c and .h): the ESP-IDF 5.5.5 build (upstream targets 6.x; its
# README says so), two config fields (frame buffer format, touch I2C speed) and the
# bridge-register entry point the dock's bring-up console uses.
#
# rrcloud: the DSI HAL headers the panel driver uses (hal/mipi_dsi_hal.h, hal/mipi_dsi_host_ll.h)
# live in the `hal` component before IDF 6.0 and in `esp_hal_lcd` from 6.0.
if(IDF_VERSION_MAJOR GREATER_EQUAL 6)
    set(dsi_hal_component esp_hal_lcd)
else()
    set(dsi_hal_component hal)
endif()

idf_component_register(SRCS "esp_lcd_rpi_dsi.c" "esp_lcd_rpi_dsi_panel.c"
                       # rrcloud: the panel-level header too, for the bring-up register writes (console dsi-reg)
                       INCLUDE_DIRS "include" "."
                       REQUIRES esp_lcd esp_driver_i2c esp_hw_support esp_lcd_touch_ft5x06
                       PRIV_REQUIRES ${dsi_hal_component})

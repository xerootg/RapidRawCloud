/*
 * panel_rpi_dsi — 800x480 MIPI-DSI touch displays that speak the Raspberry Pi
 * 7" Touch Display protocol ("driver-free" Pi displays such as the Hosyond and
 * OSOYOO 5-inch panels): a TC358762-compatible DSI-to-DPI bridge on one data
 * lane, an ATTINY-compatible controller at I2C 0x45 for power, resets and
 * backlight, and an FT5x06 touch controller at I2C 0x38, all on the 15-pin DSI
 * connector.
 */
#pragma once
#include <stdbool.h>
#include "esp_err.h"
#include "driver/i2c_master.h"
#include "esp_lcd_types.h"
#include "esp_lcd_touch.h"

typedef struct {
    esp_lcd_panel_handle_t panel;   /* DPI panel, RGB565 framebuffer, 800x480 landscape */
    esp_lcd_touch_handle_t touch;   /* NULL when the FT5x06 does not answer */
    uint8_t controller_id;          /* REG_ID: 0xde (firmware rev 1) or 0xc3 (rev 2) */
} rpi_dsi_t;

/* Probes the controller on `bus`; ESP_ERR_NOT_FOUND when nothing answers at
 * 0x45 (no display plugged in), so the caller can run headless. Otherwise
 * powers the panel, configures the bridge, starts video and attaches touch. */
esp_err_t rpi_dsi_start(i2c_master_bus_handle_t bus, rpi_dsi_t *out);

/* Backlight 0..100 % through the controller's PWM register. */
void rpi_dsi_set_backlight(int pct);

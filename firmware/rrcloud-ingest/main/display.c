/*
 * display — hardware glue for the on-screen UI, LVGL 9 and the bridge between
 * components/rrc_ui and the admin API. Two display drivers, chosen by board
 * capability (main/Kconfig.projbuild):
 *   - CONFIG_RRC_DISPLAY_ST7701 (ESP32-P4-WIFI6-Touch-LCD-4.3): 480x800 ST7701 over
 *     2-lane MIPI-DSI, GT911 touch, PWM backlight, the BOOT key. The UI's own
 *     orientation; LVGL renders straight into the DPI framebuffers.
 *   - CONFIG_RRC_DISPLAY_RPI_DSI (PoE-ETH + 5-inch Pi DSI display): an 800x480
 *     landscape panel speaking the Raspberry Pi touch-display protocol, brought up
 *     by components/esp_lcd_rpi_dsi (one DSI lane, RGB888 framebuffer, the panel's
 *     own controller doing power and backlight, FT5x06 touch), mounted standing up.
 *     LVGL renders the 480x800 UI into small draw buffers and each dirty area is
 *     rotated into the single DPI framebuffer; touch points are rotated back by LVGL.
 *
 * Rendering budget. The dock's real job is moving photos: uploads keep both
 * cores busy with BLAKE3, TLS and lwIP, and internal RAM is the scarcest
 * resource on the board (README "Throughput notes"). So the UI:
 *   - keeps every pixel buffer in PSRAM (the ST7701 path renders straight into the
 *     two DPI framebuffers, LVGL direct mode, swapped on vsync; the rotated path
 *     draws into two 113 KB buffers): no internal RAM for pixels;
 *   - keeps LVGL's heap and its task stack in PSRAM (lv_*_core below);
 *   - runs at a priority below the sync and upload tasks, on one draw unit, so a
 *     busy dock renders a little later rather than uploading slower;
 *   - stops polling and turns the backlight off when the screen sleeps.
 *
 * API bridge. rrc_ui talks to the dock through the same routes as the web page
 * (api_dispatch). Requests are queued to a worker task with an internal-RAM
 * stack (a config save writes NVS, which must not run on a PSRAM stack) and the
 * answer is delivered back under the LVGL lock.
 */
#include "display.h"
#include <string.h>
#include <stdlib.h>
#include "sdkconfig.h"
#include "esp_log.h"
#include "esp_check.h"
#include "esp_timer.h"
#include "esp_heap_caps.h"
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/queue.h"
#include "freertos/semphr.h"
#include "freertos/idf_additions.h"
#include "driver/gpio.h"
#include "driver/ledc.h"
#include "driver/i2c_master.h"
#include "esp_ldo_regulator.h"
#include "esp_lcd_mipi_dsi.h"
#include "esp_lcd_panel_ops.h"
#include "esp_lcd_panel_io.h"
#include "esp_lcd_io_i2c.h"
#if CONFIG_RRC_DISPLAY_ST7701
#include "esp_lcd_st7701.h"
#include "esp_lcd_touch_gt911.h"
#endif
#if CONFIG_RRC_DISPLAY_RPI_DSI
#include "esp_cache.h"
#include "esp_lcd_rpi_dsi.h"
#include "esp_lcd_rpi_dsi_panel.h"
#endif
#include "lvgl.h"
#include "src/draw/sw/lv_draw_sw_utils.h"
#include "src/stdlib/lv_mem.h"
#include "rrc_ui.h"
#include "board.h"
#include "api.h"
#include "log_ring.h"

static const char *TAG = "display";

static esp_lcd_panel_handle_t panel;
static esp_lcd_touch_handle_t touch;
#if CONFIG_RRC_DISPLAY_ST7701
static SemaphoreHandle_t vsync_sem;
#endif
static lv_display_t *disp;

#if CONFIG_RRC_DISPLAY_ST7701
/* ---- panel ----------------------------------------------------------------------------
 * Vendor initialisation sequence for this panel, from the board's BSP
 * (waveshare esp32_p4_wifi6_touch_lcd_4_3 1.0.1). */
static const st7701_lcd_init_cmd_t panel_init_cmds[] = {
    {0xFF, (uint8_t[]){0x77, 0x01, 0x00, 0x00, 0x13}, 5, 0},
    {0xEF, (uint8_t[]){0x08}, 1, 0},
    {0xFF, (uint8_t[]){0x77, 0x01, 0x00, 0x00, 0x10}, 5, 0},
    {0xC0, (uint8_t[]){0x63, 0x00}, 2, 0},
    {0xC1, (uint8_t[]){0x0D, 0x02}, 2, 0},
    {0xC2, (uint8_t[]){0x17, 0x08}, 2, 0},
    {0xCC, (uint8_t[]){0x10}, 1, 0},
    {0xB0, (uint8_t[]){0x40, 0xC9, 0x94, 0x0E, 0x10, 0x05, 0x0B, 0x09, 0x08, 0x26, 0x04, 0x52, 0x10, 0x69, 0x6B, 0x69}, 16, 0},
    {0xB1, (uint8_t[]){0x40, 0xD2, 0x98, 0x0C, 0x92, 0x07, 0x09, 0x08, 0x07, 0x25, 0x02, 0x0E, 0x0C, 0x6E, 0x78, 0x55}, 16, 0},
    {0xFF, (uint8_t[]){0x77, 0x01, 0x00, 0x00, 0x11}, 5, 0},
    {0xB0, (uint8_t[]){0x5D}, 1, 0},
    {0xB1, (uint8_t[]){0x4E}, 1, 0},
    {0xB2, (uint8_t[]){0x87}, 1, 0},
    {0xB3, (uint8_t[]){0x80}, 1, 0},
    {0xB5, (uint8_t[]){0x4E}, 1, 0},
    {0xB7, (uint8_t[]){0x85}, 1, 0},
    {0xB8, (uint8_t[]){0x21}, 1, 0},
    {0xB9, (uint8_t[]){0x10, 0x1F}, 2, 0},
    {0xBB, (uint8_t[]){0x03}, 1, 0},
    {0xBC, (uint8_t[]){0x00}, 1, 0},
    {0xC1, (uint8_t[]){0x78}, 1, 0},
    {0xC2, (uint8_t[]){0x78}, 1, 0},
    {0xD0, (uint8_t[]){0x88}, 1, 0},
    {0xE0, (uint8_t[]){0x00, 0x3A, 0x02}, 3, 0},
    {0xE1, (uint8_t[]){0x04, 0xA0, 0x00, 0xA0, 0x05, 0xA0, 0x00, 0xA0, 0x00, 0x40, 0x40}, 11, 0},
    {0xE2, (uint8_t[]){0x30, 0x00, 0x40, 0x40, 0x32, 0xA0, 0x00, 0xA0, 0x00, 0xA0, 0x00, 0xA0, 0x00}, 13, 0},
    {0xE3, (uint8_t[]){0x00, 0x00, 0x33, 0x33}, 4, 0},
    {0xE4, (uint8_t[]){0x44, 0x44}, 2, 0},
    {0xE5, (uint8_t[]){0x09, 0x2E, 0xA0, 0xA0, 0x0B, 0x30, 0xA0, 0xA0, 0x05, 0x2A, 0xA0, 0xA0, 0x07, 0x2C, 0xA0, 0xA0}, 16, 0},
    {0xE6, (uint8_t[]){0x00, 0x00, 0x33, 0x33}, 4, 0},
    {0xE7, (uint8_t[]){0x44, 0x44}, 2, 0},
    {0xE8, (uint8_t[]){0x08, 0x2D, 0xA0, 0xA0, 0x0A, 0x2F, 0xA0, 0xA0, 0x04, 0x29, 0xA0, 0xA0, 0x06, 0x2B, 0xA0, 0xA0}, 16, 0},
    {0xEB, (uint8_t[]){0x00, 0x00, 0x4E, 0x4E, 0x00, 0x00, 0x00}, 7, 0},
    {0xEC, (uint8_t[]){0x08, 0x01}, 2, 0},
    {0xED, (uint8_t[]){0xB0, 0x2B, 0x98, 0xA4, 0x56, 0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xF7, 0x65, 0x4A, 0x89, 0xB2, 0x0B}, 16, 0},
    {0xEF, (uint8_t[]){0x08, 0x08, 0x08, 0x45, 0x3F, 0x54}, 6, 0},
    {0xFF, (uint8_t[]){0x77, 0x01, 0x00, 0x00, 0x00}, 5, 0},
    {0x11, (uint8_t[]){0x00}, 0, 120},
    {0x29, (uint8_t[]){0x00}, 0, 0},
};

static bool on_refresh_done(esp_lcd_panel_handle_t p, esp_lcd_dpi_panel_event_data_t *ed, void *ctx)
{
    (void)p; (void)ed; (void)ctx;
    BaseType_t woken = pdFALSE;
    xSemaphoreGiveFromISR(vsync_sem, &woken);
    return woken == pdTRUE;
}

static esp_err_t panel_init(void)
{
    static esp_ldo_channel_handle_t phy_ldo;
    esp_ldo_channel_config_t ldo = {.chan_id = BOARD_DSI_PHY_LDO_CHAN, .voltage_mv = BOARD_DSI_PHY_LDO_MV};
    ESP_RETURN_ON_ERROR(esp_ldo_acquire_channel(&ldo, &phy_ldo), TAG, "D-PHY LDO");

    esp_lcd_dsi_bus_handle_t bus;
    esp_lcd_dsi_bus_config_t bus_cfg = {
        .bus_id = 0,
        .num_data_lanes = BOARD_LCD_DSI_LANES,
        /* phy_clk_src left 0: the driver picks the PHY PLL reference for the chip
         * generation the image is built for (PLL_F20M before rev 3, XTAL from rev 3);
         * the old MIPI_DSI_PHY_CLK_SRC_DEFAULT is the pre-rev-3 one only. */
        .lane_bit_rate_mbps = BOARD_LCD_DSI_LANE_MBPS,
    };
    ESP_RETURN_ON_ERROR(esp_lcd_new_dsi_bus(&bus_cfg, &bus), TAG, "DSI bus");
    esp_lcd_panel_io_handle_t io;
    esp_lcd_dbi_io_config_t dbi = {.virtual_channel = 0, .lcd_cmd_bits = 8, .lcd_param_bits = 8};
    ESP_RETURN_ON_ERROR(esp_lcd_new_panel_io_dbi(bus, &dbi, &io), TAG, "DBI io");
    esp_lcd_dpi_panel_config_t dpi = {
        .dpi_clk_src = MIPI_DSI_DPI_CLK_SRC_DEFAULT,
        .dpi_clock_freq_mhz = BOARD_LCD_DPI_CLK_MHZ,
        .virtual_channel = 0,
        .pixel_format = LCD_COLOR_PIXEL_FORMAT_RGB565,
        .num_fbs = 2,   /* LVGL draws into one while the DPI scans the other */
        .video_timing = {
            .h_size = BOARD_LCD_H_RES, .v_size = BOARD_LCD_V_RES,
            .hsync_back_porch = 42, .hsync_pulse_width = 12, .hsync_front_porch = 42,
            .vsync_back_porch = 2, .vsync_pulse_width = 8, .vsync_front_porch = 60,
        },
    };
    st7701_vendor_config_t vendor = {
        .init_cmds = panel_init_cmds,
        .init_cmds_size = sizeof panel_init_cmds / sizeof panel_init_cmds[0],
        .flags.use_mipi_interface = 1,
        .mipi_config = {.dsi_bus = bus, .dpi_config = &dpi},
    };
    esp_lcd_panel_dev_config_t dev = {
        .bits_per_pixel = 16,
        .rgb_ele_order = LCD_RGB_ELEMENT_ORDER_RGB,
        .reset_gpio_num = BOARD_LCD_RST_GPIO,
        .vendor_config = &vendor,
    };
    ESP_RETURN_ON_ERROR(esp_lcd_new_panel_st7701(io, &dev, &panel), TAG, "ST7701");
    ESP_RETURN_ON_ERROR(esp_lcd_panel_reset(panel), TAG, "panel reset");
    ESP_RETURN_ON_ERROR(esp_lcd_panel_init(panel), TAG, "panel init");
    vsync_sem = xSemaphoreCreateBinary();
    esp_lcd_dpi_panel_event_callbacks_t cbs = {.on_refresh_done = on_refresh_done};
    ESP_RETURN_ON_ERROR(esp_lcd_dpi_panel_register_event_callbacks(panel, &cbs, NULL), TAG, "vsync callback");
    return ESP_OK;
}

/* ---- backlight -------------------------------------------------------------------------- */
#define BL_TIMER   LEDC_TIMER_1
#define BL_CHANNEL LEDC_CHANNEL_1

static esp_err_t backlight_init(void)
{
    gpio_config_t en = {.pin_bit_mask = 1ULL << BOARD_LCD_BL_EN_GPIO, .mode = GPIO_MODE_OUTPUT};
    ESP_RETURN_ON_ERROR(gpio_config(&en), TAG, "BL_EN");
    gpio_set_level(BOARD_LCD_BL_EN_GPIO, 0);
    ledc_timer_config_t t = {.speed_mode = LEDC_LOW_SPEED_MODE, .duty_resolution = LEDC_TIMER_10_BIT, .timer_num = BL_TIMER,
                             .freq_hz = 20000, .clk_cfg = LEDC_AUTO_CLK};
    ESP_RETURN_ON_ERROR(ledc_timer_config(&t), TAG, "LEDC timer");
    ledc_channel_config_t c = {.gpio_num = BOARD_LCD_BL_PWM_GPIO, .speed_mode = LEDC_LOW_SPEED_MODE, .channel = BL_CHANNEL,
                               .timer_sel = BL_TIMER, .duty = 0, .hpoint = 0};
    return ledc_channel_config(&c);
}

static void backlight_set(int pct)
{
    if (pct < 0) pct = 0;
    if (pct > 100) pct = 100;
    ledc_set_duty(LEDC_LOW_SPEED_MODE, BL_CHANNEL, (uint32_t)(1023 * pct / 100));
    ledc_update_duty(LEDC_LOW_SPEED_MODE, BL_CHANNEL);
    gpio_set_level(BOARD_LCD_BL_EN_GPIO, pct > 0);   /* boost converter off when dark */
}

/* ---- touch --------------------------------------------------------------------------- */
static esp_err_t touch_init(void)
{
    i2c_master_bus_handle_t bus;
    i2c_master_bus_config_t bc = {
        .i2c_port = -1, .sda_io_num = BOARD_I2C_SDA_GPIO, .scl_io_num = BOARD_I2C_SCL_GPIO,
        .clk_source = I2C_CLK_SRC_DEFAULT, .glitch_ignore_cnt = 7, .flags.enable_internal_pullup = true,
    };
    ESP_RETURN_ON_ERROR(i2c_new_master_bus(&bc, &bus), TAG, "I2C bus");
    esp_lcd_panel_io_i2c_config_t io_cfg = ESP_LCD_TOUCH_IO_I2C_GT911_CONFIG();
    /* The GT911 latches 0x5D or 0x14 from INT at reset; INT is not routed here. */
    if (i2c_master_probe(bus, ESP_LCD_TOUCH_IO_I2C_GT911_ADDRESS, 50) == ESP_OK) io_cfg.dev_addr = ESP_LCD_TOUCH_IO_I2C_GT911_ADDRESS;
    else if (i2c_master_probe(bus, ESP_LCD_TOUCH_IO_I2C_GT911_ADDRESS_BACKUP, 50) == ESP_OK) io_cfg.dev_addr = ESP_LCD_TOUCH_IO_I2C_GT911_ADDRESS_BACKUP;
    else return ESP_ERR_NOT_FOUND;
    io_cfg.scl_speed_hz = 400000;
    esp_lcd_panel_io_handle_t io;
    ESP_RETURN_ON_ERROR(esp_lcd_new_panel_io_i2c_v2(bus, &io_cfg, &io), TAG, "touch io");
    esp_lcd_touch_config_t tc = {
        .x_max = BOARD_LCD_H_RES, .y_max = BOARD_LCD_V_RES,
        .rst_gpio_num = BOARD_TOUCH_RST_GPIO, .int_gpio_num = GPIO_NUM_NC,
    };
    return esp_lcd_touch_new_i2c_gt911(io, &tc, &touch);
}

#endif /* CONFIG_RRC_DISPLAY_ST7701 */

#if CONFIG_RRC_DISPLAY_RPI_DSI
/* ---- Raspberry Pi DSI display ---------------------------------------------------------
 * components/esp_lcd_rpi_dsi owns the D-PHY, the DSI bus, the panel's controller at
 * I2C 0x45 (power sequence, bridge reset, backlight PWM) and the FT5x06. The dock
 * only brings the I2C bus, which is the board's internal one (shared with the codec)
 * and the place to find out whether a display is plugged in at all. */
#define RPI_BPP 3   /* the driver feeds the bridge RGB888: three bytes per pixel, blue first */

static esp_lcd_rpi_dsi_handle_t rpi;
static i2c_master_bus_handle_t rpi_i2c;
static esp_lcd_rpi_dsi_config_t rpi_cfg;
static uint8_t *rpi_fb;   /* the DPI framebuffer, 800x480 RGB888, native orientation */

static void backlight_set(int pct)
{
    if (pct < 0) pct = 0;
    if (pct > 100) pct = 100;
    if (rpi) esp_lcd_rpi_dsi_set_brightness(rpi, (uint8_t)(255 * pct / 100));
}

/* Bring the driver up on the bus with rpi_cfg and take the framebuffer. */
static esp_err_t rpi_panel_up(void)
{
    ESP_RETURN_ON_ERROR(esp_lcd_rpi_dsi_start(&rpi_cfg, &rpi), TAG, "rpi_dsi");
    panel = esp_lcd_rpi_dsi_get_panel(rpi);
    touch = esp_lcd_rpi_dsi_get_touch(rpi);
    void *fb = NULL;
    ESP_RETURN_ON_ERROR(esp_lcd_dpi_panel_get_frame_buffer(panel, 1, &fb), TAG, "framebuffer");
    rpi_fb = fb;
    memset(rpi_fb, 0, BOARD_LCD_H_RES * BOARD_LCD_V_RES * RPI_BPP);
    esp_cache_msync(rpi_fb, BOARD_LCD_H_RES * BOARD_LCD_V_RES * RPI_BPP, ESP_CACHE_MSYNC_FLAG_DIR_C2M | ESP_CACHE_MSYNC_FLAG_UNALIGNED);
    return ESP_OK;
}

static esp_err_t rpi_panel_init(void)
{
    i2c_master_bus_config_t bc = {
        .i2c_port = -1, .sda_io_num = BOARD_I2C_SDA_GPIO, .scl_io_num = BOARD_I2C_SCL_GPIO,
        .clk_source = I2C_CLK_SRC_DEFAULT, .glitch_ignore_cnt = 7, .flags.enable_internal_pullup = true,
    };
    ESP_RETURN_ON_ERROR(i2c_new_master_bus(&bc, &rpi_i2c), TAG, "I2C bus");
    /* Nothing at 0x45 means no display on the connector: run headless rather than
     * bring up a DSI bus with nobody listening. */
    if (i2c_master_probe(rpi_i2c, ESP_LCD_RPI_DSI_ATTINY_ADDR, 50) != ESP_OK) {
        i2c_del_master_bus(rpi_i2c);
        rpi_i2c = NULL;
        return ESP_ERR_NOT_FOUND;
    }
    rpi_cfg = (esp_lcd_rpi_dsi_config_t)ESP_LCD_RPI_DSI_DEFAULT_CONFIG();
    rpi_cfg.i2c.bus = rpi_i2c;
    rpi_cfg.ldo_chan = BOARD_DSI_PHY_LDO_CHAN;
    rpi_cfg.ldo_voltage_mv = BOARD_DSI_PHY_LDO_MV;
    rpi_cfg.brightness = 255;
    /* Link timing, measured on the Hosyond 5-inch panel with the console's
     * `dsi-timing` (every setting below was A/B'd by eye on the panel):
     *   - the lane rate is exactly 24x the pixel clock. The bridge derives its DPI
     *     pixel clock from the DSI clock lane (lane / 24 for RGB888 on one lane); any
     *     other ratio has it clocking pixels out at a different rate than they
     *     arrive, and the driver's defaults (700 Mbit/s for 26 MHz) dropped every
     *     other pixel;
     *   - the P4 makes the pixel clock from PLL_F240M with an integer divider and the
     *     D-PHY PLL from 20 MHz in even multiples, so the exact-24x pairs are
     *     26.67/640, 30/720 and 40/960. Faint column banding on this glass got weaker
     *     with every step up; 40 MHz is where it was no longer visible;
     *   - the porches are then stretched to hold the glass at 60 Hz (1056 x 631; the
     *     2026 kernel fix for the genuine panel's timing uses the same 20/26/210 line).
     *     They also matter for the host: it programs its line in lane byte clocks,
     *     and the legacy hfp of 1 pixel (3 bytes) cannot hold a blanking packet;
     *   - burst mode. Non-burst with sync pulses worked just as well and LP
     *     excursions in blanking made no difference either way. */
    rpi_cfg.timing = (esp_lcd_video_timing_t){
        .h_size = ESP_LCD_RPI_DSI_H_RES, .v_size = ESP_LCD_RPI_DSI_V_RES,
        .hsync_pulse_width = 20, .hsync_back_porch = 26, .hsync_front_porch = 210,
        .vsync_pulse_width = 2, .vsync_back_porch = 21, .vsync_front_porch = 128,
    };
    rpi_cfg.dpi_clock_freq_mhz = 40.0f;
    rpi_cfg.lane_bit_rate_mbps = 24.0f * rpi_cfg.dpi_clock_freq_mhz;
    rpi_cfg.flags.non_burst_video = 0;
    rpi_cfg.flags.disable_lp = 0;
    esp_err_t e = rpi_panel_up();
    if (e != ESP_OK) { i2c_del_master_bus(rpi_i2c); rpi_i2c = NULL; }
    return e;
}

void display_dsi_get_timing(display_dsi_timing_t *out)
{
    const esp_lcd_video_timing_t *t = &rpi_cfg.timing;
    *out = (display_dsi_timing_t){
        .lane_mbps = rpi_cfg.lane_bit_rate_mbps, .dpi_mhz = rpi_cfg.dpi_clock_freq_mhz,
        .hsw = t->hsync_pulse_width, .hbp = t->hsync_back_porch, .hfp = t->hsync_front_porch,
        .vsw = t->vsync_pulse_width, .vbp = t->vsync_back_porch, .vfp = t->vsync_front_porch,
        .non_burst = rpi_cfg.flags.non_burst_video, .disable_lp = rpi_cfg.flags.disable_lp,
    };
}

/* Bring-up: a known-clean pattern straight into the framebuffer, bypassing LVGL
 * (which overwrites it on its next redraw): black, a white 1-px grid every 100 px,
 * a white box, and R/G/B bars at the top. Whatever smears on this is the panel. */
static bool pattern_hold;   /* flushes are dropped while the pattern is up */

void display_dsi_test_pattern(bool on)
{
    if (!rpi_fb) return;
    lv_lock();
    pattern_hold = on;
    if (!on) { lv_obj_invalidate(lv_screen_active()); lv_unlock(); return; }
    memset(rpi_fb, 0, BOARD_LCD_H_RES * BOARD_LCD_V_RES * RPI_BPP);
    for (int y = 0; y < BOARD_LCD_V_RES; y++) {
        uint8_t *row = rpi_fb + (size_t)y * BOARD_LCD_H_RES * RPI_BPP;
        for (int x = 0; x < BOARD_LCD_H_RES; x++) {
            uint8_t b = 0, g = 0, r = 0;
            if (y < 40) { if (x < 266) r = 255; else if (x < 533) g = 255; else b = 255; }
            else if (x % 100 == 0 || y % 100 == 0) r = g = b = 255;
            else if (x >= 300 && x < 500 && y >= 200 && y < 300) r = g = b = 255;
            row[x * 3] = b; row[x * 3 + 1] = g; row[x * 3 + 2] = r;
        }
    }
    esp_cache_msync(rpi_fb, BOARD_LCD_H_RES * BOARD_LCD_V_RES * RPI_BPP, ESP_CACHE_MSYNC_FLAG_DIR_C2M | ESP_CACHE_MSYNC_FLAG_UNALIGNED);
    lv_unlock();
}

/* Bring-up: the framebuffer as it is, base64, one native row per line, under the
 * LVGL lock so the dump is one consistent frame. */
void display_dsi_dump_framebuffer(void)
{
    if (!rpi_fb) return;
    static const char b64[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    const size_t row_bytes = BOARD_LCD_H_RES * RPI_BPP;   /* 2400: a multiple of 3 */
    char *line = malloc(row_bytes / 3 * 4 + 1);
    if (!line) return;
    lv_lock();
    printf("FBDUMP BEGIN %dx%d rgb888\n", BOARD_LCD_H_RES, BOARD_LCD_V_RES);
    for (int y = 0; y < BOARD_LCD_V_RES; y++) {
        const uint8_t *p = rpi_fb + (size_t)y * row_bytes;
        char *o = line;
        for (size_t i = 0; i < row_bytes; i += 3) {
            uint32_t v = (p[i] << 16) | (p[i + 1] << 8) | p[i + 2];
            *o++ = b64[(v >> 18) & 63]; *o++ = b64[(v >> 12) & 63]; *o++ = b64[(v >> 6) & 63]; *o++ = b64[v & 63];
        }
        *o = 0;
        printf("FB %s\n", line);
        vTaskDelay(1);   /* let the console UART drain */
    }
    printf("FBDUMP END\n");
    lv_unlock();
    free(line);
}

esp_err_t display_dsi_bridge_write(uint16_t reg, uint32_t val)
{
    if (!panel) return ESP_ERR_INVALID_STATE;
    return esp_lcd_panel_rpi_dsi_bridge_write(panel, reg, val);
}

esp_err_t display_dsi_retime(const display_dsi_timing_t *t)
{
    if (!rpi || !disp) return ESP_ERR_INVALID_STATE;
    esp_lcd_rpi_dsi_config_t keep = rpi_cfg;
    rpi_cfg.lane_bit_rate_mbps = t->lane_mbps;
    rpi_cfg.dpi_clock_freq_mhz = t->dpi_mhz;
    rpi_cfg.timing.hsync_pulse_width = t->hsw; rpi_cfg.timing.hsync_back_porch = t->hbp; rpi_cfg.timing.hsync_front_porch = t->hfp;
    rpi_cfg.timing.vsync_pulse_width = t->vsw; rpi_cfg.timing.vsync_back_porch = t->vbp; rpi_cfg.timing.vsync_front_porch = t->vfp;
    rpi_cfg.flags.non_burst_video = t->non_burst;
    rpi_cfg.flags.disable_lp = t->disable_lp;
    /* Under the LVGL lock nothing flushes into the framebuffer the driver is about
     * to free, and touch_read_cb sees touch == NULL meanwhile. */
    lv_lock();
    touch = NULL;
    panel = NULL;
    rpi_fb = NULL;
    esp_lcd_rpi_dsi_stop(rpi);
    rpi = NULL;
    esp_err_t e = rpi_panel_up();
    if (e != ESP_OK) {
        log_ring_printf("display: dsi-timing failed (%s), restoring the previous link", esp_err_to_name(e));
        rpi_cfg = keep;
        e = rpi_panel_up();
    }
    if (e == ESP_OK) lv_obj_invalidate(lv_screen_active());
    lv_unlock();
    return e;
}

/* Partial mode: `area` is in the UI's portrait coordinates and `px` holds it
 * row by row. Rotate it straight into its place in the landscape framebuffer,
 * then write the touched rows back from the cache (the DPI DMA reads PSRAM). */
static void flush_rotated_cb(lv_display_t *d, const lv_area_t *area, uint8_t *px)
{
    if (pattern_hold) { lv_display_flush_ready(d); return; }
    lv_area_t a = *area;
    int32_t w = lv_area_get_width(&a), h = lv_area_get_height(&a);
    lv_color_format_t cf = lv_display_get_color_format(d);
    uint32_t src_stride = lv_draw_buf_width_to_stride(w, cf);
    lv_display_rotate_area(d, &a);
    const uint32_t row = BOARD_LCD_H_RES * RPI_BPP;
    uint8_t *dst = rpi_fb + a.y1 * row + a.x1 * RPI_BPP;
    lv_draw_sw_rotate(px, dst, w, h, src_stride, row, lv_display_get_rotation(d), cf);
    esp_cache_msync(rpi_fb + a.y1 * row, (size_t)(a.y2 - a.y1 + 1) * row,
                    ESP_CACHE_MSYNC_FLAG_DIR_C2M | ESP_CACHE_MSYNC_FLAG_UNALIGNED);
    lv_display_flush_ready(d);
}

#endif /* CONFIG_RRC_DISPLAY_RPI_DSI */

/* ---- LVGL glue -------------------------------------------------------------------------- */
static uint32_t tick_ms(void) { return (uint32_t)(esp_timer_get_time() / 1000); }

#if CONFIG_RRC_DISPLAY_ST7701
static void flush_cb(lv_display_t *d, const lv_area_t *area, uint8_t *px)
{
    (void)area;
    /* Direct mode: px is one of the DPI framebuffers. On the last area of a frame,
     * hand that buffer to the DPI (a pointer swap, no copy) and wait for the
     * frame boundary, so LVGL never draws into the buffer being scanned out. */
    if (lv_display_flush_is_last(d)) {
        xSemaphoreTake(vsync_sem, 0);
        esp_lcd_panel_draw_bitmap(panel, 0, 0, BOARD_LCD_H_RES, BOARD_LCD_V_RES, px);
        xSemaphoreTake(vsync_sem, pdMS_TO_TICKS(50));
    }
    lv_display_flush_ready(d);
}

#endif

static void touch_read_cb(lv_indev_t *indev, lv_indev_data_t *data)
{
    (void)indev;
    static int64_t last_us;
    static lv_indev_state_t last_state = LV_INDEV_STATE_RELEASED;
    /* Asleep: sample at 10 Hz instead of every LVGL read (the INT line is not
     * routed, so the controller is polled over I2C either way). */
    int64_t now = esp_timer_get_time();
    if (!touch) { data->state = last_state = LV_INDEV_STATE_RELEASED; return; }   /* link being re-timed */
    if (rrc_ui_asleep() && now - last_us < 100000) { data->state = last_state; return; }
    last_us = now;
    esp_lcd_touch_point_data_t pt[1];
    uint8_t n = 0;
    if (esp_lcd_touch_read_data(touch) == ESP_OK && esp_lcd_touch_get_data(touch, pt, &n, 1) == ESP_OK && n > 0) {
        data->point.x = pt[0].x;
        data->point.y = pt[0].y;
        data->state = LV_INDEV_STATE_PRESSED;
    } else {
        data->state = LV_INDEV_STATE_RELEASED;
    }
    last_state = data->state;
}

#ifdef BOARD_USER_KEY_GPIO
/* BOOT key, active low, debounced over three 20 ms samples. */
static void key_timer_cb(lv_timer_t *t)
{
    (void)t;
    static uint8_t hist = 0xFF;
    static bool down;
    hist = (uint8_t)((hist << 1) | (gpio_get_level(BOARD_USER_KEY_GPIO) & 1));
    if (!down && (hist & 0x07) == 0x00) { down = true; rrc_ui_user_key(); }
    else if (down && (hist & 0x07) == 0x07) down = false;
}
#endif

/* LVGL heap in PSRAM (CONFIG_LV_USE_CUSTOM_MALLOC): malloc() would put every
 * small widget allocation in internal RAM (CONFIG_SPIRAM_MALLOC_ALWAYSINTERNAL). */
void lv_mem_init(void) {}
void lv_mem_deinit(void) {}
lv_mem_pool_t lv_mem_add_pool(void *mem, size_t bytes) { (void)mem; (void)bytes; return NULL; }
void lv_mem_remove_pool(lv_mem_pool_t pool) { (void)pool; }
void *lv_malloc_core(size_t size) { return heap_caps_malloc(size, MALLOC_CAP_SPIRAM | MALLOC_CAP_8BIT); }
void *lv_realloc_core(void *p, size_t new_size) { return heap_caps_realloc(p, new_size, MALLOC_CAP_SPIRAM | MALLOC_CAP_8BIT); }
void lv_free_core(void *p) { heap_caps_free(p); }
void lv_mem_monitor_core(lv_mem_monitor_t *mon) { memset(mon, 0, sizeof *mon); mon->free_size = heap_caps_get_free_size(MALLOC_CAP_SPIRAM); }
lv_result_t lv_mem_test_core(void) { return LV_RESULT_OK; }

/* ---- API bridge -------------------------------------------------------------------------- */
typedef struct {
    char method[8];
    char path[48];
    char *body;
    uint32_t token;
} ui_job_t;

static QueueHandle_t job_q;
static SemaphoreHandle_t api_ready;   /* given by display_set_api_ready() */

static void host_request(const char *method, const char *path, const char *body, uint32_t token)
{
    ui_job_t j = {.token = token};
    strlcpy(j.method, method, sizeof j.method);
    strlcpy(j.path, path, sizeof j.path);
    j.body = body ? strdup(body) : NULL;
    if (xQueueSend(job_q, &j, 0) != pdTRUE) {
        free(j.body);
        rrc_ui_complete(token, 503, "{\"ok\":false,\"error\":\"busy\"}", 28);   /* caller holds the LVGL lock */
    }
}

void display_set_api_ready(void)
{
    if (api_ready) xSemaphoreGive(api_ready);
}

static void worker_task(void *arg)
{
    (void)arg;
    ui_job_t j;
    /* The panel comes up before the sync engine and camera sources exist; the
     * first /api/status would take a mutex that is not created yet. Requests
     * queue up until app_main says the dock is ready. */
    xSemaphoreTake(api_ready, portMAX_DELAY);
    for (;;) {
        if (xQueueReceive(job_q, &j, portMAX_DELAY) != pdTRUE) continue;
        api_resp_t r = {0};
        if (!strcmp(j.method, "AUTH")) {
            /* The on-screen unlock: the same check as the web page's Basic auth. */
            bool ok = api_password_ok(j.body ? j.body : "");
            r.status = ok ? 200 : 401;
            r.json = strdup(ok ? "{\"ok\":true}" : "{\"ok\":false,\"error\":\"wrong password\"}");
            r.len = r.json ? strlen(r.json) : 0;
            if (!ok) log_ring_printf("display: wrong admin password entered on the touchscreen");
        } else {
            api_dispatch(j.method, j.path, j.body, j.body ? strlen(j.body) : 0, &r);
        }
        if (j.body) { memset(j.body, 0, strlen(j.body)); free(j.body); }   /* may hold a password */
        lv_lock();
        rrc_ui_complete(j.token, r.status, r.json, r.len);
        lv_unlock();
        api_resp_free(&r);
    }
}

static void lvgl_task(void *arg)
{
    (void)arg;
    for (;;) {
        uint32_t wait = lv_timer_handler();
        if (wait < 5) wait = 5;
        if (wait > 50) wait = 50;
        vTaskDelay(pdMS_TO_TICKS(wait));
    }
}

static const rrc_ui_host_t host = {.request = host_request, .set_backlight = backlight_set};

esp_err_t display_start(void)
{
#if CONFIG_RRC_DISPLAY_ST7701
    esp_err_t e = backlight_init();
    if (e == ESP_OK) e = panel_init();
    if (e != ESP_OK) { log_ring_printf("display: panel init failed (%s); running headless", esp_err_to_name(e)); return e; }
    if (touch_init() != ESP_OK) log_ring_printf("display: GT911 touch controller not found; the screen is view-only");
#elif CONFIG_RRC_DISPLAY_RPI_DSI
    esp_err_t e = rpi_panel_init();
    if (e == ESP_ERR_NOT_FOUND) { log_ring_printf("display: no DSI display answers on I2C 0x45; running headless"); return e; }
    if (e != ESP_OK) { log_ring_printf("display: panel init failed (%s); running headless", esp_err_to_name(e)); return e; }
    if (!touch) log_ring_printf("display: FT5x06 touch controller not found; the screen is view-only");
#endif
#ifdef BOARD_USER_KEY_GPIO
    gpio_config_t key = {.pin_bit_mask = 1ULL << BOARD_USER_KEY_GPIO, .mode = GPIO_MODE_INPUT, .pull_up_en = GPIO_PULLUP_ENABLE};
    gpio_config(&key);
#endif

    lv_init();
    lv_tick_set_cb(tick_ms);
    disp = lv_display_create(BOARD_LCD_H_RES, BOARD_LCD_V_RES);
#if CONFIG_RRC_DISPLAY_ST7701
    lv_display_set_color_format(disp, LV_COLOR_FORMAT_RGB565);
    void *fb0 = NULL, *fb1 = NULL;
    ESP_RETURN_ON_ERROR(esp_lcd_dpi_panel_get_frame_buffer(panel, 2, &fb0, &fb1), TAG, "framebuffers");
    lv_display_set_buffers(disp, fb0, fb1, BOARD_LCD_H_RES * BOARD_LCD_V_RES * 2, LV_DISPLAY_RENDER_MODE_DIRECT);
    lv_display_set_flush_cb(disp, flush_cb);
#elif CONFIG_RRC_DISPLAY_RPI_DSI
    /* RGB888 end to end: LVGL draws the format the bridge is fed (CONFIG_LV_COLOR_DEPTH_24
     * in sdkconfig.board.poe_eth_dsi5), so the rotate is a plain copy.
     * The UI is 480x800 portrait; LVGL swaps the resolutions and rotates touch. */
    lv_display_set_color_format(disp, LV_COLOR_FORMAT_RGB888);
#if CONFIG_RRC_DISPLAY_ROTATION_270
    lv_display_set_rotation(disp, LV_DISPLAY_ROTATION_270);
#else
    lv_display_set_rotation(disp, LV_DISPLAY_ROTATION_90);
#endif
    const size_t buf_bytes = 480 * 80 * RPI_BPP;   /* 80 UI rows per pass */
    void *b0 = heap_caps_aligned_alloc(64, buf_bytes, MALLOC_CAP_SPIRAM | MALLOC_CAP_8BIT);
    void *b1 = heap_caps_aligned_alloc(64, buf_bytes, MALLOC_CAP_SPIRAM | MALLOC_CAP_8BIT);
    if (!b0 || !b1) { free(b0); free(b1); log_ring_printf("display: no memory for draw buffers"); return ESP_ERR_NO_MEM; }
    lv_display_set_buffers(disp, b0, b1, buf_bytes, LV_DISPLAY_RENDER_MODE_PARTIAL);
    lv_display_set_flush_cb(disp, flush_rotated_cb);
#endif
    if (touch) {
        lv_indev_t *in = lv_indev_create();
        lv_indev_set_type(in, LV_INDEV_TYPE_POINTER);
        lv_indev_set_read_cb(in, touch_read_cb);
        lv_indev_set_display(in, disp);
    }
#ifdef BOARD_USER_KEY_GPIO
    lv_timer_create(key_timer_cb, 20, NULL);
#endif

    job_q = xQueueCreate(16, sizeof(ui_job_t));
    api_ready = xSemaphoreCreateBinary();
    /* Below the sync/upload tasks; the worker's stack is internal RAM because
     * api_dispatch can write NVS (flash operations need an internal stack). */
    xTaskCreatePinnedToCore(worker_task, "ui_api", 6144, NULL, 3, NULL, 1);
    rrc_ui_start(&host, disp);
    if (xTaskCreatePinnedToCoreWithCaps(lvgl_task, "lvgl", 16384, NULL, 2, NULL, 1, MALLOC_CAP_SPIRAM) != pdPASS) {
        log_ring_printf("display: could not start the LVGL task");
        return ESP_ERR_NO_MEM;
    }
    log_ring_printf("display: %dx%d MIPI-DSI panel up, touch %s", BOARD_LCD_H_RES, BOARD_LCD_V_RES, touch ? "ok" : "absent");
    return ESP_OK;
}

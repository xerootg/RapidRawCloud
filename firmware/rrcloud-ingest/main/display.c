/*
 * display — hardware glue for the on-screen UI on the ESP32-P4-WIFI6-Touch-LCD-4.3:
 * ST7701 over 2-lane MIPI-DSI, GT911 touch, PWM backlight, the BOOT key, LVGL 9
 * and the bridge between components/rrc_ui and the admin API.
 *
 * Rendering budget. The dock's real job is moving photos: uploads keep both
 * cores busy with BLAKE3, TLS and lwIP, and internal RAM is the scarcest
 * resource on the board (README "Throughput notes"). So the UI:
 *   - renders straight into the two DPI framebuffers in PSRAM (LVGL direct mode,
 *     only dirty areas, buffers swapped on vsync): no extra draw buffers and no
 *     internal RAM for pixels;
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
#include "esp_lcd_st7701.h"
#include "esp_lcd_touch_gt911.h"
#include "lvgl.h"
#include "src/stdlib/lv_mem.h"
#include "rrc_ui.h"
#include "board.h"
#include "api.h"
#include "log_ring.h"

static const char *TAG = "display";

static esp_lcd_panel_handle_t panel;
static esp_lcd_touch_handle_t touch;
static SemaphoreHandle_t vsync_sem;
static lv_display_t *disp;

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
        .phy_clk_src = MIPI_DSI_PHY_CLK_SRC_DEFAULT,
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

/* ---- LVGL glue -------------------------------------------------------------------------- */
static uint32_t tick_ms(void) { return (uint32_t)(esp_timer_get_time() / 1000); }

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

static void touch_read_cb(lv_indev_t *indev, lv_indev_data_t *data)
{
    (void)indev;
    static int64_t last_us;
    static lv_indev_state_t last_state = LV_INDEV_STATE_RELEASED;
    /* Asleep: sample at 10 Hz instead of every LVGL read (the INT line is not
     * routed, so the controller is polled over I2C either way). */
    int64_t now = esp_timer_get_time();
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

static void worker_task(void *arg)
{
    (void)arg;
    ui_job_t j;
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
    esp_err_t e = backlight_init();
    if (e == ESP_OK) e = panel_init();
    if (e != ESP_OK) { log_ring_printf("display: panel init failed (%s); running headless", esp_err_to_name(e)); return e; }
    if (touch_init() != ESP_OK) log_ring_printf("display: GT911 touch controller not found; the screen is view-only");
    gpio_config_t key = {.pin_bit_mask = 1ULL << BOARD_USER_KEY_GPIO, .mode = GPIO_MODE_INPUT, .pull_up_en = GPIO_PULLUP_ENABLE};
    gpio_config(&key);

    lv_init();
    lv_tick_set_cb(tick_ms);
    disp = lv_display_create(BOARD_LCD_H_RES, BOARD_LCD_V_RES);
    void *fb0 = NULL, *fb1 = NULL;
    ESP_RETURN_ON_ERROR(esp_lcd_dpi_panel_get_frame_buffer(panel, 2, &fb0, &fb1), TAG, "framebuffers");
    lv_display_set_color_format(disp, LV_COLOR_FORMAT_RGB565);
    lv_display_set_buffers(disp, fb0, fb1, BOARD_LCD_H_RES * BOARD_LCD_V_RES * 2, LV_DISPLAY_RENDER_MODE_DIRECT);
    lv_display_set_flush_cb(disp, flush_cb);
    if (touch) {
        lv_indev_t *in = lv_indev_create();
        lv_indev_set_type(in, LV_INDEV_TYPE_POINTER);
        lv_indev_set_read_cb(in, touch_read_cb);
        lv_indev_set_display(in, disp);
    }
    lv_timer_create(key_timer_cb, 20, NULL);

    job_q = xQueueCreate(16, sizeof(ui_job_t));
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

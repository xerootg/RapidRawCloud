/*
 * panel_rpi_dsi — see panel_rpi_dsi.h.
 *
 * The protocol is the Raspberry Pi 7" Touch Display's. "Driver-free" Pi panels
 * emulate it (some with the original Toshiba bridge and an ATTINY88, some with
 * an FPGA) so the Pi firmware recognises them as the official display; this
 * driver talks to that interface and does not care which parts implement it.
 *
 * Controller (I2C 0x45). REG_ID reads 0xde on firmware revision 1, which powers
 * the panel with REG_POWERON and reports completion in PORTB bit 0; revision 2
 * (0xc3) instead exposes the port pins, write-only: PORTB bit 7 switches the
 * main regulator, PORTC carries the LED enable and three active-low resets
 * (touch, LCD, bridge). The resets have to be released explicitly, or the
 * panel stays white behind a working backlight. Writes are retried: the
 * controller is known to NAK occasionally.
 *
 * Bridge. TC358762 registers are written with DSI Generic Long Write packets
 * (16-bit register address, 32-bit value, little-endian), in low-power mode,
 * before video starts. The values are the ones the original Pi firmware and
 * the legacy Linux panel driver used for this panel; the newer bridge setup is
 * reported to give wrong colours on clone panels. The bridge generates its
 * pixel clock from the DSI clock lane, so the clock lane is kept in high-speed
 * mode continuously and the lane rate is exactly 24 bits x the pixel clock.
 *
 * Timing: 800x480, HFP 1 / HSW 2 / HBP 46, VFP 7 / VSW 2 / VBP 21 (849 x 510
 * total). The P4 derives the DPI clock from PLL_F240M by an integer divider:
 * 240/9 = 26.67 MHz, 61.6 Hz, against the original 25.98 MHz / 60 Hz.
 */
#include "panel_rpi_dsi.h"
#include <string.h>
#include "esp_log.h"
#include "esp_check.h"
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "esp_ldo_regulator.h"
#include "esp_lcd_mipi_dsi.h"
#include "esp_lcd_panel_ops.h"
#include "esp_lcd_io_i2c.h"
#include "esp_lcd_touch_ft5x06.h"
#include "hal/mipi_dsi_hal.h"
#include "hal/mipi_dsi_host_ll.h"
#include "hal/mipi_dsi_brg_ll.h"
#include "board.h"

static const char *TAG = "rpi_dsi";

/* ---- controller (I2C 0x45) ---- */
#define CTL_ADDR        0x45
#define REG_ID          0x80
#define REG_PORTA       0x81
#define REG_PORTB       0x82
#define REG_PORTC       0x83
#define REG_POWERON     0x85
#define REG_PWM         0x86
#define ID_REV1         0xde
#define ID_REV2         0xc3
#define PA_LCD_LR       (1u << 2)   /* scan direction: the orientation the Pi firmware used */
#define PB_LCD_MAIN     (1u << 7)
#define PC_LED_EN       (1u << 0)
#define PC_RST_TP_N     (1u << 1)
#define PC_RST_LCD_N    (1u << 2)
#define PC_RST_BRIDGE_N (1u << 3)

/* ---- TC358762 bridge registers ---- */
#define PPI_STARTPPI         0x0104
#define PPI_LPTXTIMECNT      0x0114
#define PPI_D0S_ATMR         0x0144
#define PPI_D1S_ATMR         0x0148
#define PPI_D0S_CLRSIPOCOUNT 0x0164
#define PPI_D1S_CLRSIPOCOUNT 0x0168
#define DSI_STARTDSI         0x0204
#define DSI_LANEENABLE       0x0210
#define LCDCTRL              0x0420
#define SPICMR               0x0450
#define SYSCTRL              0x0464

static i2c_master_dev_handle_t ctl;

static esp_err_t ctl_write(uint8_t reg, uint8_t val)
{
    uint8_t b[2] = {reg, val};
    esp_err_t e = ESP_FAIL;
    for (int i = 0; i < 5 && e != ESP_OK; i++) {
        e = i2c_master_transmit(ctl, b, sizeof b, 50);
        if (e != ESP_OK) vTaskDelay(pdMS_TO_TICKS(2));
    }
    return e;
}

static esp_err_t ctl_read(uint8_t reg, uint8_t *val)
{
    esp_err_t e = ESP_FAIL;
    for (int i = 0; i < 5 && e != ESP_OK; i++) {
        e = i2c_master_transmit_receive(ctl, &reg, 1, val, 1, 50);
        if (e != ESP_OK) vTaskDelay(pdMS_TO_TICKS(2));
    }
    return e;
}

static void bridge_write(mipi_dsi_hal_context_t *hal, uint16_t reg, uint32_t val)
{
    const uint8_t msg[6] = {reg & 0xff, reg >> 8, val & 0xff, (val >> 8) & 0xff, (val >> 16) & 0xff, val >> 24};
    mipi_dsi_hal_host_gen_write_long_packet(hal, 0, MIPI_DSI_DT_GENERIC_LONG_WRITE, msg, sizeof msg);
}

static esp_err_t power_on(uint8_t id)
{
    if (id == ID_REV1) {
        ESP_RETURN_ON_ERROR(ctl_write(REG_POWERON, 1), TAG, "POWERON");
        uint8_t pb = 0;
        for (int i = 0; i < 100; i++) {   /* PORTB bit 0 (bridge nPWRDWN) goes high when power-up is done */
            if (ctl_read(REG_PORTB, &pb) == ESP_OK && (pb & 1)) return ESP_OK;
            vTaskDelay(pdMS_TO_TICKS(1));
        }
        ESP_LOGW(TAG, "power-up not confirmed (PORTB 0x%02x); continuing", pb);
        return ESP_OK;
    }
    /* Revision 2 (and unknown ids, which clones are most likely to copy). */
    ESP_RETURN_ON_ERROR(ctl_write(REG_PORTC, 0), TAG, "PORTC");            /* everything held in reset */
    vTaskDelay(pdMS_TO_TICKS(10));
    ESP_RETURN_ON_ERROR(ctl_write(REG_PORTA, PA_LCD_LR), TAG, "PORTA");
    vTaskDelay(pdMS_TO_TICKS(10));
    ESP_RETURN_ON_ERROR(ctl_write(REG_PORTB, PB_LCD_MAIN), TAG, "PORTB");  /* main regulator, LCD supply */
    vTaskDelay(pdMS_TO_TICKS(10));
    ESP_RETURN_ON_ERROR(ctl_write(REG_PORTC, PC_LED_EN), TAG, "PORTC");
    vTaskDelay(pdMS_TO_TICKS(80));
    ESP_RETURN_ON_ERROR(ctl_write(REG_PORTC, PC_LED_EN | PC_RST_TP_N | PC_RST_LCD_N | PC_RST_BRIDGE_N), TAG, "resets");
    vTaskDelay(pdMS_TO_TICKS(20));
    return ESP_OK;
}

esp_err_t rpi_dsi_start(i2c_master_bus_handle_t bus, rpi_dsi_t *out)
{
    memset(out, 0, sizeof *out);
    if (i2c_master_probe(bus, CTL_ADDR, 50) != ESP_OK) return ESP_ERR_NOT_FOUND;
    i2c_device_config_t dc = {.dev_addr_length = I2C_ADDR_BIT_LEN_7, .device_address = CTL_ADDR, .scl_speed_hz = 100000};
    ESP_RETURN_ON_ERROR(i2c_master_bus_add_device(bus, &dc, &ctl), TAG, "controller");
    uint8_t id = 0;
    ESP_RETURN_ON_ERROR(ctl_read(REG_ID, &id), TAG, "REG_ID");
    out->controller_id = id;
    if (id != ID_REV1 && id != ID_REV2) ESP_LOGW(TAG, "unknown controller id 0x%02x; using the revision 2 sequence", id);

    static esp_ldo_channel_handle_t phy_ldo;
    esp_ldo_channel_config_t ldo = {.chan_id = BOARD_DSI_PHY_LDO_CHAN, .voltage_mv = BOARD_DSI_PHY_LDO_MV};
    ESP_RETURN_ON_ERROR(esp_ldo_acquire_channel(&ldo, &phy_ldo), TAG, "D-PHY LDO");

    esp_lcd_dsi_bus_handle_t dsi;
    esp_lcd_dsi_bus_config_t bus_cfg = {
        .bus_id = 0,
        .num_data_lanes = 1,
        /* phy_clk_src left 0: the driver picks the PHY PLL reference for the chip revision. */
        .lane_bit_rate_mbps = BOARD_LCD_DPI_CLK_MHZ * 24.0f,
        .flags.clock_lane_force_hs = 1,   /* the bridge's pixel clock comes from the clock lane */
    };
    ESP_RETURN_ON_ERROR(esp_lcd_new_dsi_bus(&bus_cfg, &dsi), TAG, "DSI bus");
    /* The DBI io is not used for the bridge (it sends DCS packets); creating it
     * switches every command type, generic long writes included, to LP mode. */
    esp_lcd_panel_io_handle_t dbi;
    esp_lcd_dbi_io_config_t dbi_cfg = {.virtual_channel = 0, .lcd_cmd_bits = 8, .lcd_param_bits = 8};
    ESP_RETURN_ON_ERROR(esp_lcd_new_panel_io_dbi(dsi, &dbi_cfg, &dbi), TAG, "DBI io");
    esp_lcd_dpi_panel_config_t dpi = {
        .virtual_channel = 0,
        .dpi_clk_src = MIPI_DSI_DPI_CLK_SRC_DEFAULT,
        .dpi_clock_freq_mhz = BOARD_LCD_DPI_CLK_MHZ,
        .in_color_format = LCD_COLOR_FMT_RGB565,    /* LVGL's framebuffer */
        .out_color_format = LCD_COLOR_FMT_RGB888,   /* what the bridge is configured for */
        .num_fbs = 1,
        .video_timing = {
            .h_size = BOARD_LCD_H_RES, .v_size = BOARD_LCD_V_RES,
            .hsync_front_porch = 1, .hsync_pulse_width = 2, .hsync_back_porch = 46,
            .vsync_front_porch = 7, .vsync_pulse_width = 2, .vsync_back_porch = 21,
        },
    };
    ESP_RETURN_ON_ERROR(esp_lcd_new_panel_dpi(dsi, &dpi, &out->panel), TAG, "DPI panel");

    ESP_RETURN_ON_ERROR(power_on(id), TAG, "power on");

    mipi_dsi_hal_context_t hal = {.host = MIPI_DSI_LL_GET_HOST(0), .bridge = MIPI_DSI_LL_GET_BRG(0)};
    bridge_write(&hal, DSI_LANEENABLE, 0x3);          /* clock lane + data lane 0 */
    bridge_write(&hal, PPI_D0S_CLRSIPOCOUNT, 0x05);
    bridge_write(&hal, PPI_D1S_CLRSIPOCOUNT, 0x05);
    bridge_write(&hal, PPI_D0S_ATMR, 0x00);
    bridge_write(&hal, PPI_D1S_ATMR, 0x00);
    bridge_write(&hal, PPI_LPTXTIMECNT, 0x03);
    bridge_write(&hal, SPICMR, 0x00);
    bridge_write(&hal, LCDCTRL, 0x00100150);
    bridge_write(&hal, SYSCTRL, 0x040f);
    vTaskDelay(pdMS_TO_TICKS(100));
    bridge_write(&hal, PPI_STARTPPI, 0x01);
    bridge_write(&hal, DSI_STARTDSI, 0x01);
    vTaskDelay(pdMS_TO_TICKS(100));

    ESP_RETURN_ON_ERROR(esp_lcd_panel_init(out->panel), TAG, "start video");
    if (id == ID_REV1) ctl_write(REG_PORTA, PA_LCD_LR);
    rpi_dsi_set_backlight(100);

    /* Touch: FT5x06 on the same bus; its reset was released with the others. */
    if (i2c_master_probe(bus, ESP_LCD_TOUCH_IO_I2C_FT5x06_ADDRESS, 50) == ESP_OK) {
        esp_lcd_panel_io_handle_t tio;
        esp_lcd_panel_io_i2c_config_t tcfg = ESP_LCD_TOUCH_IO_I2C_FT5x06_CONFIG();
        tcfg.scl_speed_hz = 400000;
        esp_lcd_touch_config_t tc = {
            .x_max = BOARD_LCD_H_RES, .y_max = BOARD_LCD_V_RES,
            .rst_gpio_num = GPIO_NUM_NC, .int_gpio_num = GPIO_NUM_NC,
        };
        if (esp_lcd_new_panel_io_i2c_v2(bus, &tcfg, &tio) == ESP_OK) esp_lcd_touch_new_i2c_ft5x06(tio, &tc, &out->touch);
    }
    ESP_LOGI(TAG, "controller id 0x%02x, %dx%d up, touch %s", id, BOARD_LCD_H_RES, BOARD_LCD_V_RES, out->touch ? "ok" : "absent");
    return ESP_OK;
}

void rpi_dsi_set_backlight(int pct)
{
    if (!ctl) return;
    if (pct < 0) pct = 0;
    if (pct > 100) pct = 100;
    ctl_write(REG_PWM, (uint8_t)(255 * pct / 100));
}

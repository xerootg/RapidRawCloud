/*
 * power — battery voltage for the status bar and /api/status.
 *
 * The Touch-LCD-4.3 feeds VBAT through 200k/100k into GPIO20 (ADC1 ch 4). The
 * ETA6098 charger's STAT pin is not routed to the P4, so the dock cannot tell
 * charging from discharging, and with no cell fitted the charger output idles
 * near its 4.2 V regulation point: a reading that never moves and sits at the
 * top of the curve is reported as "maybe absent" rather than as a full battery.
 * The percentage is an open-circuit estimate under the board's own load —
 * honest to ±10 %, which is what a status-bar glyph needs.
 */
#include "power.h"
#include <stdio.h>
#include "sdkconfig.h"

#if CONFIG_RRC_HAS_BATTERY
#include "esp_log.h"
#include "esp_timer.h"
#include "esp_adc/adc_oneshot.h"
#include "esp_adc/adc_cali.h"
#include "esp_adc/adc_cali_scheme.h"
#include "board.h"

static const char *TAG = "power";
static adc_oneshot_unit_handle_t adc;
static adc_cali_handle_t cali;
static volatile int mv_avg;            /* exponential moving average, mV at the cell */
static volatile int mv_min, mv_max;    /* over the last ~5 minutes, for the "present" guess */
static esp_timer_handle_t timer;

static int sample_mv(void)
{
    int raw = 0, acc = 0, n = 0;
    for (int i = 0; i < 8; i++) {
        if (adc_oneshot_read(adc, BOARD_BAT_ADC_CHANNEL, &raw) != ESP_OK) continue;
        int mv = 0;
        if (cali && adc_cali_raw_to_voltage(cali, raw, &mv) == ESP_OK) acc += mv;
        else acc += raw * 3300 / 4095;  /* uncalibrated fallback */
        n++;
    }
    return n ? acc / n * BOARD_BAT_DIVIDER_NUM : 0;
}

static void tick(void *arg)
{
    (void)arg;
    static int window_ticks;
    int mv = sample_mv();
    if (mv <= 0) return;
    if (!mv_avg) { mv_avg = mv; mv_min = mv_max = mv; }
    mv_avg = (mv_avg * 7 + mv) / 8;
    if (mv_avg < mv_min) mv_min = mv_avg;
    if (mv_avg > mv_max) mv_max = mv_avg;
    if (++window_ticks >= 60) { window_ticks = 0; mv_min = mv_max = mv_avg; }  /* 60 × 5 s */
}

esp_err_t power_init(void)
{
    adc_oneshot_unit_init_cfg_t ucfg = {.unit_id = ADC_UNIT_1};
    esp_err_t e = adc_oneshot_new_unit(&ucfg, &adc);
    if (e != ESP_OK) { ESP_LOGW(TAG, "adc unit: %s", esp_err_to_name(e)); return e; }
    adc_oneshot_chan_cfg_t ccfg = {.atten = ADC_ATTEN_DB_12, .bitwidth = ADC_BITWIDTH_DEFAULT};
    e = adc_oneshot_config_channel(adc, BOARD_BAT_ADC_CHANNEL, &ccfg);
    if (e != ESP_OK) { ESP_LOGW(TAG, "adc channel: %s", esp_err_to_name(e)); return e; }
#if ADC_CALI_SCHEME_CURVE_FITTING_SUPPORTED
    adc_cali_curve_fitting_config_t cc = {.unit_id = ADC_UNIT_1, .chan = BOARD_BAT_ADC_CHANNEL, .atten = ADC_ATTEN_DB_12, .bitwidth = ADC_BITWIDTH_DEFAULT};
    if (adc_cali_create_scheme_curve_fitting(&cc, &cali) != ESP_OK) cali = NULL;
#elif ADC_CALI_SCHEME_LINE_FITTING_SUPPORTED
    adc_cali_line_fitting_config_t lc = {.unit_id = ADC_UNIT_1, .atten = ADC_ATTEN_DB_12, .bitwidth = ADC_BITWIDTH_DEFAULT};
    if (adc_cali_create_scheme_line_fitting(&lc, &cali) != ESP_OK) cali = NULL;
#endif
    if (!cali) ESP_LOGW(TAG, "no ADC calibration scheme; battery voltage is approximate");
    tick(NULL);
    const esp_timer_create_args_t ta = {.callback = tick, .name = "battery"};
    if (esp_timer_create(&ta, &timer) == ESP_OK) esp_timer_start_periodic(timer, 5 * 1000 * 1000);
    return ESP_OK;
}

int power_battery_mv(void) { return mv_avg; }

int power_battery_pct(void)
{
    /* Li-ion resting voltage → state of charge (typical 18650/LiPo curve). */
    static const int curve[][2] = {
        {4180, 100}, {4100, 92}, {4000, 82}, {3920, 72}, {3850, 62}, {3790, 52},
        {3740, 42}, {3700, 32}, {3660, 22}, {3610, 12}, {3500, 5}, {3300, 0},
    };
    int mv = mv_avg;
    if (!mv) return -1;
    if (mv >= curve[0][0]) return 100;
    for (size_t i = 1; i < sizeof curve / sizeof curve[0]; i++) {
        if (mv >= curve[i][0]) {
            int v0 = curve[i][0], v1 = curve[i - 1][0], p0 = curve[i][1], p1 = curve[i - 1][1];
            return p0 + (mv - v0) * (p1 - p0) / (v1 - v0);
        }
    }
    return 0;
}

int power_status_json(char *out, size_t cap)
{
    /* A fitted cell under load moves by tens of mV over minutes; the charger's
     * idle output with no cell sits flat at its regulation voltage. */
    bool maybe_absent = mv_avg >= 4150 && mv_max - mv_min < 8;
    return snprintf(out, cap, "{\"battery\":true,\"mv\":%d,\"pct\":%d,\"present\":%s}", mv_avg, power_battery_pct(), maybe_absent ? "false" : "true");
}

#else

esp_err_t power_init(void) { return ESP_OK; }
int power_battery_mv(void) { return 0; }
int power_battery_pct(void) { return -1; }
int power_status_json(char *out, size_t cap) { return snprintf(out, cap, "{\"battery\":false}"); }

#endif

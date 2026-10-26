#pragma once
#include <stdbool.h>
#include <stddef.h>
#include "esp_err.h"

/* Battery monitoring on boards with CONFIG_RRC_HAS_BATTERY (the Touch-LCD-4.3's
 * Li-ion header, measured through a 1/3 divider on ADC1). Elsewhere every call
 * is a no-op and the JSON says {"battery":false}. */

esp_err_t power_init(void);
/* Smoothed cell voltage in mV (0 = no battery board / not measured yet). */
int power_battery_mv(void);
/* State of charge estimated from the open-circuit-voltage curve, 0..100, or -1. */
int power_battery_pct(void);
/* {"battery":true,"mv":3912,"pct":71,"present":true} */
int power_status_json(char *out, size_t cap);

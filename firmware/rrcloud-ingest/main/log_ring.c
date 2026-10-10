#include "log_ring.h"
#include <stdarg.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include "esp_log.h"
#include "freertos/FreeRTOS.h"
#include "freertos/semphr.h"
#include "rrc_proto.h"

#define RING_LINES 128
#define RING_LINE_LEN 160

static char lines[RING_LINES][RING_LINE_LEN];
static int head, count;
static SemaphoreHandle_t mtx;

void log_ring_init(void)
{
    mtx = xSemaphoreCreateMutex();
}

void log_ring_printf(const char *fmt, ...)
{
    char msg[RING_LINE_LEN];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(msg, sizeof msg, fmt, ap);
    va_end(ap);
    ESP_LOGI("rrc", "%s", msg);
    char stamp[21] = "";
    time_t now = time(NULL);
    if (now > 1600000000) rrc_format_iso8601((int64_t)now, stamp);
    if (mtx) xSemaphoreTake(mtx, portMAX_DELAY);
    snprintf(lines[head], RING_LINE_LEN, "%s %.*s", stamp[0] ? stamp : "-", RING_LINE_LEN - 24, msg);
    head = (head + 1) % RING_LINES;
    if (count < RING_LINES) count++;
    if (mtx) xSemaphoreGive(mtx);
}

int log_ring_to_json(char *out, size_t cap)
{
    rrc_jsonw w;
    rrc_jsonw_init(&w, out, cap);
    rrc_jsonw_raw(&w, "[");
    if (mtx) xSemaphoreTake(mtx, portMAX_DELAY);
    int start = (head - count + RING_LINES) % RING_LINES;
    for (int i = 0; i < count; i++) {
        if (i) rrc_jsonw_raw(&w, ",");
        rrc_jsonw_str(&w, lines[(start + i) % RING_LINES]);
    }
    if (mtx) xSemaphoreGive(mtx);
    rrc_jsonw_raw(&w, "]");
    return rrc_jsonw_finish(&w);
}

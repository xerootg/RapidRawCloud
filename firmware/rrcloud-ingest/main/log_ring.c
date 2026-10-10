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
static vprintf_like_t chain;

static void push(const char *msg)
{
    char stamp[21] = "";
    time_t now = time(NULL);
    if (now > 1600000000) rrc_format_iso8601((int64_t)now, stamp);
    if (mtx) xSemaphoreTake(mtx, portMAX_DELAY);
    snprintf(lines[head], RING_LINE_LEN, "%s %.*s", stamp[0] ? stamp : "-", RING_LINE_LEN - 24, msg);
    head = (head + 1) % RING_LINES;
    if (count < RING_LINES) count++;
    if (mtx) xSemaphoreGive(mtx);
}

/* Every ESP_LOG line at INFO or above also lands in the ring, so the web UI
 * shows the USB host stack's own errors ("HUB: Root port reset failed") next
 * to the dock's messages. DEBUG/VERBOSE stay on the serial console only. */
static int log_hook(const char *fmt, va_list ap)
{
    char buf[RING_LINE_LEN + 32];
    va_list ap2;
    va_copy(ap2, ap);
    int n = vsnprintf(buf, sizeof buf, fmt, ap2);
    va_end(ap2);
    if (n > 0) {
        const char *p = buf;
        if (p[0] == 0x1b) { while (*p && *p != 'm') p++; if (*p) p++; }   /* ANSI color prefix */
        if ((p[0] == 'E' || p[0] == 'W' || p[0] == 'I') && p[1] == ' ') {
            char line[RING_LINE_LEN];
            size_t m = 0;
            for (; p[m] && p[m] != '\n' && p[m] != 0x1b && m + 1 < sizeof line; m++) line[m] = p[m];
            line[m] = 0;
            push(line);
        }
    }
    return chain ? chain(fmt, ap) : vprintf(fmt, ap);
}

void log_ring_init(void)
{
    mtx = xSemaphoreCreateMutex();
    chain = esp_log_set_vprintf(log_hook);
}

void log_ring_printf(const char *fmt, ...)
{
    char msg[RING_LINE_LEN];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(msg, sizeof msg, fmt, ap);
    va_end(ap);
    ESP_LOGI("rrc", "%s", msg);   /* the hook above copies it into the ring */
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

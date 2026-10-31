/*
 * crashlog — see crashlog.h.
 */
#include "crashlog.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "esp_core_dump.h"
#include "esp_system.h"
#include "esp_flash.h"
#include "freertos/FreeRTOS.h"
#include "freertos/semphr.h"
#include "log_ring.h"
#include "ota.h"
#include "rrc_proto.h"

static SemaphoreHandle_t mtx;
static size_t download_addr, download_size;
static esp_core_dump_summary_t s;   /* ~1.2 KB with the stack dump: off the stack */
static size_t size;
static bool valid;

static const char *reason_str(esp_reset_reason_t r)
{
    switch (r) {
    case ESP_RST_POWERON: return "power-on";
    case ESP_RST_EXT: return "external pin";
    case ESP_RST_SW: return "software";
    case ESP_RST_PANIC: return "panic";
    case ESP_RST_INT_WDT: return "interrupt watchdog";
    case ESP_RST_TASK_WDT: return "task watchdog";
    case ESP_RST_WDT: return "watchdog";
    case ESP_RST_DEEPSLEEP: return "deep sleep";
    case ESP_RST_BROWNOUT: return "brownout";
    case ESP_RST_SDIO: return "sdio";
    case ESP_RST_USB: return "usb";
    case ESP_RST_JTAG: return "jtag";
    case ESP_RST_EFUSE: return "efuse";
    case ESP_RST_PWR_GLITCH: return "power glitch";
    case ESP_RST_CPU_LOCKUP: return "cpu lockup";
    default: return "unknown";
    }
}

const char *crashlog_reset_reason_str(void) { return reason_str(esp_reset_reason()); }

/* RISC-V mcause codes (privileged spec) for the exceptions a firmware bug produces. */
static const char *cause_text(uint32_t mcause)
{
    switch (mcause) {
    case 0: return "instruction address misaligned";
    case 1: return "instruction access fault";
    case 2: return "illegal instruction (or abort/assert)";
    case 3: return "breakpoint (or abort/assert)";
    case 4: return "load address misaligned";
    case 5: return "load access fault";
    case 6: return "store address misaligned";
    case 7: return "store access fault";
    case 27: return "stack protection fault (stack overflow)";
    default: return "other";
    }
}

static bool get(esp_core_dump_summary_t *s, size_t *size)
{
    *size = 0;
    if (crashlog_download_begin(size) != ESP_OK) return false;
    /* A reset during writing can leave a length but incomplete ELF headers.
     * Check the checksum before the summary parser follows those headers. */
    bool valid = esp_core_dump_image_check() == ESP_OK;
    if (valid) valid = s && esp_core_dump_get_summary(s) == ESP_OK;
    crashlog_download_end();
    return valid;
}

void crashlog_init(void)
{
    if (!mtx) mtx = xSemaphoreCreateMutex();
    if (!mtx) { log_ring_printf("crash report: could not create mutex"); return; }
    valid = get(&s, &size);
    if (!valid) {
        if (size) log_ring_printf("crash report: incomplete/corrupt dump is stored");
        return;
    }
    /* three short lines: the log ring keeps at most ~136 characters of a message */
    log_ring_printf("crash report (kept until erased): task \"%.16s\", %s (mcause %lu)",
                    s.exc_task, cause_text(s.ex_info.mcause), (unsigned long)s.ex_info.mcause);
    log_ring_printf("crash report: pc 0x%08lx ra 0x%08lx sp 0x%08lx mtval 0x%08lx", (unsigned long)s.exc_pc,
                    (unsigned long)s.ex_info.ra, (unsigned long)s.ex_info.sp, (unsigned long)s.ex_info.mtval);
    log_ring_printf("crash report: firmware ELF %.16s..., %u-byte core dump at GET /api/coredump",
                    (const char *)s.app_elf_sha256, (unsigned)size);
}

int crashlog_status_json(char *out, size_t cap)
{
    if (mtx) xSemaphoreTake(mtx, portMAX_DELAY);
    const esp_core_dump_summary_t *sm = &s;
    rrc_jsonw w;
    rrc_jsonw_init(&w, out, cap);
    rrc_jsonw_raw(&w, "{");
    rrc_jsonw_key(&w, "present", true); rrc_jsonw_raw(&w, size ? "true" : "false");
    rrc_jsonw_key(&w, "reset_reason", false); rrc_jsonw_str(&w, crashlog_reset_reason_str());
    rrc_jsonw_key(&w, "valid", false); rrc_jsonw_raw(&w, valid ? "true" : "false");
    if (valid) {
        char hex[12], task[17], sha[APP_ELF_SHA256_SZ + 1];
        snprintf(task, sizeof task, "%.16s", sm->exc_task);
        rrc_jsonw_key(&w, "task", false); rrc_jsonw_str(&w, task);
        snprintf(hex, sizeof hex, "0x%08lx", (unsigned long)sm->exc_pc);
        rrc_jsonw_key(&w, "pc", false); rrc_jsonw_str(&w, hex);
        rrc_jsonw_key(&w, "cause", false); rrc_jsonw_u64(&w, sm->ex_info.mcause);
        rrc_jsonw_key(&w, "cause_text", false); rrc_jsonw_str(&w, cause_text(sm->ex_info.mcause));
        snprintf(hex, sizeof hex, "0x%08lx", (unsigned long)sm->ex_info.ra);
        rrc_jsonw_key(&w, "ra", false); rrc_jsonw_str(&w, hex);
        snprintf(hex, sizeof hex, "0x%08lx", (unsigned long)sm->ex_info.sp);
        rrc_jsonw_key(&w, "sp", false); rrc_jsonw_str(&w, hex);
        snprintf(sha, sizeof sha, "%.*s", (int)APP_ELF_SHA256_SZ, (const char *)sm->app_elf_sha256);
        rrc_jsonw_key(&w, "elf_sha256", false); rrc_jsonw_str(&w, sha);
        rrc_jsonw_key(&w, "size", false); rrc_jsonw_u64(&w, size);
    }
    rrc_jsonw_raw(&w, "}");
    if (mtx) xSemaphoreGive(mtx);
    return rrc_jsonw_finish(&w);
}

/* A successful begin keeps erase and other readers out until end. Invalid
 * images remain downloadable: only the summary parser requires a checksum. */
esp_err_t crashlog_download_begin(size_t *size)
{
    if (!size) return ESP_ERR_INVALID_ARG;
    *size = 0;
    if (!mtx) return ESP_ERR_INVALID_STATE;
    if (xSemaphoreTake(mtx, portMAX_DELAY) != pdTRUE) return ESP_ERR_TIMEOUT;
    esp_err_t e = esp_core_dump_image_get(&download_addr, &download_size);
    if (e != ESP_OK || !download_size) {
        crashlog_download_end();
        return ESP_ERR_NOT_FOUND;
    }
    *size = download_size;
    return ESP_OK;
}

esp_err_t crashlog_read(size_t off, void *buf, size_t len)
{
    if (!mtx || !download_size) return ESP_ERR_INVALID_STATE;
    if (!buf || off > download_size || len > download_size - off) return ESP_ERR_INVALID_ARG;
    return esp_flash_read(NULL, buf, download_addr + off, len);
}

void crashlog_download_end(void)
{
    if (!mtx) return;
    download_size = 0;
    xSemaphoreGive(mtx);
}

esp_err_t crashlog_erase(void)
{
    if (!mtx) return ESP_ERR_INVALID_STATE;
    if (xSemaphoreTake(mtx, portMAX_DELAY) != pdTRUE) return ESP_ERR_TIMEOUT;
    esp_err_t e = ota_external_begin();
    if (e == ESP_OK) {
        e = esp_core_dump_image_erase();
        if (e == ESP_OK) { valid = false; size = 0; memset(&s, 0, sizeof s); }
        ota_external_end();
    }
    xSemaphoreGive(mtx);
    if (e == ESP_OK) log_ring_printf("crash report erased");
    return e;
}

/*
 * crashlog — see crashlog.h.
 */
#include "crashlog.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "esp_core_dump.h"
#include "esp_system.h"
#include "log_ring.h"
#include "rrc_proto.h"

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
    size_t addr = 0;
    if (esp_core_dump_image_get(&addr, size) != ESP_OK || !*size) return false;
    return esp_core_dump_get_summary(s) == ESP_OK;
}

void crashlog_init(void)
{
    static esp_core_dump_summary_t s;   /* ~1.2 KB with the stack dump: off the stack */
    size_t size = 0;
    if (!get(&s, &size)) return;
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
    /* the summary carries a ~1 KB stack dump: on the heap, not the caller's stack */
    esp_core_dump_summary_t *sm = malloc(sizeof *sm);
    size_t size = 0;
    bool have = sm && get(sm, &size);
    rrc_jsonw w;
    rrc_jsonw_init(&w, out, cap);
    rrc_jsonw_raw(&w, "{");
    rrc_jsonw_key(&w, "present", true); rrc_jsonw_raw(&w, have ? "true" : "false");
    rrc_jsonw_key(&w, "reset_reason", false); rrc_jsonw_str(&w, crashlog_reset_reason_str());
    if (have) {
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
    free(sm);
    return rrc_jsonw_finish(&w);
}

esp_err_t crashlog_dump_location(size_t *addr, size_t *size)
{
    if (esp_core_dump_image_get(addr, size) != ESP_OK || !*size) return ESP_ERR_NOT_FOUND;
    return ESP_OK;
}

esp_err_t crashlog_erase(void)
{
    esp_err_t e = esp_core_dump_image_erase();
    if (e == ESP_OK) log_ring_printf("crash report erased");
    return e;
}

#pragma once
/*
 * crashlog — what a crash leaves behind, readable without a serial cable.
 *
 * A panic writes an ELF core dump to the `coredump` partition (ESP-IDF
 * espcoredump; the first one is kept until erased, so a crash that repeats
 * on every boot does not overwrite the one that started it). At the next boot
 * crashlog_init() logs its summary (task, program counter, cause, return
 * address, the firmware's ELF SHA-256) into the log ring, GET /api/coredump
 * downloads the dump for `esp-coredump info_corefile -c dump.bin build.elf`
 * against the matching ELF, and POST /api/coredump/erase drops it.
 */
#include <stdbool.h>
#include <stddef.h>
#include "esp_err.h"

/* After log_ring_init(): logs the stored crash, if any. */
void crashlog_init(void);

/* {"present":bool,"valid":bool,"task":"…","pc":"0x…","cause":n,"cause_text":"…","ra":"0x…","sp":"0x…","elf_sha256":"…","size":n} */
int crashlog_status_json(char *out, size_t cap);

/* Begin a download (even if corrupt), holding the crashlog mutex on ESP_OK.
 * Returns ESP_ERR_NOT_FOUND when absent; errors release the lock. Before init,
 * returns ESP_ERR_INVALID_STATE. The same task must call end after success. */
esp_err_t crashlog_download_begin(size_t *size);

/* Read within the stored image; only between a successful begin and end. */
esp_err_t crashlog_read(size_t off, void *buf, size_t len);

/* Release the download lock, including on read/send failure or disconnect. */
void crashlog_download_end(void);

esp_err_t crashlog_erase(void);

/* Why this boot happened: "power-on", "panic", "task watchdog", … */
const char *crashlog_reset_reason_str(void);

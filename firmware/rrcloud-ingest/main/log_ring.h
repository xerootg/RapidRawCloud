/* In-memory ring of recent log lines, exposed by the web UI (/api/log). */
#pragma once
#include <stddef.h>
#include <stdbool.h>

void log_ring_init(void);
/* printf-style; also forwards to ESP_LOGI under tag "rrc". */
void log_ring_printf(const char *fmt, ...) __attribute__((format(printf, 1, 2)));
/* Appends the ring as a JSON array of strings (oldest first). Returns bytes written or -1. */
int log_ring_to_json(char *out, size_t cap);

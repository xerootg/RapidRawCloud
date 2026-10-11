/* Serial console (UART REPL on the USB-C console port): configure and drive the
 * dock without the web UI — the fallback when the network path to port 80 is
 * not there yet. */
#pragma once
#include "esp_err.h"

esp_err_t console_start(void);

/*
 * USB host diagnostics: log verbosity for the ESP-IDF USB stack, a per-device
 * descriptor dump, and the root-port power-cycle retry that recovers a device
 * whose first reset failed ("HUB: Root port reset failed" is terminal in the
 * host library until the device is unplugged or the port is power-cycled).
 */
#pragma once
#include <stdbool.h>
#include "esp_err.h"
#include "usb/usb_host.h"

/* DEBUG for HUB / HCD DWC / USBH / ENUM / USB HOST / class drivers when verbose,
 * INFO otherwise. Safe to call repeatedly (config save). */
void usb_diag_set_verbose(bool verbose);

/* Logs what a freshly enumerated device presented (speed, VID:PID, strings,
 * every interface's class triple). Called from the first client that sees it. */
void usb_diag_log_device(usb_device_handle_t hdl);

/* Starts the watchdog task: when no device has been enumerated for a while the
 * root port is power-cycled so a stuck connection gets a fresh reset. */
esp_err_t usb_diag_start_watchdog(void);

/* Power-cycles the root port now (web UI "retry USB" button). */
esp_err_t usb_diag_power_cycle(void);

/* Number of devices the host library currently has enumerated. */
int usb_diag_device_count(void);

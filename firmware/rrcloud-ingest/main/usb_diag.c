#include "usb_diag.h"
#include <stdio.h>
#include <string.h>
#include "esp_log.h"
#include "esp_timer.h"
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "usb/usb_helpers.h"
#include "log_ring.h"

static const char *TAG = "usb";

/* Seconds without any enumerated device before the root port is power-cycled.
 * Long enough that a healthy enumeration (well under a second, a few seconds for
 * slow cameras) is never interrupted; short enough that a camera whose first
 * reset failed is retried before the user gives up. */
#define WATCHDOG_IDLE_S 15
#define WATCHDOG_POLL_MS 5000

static int64_t last_cycle_us;

void usb_diag_set_verbose(bool verbose)
{
    const esp_log_level_t lvl = verbose ? ESP_LOG_DEBUG : ESP_LOG_INFO;
    static const char *const tags[] = {"HUB", "HCD DWC", "USBH", "ENUM", "USB HOST", "USB MSC", "rrc_ptp", "usb"};
    for (size_t i = 0; i < sizeof tags / sizeof tags[0]; i++) esp_log_level_set(tags[i], lvl);
    ESP_LOGI(TAG, "usb stack log level: %s", verbose ? "debug" : "info");
}

/* UTF-16LE string descriptor → ASCII (non-ASCII code units become '?'). */
static void str_desc_ascii(const usb_str_desc_t *d, char *out, size_t cap)
{
    size_t n = 0;
    if (d && d->bLength >= 2) {
        size_t units = (d->bLength - 2) / 2;
        for (size_t i = 0; i < units && n + 1 < cap; i++) {
            uint16_t c = d->wData[i];
            out[n++] = (c >= 0x20 && c < 0x7f) ? (char)c : '?';
        }
    }
    out[n] = 0;
}

static const char *speed_name(usb_speed_t s)
{
    switch (s) {
    case USB_SPEED_LOW: return "low";
    case USB_SPEED_FULL: return "full";
    case USB_SPEED_HIGH: return "high";
    default: return "?";
    }
}

void usb_diag_log_device(usb_device_handle_t hdl)
{
    usb_device_info_t info;
    const usb_device_desc_t *dd = NULL;
    const usb_config_desc_t *cfg = NULL;
    if (usb_host_device_info(hdl, &info) != ESP_OK || usb_host_get_device_descriptor(hdl, &dd) != ESP_OK) {
        ESP_LOGW(TAG, "new device: descriptors unreadable");
        return;
    }
    char mfr[48], prod[48], ser[48];
    str_desc_ascii(info.str_desc_manufacturer, mfr, sizeof mfr);
    str_desc_ascii(info.str_desc_product, prod, sizeof prod);
    str_desc_ascii(info.str_desc_serial_num, ser, sizeof ser);
    log_ring_printf("usb: addr %u %s-speed %04x:%04x usb%x.%02x class %02x/%02x/%02x \"%s\" \"%s\" sn \"%s\"",
                    info.dev_addr, speed_name(info.speed), dd->idVendor, dd->idProduct,
                    dd->bcdUSB >> 8, dd->bcdUSB & 0xff, dd->bDeviceClass, dd->bDeviceSubClass, dd->bDeviceProtocol, mfr, prod, ser);
    if (usb_host_get_active_config_descriptor(hdl, &cfg) != ESP_OK) {
        ESP_LOGW(TAG, "  active configuration descriptor unreadable");
        return;
    }
    for (uint8_t i = 0; i < cfg->bNumInterfaces; i++) {
        int off = 0;
        const usb_intf_desc_t *it = usb_parse_interface_descriptor(cfg, i, 0, &off);
        if (!it) continue;
        ESP_LOGI(TAG, "  intf %u: class %02x/%02x/%02x, %u endpoints%s", it->bInterfaceNumber, it->bInterfaceClass, it->bInterfaceSubClass,
                 it->bInterfaceProtocol, it->bNumEndpoints,
                 it->bInterfaceClass == 0x08 ? " (mass storage)" : it->bInterfaceClass == 0x06 ? " (still image / PTP)" : it->bInterfaceClass == 0xff ? " (vendor; MTP-shaped if 3 endpoints)" : "");
    }
}

int usb_diag_device_count(void)
{
    usb_host_lib_info_t li;
    return usb_host_lib_info(&li) == ESP_OK ? li.num_devices : -1;
}

esp_err_t usb_diag_power_cycle(void)
{
    esp_err_t e = usb_host_lib_set_root_port_power(false);
    if (e != ESP_OK) { ESP_LOGW(TAG, "root port power off: %s", esp_err_to_name(e)); return e; }
    vTaskDelay(pdMS_TO_TICKS(500));
    e = usb_host_lib_set_root_port_power(true);
    if (e != ESP_OK) ESP_LOGW(TAG, "root port power on: %s", esp_err_to_name(e));
    last_cycle_us = esp_timer_get_time();
    return e;
}

static void watchdog_task(void *arg)
{
    (void)arg;
    int idle_polls = 0;
    for (;;) {
        vTaskDelay(pdMS_TO_TICKS(WATCHDOG_POLL_MS));
        int n = usb_diag_device_count();
        if (n != 0) { idle_polls = 0; continue; }
        if (++idle_polls * WATCHDOG_POLL_MS < WATCHDOG_IDLE_S * 1000) continue;
        idle_polls = 0;
        /* Nothing enumerated for WATCHDOG_IDLE_S. Either nothing is plugged in
         * (the cycle is a no-op) or a connection is stuck after a failed reset
         * (the cycle re-runs debounce + reset). Debug-level: this fires forever
         * on an empty port. */
        ESP_LOGD(TAG, "no device enumerated for %ds; power-cycling the root port", WATCHDOG_IDLE_S);
        usb_diag_power_cycle();
    }
}

esp_err_t usb_diag_start_watchdog(void)
{
    return xTaskCreate(watchdog_task, "usb_wd", 3072, NULL, 3, NULL) == pdPASS ? ESP_OK : ESP_ERR_NO_MEM;
}

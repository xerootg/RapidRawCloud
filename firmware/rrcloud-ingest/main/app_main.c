/*
 * rrcloud-ingest — RapidRawCloud camera-ingest dock for the Waveshare
 * ESP32-P4-WIFI6-POE-ETH. Plug a camera into the USB-A port; new photos are
 * uploaded into the library bucket as journaled originals (architecture §2.2),
 * exactly as a phone or desktop would publish them.
 */
#include <stdio.h>
#include "esp_log.h"
#include "esp_system.h"
#include "esp_heap_caps.h"
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "usb/usb_host.h"
#include "app_config.h"
#include "store.h"
#include "net.h"
#include "sync.h"
#include "web.h"
#include "camera_source.h"
#include "log_ring.h"
#include "usb_diag.h"
#include "coproc.h"
#include "ble.h"
#include "console.h"
#include "usb_fault.h"
#include "esp_timer.h"

static const char *TAG = "main";

static void usb_lib_task(void *arg)
{
    (void)arg;
    for (;;) {
        uint32_t flags;
        usb_host_lib_handle_events(portMAX_DELAY, &flags);
        if (flags & USB_HOST_LIB_EVENT_FLAGS_ALL_FREE) ESP_LOGD(TAG, "usb: all devices freed");
    }
}

static const char *reset_reason_str(esp_reset_reason_t r)
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

/* A heartbeat on the serial console (debug level, so it stays out of the web
 * log ring): memory headroom and the USB counters, for telling a quiet dock
 * from a dead one and for spotting a slow leak over a long idle stretch. */
#define HEALTH_PERIOD_US (5 * 60 * 1000000LL)
static void health_tick(void *arg)
{
    (void)arg;
    ESP_LOGD("health", "up %llu s; internal free %u min %u largest %u; psram free %u; usb devices %d faults %u chan irqs %u",
             (unsigned long long)(esp_timer_get_time() / 1000000),
             (unsigned)heap_caps_get_free_size(MALLOC_CAP_INTERNAL), (unsigned)heap_caps_get_minimum_free_size(MALLOC_CAP_INTERNAL),
             (unsigned)heap_caps_get_largest_free_block(MALLOC_CAP_INTERNAL), (unsigned)heap_caps_get_free_size(MALLOC_CAP_SPIRAM),
             usb_diag_device_count(), (unsigned)usb_fault_count(), (unsigned)usb_fault_decode_calls());
}

void app_main(void)
{
    log_ring_init();
    ESP_ERROR_CHECK(app_config_init());
    ESP_ERROR_CHECK(store_init());
    store_ledger_load();
    esp_reset_reason_t rr = esp_reset_reason();
    log_ring_printf("rrcloud-ingest starting after %s reset (%d); device %s; psram free %u KiB", reset_reason_str(rr), (int)rr, app_device_id(), (unsigned)(heap_caps_get_free_size(MALLOC_CAP_SPIRAM) / 1024));
    usb_fault_init();
    esp_log_level_set("health", ESP_LOG_DEBUG);
    const esp_timer_create_args_t ht = {.callback = health_tick, .name = "health"};
    esp_timer_handle_t health;
    if (esp_timer_create(&ht, &health) == ESP_OK) esp_timer_start_periodic(health, HEALTH_PERIOD_US);

    ESP_ERROR_CHECK(net_init());
    coproc_start(); /* ESP32-C6 link for BLE/version reporting, with or without Wi-Fi */
    ble_start();    /* BLE admin service once the C6 is linked */

    /* The host library's default peripheral on the P4 is the USB 2.0 High-Speed
     * OTG controller (UTMI PHY), which is what the board's USB-A port is wired
     * to; the Full-Speed OTG 1.1 controller (GPIO 26/27) is unused. The
     * `peripheral_map` selector only exists in IDF ≥ 5.5.1, so the default is
     * relied on rather than spelled out (CI builds with the 5.5.0 image). */
    usb_diag_set_verbose(app_config_get()->usb_debug);
    const usb_host_config_t host_cfg = {.skip_phy_setup = false, .intr_flags = ESP_INTR_FLAG_LEVEL1};
    ESP_ERROR_CHECK(usb_host_install(&host_cfg));
    xTaskCreate(usb_lib_task, "usb_lib", 4096, NULL, 7, NULL);
    ESP_ERROR_CHECK(usb_diag_start_watchdog());

    ESP_ERROR_CHECK(sync_init());
    ESP_ERROR_CHECK(source_msc_install(sync_on_camera_event, NULL));
    ESP_ERROR_CHECK(source_ptp_install(sync_on_camera_event, NULL));

    ESP_ERROR_CHECK(web_start());
    console_start();
    log_ring_printf("ready — plug in a camera");
}

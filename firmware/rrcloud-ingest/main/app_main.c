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

void app_main(void)
{
    log_ring_init();
    ESP_ERROR_CHECK(app_config_init());
    ESP_ERROR_CHECK(store_init());
    store_ledger_load();
    log_ring_printf("rrcloud-ingest starting; device %s; psram free %u KiB", app_device_id(), (unsigned)(heap_caps_get_free_size(MALLOC_CAP_SPIRAM) / 1024));

    ESP_ERROR_CHECK(net_init());

    const usb_host_config_t host_cfg = {.skip_phy_setup = false, .intr_flags = ESP_INTR_FLAG_LEVEL1};
    ESP_ERROR_CHECK(usb_host_install(&host_cfg));
    xTaskCreate(usb_lib_task, "usb_lib", 4096, NULL, 7, NULL);

    ESP_ERROR_CHECK(sync_init());
    ESP_ERROR_CHECK(source_msc_install(sync_on_camera_event, NULL));
    ESP_ERROR_CHECK(source_ptp_install(sync_on_camera_event, NULL));

    ESP_ERROR_CHECK(web_start());
    log_ring_printf("ready — plug in a camera");
}

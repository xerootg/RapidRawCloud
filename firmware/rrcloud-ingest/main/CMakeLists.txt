set(srcs
    "app_main.c" "app_config.c" "store.c" "log_ring.c" "net.c" "sync.c" "pairing.c" "web.c"
    "source_msc.c" "upload_engine.c" "source_ptp.c" "usb_diag.c" "console.c" "coproc.c" "api.c" "ble.c" "power.c")
# On-screen UI glue (panel, touch, backlight, LVGL) only for boards with a display
# (CONFIG_RRC_HAS_DISPLAY, main/Kconfig.projbuild). The requirements stay
# unconditional: IDF resolves them before Kconfig values are final.
if(CONFIG_RRC_HAS_DISPLAY)
    list(APPEND srcs "display.c")
endif()
idf_component_register(
    SRCS ${srcs}
    INCLUDE_DIRS "."
    PRIV_REQUIRES
        rrc_hash rrc_blake3 rrc_s3 rrc_glob rrc_proto rrc_ptp rrc_exfat rrc_ui
        esp_eth esp_netif esp_event esp_timer nvs_flash esp_http_server esp_http_client
        esp-tls mbedtls json usb fatfs vfs esp_wifi app_update esp_partition driver spi_flash esp_hw_support esp_psram
        usb_host_msc littlefs mdns esp_wifi_remote esp_hosted console lwip bt
        esp_adc esp_lcd lvgl esp_lcd_st7701 esp_lcd_touch esp_lcd_touch_gt911
    EMBED_TXTFILES
        "../www/index.html"
)

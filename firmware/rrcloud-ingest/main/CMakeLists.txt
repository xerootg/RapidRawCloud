idf_component_register(
    SRCS
        "app_main.c" "app_config.c" "store.c" "log_ring.c" "net.c" "sync.c" "pairing.c" "web.c"
        "source_msc.c" "source_ptp.c" "usb_diag.c" "console.c"
    INCLUDE_DIRS "."
    PRIV_REQUIRES
        rrc_hash rrc_blake3 rrc_s3 rrc_glob rrc_proto rrc_ptp
        esp_eth esp_netif esp_event esp_timer nvs_flash esp_http_server esp_http_client
        esp-tls mbedtls json usb fatfs vfs esp_wifi app_update esp_partition driver spi_flash esp_hw_support esp_psram
        usb_host_msc littlefs mdns esp_wifi_remote esp_hosted console lwip
    EMBED_TXTFILES
        "../www/index.html"
)

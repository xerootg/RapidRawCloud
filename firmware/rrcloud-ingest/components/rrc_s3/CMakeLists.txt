# rrc_sigv4.c is pure C (host-testable); rrc_s3_client.c needs esp_http_client.
idf_component_register(
    SRCS "rrc_sigv4.c" "rrc_s3_client.c"
    INCLUDE_DIRS "include"
    REQUIRES rrc_hash
    PRIV_REQUIRES esp_http_client esp-tls mbedtls esp_timer)

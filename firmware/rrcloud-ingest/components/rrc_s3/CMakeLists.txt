# rrc_sigv4.c is pure C (host-testable); rrc_s3_client.c talks HTTP/1.1 over esp-tls / lwIP sockets itself.
idf_component_register(
    SRCS "rrc_sigv4.c" "rrc_s3_client.c"
    INCLUDE_DIRS "include"
    REQUIRES rrc_hash
    PRIV_REQUIRES esp-tls mbedtls esp_timer lwip)

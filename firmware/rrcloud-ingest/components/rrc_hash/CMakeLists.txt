idf_component_register(
    SRCS "rrc_hash_esp.c" "rrc_hmac.c" "rrc_crc32.c" "rrc_b64.c"
    INCLUDE_DIRS "include"
    REQUIRES mbedtls)

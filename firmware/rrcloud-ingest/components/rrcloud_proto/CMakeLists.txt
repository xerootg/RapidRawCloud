# The generated protocol SDK (protocol/gen/c), compiled as an IDF component. Regenerate with
# `python3 protocol/rrcgen` after editing protocol/rrcloud.protocol.toml.
set(RRCP_DIR ${CMAKE_CURRENT_LIST_DIR}/../../../../protocol/gen/c)
idf_component_register(
    SRCS "${RRCP_DIR}/rrcloud_proto.c" "rrcp_blake3_hook.c"
    INCLUDE_DIRS "${RRCP_DIR}"
    REQUIRES rrc_blake3)

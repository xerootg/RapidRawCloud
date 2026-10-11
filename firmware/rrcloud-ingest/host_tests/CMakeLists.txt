# Host-side unit tests for the pure components (no ESP-IDF needed):
#   cmake -S host_tests -B host_tests/build && cmake --build host_tests/build && ctest --test-dir host_tests/build --output-on-failure
cmake_minimum_required(VERSION 3.16)
project(rrcloud_ingest_host_tests C)
set(CMAKE_C_STANDARD 99)
set(CMAKE_C_EXTENSIONS ON)
add_compile_options(-Wall -Wextra -O2 -g)
set(COMP ${CMAKE_CURRENT_SOURCE_DIR}/../components)
set(VEC ${CMAKE_CURRENT_SOURCE_DIR}/vectors)
set(GEN ${CMAKE_CURRENT_SOURCE_DIR}/../../../protocol/gen/c)

add_library(rrc_pure STATIC
    ${COMP}/rrc_hash/rrc_sha256.c ${COMP}/rrc_hash/rrc_md5.c ${COMP}/rrc_hash/rrc_hmac.c ${COMP}/rrc_hash/rrc_crc32.c ${COMP}/rrc_hash/rrc_b64.c
    ${COMP}/rrc_blake3/rrc_blake3.c
    ${COMP}/rrc_s3/rrc_sigv4.c
    ${COMP}/rrc_glob/rrc_glob.c
    ${COMP}/rrc_proto/rrc_relkey.c ${COMP}/rrc_proto/rrc_keys.c ${COMP}/rrc_proto/rrc_jsonw.c ${COMP}/rrc_proto/rrc_journal.c
    ${COMP}/rrc_proto/rrc_manifest.c ${COMP}/rrc_proto/rrc_gzip_store.c ${COMP}/rrc_proto/rrc_timefmt.c ${COMP}/rrc_proto/rrc_ledger.c
    ${COMP}/rrc_ptp/rrc_ptp_codec.c
    ${GEN}/rrcloud_proto.c ${COMP}/rrcloud_proto/rrcp_blake3_hook.c)
target_include_directories(rrc_pure PUBLIC
    ${COMP}/rrc_hash/include ${COMP}/rrc_blake3/include ${COMP}/rrc_s3/include ${COMP}/rrc_glob/include ${COMP}/rrc_proto/include ${COMP}/rrc_ptp/include ${GEN})

enable_testing()
foreach(t test_hash test_sigv4 test_glob test_proto test_ptp_codec)
    add_executable(${t} ${t}.c)
    target_link_libraries(${t} rrc_pure)
endforeach()
add_test(NAME hash_and_blake3 COMMAND test_hash ${VEC}/blake3_vectors.txt)
add_test(NAME sigv4_vs_botocore COMMAND test_sigv4 ${VEC}/sigv4_vectors.txt)
add_test(NAME glob COMMAND test_glob)
add_test(NAME proto COMMAND test_proto ${CMAKE_CURRENT_BINARY_DIR}/manifest_test.gz)
add_test(NAME ptp_codec COMMAND test_ptp_codec)
find_program(PYTHON3 python3)
if(PYTHON3)
    add_test(NAME gzip_decodes_with_stdlib COMMAND ${PYTHON3} -I ${CMAKE_CURRENT_SOURCE_DIR}/tools/check_gzip.py ${CMAKE_CURRENT_BINARY_DIR}/manifest_test.gz)
    set_tests_properties(gzip_decodes_with_stdlib PROPERTIES DEPENDS proto)
endif()

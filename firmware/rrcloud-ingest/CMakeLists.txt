# RapidRawCloud camera-ingest dock firmware for the Waveshare ESP32-P4-WIFI6-POE-ETH.
# See README.md in this directory.
cmake_minimum_required(VERSION 3.16)

set(EXTRA_COMPONENT_DIRS ${CMAKE_CURRENT_LIST_DIR}/components)

# Firmware version (also drives the GitHub release tag firmware-v<version>; see .github/workflows/firmware-release.yml).
file(STRINGS ${CMAKE_CURRENT_LIST_DIR}/version.txt PROJECT_VER LIMIT_COUNT 1)

include($ENV{IDF_PATH}/tools/cmake/project.cmake)
project(rrcloud-ingest)

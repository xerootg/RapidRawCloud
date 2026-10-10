# RapidRawCloud camera-ingest dock firmware for the Waveshare ESP32-P4-WIFI6-POE-ETH.
# See README.md in this directory.
cmake_minimum_required(VERSION 3.16)

set(EXTRA_COMPONENT_DIRS ${CMAKE_CURRENT_LIST_DIR}/components)

include($ENV{IDF_PATH}/tools/cmake/project.cmake)
project(rrcloud-ingest)

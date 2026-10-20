# Host simulator for components/rrc_ui: renders every screen of the on-device UI against a
# fake dock (canned /api/* documents, a config that POST /api/config really patches) into
# PNG screenshots, drives it with synthetic touches through LVGL's own input path, and
# fails on LVGL asserts, compiler warnings in the UI sources, or leaked LVGL memory.
#
#   cmake -S firmware/rrcloud-ingest/ui_sim -B /tmp/ui_sim && cmake --build /tmp/ui_sim
#   /tmp/ui_sim/rrc_ui_sim /tmp/ui_sim/shots
#
# LVGL and cJSON are fetched at the versions the firmware pins (main/idf_component.yml and
# ESP-IDF 5.5's json component). Offline: -DFETCHCONTENT_SOURCE_DIR_LVGL=<lvgl checkout>.
cmake_minimum_required(VERSION 3.16)
project(rrc_ui_sim C)
set(CMAKE_C_STANDARD 11)

include(FetchContent)
FetchContent_Declare(lvgl GIT_REPOSITORY https://github.com/lvgl/lvgl.git GIT_TAG v9.5.0 GIT_SHALLOW TRUE)
FetchContent_Declare(cjson GIT_REPOSITORY https://github.com/DaveGamble/cJSON.git GIT_TAG v1.7.18 GIT_SHALLOW TRUE)
FetchContent_GetProperties(lvgl)
if(NOT lvgl_POPULATED)
    FetchContent_Populate(lvgl)
endif()
FetchContent_GetProperties(cjson)
if(NOT cjson_POPULATED)
    FetchContent_Populate(cjson)
endif()

file(GLOB_RECURSE LVGL_SRCS ${lvgl_SOURCE_DIR}/src/*.c)
add_library(lvgl_sim STATIC ${LVGL_SRCS} ${cjson_SOURCE_DIR}/cJSON.c)
target_include_directories(lvgl_sim PUBLIC ${lvgl_SOURCE_DIR} ${CMAKE_CURRENT_SOURCE_DIR} ${cjson_SOURCE_DIR})
target_compile_definitions(lvgl_sim PUBLIC LV_CONF_INCLUDE_SIMPLE)
target_compile_options(lvgl_sim PRIVATE -w)

set(UI_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../components/rrc_ui)
file(GLOB UI_FONTS ${UI_DIR}/fonts/ui_font_*.c)
add_library(ui_fonts STATIC ${UI_FONTS})
target_link_libraries(ui_fonts PUBLIC lvgl_sim)
add_executable(rrc_ui_sim
    sim.c fake_dock.c png.c
    ${UI_DIR}/ui_core.c ${UI_DIR}/ui_theme.c ${UI_DIR}/ui_widgets.c
    ${UI_DIR}/page_home.c ${UI_DIR}/page_sync.c ${UI_DIR}/page_wifi.c ${UI_DIR}/page_device.c)
target_include_directories(rrc_ui_sim PRIVATE ${UI_DIR} ${UI_DIR}/include)
target_link_libraries(rrc_ui_sim PRIVATE ui_fonts lvgl_sim m)
# The UI sources must stay warning-free (the firmware builds them with -Werror=all).
target_compile_options(rrc_ui_sim PRIVATE -Wall -Wextra -Werror -Wno-unused-parameter -Wno-missing-field-initializers -fsanitize=address,undefined -fno-omit-frame-pointer)
target_link_options(rrc_ui_sim PRIVATE -fsanitize=address,undefined)

enable_testing()
add_test(NAME rrc_ui_screens COMMAND rrc_ui_sim ${CMAKE_CURRENT_BINARY_DIR}/shots)

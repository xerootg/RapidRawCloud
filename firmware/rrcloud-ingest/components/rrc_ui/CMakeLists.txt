# The on-screen UI. Pure LVGL + cJSON (no ESP-IDF calls) so ui_sim/ builds the same sources
# on the host. Compiled only for boards with a display; the requirements stay unconditional
# because IDF resolves them before Kconfig values are final.
set(srcs)
if(CONFIG_RRC_HAS_DISPLAY)
    file(GLOB fonts ${CMAKE_CURRENT_LIST_DIR}/fonts/ui_font_*.c)
    set(srcs ui_core.c ui_theme.c ui_widgets.c page_home.c page_sync.c page_wifi.c page_device.c ${fonts})
endif()
idf_component_register(
    SRCS ${srcs}
    INCLUDE_DIRS include
    PRIV_INCLUDE_DIRS .
    REQUIRES lvgl json
)

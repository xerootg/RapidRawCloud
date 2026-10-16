/*
 * LVGL configuration for the host simulator. Mirrors the firmware's LVGL settings in
 * ../sdkconfig.board.touch_lcd_43 (color depth, fonts, widgets, refresh period) so the
 * screenshots and the leak check exercise what runs on the board. Differences: LVGL's
 * built-in allocator (so lv_mem_monitor can count leaks) and no OS layer.
 */
#ifndef LV_CONF_H
#define LV_CONF_H

#define LV_COLOR_DEPTH 16
#define LV_USE_STDLIB_MALLOC LV_STDLIB_BUILTIN
#define LV_MEM_SIZE (8 * 1024 * 1024)
#define LV_DEF_REFR_PERIOD 16
#define LV_DPI_DEF 217
#define LV_USE_OS LV_OS_NONE
#define LV_DRAW_SW_DRAW_UNIT_CNT 1

#define LV_USE_LOG 1
#define LV_LOG_LEVEL LV_LOG_LEVEL_WARN
#define LV_LOG_PRINTF 1
#define LV_USE_ASSERT_NULL 1
#define LV_USE_ASSERT_MALLOC 1
#define LV_USE_ASSERT_STYLE 1
#define LV_USE_ASSERT_MEM_INTEGRITY 0
#define LV_USE_ASSERT_OBJ 1
/* an LVGL assert fails the simulator run */
#define LV_ASSERT_HANDLER_INCLUDE <stdlib.h>
#define LV_ASSERT_HANDLER abort();

#define LV_FONT_MONTSERRAT_14 1
#define LV_FONT_DEFAULT &lv_font_montserrat_14

#define LV_USE_THEME_DEFAULT 1
#define LV_USE_QRCODE 1
#define LV_USE_SNAPSHOT 0
#define LV_USE_SYSMON 0
#define LV_BUILD_EXAMPLES 0
#define LV_BUILD_DEMOS 0

#define LV_USE_CALENDAR 0
#define LV_USE_CHART 0
#define LV_USE_IMAGEBUTTON 0
#define LV_USE_LED 0
#define LV_USE_MENU 0
#define LV_USE_SPAN 0
#define LV_USE_SPINBOX 0
#define LV_USE_TABVIEW 0
#define LV_USE_TILEVIEW 0
#define LV_USE_WIN 0
#define LV_USE_ANIMIMG 0
#define LV_USE_SCALE 0

#endif

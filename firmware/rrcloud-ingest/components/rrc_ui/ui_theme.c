/*
 * Material 3 color scheme and the LVGL theme hook that applies it.
 *
 * The token values are the "tonal spot" scheme the Material Color Utilities
 * derive from seed #2B6CB0 (materialyoucolor, contrast 0). Widgets in
 * ui_widgets.c style themselves from M; the LVGL default theme only provides the
 * base the widget styles override, so its own palette is set to match.
 */
#include "ui_priv.h"

ui_scheme_t M;
bool ui_dark;

#define C(x) lv_color_hex(x)

static void scheme_light(ui_scheme_t *s)
{
    *s = (ui_scheme_t){
        .primary = C(0x416189), .on_primary = C(0xF8F8FF), .primary_container = C(0xA9C9F7), .on_primary_container = C(0x204168),
        .secondary = C(0x546071), .on_secondary = C(0xF8F8FF), .secondary_container = C(0xD8E3F8), .on_secondary_container = C(0x475263),
        .tertiary = C(0x635983), .on_tertiary = C(0xFDF7FF), .tertiary_container = C(0xDACDFF), .on_tertiary_container = C(0x4C436C),
        .error = C(0xA83836), .on_error = C(0xFFF7F6), .error_container = C(0xFA746F), .on_error_container = C(0x6E0A12),
        .surface = C(0xF9F9FE), .on_surface = C(0x2E333A), .on_surface_variant = C(0x5B5F67), .outline = C(0x777B83), .outline_variant = C(0xAEB2BB),
        .surface_container_lowest = C(0xFFFFFF), .surface_container_low = C(0xF2F3FA), .surface_container = C(0xECEEF5),
        .surface_container_high = C(0xE5E8F0), .surface_container_highest = C(0xDFE2EC),
        .inverse_surface = C(0x0C0E12), .inverse_on_surface = C(0x9C9DA1), .inverse_primary = C(0xA9C9F7), .scrim = C(0x000000),
        .success = C(0x2E7D32), .warning = C(0x9A6700),
    };
}

static void scheme_dark(ui_scheme_t *s)
{
    *s = (ui_scheme_t){
        .primary = C(0xB0C8EC), .on_primary = C(0x2A415F), .primary_container = C(0x3D5472), .on_primary_container = C(0xD4E4FF),
        .secondary = C(0xBCC7DB), .on_secondary = C(0x364151), .secondary_container = C(0x313C4C), .on_secondary_container = C(0xB4C0D4),
        .tertiary = C(0xE7DEFF), .on_tertiary = C(0x554C75), .tertiary_container = C(0xDACDFF), .on_tertiary_container = C(0x4C436C),
        .error = C(0xFA746F), .on_error = C(0x490006), .error_container = C(0x871F21), .on_error_container = C(0xFF9993),
        .surface = C(0x0C0E12), .on_surface = C(0xE2E5EF), .on_surface_variant = C(0xA7ABB4), .outline = C(0x71767E), .outline_variant = C(0x444850),
        .surface_container_lowest = C(0x000000), .surface_container_low = C(0x111418), .surface_container = C(0x161A1F),
        .surface_container_high = C(0x1C2026), .surface_container_highest = C(0x21262D),
        .inverse_surface = C(0xF9F9FE), .inverse_on_surface = C(0x535559), .inverse_primary = C(0x496080), .scrim = C(0x000000),
        .success = C(0x7BD88F), .warning = C(0xF2C060),
    };
}

void ui_theme_init(lv_display_t *disp, bool dark)
{
    ui_dark = dark;
    if (dark) scheme_dark(&M); else scheme_light(&M);
    lv_theme_t *th = lv_theme_default_init(disp, M.primary, M.tertiary, dark, UI_FONT_BODY);
    lv_display_set_theme(disp, th);
}

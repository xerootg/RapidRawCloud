/*
 * rrc_ui internals: Material 3 tokens, widgets and the page framework shared by
 * the page modules (page_*.c). Everything here runs on the LVGL task.
 */
#pragma once
#include <stdbool.h>
#include <stdint.h>
#include "lvgl.h"
#include "cJSON.h"
#include "rrc_ui.h"
#include "ui_icons.h"

/* ---- Material 3 color scheme (ui_theme.c) ----------------------------------
 * Tonal-spot scheme generated from the seed #2B6CB0 (the web UI's accent) with
 * the Material Color Utilities; light and dark sets. */
typedef struct {
    lv_color_t primary, on_primary, primary_container, on_primary_container;
    lv_color_t secondary, on_secondary, secondary_container, on_secondary_container;
    lv_color_t tertiary, on_tertiary, tertiary_container, on_tertiary_container;
    lv_color_t error, on_error, error_container, on_error_container;
    lv_color_t surface, on_surface, on_surface_variant, outline, outline_variant;
    lv_color_t surface_container_lowest, surface_container_low, surface_container, surface_container_high, surface_container_highest;
    lv_color_t inverse_surface, inverse_on_surface, inverse_primary, scrim;
    lv_color_t success, warning;   /* status colors outside the M3 set, tuned per scheme */
} ui_scheme_t;

extern ui_scheme_t M;
extern bool ui_dark;
void ui_theme_init(lv_display_t *disp, bool dark);

/* Type scale. Roboto and Material Symbols (fonts/gen_fonts.py). The panel is 480 px
 * wide at ~217 ppi, i.e. a phone-width screen of ~353 dp at ~1.36 px/dp; sizes sit a
 * little under the M3 spec × 1.36 so a settings row still fits one line of help text. */
LV_FONT_DECLARE(ui_font_r16);
LV_FONT_DECLARE(ui_font_r20);
LV_FONT_DECLARE(ui_font_r24);
LV_FONT_DECLARE(ui_font_r30);
LV_FONT_DECLARE(ui_font_r44);
LV_FONT_DECLARE(ui_font_m16);
LV_FONT_DECLARE(ui_font_m18);
LV_FONT_DECLARE(ui_font_icon28);
#define UI_FONT_SMALL     (&ui_font_r16)     /* body small: supporting text, help, log */
#define UI_FONT_BODY      (&ui_font_r20)     /* body large: list headlines; + icons (status bar, inline) */
#define UI_FONT_TITLE     (&ui_font_r24)     /* title large: card/dialog titles, text fields, keys; + icons */
#define UI_FONT_HEADLINE  (&ui_font_r30)     /* headline: top app bar */
#define UI_FONT_DISPLAY   (&ui_font_r44)     /* display: the pairing code (ASCII only) */
#define UI_FONT_LABEL     (&ui_font_m16)     /* label: section headers, chips, navigation, status bar */
#define UI_FONT_BUTTON    (&ui_font_m18)     /* label large: buttons */
#define UI_FONT_ICON      (&ui_font_icon28)  /* 24 dp icons: navigation, list leading, app bar */

/* Geometry (px), M3 dp × ~1.2–1.36: touch targets ≥ 48 dp while 480 px still fits a
 * ten-key keyboard row. The screen size comes from the display at rrc_ui_start()
 * and again after each rotation: 480x800 portrait gets the bottom navigation bar,
 * 800x480 landscape a navigation rail on the left and a shorter app bar
 * (ui_core.c build_shell). */
extern int ui_w, ui_h;
extern bool ui_landscape;
#define UI_W              ui_w
#define UI_H              ui_h
#define UI_STATUS_H       40
#define UI_APPBAR_H       (ui_landscape ? 64 : 80)
#define UI_NAVBAR_H       104      /* portrait: bottom navigation bar */
#define UI_RAIL_W         96       /* landscape: navigation rail */
#define UI_RAIL_ITEM_H    96       /* rail destinations: fixed pitch from the top (ui_sim taps them by position) */
/* The content area's origin and width (right of the rail in landscape). */
#define UI_CONTENT_X      (ui_landscape ? UI_RAIL_W : 0)
#define UI_CONTENT_Y      (UI_STATUS_H + UI_APPBAR_H)
#define UI_CONTENT_W      (UI_W - UI_CONTENT_X)
#define UI_PAD            20       /* screen margin */
#define UI_GAP            14
#define UI_RADIUS_CARD    20
#define UI_RADIUS_FULL    LV_RADIUS_CIRCLE
#define UI_ITEM_H         84       /* two-line list item */
#define UI_BTN_H          56

/* ---- requests (ui_core.c) -------------------------------------------------- */
/* `json` is the parsed body (NULL when absent or not JSON); valid for the call only. */
typedef void (*ui_resp_fn)(int status, const cJSON *json, void *ud);
/* Page-scoped requests are dropped if the page changed before the answer came. */
void ui_req(const char *method, const char *path, const char *body, ui_resp_fn fn, void *ud);
void ui_req_global(const char *method, const char *path, const char *body, ui_resp_fn fn, void *ud);
/* {"ok":false,"error":"…"} → "…"; otherwise a generic "HTTP <n>" message. */
const char *ui_resp_error(int status, const cJSON *json);

/* Latest /api/status and /api/config documents (NULL until the first answer). */
const cJSON *ui_status(void);
const cJSON *ui_config(void);
void ui_config_reload(void);
/* POSTs a config patch; on success reloads the config and calls fn(200, …). */
void ui_config_patch(cJSON *patch /* consumed */, ui_resp_fn fn, void *ud);

/* JSON accessors that tolerate missing keys. */
const char *jstr(const cJSON *o, const char *k);
double jnum(const cJSON *o, const char *k);
bool jbool(const cJSON *o, const char *k);
const cJSON *jobj(const cJSON *o, const char *k);

/* ---- pages / navigation ----------------------------------------------------- */
typedef struct ui_page {
    const char *title;
    void (*build)(lv_obj_t *content);         /* populate the scrollable content area */
    void (*on_status)(const cJSON *status);   /* every status poll (≈1 s) while visible */
    void (*on_config)(const cJSON *config);   /* after each config (re)load */
    void (*tick)(void);                       /* ≈1 s while visible: page-specific polling */
    void (*destroy)(void);                    /* drop page-static pointers */
    bool locked;                              /* hidden behind the admin password when one is set */
} ui_page_t;

extern const ui_page_t page_home, page_sync, page_wifi, page_device;
extern const ui_page_t page_cloud, page_pairing, page_log, page_about;

/* Top-level destinations swap the content; sub-pages stack on top with a back arrow. */
void ui_nav_tab(int index);                    /* 0 home, 1 sync, 2 wifi, 3 device */
void ui_nav_push(const ui_page_t *page);
void ui_nav_pop(void);
/* Rebuilds the visible page (after a scheme change or unlock). */
void ui_nav_rebuild(void);
/* Sets the trailing action of the top app bar (icon symbol or NULL to clear). */
void ui_appbar_action(const char *symbol, void (*fn)(void));

/* Applies a backlight level now without saving it (brightness slider while dragging). */
void ui_preview_backlight(int percent);
/* True when the host can rotate the screen (the Orientation setting is shown). */
bool ui_can_rotate(void);

/* ---- admin lock ------------------------------------------------------------- */
bool ui_lock_required(void);    /* admin_auth on and a password stored */
bool ui_unlocked(void);
void ui_unlock_prompt(void);

/* ---- config bindings -------------------------------------------------------- */
typedef enum { BIND_TEXT, BIND_SECRET, BIND_NUMBER, BIND_SWITCH } ui_bind_kind_t;
typedef enum { EDIT_TEXT, EDIT_PASSWORD, EDIT_NUMBER } ui_edit_mode_t;
typedef struct {
    const char *symbol;       /* leading icon (LV_SYMBOL_*) or NULL */
    const char *headline;
    const char *key;          /* config key; for BIND_SECRET the write key */
    const char *has_key;      /* BIND_SECRET: config flag saying a value is stored */
    const char *help;         /* supporting text in the editor */
    const char *placeholder;  /* shown when the value is empty */
    const char *suffix;       /* BIND_NUMBER unit, e.g. " KB" */
    ui_bind_kind_t kind;
    uint16_t max_len;
} ui_setting_t;
/* A list item bound to one config key: shows the current value, opens the editor
 * (or toggles the switch) and saves a one-key patch. Kept in sync on each config load. */
lv_obj_t *ui_setting(lv_obj_t *list, const ui_setting_t *s);

/* ---- widgets (ui_widgets.c) ------------------------------------------------- */
typedef enum { BTN_FILLED, BTN_TONAL, BTN_OUTLINED, BTN_TEXT, BTN_ERROR } ui_btn_kind_t;
typedef enum { TONE_NEUTRAL, TONE_PRIMARY, TONE_SUCCESS, TONE_WARNING, TONE_ERROR } ui_tone_t;

lv_obj_t *ui_column(lv_obj_t *parent, int gap);
lv_obj_t *ui_row(lv_obj_t *parent, int gap);
lv_obj_t *ui_card(lv_obj_t *parent);                         /* filled card, column layout */
lv_obj_t *ui_label(lv_obj_t *parent, const lv_font_t *font, lv_color_t color, const char *text);
lv_obj_t *ui_section(lv_obj_t *parent, const char *title);   /* section header + list container */
lv_obj_t *ui_list_item(lv_obj_t *list, const char *symbol, const char *headline, const char *supporting);
void ui_item_supporting(lv_obj_t *item, const char *text);
void ui_item_supporting_color(lv_obj_t *item, lv_color_t color);
lv_obj_t *ui_item_trailing_text(lv_obj_t *item, const char *text);
lv_obj_t *ui_item_switch(lv_obj_t *item, bool on);
lv_obj_t *ui_button(lv_obj_t *parent, ui_btn_kind_t kind, const char *symbol, const char *text, lv_event_cb_t cb, void *ud);
void ui_button_text(lv_obj_t *btn, const char *text);
lv_obj_t *ui_chip(lv_obj_t *parent, const char *text, ui_tone_t tone);
void ui_chip_set(lv_obj_t *chip, const char *text, ui_tone_t tone);
lv_obj_t *ui_progress(lv_obj_t *parent);
lv_obj_t *ui_switch(lv_obj_t *parent, bool on);
lv_obj_t *ui_icon_circle(lv_obj_t *parent, const char *symbol, ui_tone_t tone);
lv_color_t ui_tone_fg(ui_tone_t t);
lv_color_t ui_tone_bg(ui_tone_t t);

/* Signal-strength glyph: four bars filled by RSSI. */
lv_obj_t *ui_signal(lv_obj_t *parent, int height);
void ui_signal_set(lv_obj_t *sig, int rssi, bool connected, lv_color_t color);
/* Battery glyph: outline + fill; pct < 0 hides the fill. */
lv_obj_t *ui_battery(lv_obj_t *parent);
void ui_battery_set(lv_obj_t *bat, int pct, bool present);

/* Transient message above the navigation bar (M3 snackbar). */
void ui_snackbar(const char *fmt, ...) LV_FORMAT_ATTRIBUTE(1, 2);
/* Basic dialog with scrim. `confirm` NULL = information only ("OK"). */
void ui_dialog(const char *title, const char *body, const char *confirm, bool destructive, void (*on_confirm)(void *ud), void *ud);
/* Single-choice dialog (radio list); on_pick(index, ud). */
void ui_choose(const char *title, const char *const *options, int n, int selected, void (*on_pick)(int, void *), void *ud);

/* Full-screen text editor with the on-screen keyboard (M3 full-screen dialog).
 * save(value, ud) is called on "Save"; the caller answers with ui_editor_done():
 * NULL closes the editor, a message is shown as the field's error text. */
typedef struct {
    const char *title;
    const char *label;
    const char *initial;
    const char *help;
    const char *placeholder;
    ui_edit_mode_t mode;
    uint16_t max_len;
    bool allow_empty;
    void (*save)(const char *value, void *ud);
    void *ud;
} ui_editor_t;
void ui_editor_open(const ui_editor_t *e);
void ui_editor_done(const char *error);
bool ui_editor_is_open(void);

/* Formatting helpers. */
void ui_fmt_bytes(char *out, size_t cap, double n);
void ui_fmt_duration(char *out, size_t cap, double seconds);
void ui_fmt_ago(char *out, size_t cap, double ts, double now);
/* Device clock (epoch seconds) from the latest status, 0 when not synchronized. */
double ui_now(void);

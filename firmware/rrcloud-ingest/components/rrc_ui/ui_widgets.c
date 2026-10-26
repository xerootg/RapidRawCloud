/*
 * Material 3 building blocks on LVGL 9: containers, grouped lists, buttons,
 * chips, switches, progress, status glyphs, snackbar, dialogs and the
 * full-screen text editor with its keyboard.
 *
 * Every widget starts from lv_obj_remove_style_all() and is styled from the
 * scheme in M, so what is on screen is the M3 token set rather than LVGL's
 * default theme. Widgets are created per page and deleted with it; nothing here
 * keeps pointers across pages except the editor/snackbar/dialog singletons on
 * lv_layer_top(), which clean themselves up.
 */
#include "ui_priv.h"
#include <stdio.h>
#include <string.h>
#include <stdarg.h>

/* ---- containers --------------------------------------------------------------- */
static lv_obj_t *bare(lv_obj_t *parent)
{
    lv_obj_t *o = lv_obj_create(parent);
    lv_obj_remove_style_all(o);
    lv_obj_remove_flag(o, LV_OBJ_FLAG_SCROLLABLE);
    return o;
}

lv_obj_t *ui_column(lv_obj_t *parent, int gap)
{
    lv_obj_t *o = bare(parent);
    lv_obj_set_size(o, LV_PCT(100), LV_SIZE_CONTENT);
    lv_obj_set_flex_flow(o, LV_FLEX_FLOW_COLUMN);
    lv_obj_set_style_pad_row(o, gap, 0);
    return o;
}

lv_obj_t *ui_row(lv_obj_t *parent, int gap)
{
    lv_obj_t *o = bare(parent);
    lv_obj_set_size(o, LV_PCT(100), LV_SIZE_CONTENT);
    lv_obj_set_flex_flow(o, LV_FLEX_FLOW_ROW);
    lv_obj_set_flex_align(o, LV_FLEX_ALIGN_START, LV_FLEX_ALIGN_CENTER, LV_FLEX_ALIGN_CENTER);
    lv_obj_set_style_pad_column(o, gap, 0);
    return o;
}

lv_obj_t *ui_card(lv_obj_t *parent)
{
    lv_obj_t *o = ui_column(parent, 10);
    lv_obj_set_style_bg_color(o, M.surface_container_high, 0);
    lv_obj_set_style_bg_opa(o, LV_OPA_COVER, 0);
    lv_obj_set_style_radius(o, UI_RADIUS_CARD, 0);
    lv_obj_set_style_pad_all(o, 18, 0);
    return o;
}

lv_obj_t *ui_label(lv_obj_t *parent, const lv_font_t *font, lv_color_t color, const char *text)
{
    lv_obj_t *l = lv_label_create(parent);
    lv_obj_set_style_text_font(l, font, 0);
    lv_obj_set_style_text_color(l, color, 0);
    lv_label_set_text(l, text ? text : "");
    return l;
}

/* A label that wraps inside its parent's width. */
static lv_obj_t *wrap_label(lv_obj_t *parent, const lv_font_t *font, lv_color_t color, const char *text)
{
    lv_obj_t *l = ui_label(parent, font, color, text);
    lv_obj_set_width(l, LV_PCT(100));
    lv_label_set_long_mode(l, LV_LABEL_LONG_MODE_WRAP);
    return l;
}

lv_obj_t *ui_section(lv_obj_t *parent, const char *title)
{
    lv_obj_t *h = ui_label(parent, UI_FONT_LABEL, M.primary, title);
    lv_obj_set_style_pad_left(h, 8, 0);
    lv_obj_set_style_pad_top(h, 10, 0);
    lv_obj_t *list = ui_column(parent, 2);
    lv_obj_set_style_bg_color(list, M.surface_container, 0);
    lv_obj_set_style_bg_opa(list, LV_OPA_COVER, 0);
    lv_obj_set_style_radius(list, UI_RADIUS_CARD, 0);
    lv_obj_set_style_clip_corner(list, true, 0);
    return list;
}

/* ---- list items ----------------------------------------------------------------
 * Children by index: [0] leading icon (or hidden), [1] text column, [2] trailing. */
lv_obj_t *ui_list_item(lv_obj_t *list, const char *symbol, const char *headline, const char *supporting)
{
    lv_obj_t *it = bare(list);
    lv_obj_set_size(it, LV_PCT(100), LV_SIZE_CONTENT);
    lv_obj_set_style_min_height(it, UI_ITEM_H - 16, 0);
    lv_obj_set_style_pad_hor(it, 20, 0);
    lv_obj_set_style_pad_ver(it, 12, 0);
    lv_obj_set_flex_flow(it, LV_FLEX_FLOW_ROW);
    lv_obj_set_flex_align(it, LV_FLEX_ALIGN_START, LV_FLEX_ALIGN_CENTER, LV_FLEX_ALIGN_CENTER);
    lv_obj_set_style_pad_column(it, 18, 0);
    lv_obj_set_style_bg_color(it, M.on_surface, LV_STATE_PRESSED);
    lv_obj_set_style_bg_opa(it, LV_OPA_10, LV_STATE_PRESSED);

    lv_obj_t *icon = ui_label(it, UI_FONT_ICON, M.on_surface_variant, symbol ? symbol : "");
    lv_obj_set_width(icon, 30);
    if (!symbol) lv_obj_add_flag(icon, LV_OBJ_FLAG_HIDDEN);

    lv_obj_t *col = ui_column(it, 2);
    lv_obj_set_flex_grow(col, 1);
    lv_obj_set_width(col, 0);    /* flex-grow decides; 0 lets long text wrap instead of pushing the trailing widget out */
    lv_obj_add_flag(col, LV_OBJ_FLAG_EVENT_BUBBLE);
    lv_obj_t *h = wrap_label(col, UI_FONT_BODY, M.on_surface, headline);
    lv_obj_set_style_text_line_space(h, 2, 0);
    lv_obj_t *s = wrap_label(col, UI_FONT_SMALL, M.on_surface_variant, supporting);
    if (!supporting || !supporting[0]) lv_obj_add_flag(s, LV_OBJ_FLAG_HIDDEN);
    return it;
}

static lv_obj_t *item_supporting_label(lv_obj_t *item)
{
    lv_obj_t *col = lv_obj_get_child(item, 1);
    return col ? lv_obj_get_child(col, 1) : NULL;
}

void ui_item_supporting(lv_obj_t *item, const char *text)
{
    lv_obj_t *s = item_supporting_label(item);
    if (!s) return;
    if (strcmp(lv_label_get_text(s), text ? text : "")) lv_label_set_text(s, text ? text : "");
    if (text && text[0]) lv_obj_remove_flag(s, LV_OBJ_FLAG_HIDDEN); else lv_obj_add_flag(s, LV_OBJ_FLAG_HIDDEN);
}

void ui_item_supporting_color(lv_obj_t *item, lv_color_t color)
{
    lv_obj_t *s = item_supporting_label(item);
    if (s) lv_obj_set_style_text_color(s, color, 0);
}

lv_obj_t *ui_item_trailing_text(lv_obj_t *item, const char *text)
{
    lv_obj_t *l = ui_label(item, UI_FONT_ICON, M.on_surface_variant, text);
    return l;
}

lv_obj_t *ui_switch(lv_obj_t *parent, bool on)
{
    lv_obj_t *sw = lv_switch_create(parent);
    lv_obj_set_size(sw, 60, 34);
    lv_obj_set_style_bg_color(sw, M.surface_container_highest, LV_PART_MAIN);
    lv_obj_set_style_bg_opa(sw, LV_OPA_COVER, LV_PART_MAIN);
    lv_obj_set_style_border_color(sw, M.outline, LV_PART_MAIN);
    lv_obj_set_style_border_width(sw, 2, LV_PART_MAIN);
    lv_obj_set_style_bg_color(sw, M.primary, LV_PART_INDICATOR | LV_STATE_CHECKED);
    lv_obj_set_style_bg_opa(sw, LV_OPA_COVER, LV_PART_INDICATOR | LV_STATE_CHECKED);
    lv_obj_set_style_border_color(sw, M.primary, LV_PART_MAIN | LV_STATE_CHECKED);
    lv_obj_set_style_bg_color(sw, M.outline, LV_PART_KNOB);
    lv_obj_set_style_pad_all(sw, -5, LV_PART_KNOB);
    lv_obj_set_style_bg_color(sw, M.on_primary, LV_PART_KNOB | LV_STATE_CHECKED);
    lv_obj_set_style_pad_all(sw, -3, LV_PART_KNOB | LV_STATE_CHECKED);
    lv_obj_set_style_bg_color(sw, M.on_surface, LV_PART_MAIN | LV_STATE_DISABLED);
    lv_obj_set_style_bg_opa(sw, LV_OPA_20, LV_PART_MAIN | LV_STATE_DISABLED);
    lv_obj_set_ext_click_area(sw, 12);
    if (on) lv_obj_add_state(sw, LV_STATE_CHECKED);
    return sw;
}

lv_obj_t *ui_item_switch(lv_obj_t *item, bool on)
{
    return ui_switch(item, on);
}

/* ---- buttons -------------------------------------------------------------------- */
lv_obj_t *ui_button(lv_obj_t *parent, ui_btn_kind_t kind, const char *symbol, const char *text, lv_event_cb_t cb, void *ud)
{
    lv_obj_t *b = lv_button_create(parent);
    lv_obj_remove_style_all(b);
    lv_obj_set_height(b, UI_BTN_H);
    lv_obj_set_width(b, LV_SIZE_CONTENT);
    lv_obj_set_style_radius(b, UI_RADIUS_FULL, 0);
    lv_obj_set_style_pad_hor(b, kind == BTN_TEXT ? 14 : 24, 0);
    lv_obj_set_flex_flow(b, LV_FLEX_FLOW_ROW);
    lv_obj_set_flex_align(b, LV_FLEX_ALIGN_CENTER, LV_FLEX_ALIGN_CENTER, LV_FLEX_ALIGN_CENTER);
    lv_obj_set_style_pad_column(b, 8, 0);
    lv_color_t bg = M.primary, fg = M.on_primary;
    lv_opa_t opa = LV_OPA_COVER;
    switch (kind) {
    case BTN_FILLED: break;
    case BTN_TONAL: bg = M.secondary_container; fg = M.on_secondary_container; break;
    case BTN_ERROR: bg = M.error; fg = M.on_error; break;
    case BTN_OUTLINED:
        opa = LV_OPA_TRANSP; fg = M.primary;
        lv_obj_set_style_border_color(b, M.outline, 0);
        lv_obj_set_style_border_width(b, 1, 0);
        break;
    case BTN_TEXT: opa = LV_OPA_TRANSP; fg = M.primary; break;
    }
    lv_obj_set_style_bg_color(b, bg, 0);
    lv_obj_set_style_bg_opa(b, opa, 0);
    lv_obj_set_style_text_color(b, fg, 0);
    lv_obj_set_style_text_font(b, UI_FONT_BUTTON, 0);
    /* M3 state layers: pressed = content color at 10 % over the container */
    lv_obj_set_style_shadow_width(b, 0, 0);
    lv_obj_set_style_bg_color(b, opa == LV_OPA_TRANSP ? fg : lv_color_mix(fg, bg, LV_OPA_20), LV_STATE_PRESSED);
    lv_obj_set_style_bg_opa(b, opa == LV_OPA_TRANSP ? LV_OPA_10 : LV_OPA_COVER, LV_STATE_PRESSED);
    lv_obj_set_style_bg_color(b, M.on_surface, LV_STATE_DISABLED);
    lv_obj_set_style_bg_opa(b, kind == BTN_TEXT || kind == BTN_OUTLINED ? LV_OPA_TRANSP : LV_OPA_10, LV_STATE_DISABLED);
    lv_obj_set_style_text_color(b, M.on_surface, LV_STATE_DISABLED);
    lv_obj_set_style_text_opa(b, LV_OPA_40, LV_STATE_DISABLED);
    lv_obj_set_style_border_opa(b, LV_OPA_20, LV_STATE_DISABLED);
    if (symbol) {
        lv_obj_t *ic = lv_label_create(b);
        lv_obj_set_style_text_font(ic, UI_FONT_BODY, 0);   /* the icon glyphs live in the body font */
        lv_label_set_text(ic, symbol);
    }
    if (text) lv_label_set_text(lv_label_create(b), text);
    if (cb) lv_obj_add_event_cb(b, cb, LV_EVENT_CLICKED, ud);
    return b;
}

void ui_button_text(lv_obj_t *btn, const char *text)
{
    lv_obj_t *l = lv_obj_get_child(btn, -1);
    if (l && lv_obj_check_type(l, &lv_label_class)) lv_label_set_text(l, text);
}

/* ---- chips, tones, progress ---------------------------------------------------- */
lv_color_t ui_tone_fg(ui_tone_t t)
{
    switch (t) {
    case TONE_PRIMARY: return M.on_primary_container;
    case TONE_SUCCESS: return ui_dark ? lv_color_hex(0x0B3A16) : lv_color_hex(0xFFFFFF);
    case TONE_WARNING: return ui_dark ? lv_color_hex(0x3D2A00) : lv_color_hex(0xFFFFFF);
    case TONE_ERROR: return M.on_error_container;
    default: return M.on_surface_variant;
    }
}

lv_color_t ui_tone_bg(ui_tone_t t)
{
    switch (t) {
    case TONE_PRIMARY: return M.primary_container;
    case TONE_SUCCESS: return M.success;
    case TONE_WARNING: return M.warning;
    case TONE_ERROR: return M.error_container;
    default: return M.surface_container_highest;
    }
}

lv_obj_t *ui_chip(lv_obj_t *parent, const char *text, ui_tone_t tone)
{
    lv_obj_t *c = bare(parent);
    lv_obj_set_size(c, LV_SIZE_CONTENT, 32);
    lv_obj_set_style_radius(c, 8, 0);
    lv_obj_set_style_pad_hor(c, 12, 0);
    lv_obj_set_style_bg_opa(c, LV_OPA_COVER, 0);
    lv_obj_t *l = ui_label(c, UI_FONT_LABEL, M.on_surface, text);
    lv_obj_center(l);
    ui_chip_set(c, text, tone);
    return c;
}

void ui_chip_set(lv_obj_t *chip, const char *text, ui_tone_t tone)
{
    lv_obj_t *l = lv_obj_get_child(chip, 0);
    lv_obj_set_style_bg_color(chip, ui_tone_bg(tone), 0);
    lv_obj_set_style_text_color(l, ui_tone_fg(tone), 0);
    if (strcmp(lv_label_get_text(l), text)) lv_label_set_text(l, text);
}

lv_obj_t *ui_progress(lv_obj_t *parent)
{
    lv_obj_t *b = lv_bar_create(parent);
    lv_obj_remove_style_all(b);
    lv_obj_set_size(b, LV_PCT(100), 8);
    lv_obj_set_style_radius(b, 4, LV_PART_MAIN);
    lv_obj_set_style_bg_color(b, M.secondary_container, LV_PART_MAIN);
    lv_obj_set_style_bg_opa(b, LV_OPA_COVER, LV_PART_MAIN);
    lv_obj_set_style_radius(b, 4, LV_PART_INDICATOR);
    lv_obj_set_style_bg_color(b, M.primary, LV_PART_INDICATOR);
    lv_obj_set_style_bg_opa(b, LV_OPA_COVER, LV_PART_INDICATOR);
    lv_bar_set_range(b, 0, 1000);
    return b;
}

lv_obj_t *ui_icon_circle(lv_obj_t *parent, const char *symbol, ui_tone_t tone)
{
    lv_obj_t *c = bare(parent);
    lv_obj_set_size(c, 64, 64);
    lv_obj_set_style_radius(c, UI_RADIUS_FULL, 0);
    lv_obj_set_style_bg_opa(c, LV_OPA_COVER, 0);
    lv_obj_set_style_bg_color(c, ui_tone_bg(tone), 0);
    lv_obj_t *l = ui_label(c, UI_FONT_ICON, ui_tone_fg(tone), symbol);
    lv_obj_center(l);
    return c;
}

/* ---- status glyphs ---------------------------------------------------------------- */
lv_obj_t *ui_signal(lv_obj_t *parent, int height)
{
    lv_obj_t *s = bare(parent);
    int bw = height / 4 + 1;
    lv_obj_set_size(s, 4 * bw + 3 * 2, height);
    for (int i = 0; i < 4; i++) {
        lv_obj_t *b = bare(s);
        int h = height * (i + 1) / 4;
        lv_obj_set_size(b, bw, h);
        lv_obj_set_pos(b, i * (bw + 2), height - h);
        lv_obj_set_style_radius(b, 1, 0);
        lv_obj_set_style_bg_opa(b, LV_OPA_COVER, 0);
    }
    ui_signal_set(s, 0, false, M.on_surface);
    return s;
}

void ui_signal_set(lv_obj_t *sig, int rssi, bool connected, lv_color_t color)
{
    int bars = !connected ? 0 : rssi >= -55 ? 4 : rssi >= -67 ? 3 : rssi >= -75 ? 2 : 1;
    for (int i = 0; i < 4; i++) {
        lv_obj_t *b = lv_obj_get_child(sig, i);
        lv_obj_set_style_bg_color(b, color, 0);
        lv_obj_set_style_bg_opa(b, i < bars ? LV_OPA_COVER : LV_OPA_30, 0);
    }
}

lv_obj_t *ui_battery(lv_obj_t *parent)
{
    lv_obj_t *wrap = bare(parent);
    lv_obj_set_size(wrap, 30, 16);
    lv_obj_t *body = bare(wrap);
    lv_obj_set_size(body, 27, 16);
    lv_obj_set_style_border_width(body, 2, 0);
    lv_obj_set_style_radius(body, 4, 0);
    lv_obj_set_style_pad_all(body, 2, 0);
    lv_obj_t *fill = bare(body);
    lv_obj_set_height(fill, LV_PCT(100));
    lv_obj_set_style_radius(fill, 1, 0);
    lv_obj_set_style_bg_opa(fill, LV_OPA_COVER, 0);
    lv_obj_t *nub = bare(wrap);
    lv_obj_set_size(nub, 3, 6);
    lv_obj_set_pos(nub, 27, 5);
    lv_obj_set_style_bg_opa(nub, LV_OPA_COVER, 0);
    lv_obj_set_style_radius(nub, 1, 0);
    ui_battery_set(wrap, -1, true);
    return wrap;
}

void ui_battery_set(lv_obj_t *bat, int pct, bool present)
{
    lv_obj_t *body = lv_obj_get_child(bat, 0), *nub = lv_obj_get_child(bat, 1), *fill = lv_obj_get_child(body, 0);
    lv_color_t c = !present ? M.on_surface_variant : (pct >= 0 && pct < 15) ? M.error : M.on_surface;
    lv_obj_set_style_border_color(body, c, 0);
    lv_obj_set_style_bg_color(nub, c, 0);
    lv_obj_set_style_bg_color(fill, c, 0);
    if (pct < 0 || !present) { lv_obj_add_flag(fill, LV_OBJ_FLAG_HIDDEN); return; }
    lv_obj_remove_flag(fill, LV_OBJ_FLAG_HIDDEN);
    lv_obj_set_width(fill, 19 * (pct > 100 ? 100 : pct) / 100 + 1);
}

/* ---- snackbar ---------------------------------------------------------------------- */
static lv_obj_t *snack;
static lv_timer_t *snack_timer;

static void snack_hide(lv_timer_t *t)
{
    (void)t;
    if (snack) { lv_obj_delete(snack); snack = NULL; }
    snack_timer = NULL;
}

void ui_snackbar(const char *fmt, ...)
{
    char msg[200];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(msg, sizeof msg, fmt, ap);
    va_end(ap);
    if (!snack) {
        snack = bare(lv_layer_top());
        /* above the navigation bar in portrait; in landscape at the bottom of the
         * content area, right of the rail, no wider than a comfortable line */
        int w = UI_CONTENT_W - 2 * 16;
        if (w > 560) w = 560;
        lv_obj_set_size(snack, w, LV_SIZE_CONTENT);
        lv_obj_align(snack, LV_ALIGN_BOTTOM_MID, UI_CONTENT_X / 2, -(ui_landscape ? 12 : UI_NAVBAR_H + 12));
        lv_obj_set_style_bg_color(snack, M.inverse_surface, 0);
        lv_obj_set_style_bg_opa(snack, LV_OPA_COVER, 0);
        lv_obj_set_style_radius(snack, 6, 0);
        lv_obj_set_style_pad_hor(snack, 18, 0);
        lv_obj_set_style_pad_ver(snack, 16, 0);
        lv_obj_set_style_shadow_width(snack, 18, 0);
        lv_obj_set_style_shadow_opa(snack, LV_OPA_30, 0);
        lv_obj_remove_flag(snack, LV_OBJ_FLAG_CLICKABLE);
        wrap_label(snack, UI_FONT_BODY, M.inverse_on_surface, "");
        if (ui_editor_is_open()) lv_obj_align(snack, LV_ALIGN_TOP_MID, 0, UI_STATUS_H + UI_APPBAR_H + 8);
    }
    lv_label_set_text(lv_obj_get_child(snack, 0), msg);
    if (snack_timer) lv_timer_reset(snack_timer);
    else { snack_timer = lv_timer_create(snack_hide, 3500, NULL); lv_timer_set_repeat_count(snack_timer, 1); }
}

/* ---- dialogs --------------------------------------------------------------------- */
typedef struct {
    void (*on_confirm)(void *ud);
    void *ud;
    void (*on_pick)(int, void *);
    int picked;
} dlg_ctx_t;

static lv_obj_t *scrim_create(void)
{
    lv_obj_t *s = bare(lv_layer_top());
    lv_obj_set_size(s, LV_PCT(100), LV_PCT(100));
    lv_obj_set_style_bg_color(s, M.scrim, 0);
    lv_obj_set_style_bg_opa(s, LV_OPA_50, 0);
    lv_obj_add_flag(s, LV_OBJ_FLAG_CLICKABLE);   /* swallow taps behind the dialog */
    return s;
}

static void dlg_free(lv_event_t *e) { lv_free(lv_event_get_user_data(e)); }

static lv_obj_t *dialog_box(lv_obj_t *scrim, const char *title)
{
    lv_obj_t *d = ui_column(scrim, 16);
    lv_obj_set_width(d, UI_W - 2 * 40 > 560 ? 560 : UI_W - 2 * 40);   /* M3: 280..560 dp */
    lv_obj_set_style_bg_color(d, M.surface_container_high, 0);
    lv_obj_set_style_bg_opa(d, LV_OPA_COVER, 0);
    lv_obj_set_style_radius(d, 28, 0);
    lv_obj_set_style_pad_all(d, 24, 0);
    lv_obj_center(d);
    wrap_label(d, UI_FONT_TITLE, M.on_surface, title);
    return d;
}

static lv_obj_t *dialog_actions(lv_obj_t *d)
{
    lv_obj_t *r = ui_row(d, 8);
    lv_obj_set_flex_align(r, LV_FLEX_ALIGN_END, LV_FLEX_ALIGN_CENTER, LV_FLEX_ALIGN_CENTER);
    return r;
}

static lv_obj_t *dialog_scrim_of(lv_obj_t *btn)
{
    lv_obj_t *o = btn;
    while (o && lv_obj_get_parent(o) != lv_layer_top()) o = lv_obj_get_parent(o);
    return o;
}

static void dlg_close_cb(lv_event_t *e) { lv_obj_delete_async(dialog_scrim_of(lv_event_get_target_obj(e))); }

static void dlg_confirm_cb(lv_event_t *e)
{
    dlg_ctx_t *c = lv_event_get_user_data(e);
    void (*cb)(void *) = c->on_confirm;
    void *ud = c->ud;
    lv_obj_delete_async(dialog_scrim_of(lv_event_get_target_obj(e)));
    if (cb) cb(ud);
}

void ui_dialog(const char *title, const char *body, const char *confirm, bool destructive, void (*on_confirm)(void *ud), void *ud)
{
    lv_obj_t *s = scrim_create();
    dlg_ctx_t *c = lv_malloc_zeroed(sizeof *c);
    c->on_confirm = on_confirm;
    c->ud = ud;
    lv_obj_add_event_cb(s, dlg_free, LV_EVENT_DELETE, c);
    lv_obj_t *d = dialog_box(s, title);
    if (body) wrap_label(d, UI_FONT_BODY, M.on_surface_variant, body);
    lv_obj_t *a = dialog_actions(d);
    if (confirm) {
        ui_button(a, BTN_TEXT, NULL, "Cancel", dlg_close_cb, NULL);
        lv_obj_t *b = ui_button(a, BTN_TEXT, NULL, confirm, dlg_confirm_cb, c);
        if (destructive) lv_obj_set_style_text_color(b, M.error, 0);
    } else {
        ui_button(a, BTN_TEXT, NULL, "OK", dlg_close_cb, NULL);
    }
}

static void choose_cb(lv_event_t *e)
{
    dlg_ctx_t *c = lv_event_get_user_data(e);
    lv_obj_t *item = lv_event_get_current_target_obj(e);
    int idx = (int)lv_obj_get_index(item);
    void (*fn)(int, void *) = c->on_pick;
    void *ud = c->ud;
    lv_obj_delete_async(dialog_scrim_of(item));
    if (fn) fn(idx, ud);
}

void ui_choose(const char *title, const char *const *options, int n, int selected, void (*on_pick)(int, void *), void *ud)
{
    lv_obj_t *s = scrim_create();
    dlg_ctx_t *c = lv_malloc_zeroed(sizeof *c);
    c->on_pick = on_pick;
    c->ud = ud;
    lv_obj_add_event_cb(s, dlg_free, LV_EVENT_DELETE, c);
    lv_obj_t *d = dialog_box(s, title);
    lv_obj_t *list = ui_column(d, 0);
    /* seven timeouts do not fit a landscape screen: the list scrolls inside the dialog */
    lv_obj_set_style_max_height(list, UI_H - 2 * 24 - 56 - 2 * 40 - 56, 0);
    lv_obj_add_flag(list, LV_OBJ_FLAG_SCROLLABLE);
    lv_obj_set_scroll_dir(list, LV_DIR_VER);
    for (int i = 0; i < n; i++) {
        lv_obj_t *r = ui_row(list, 16);
        lv_obj_set_height(r, 56);
        lv_obj_add_flag(r, LV_OBJ_FLAG_CLICKABLE);
        lv_obj_set_style_bg_color(r, M.on_surface, LV_STATE_PRESSED);
        lv_obj_set_style_bg_opa(r, LV_OPA_10, LV_STATE_PRESSED);
        lv_obj_set_style_radius(r, 12, 0);
        /* radio: outlined circle, filled dot when selected */
        lv_obj_t *radio = bare(r);
        lv_obj_set_size(radio, 22, 22);
        lv_obj_set_style_radius(radio, UI_RADIUS_FULL, 0);
        lv_obj_set_style_border_width(radio, 2, 0);
        lv_obj_set_style_border_color(radio, i == selected ? M.primary : M.on_surface_variant, 0);
        if (i == selected) {
            lv_obj_t *dot = bare(radio);
            lv_obj_set_size(dot, 10, 10);
            lv_obj_set_style_radius(dot, UI_RADIUS_FULL, 0);
            lv_obj_set_style_bg_color(dot, M.primary, 0);
            lv_obj_set_style_bg_opa(dot, LV_OPA_COVER, 0);
            lv_obj_center(dot);
        }
        ui_label(r, UI_FONT_BODY, M.on_surface, options[i]);
        lv_obj_add_event_cb(r, choose_cb, LV_EVENT_CLICKED, c);
    }
    ui_button(dialog_actions(d), BTN_TEXT, NULL, "Cancel", dlg_close_cb, NULL);
}

/* ---- editor + keyboard -------------------------------------------------------------
 * A full-screen dialog: app bar (close, title, Save), one outlined text field,
 * supporting/error text, and the keyboard docked at the bottom. The field never
 * hides behind the keyboard because nothing scrolls: in landscape the keyboard is
 * shorter and no wider than it needs to be, and the field is a single line. */
#define KB_H      (ui_landscape ? 232 : 330)
#define KB_W      (ui_landscape ? 640 : UI_W)
#define KB_NUM_W  (ui_landscape ? 360 : UI_W)

static const char *const kb_lower[] = {
    "q", "w", "e", "r", "t", "y", "u", "i", "o", "p", "\n",
    " ", "a", "s", "d", "f", "g", "h", "j", "k", "l", " ", "\n",
    ICON_SHIFT, "z", "x", "c", "v", "b", "n", "m", ICON_BACKSPACE, "\n",
    "?123", ",", "space", ".", ICON_CHECK, ""};
static const char *const kb_upper[] = {
    "Q", "W", "E", "R", "T", "Y", "U", "I", "O", "P", "\n",
    " ", "A", "S", "D", "F", "G", "H", "J", "K", "L", " ", "\n",
    ICON_SHIFT, "Z", "X", "C", "V", "B", "N", "M", ICON_BACKSPACE, "\n",
    "?123", ",", "space", ".", ICON_CHECK, ""};
/* Two symbol pages cover every printable ASCII character (passwords). */
static const char *const kb_sym1[] = {
    "1", "2", "3", "4", "5", "6", "7", "8", "9", "0", "\n",
    "@", "#", "$", "_", "&", "-", "+", "(", ")", "/", "\n",
    "=\\<", "*", "\"", "'", ":", ";", "!", "?", ICON_BACKSPACE, "\n",
    "abc", ",", "space", ".", ICON_CHECK, ""};
static const char *const kb_sym2[] = {
    "~", "`", "|", "^", "%", "=", "{", "}", "[", "]", "\n",
    " ", "\\", "<", ">", "@", "#", "$", "&", " ", "\n",
    "?123", "*", "-", "+", "/", "!", "?", ICON_BACKSPACE, "\n",
    "abc", ",", "space", ".", ICON_CHECK, ""};
static const char *const kb_num[] = {
    "1", "2", "3", "\n", "4", "5", "6", "\n", "7", "8", "9", "\n", ICON_BACKSPACE, "0", ICON_CHECK, ""};

#define K_W1 (LV_BUTTONMATRIX_CTRL_WIDTH_2 | LV_BUTTONMATRIX_CTRL_NO_REPEAT)
#define K_SP (LV_BUTTONMATRIX_CTRL_WIDTH_1 | LV_BUTTONMATRIX_CTRL_HIDDEN | LV_BUTTONMATRIX_CTRL_DISABLED)
#define K_FN (LV_BUTTONMATRIX_CTRL_WIDTH_3 | LV_BUTTONMATRIX_CTRL_NO_REPEAT | LV_BUTTONMATRIX_CTRL_CHECKED)
static const lv_buttonmatrix_ctrl_t kb_ctrl_text[] = {
    K_W1, K_W1, K_W1, K_W1, K_W1, K_W1, K_W1, K_W1, K_W1, K_W1,
    K_SP, K_W1, K_W1, K_W1, K_W1, K_W1, K_W1, K_W1, K_W1, K_W1, K_SP,
    K_FN, K_W1, K_W1, K_W1, K_W1, K_W1, K_W1, K_W1, LV_BUTTONMATRIX_CTRL_WIDTH_3 | LV_BUTTONMATRIX_CTRL_CHECKED,
    K_FN, K_W1, LV_BUTTONMATRIX_CTRL_WIDTH_10 | LV_BUTTONMATRIX_CTRL_NO_REPEAT, K_W1, K_FN};
static const lv_buttonmatrix_ctrl_t kb_ctrl_sym1[] = {
    K_W1, K_W1, K_W1, K_W1, K_W1, K_W1, K_W1, K_W1, K_W1, K_W1,
    K_W1, K_W1, K_W1, K_W1, K_W1, K_W1, K_W1, K_W1, K_W1, K_W1,
    K_FN, K_W1, K_W1, K_W1, K_W1, K_W1, K_W1, K_W1, LV_BUTTONMATRIX_CTRL_WIDTH_3 | LV_BUTTONMATRIX_CTRL_CHECKED,
    K_FN, K_W1, LV_BUTTONMATRIX_CTRL_WIDTH_10 | LV_BUTTONMATRIX_CTRL_NO_REPEAT, K_W1, K_FN};
static const lv_buttonmatrix_ctrl_t kb_ctrl_sym2[] = {
    K_W1, K_W1, K_W1, K_W1, K_W1, K_W1, K_W1, K_W1, K_W1, K_W1,
    LV_BUTTONMATRIX_CTRL_WIDTH_3 | K_SP, K_W1, K_W1, K_W1, K_W1, K_W1, K_W1, K_W1, LV_BUTTONMATRIX_CTRL_WIDTH_3 | K_SP,
    K_FN, K_W1, K_W1, K_W1, K_W1, K_W1, K_W1, LV_BUTTONMATRIX_CTRL_WIDTH_3 | LV_BUTTONMATRIX_CTRL_CHECKED,
    K_FN, K_W1, LV_BUTTONMATRIX_CTRL_WIDTH_10 | LV_BUTTONMATRIX_CTRL_NO_REPEAT, K_W1, K_FN};
static const lv_buttonmatrix_ctrl_t kb_ctrl_num[] = {
    K_W1, K_W1, K_W1, K_W1, K_W1, K_W1, K_W1, K_W1, K_W1,
    LV_BUTTONMATRIX_CTRL_WIDTH_2 | LV_BUTTONMATRIX_CTRL_CHECKED, K_W1, K_FN};

typedef enum { KB_LOWER, KB_UPPER, KB_SYM1, KB_SYM2, KB_NUM } kb_page_t;

static struct {
    lv_obj_t *root, *ta, *kb, *msg, *save, *eye;
    ui_editor_t spec;
    char *initial, *title, *label, *help, *placeholder;   /* copies: spec strings may be page-owned */
    kb_page_t page;
    bool shift_once, busy, error_shown;
} ed;

static void kb_set_page(kb_page_t p)
{
    ed.page = p;
    switch (p) {
    case KB_LOWER: lv_buttonmatrix_set_map(ed.kb, kb_lower); lv_buttonmatrix_set_ctrl_map(ed.kb, kb_ctrl_text); break;
    case KB_UPPER: lv_buttonmatrix_set_map(ed.kb, kb_upper); lv_buttonmatrix_set_ctrl_map(ed.kb, kb_ctrl_text); break;
    case KB_SYM1: lv_buttonmatrix_set_map(ed.kb, kb_sym1); lv_buttonmatrix_set_ctrl_map(ed.kb, kb_ctrl_sym1); break;
    case KB_SYM2: lv_buttonmatrix_set_map(ed.kb, kb_sym2); lv_buttonmatrix_set_ctrl_map(ed.kb, kb_ctrl_sym2); break;
    case KB_NUM: lv_buttonmatrix_set_map(ed.kb, kb_num); lv_buttonmatrix_set_ctrl_map(ed.kb, kb_ctrl_num); break;
    }
}

static void ed_message(const char *text, bool error)
{
    lv_label_set_text(ed.msg, text ? text : "");
    lv_obj_set_style_text_color(ed.msg, error ? M.error : M.on_surface_variant, 0);
    lv_obj_set_style_border_color(ed.ta, error ? M.error : M.primary, LV_STATE_FOCUSED);
    ed.error_shown = error;
}

static void ed_close(void)
{
    if (!ed.root) return;
    lv_obj_delete(ed.root);
    lv_free(ed.initial); lv_free(ed.title); lv_free(ed.label); lv_free(ed.help); lv_free(ed.placeholder);
    memset(&ed, 0, sizeof ed);
}

static void ed_save(void)
{
    if (ed.busy) return;
    const char *v = lv_textarea_get_text(ed.ta);
    if (!ed.spec.allow_empty && !v[0]) { ed_message("Required", true); return; }
    ed.busy = true;
    lv_obj_add_state(ed.save, LV_STATE_DISABLED);
    ed_message("Saving…", false);
    if (ed.spec.save) ed.spec.save(v, ed.spec.ud);
    else ui_editor_done(NULL);
}

void ui_editor_done(const char *error)
{
    if (!ed.root) return;
    if (!error) { ed_close(); return; }
    ed.busy = false;
    lv_obj_remove_state(ed.save, LV_STATE_DISABLED);
    ed_message(error, true);
}

bool ui_editor_is_open(void) { return ed.root != NULL; }

static void kb_event(lv_event_t *e)
{
    uint32_t id = lv_buttonmatrix_get_selected_button(ed.kb);
    if (id == LV_BUTTONMATRIX_BUTTON_NONE) return;
    const char *t = lv_buttonmatrix_get_button_text(ed.kb, id);
    if (!t) return;
    (void)e;
    if (!strcmp(t, ICON_BACKSPACE)) { lv_textarea_delete_char(ed.ta); return; }
    if (!strcmp(t, ICON_CHECK)) { ed_save(); return; }
    if (!strcmp(t, ICON_SHIFT)) { kb_set_page(ed.page == KB_UPPER ? KB_LOWER : KB_UPPER); ed.shift_once = ed.page == KB_UPPER; return; }
    if (!strcmp(t, "?123")) { kb_set_page(KB_SYM1); return; }
    if (!strcmp(t, "=\\<")) { kb_set_page(KB_SYM2); return; }
    if (!strcmp(t, "abc")) { kb_set_page(KB_LOWER); return; }
    if (!strcmp(t, "space")) { lv_textarea_add_char(ed.ta, ' '); return; }
    lv_textarea_add_text(ed.ta, t);
    if (ed.shift_once && ed.page == KB_UPPER) { ed.shift_once = false; kb_set_page(KB_LOWER); }
    if (ed.error_shown) ed_message(ed.help, false);   /* typing clears a validation error */
}

/* From the close button's own click: the root (its ancestor) is deleted once the
 * event has finished dispatching, as the dialogs do. */
static void ed_close_cb(lv_event_t *e)
{
    (void)e;
    if (!ed.root) return;
    lv_obj_t *root = ed.root;
    lv_free(ed.initial); lv_free(ed.title); lv_free(ed.label); lv_free(ed.help); lv_free(ed.placeholder);
    memset(&ed, 0, sizeof ed);
    lv_obj_delete_async(root);
}
static void ed_save_cb(lv_event_t *e) { (void)e; ed_save(); }

static void ed_eye_cb(lv_event_t *e)
{
    (void)e;
    bool hidden = lv_textarea_get_password_mode(ed.ta);
    lv_textarea_set_password_mode(ed.ta, !hidden);
    lv_label_set_text(lv_obj_get_child(ed.eye, 0), hidden ? ICON_VISIBILITY_OFF : ICON_VISIBILITY);
}

static char *dup(const char *s) { return s ? lv_strdup(s) : NULL; }

void ui_editor_open(const ui_editor_t *spec)
{
    ed_close();
    ed.spec = *spec;
    ed.initial = dup(spec->initial); ed.title = dup(spec->title); ed.label = dup(spec->label);
    ed.help = dup(spec->help); ed.placeholder = dup(spec->placeholder);
    ed.spec.initial = ed.initial; ed.spec.title = ed.title; ed.spec.label = ed.label;
    ed.spec.help = ed.help; ed.spec.placeholder = ed.placeholder;

    ed.root = bare(lv_layer_top());
    lv_obj_set_size(ed.root, UI_W, UI_H - UI_STATUS_H);
    lv_obj_set_pos(ed.root, 0, UI_STATUS_H);
    lv_obj_set_style_bg_color(ed.root, M.surface, 0);
    lv_obj_set_style_bg_opa(ed.root, LV_OPA_COVER, 0);
    lv_obj_add_flag(ed.root, LV_OBJ_FLAG_CLICKABLE);

    /* app bar: close · title · Save */
    lv_obj_t *bar = ui_row(ed.root, 4);
    lv_obj_set_height(bar, UI_APPBAR_H);
    lv_obj_set_style_pad_hor(bar, 8, 0);
    lv_obj_t *x = ui_button(bar, BTN_TEXT, ICON_CLOSE, NULL, ed_close_cb, NULL);
    lv_obj_set_style_text_font(lv_obj_get_child(x, 0), UI_FONT_ICON, 0);
    lv_obj_set_style_text_color(x, M.on_surface, 0);
    lv_obj_t *title = ui_label(bar, UI_FONT_TITLE, M.on_surface, ed.title);
    lv_obj_set_flex_grow(title, 1);
    lv_label_set_long_mode(title, LV_LABEL_LONG_MODE_DOTS);
    ed.save = ui_button(bar, BTN_TEXT, NULL, "Save", ed_save_cb, NULL);

    lv_obj_t *body = ui_column(ed.root, 8);
    lv_obj_set_style_pad_hor(body, UI_PAD, 0);
    lv_obj_set_pos(body, 0, UI_APPBAR_H + 8);

    if (ed.label) ui_label(body, UI_FONT_LABEL, M.primary, ed.label);
    lv_obj_t *fieldrow = ui_row(body, 8);
    ed.ta = lv_textarea_create(fieldrow);
    bool multiline = spec->mode == EDIT_TEXT && spec->max_len > 64 && !ui_landscape;
    lv_textarea_set_one_line(ed.ta, !multiline);
    lv_obj_set_flex_grow(ed.ta, 1);
    lv_obj_set_height(ed.ta, multiline ? 120 : ui_landscape ? 52 : 60);
    lv_obj_set_style_text_font(ed.ta, UI_FONT_TITLE, 0);
    lv_obj_set_style_text_color(ed.ta, M.on_surface, 0);
    lv_obj_set_style_bg_color(ed.ta, M.surface, 0);
    lv_obj_set_style_border_color(ed.ta, M.outline, 0);
    lv_obj_set_style_border_width(ed.ta, 1, 0);
    lv_obj_set_style_border_color(ed.ta, M.primary, LV_STATE_FOCUSED);
    lv_obj_set_style_border_width(ed.ta, 2, LV_STATE_FOCUSED);
    lv_obj_set_style_radius(ed.ta, 6, 0);
    lv_obj_set_style_pad_hor(ed.ta, 14, 0);
    lv_obj_set_style_pad_ver(ed.ta, 14, 0);
    lv_obj_set_style_bg_color(ed.ta, M.primary, LV_PART_CURSOR | LV_STATE_FOCUSED);
    lv_obj_set_style_border_color(ed.ta, M.primary, LV_PART_CURSOR | LV_STATE_FOCUSED);
    lv_obj_set_style_text_color(ed.ta, M.on_surface_variant, LV_PART_TEXTAREA_PLACEHOLDER);
    if (spec->max_len) lv_textarea_set_max_length(ed.ta, spec->max_len);
    if (ed.placeholder) lv_textarea_set_placeholder_text(ed.ta, ed.placeholder);
    if (spec->mode == EDIT_NUMBER) lv_textarea_set_accepted_chars(ed.ta, "0123456789");
    if (spec->mode == EDIT_PASSWORD) {
        lv_textarea_set_password_mode(ed.ta, true);
        lv_textarea_set_password_show_time(ed.ta, 1200);
        ed.eye = ui_button(fieldrow, BTN_TEXT, ICON_VISIBILITY, NULL, ed_eye_cb, NULL);
        lv_obj_set_style_text_font(lv_obj_get_child(ed.eye, 0), UI_FONT_ICON, 0);
        lv_obj_set_style_text_color(ed.eye, M.on_surface_variant, 0);
    }
    lv_textarea_set_text(ed.ta, ed.initial ? ed.initial : "");
    lv_obj_add_state(ed.ta, LV_STATE_FOCUSED);   /* cursor visible; there is no other focus target */
    lv_obj_remove_flag(ed.ta, LV_OBJ_FLAG_CLICK_FOCUSABLE);

    ed.msg = wrap_label(body, UI_FONT_SMALL, M.on_surface_variant, ed.help);

    /* keyboard */
    ed.kb = lv_buttonmatrix_create(ed.root);
    lv_obj_set_size(ed.kb, spec->mode == EDIT_NUMBER ? KB_NUM_W : KB_W, KB_H);
    lv_obj_align(ed.kb, LV_ALIGN_BOTTOM_MID, 0, 0);
    lv_obj_set_style_bg_color(ed.kb, M.surface_container_high, 0);
    lv_obj_set_style_bg_opa(ed.kb, LV_OPA_COVER, 0);
    lv_obj_set_style_radius(ed.kb, 0, 0);
    lv_obj_set_style_border_width(ed.kb, 0, 0);
    lv_obj_set_style_pad_all(ed.kb, 6, 0);
    lv_obj_set_style_pad_row(ed.kb, 8, 0);
    lv_obj_set_style_pad_column(ed.kb, 5, 0);
    lv_obj_set_style_bg_color(ed.kb, M.surface_container_highest, LV_PART_ITEMS);
    lv_obj_set_style_bg_color(ed.kb, M.surface_container_lowest, LV_PART_ITEMS);
    lv_obj_set_style_bg_color(ed.kb, ui_dark ? lv_color_hex(0x30353D) : lv_color_hex(0xFFFFFF), LV_PART_ITEMS);
    lv_obj_set_style_bg_opa(ed.kb, LV_OPA_COVER, LV_PART_ITEMS);
    lv_obj_set_style_text_color(ed.kb, M.on_surface, LV_PART_ITEMS);
    lv_obj_set_style_text_font(ed.kb, UI_FONT_TITLE, LV_PART_ITEMS);
    lv_obj_set_style_radius(ed.kb, 8, LV_PART_ITEMS);
    lv_obj_set_style_border_width(ed.kb, 0, LV_PART_ITEMS);
    lv_obj_set_style_shadow_width(ed.kb, 0, LV_PART_ITEMS);
    /* function keys (checked ctrl) in the secondary container; pressed in primary container */
    lv_obj_set_style_bg_color(ed.kb, M.secondary_container, LV_PART_ITEMS | LV_STATE_CHECKED);
    lv_obj_set_style_text_color(ed.kb, M.on_secondary_container, LV_PART_ITEMS | LV_STATE_CHECKED);
    lv_obj_set_style_text_font(ed.kb, UI_FONT_BODY, LV_PART_ITEMS | LV_STATE_CHECKED);
    lv_obj_set_style_bg_color(ed.kb, M.primary_container, LV_PART_ITEMS | LV_STATE_PRESSED);
    lv_obj_set_style_text_color(ed.kb, M.on_primary_container, LV_PART_ITEMS | LV_STATE_PRESSED);
    lv_obj_add_event_cb(ed.kb, kb_event, LV_EVENT_VALUE_CHANGED, NULL);
    kb_set_page(spec->mode == EDIT_NUMBER ? KB_NUM : KB_LOWER);
}

/* ---- formatting ------------------------------------------------------------------- */
void ui_fmt_bytes(char *out, size_t cap, double n)
{
    static const char *u[] = {"B", "KB", "MB", "GB", "TB"};
    int i = 0;
    while (n >= 1000 && i < 4) { n /= 1000; i++; }
    if (i == 0) snprintf(out, cap, "%.0f B", n);
    else snprintf(out, cap, n < 10 ? "%.2f %s" : n < 100 ? "%.1f %s" : "%.0f %s", n, u[i]);
}

void ui_fmt_duration(char *out, size_t cap, double s)
{
    long t = (long)(s + 0.5);
    if (t < 60) snprintf(out, cap, "%lds", t);
    else if (t < 3600) snprintf(out, cap, "%ldm %02lds", t / 60, t % 60);
    else if (t < 86400) snprintf(out, cap, "%ldh %02ldm", t / 3600, t % 3600 / 60);
    else snprintf(out, cap, "%ldd %ldh", t / 86400, t % 86400 / 3600);
}

void ui_fmt_ago(char *out, size_t cap, double ts, double now)
{
    if (ts <= 0) { snprintf(out, cap, "never"); return; }
    if (now <= 0 || now < ts) { snprintf(out, cap, "clock not set"); return; }
    double d = now - ts;
    if (d < 60) { snprintf(out, cap, "just now"); return; }
    char dur[24];
    ui_fmt_duration(dur, sizeof dur, d);
    /* "3m 12s ago" reads noisy; keep the leading unit */
    char *sp = strchr(dur, ' ');
    if (sp) *sp = 0;
    snprintf(out, cap, "%s ago", dur);
}

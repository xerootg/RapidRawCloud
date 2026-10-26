/*
 * rrc_ui host simulator: runs the on-device UI against fake_dock.c on an RGB565
 * display in LVGL direct mode (as the firmware does), drives it with synthetic touches
 * through LVGL's pointer input, checks behaviour, and writes a PNG per screen.
 *
 *   rrc_ui_sim <outdir>            480x800 portrait (bottom navigation bar)
 *   rrc_ui_sim <outdir> landscape  800x480 landscape (navigation rail), as a panel
 *                                  that is landscape by nature at display_rotation 0
 *
 * The same scenario runs in both; the Orientation setting is exercised in each, which
 * turns the "panel" (the display's resolution) the way main/display.c does.
 *
 * Exit status is non-zero when a check fails; LVGL asserts abort (lv_conf.h), and the
 * build runs under ASan/UBSan.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <sys/stat.h>
#include "lvgl.h"
#include "src/widgets/buttonmatrix/lv_buttonmatrix_private.h"
#include "rrc_ui.h"
#include "ui_icons.h"
#include "fake_dock.h"
#include "png.h"

#define PIXELS (480 * 800)   /* both orientations: the buffers are the same size */

static int W = 480, H = 800;          /* the display's current logical size */
static bool native_landscape;         /* what the "panel" is at rotation 0 */
static lv_display_t *d;
static uint16_t fb[2][PIXELS], shown[PIXELS];
static uint32_t now_ms;
static struct { int x, y; bool pressed; } ptr;
static const char *outdir;
static int failures, shots;

#define CHECK(cond, ...) do { if (!(cond)) { failures++; fprintf(stderr, "FAIL %s:%d: ", __FILE__, __LINE__); fprintf(stderr, __VA_ARGS__); fputc('\n', stderr); } } while (0)

/* ---- display / input / host ------------------------------------------------------------ */
static uint32_t tick_cb(void) { return now_ms; }

static void flush_cb(lv_display_t *d, const lv_area_t *a, uint8_t *px)
{
    (void)a;
    if (lv_display_flush_is_last(d)) memcpy(shown, px, sizeof shown);
    lv_display_flush_ready(d);
}

static void read_cb(lv_indev_t *in, lv_indev_data_t *data)
{
    (void)in;
    data->point.x = ptr.x;
    data->point.y = ptr.y;
    data->state = ptr.pressed ? LV_INDEV_STATE_PRESSED : LV_INDEV_STATE_RELEASED;
}

typedef struct { bool used; uint32_t token, due; char method[8], path[48]; char *body; } job_t;
static job_t jobs[32];

static void host_request(const char *method, const char *path, const char *body, uint32_t token)
{
    for (int i = 0; i < 32; i++) {
        if (jobs[i].used) continue;
        jobs[i] = (job_t){.used = true, .token = token, .due = now_ms + 15};
        snprintf(jobs[i].method, sizeof jobs[i].method, "%s", method);
        snprintf(jobs[i].path, sizeof jobs[i].path, "%s", path);
        jobs[i].body = body ? strdup(body) : NULL;
        return;
    }
    CHECK(0, "request queue overflow (%s %s)", method, path);
}

static void host_backlight(int pct) { fake.backlight = pct; }

/* What the UI sees of main/display.c's display_rotation: the display reports the
 * other resolution for 90/270. The rotation itself (LVGL's lv_display_set_rotation
 * plus the rotated flush) is the firmware's; LVGL's direct render mode, which the
 * simulator uses, does not rotate, so here the "panel" simply turns. */
static void host_rotation(int deg)
{
    bool land = (deg == 90 || deg == 270) ? !native_landscape : native_landscape;
    lv_display_set_resolution(d, land ? 800 : 480, land ? 480 : 800);
    W = lv_display_get_horizontal_resolution(d);
    H = lv_display_get_vertical_resolution(d);
    lv_display_set_buffers(d, fb[0], fb[1], sizeof fb[0], LV_DISPLAY_RENDER_MODE_DIRECT);   /* stride follows the width */
    lv_obj_invalidate(lv_screen_active());
}

static void pump(void)
{
    for (int i = 0; i < 32; i++) {
        if (!jobs[i].used || jobs[i].due > now_ms) continue;
        job_t j = jobs[i];
        jobs[i].used = false;
        int st = 0;
        char *resp = fake_dock_handle(j.method, j.path, j.body, &st, now_ms);
        rrc_ui_complete(j.token, st, resp, resp ? strlen(resp) : 0);
        free(resp);
        free(j.body);
    }
}

static void run(uint32_t ms)
{
    for (uint32_t t = 0; t < ms; t += 5) {
        now_ms += 5;
        pump();
        lv_timer_handler();
    }
}

/* Every visible character must exist in the font that draws it: a missing glyph renders
 * as an empty box (or nothing) and no other check would notice. */
static uint32_t utf8_next(const char **p)
{
    const unsigned char *s = (const unsigned char *)*p;
    uint32_t c = *s++;
    int n = c >= 0xF0 ? 3 : c >= 0xE0 ? 2 : c >= 0xC0 ? 1 : 0;
    if (n) c &= 0x3F >> n;
    while (n-- && (*s & 0xC0) == 0x80) c = c << 6 | (*s++ & 0x3F);
    *p = (const char *)s;
    return c;
}

static bool has_glyph(const lv_font_t *f, uint32_t cp)
{
    lv_font_glyph_dsc_t g;
    return cp == '\n' || cp == '\r' || lv_font_get_glyph_dsc(f, &g, cp, 0);
}

static void check_text(const char *where, const char *t, const lv_font_t *f, const lv_font_t *alt)
{
    for (const char *p = t; *p;) {
        uint32_t cp = utf8_next(&p);
        if (!has_glyph(f, cp) && !(alt && has_glyph(alt, cp))) { CHECK(0, "%s: U+%04X missing from the font drawing \"%s\"", where, (unsigned)cp, t); return; }
    }
}

static void check_glyphs(lv_obj_t *o)
{
    if (lv_obj_has_flag(o, LV_OBJ_FLAG_HIDDEN)) return;
    if (lv_obj_check_type(o, &lv_label_class)) check_text("label", lv_label_get_text(o), lv_obj_get_style_text_font(o, LV_PART_MAIN), NULL);
    else if (lv_obj_check_type(o, &lv_textarea_class) && !lv_textarea_get_password_mode(o)) check_text("text field", lv_textarea_get_text(o), lv_obj_get_style_text_font(o, LV_PART_MAIN), NULL);
    else if (lv_obj_check_type(o, &lv_buttonmatrix_class)) {
        lv_buttonmatrix_t *bm = (lv_buttonmatrix_t *)o;
        /* keys draw in the items font; function keys (checked) in another */
        const lv_font_t *fn = lv_obj_get_style_text_font(o, LV_PART_ITEMS);
        lv_obj_add_state(o, LV_STATE_CHECKED);
        const lv_font_t *fc = lv_obj_get_style_text_font(o, LV_PART_ITEMS);
        lv_obj_remove_state(o, LV_STATE_CHECKED);
        for (uint32_t i = 0; i < bm->btn_cnt; i++) check_text("key", lv_buttonmatrix_get_button_text(o, i), fn, fc);
    }
    for (uint32_t i = 0; i < lv_obj_get_child_count(o); i++) check_glyphs(lv_obj_get_child(o, (int32_t)i));
}

static void shot(const char *name)
{
    run(50);
    check_glyphs(lv_screen_active());
    check_glyphs(lv_layer_top());
    char path[512];
    snprintf(path, sizeof path, "%s/%02d_%s.png", outdir, ++shots, name);
    CHECK(png_write_rgb565(path, shown, W, H) == 0, "cannot write %s", path);
    printf("shot %s\n", path);
}

/* ---- finding and touching things ----------------------------------------------------------- */
static lv_obj_t *find_in(lv_obj_t *o, const char *text, bool prefix)
{
    if (!lv_obj_is_visible(o) && lv_obj_has_flag(o, LV_OBJ_FLAG_HIDDEN)) return NULL;
    if (lv_obj_check_type(o, &lv_label_class)) {
        const char *t = lv_label_get_text(o);
        if (prefix ? !strncmp(t, text, strlen(text)) : !strcmp(t, text)) return o;
    }
    for (uint32_t i = 0; i < lv_obj_get_child_count(o); i++) {
        lv_obj_t *r = find_in(lv_obj_get_child(o, (int32_t)i), text, prefix);
        if (r) return r;
    }
    return NULL;
}

static bool hidden_chain(lv_obj_t *o)
{
    for (; o; o = lv_obj_get_parent(o)) if (lv_obj_has_flag(o, LV_OBJ_FLAG_HIDDEN)) return true;
    return false;
}

static lv_obj_t *find(const char *text)
{
    lv_obj_t *roots[] = {lv_layer_sys(), lv_layer_top(), lv_screen_active()};
    for (int i = 0; i < 3; i++) {
        lv_obj_t *r = find_in(roots[i], text, false);
        if (r && !hidden_chain(r)) return r;
    }
    return NULL;
}

static void tap_at(int x, int y)
{
    ptr.x = x; ptr.y = y; ptr.pressed = true;
    run(80);
    ptr.pressed = false;
    run(250);
}

static bool tap(const char *text)
{
    lv_obj_t *o = find(text);
    CHECK(o, "nothing labelled \"%s\" on screen", text);
    if (!o) return false;
    lv_obj_scroll_to_view_recursive(o, LV_ANIM_OFF);
    lv_obj_update_layout(o);
    lv_area_t a;
    lv_obj_get_coords(o, &a);
    tap_at((a.x1 + a.x2) / 2, (a.y1 + a.y2) / 2);
    return true;
}

/* Navigation destination i (0 Status, 1 Sync, 2 Wi-Fi, 3 Device): by position,
 * because the same words also title the top app bar. Portrait: the bar along the
 * bottom; landscape: the rail down the left, items of UI_RAIL_ITEM_H from the top
 * (ui_core.c build_navbar). */
static void nav(int i)
{
    if (W > H) tap_at(48, 40 + 8 + 48 + i * 96);
    else tap_at(W / 8 + i * W / 4, H - 48);
    run(200);
}

static lv_obj_t *find_class(lv_obj_t *o, const lv_obj_class_t *cls)
{
    if (lv_obj_check_type(o, cls) && !hidden_chain(o)) return o;
    for (uint32_t i = 0; i < lv_obj_get_child_count(o); i++) {
        lv_obj_t *r = find_class(lv_obj_get_child(o, (int32_t)i), cls);
        if (r) return r;
    }
    return NULL;
}

/* Presses one key of the on-screen keyboard by its label. */
static bool key(const char *label)
{
    lv_obj_t *kb = find_class(lv_layer_top(), &lv_buttonmatrix_class);
    CHECK(kb, "no keyboard on screen");
    if (!kb) return false;
    lv_buttonmatrix_t *bm = (lv_buttonmatrix_t *)kb;
    for (uint32_t i = 0; i < bm->btn_cnt; i++) {
        const char *t = lv_buttonmatrix_get_button_text(kb, i);
        if (!t || strcmp(t, label)) continue;
        lv_area_t a = bm->button_areas[i];
        tap_at(kb->coords.x1 + (a.x1 + a.x2) / 2, kb->coords.y1 + (a.y1 + a.y2) / 2);
        return true;
    }
    CHECK(0, "no key \"%s\"", label);
    return false;
}

static void type(const char *s)
{
    for (; *s; s++) {
        char k[2] = {*s, 0};
        if (*s >= 'A' && *s <= 'Z') { key(ICON_SHIFT); key(k); }
        else if (*s >= '0' && *s <= '9') { key("?123"); key(k); key("abc"); }
        else if (*s == ' ') key("space");
        else if (*s == '.' || *s == ',' || (*s >= 'a' && *s <= 'z')) key(k);
        else { key("?123"); if (!find_class(lv_layer_top(), &lv_buttonmatrix_class)) return; key(k); key("abc"); }
    }
}

static const char *textarea_text(void)
{
    lv_obj_t *ta = find_class(lv_layer_top(), &lv_textarea_class);
    return ta ? lv_textarea_get_text(ta) : NULL;
}

static void clear_field(void)
{
    const char *t = textarea_text();
    size_t n = t ? strlen(t) : 0;
    lv_obj_t *ta = find_class(lv_layer_top(), &lv_textarea_class);
    if (ta) lv_textarea_set_cursor_pos(ta, LV_TEXTAREA_CURSOR_LAST);
    for (size_t i = 0; i < n; i++) key(ICON_BACKSPACE);
}

static const char *cfg_s(const char *k) { const cJSON *v = cJSON_GetObjectItem(fake_config, k); return cJSON_IsString(v) ? v->valuestring : "?"; }
static double cfg_n(const char *k) { return cJSON_GetNumberValue(cJSON_GetObjectItem(fake_config, k)); }
static bool cfg_b(const char *k) { return cJSON_IsTrue(cJSON_GetObjectItem(fake_config, k)); }

static size_t lv_used(void)
{
    lv_mem_monitor_t m;
    lv_mem_monitor(&m);
    return m.total_size - m.free_size;
}

/* ---- scenarios ------------------------------------------------------------------------------ */
int main(int argc, char **argv)
{
    outdir = argc > 1 ? argv[1] : "shots";
    native_landscape = argc > 2 && !strcmp(argv[2], "landscape");
    if (native_landscape) { W = 800; H = 480; }
    mkdir(outdir, 0755);
    fake_dock_init();

    lv_init();
    lv_tick_set_cb(tick_cb);
    d = lv_display_create(W, H);
    lv_display_set_color_format(d, LV_COLOR_FORMAT_RGB565);
    lv_display_set_buffers(d, fb[0], fb[1], sizeof fb[0], LV_DISPLAY_RENDER_MODE_DIRECT);
    lv_display_set_flush_cb(d, flush_cb);
    lv_indev_t *in = lv_indev_create();
    lv_indev_set_type(in, LV_INDEV_TYPE_POINTER);
    lv_indev_set_read_cb(in, read_cb);
    static const rrc_ui_host_t host = {.request = host_request, .set_backlight = host_backlight, .set_rotation = host_rotation};
    rrc_ui_start(&host, d);

    /* Status */
    run(1500);
    CHECK(fake.backlight == 80, "backlight not applied from config (got %d)", fake.backlight);
    CHECK(find("Camera connected"), "idle headline");
    shot("status_idle");
    size_t baseline = lv_used();
    fake.phase = "uploading";
    run(1200);
    CHECK(find("20 of 40 files"), "upload progress headline");
    shot("status_uploading");

    /* Sync settings + the editor and keyboard */
    nav(1);
    run(300);
    CHECK(find("Include patterns"), "sync page");
    shot("sync");
    tap("Exclude patterns");
    CHECK(textarea_text() != NULL, "editor did not open");
    type("*_small.jpg");
    CHECK(textarea_text() && !strcmp(textarea_text(), "*_small.jpg"), "typed text is \"%s\"", textarea_text() ? textarea_text() : "(null)");
    shot("editor_keyboard");
    key("?123");
    shot("keyboard_symbols");
    key("=\\<");
    shot("keyboard_symbols_2");
    key("abc");
    tap("Save");
    run(300);
    CHECK(!textarea_text(), "editor still open after save");
    CHECK(!strcmp(cfg_s("exclude_globs"), "*_small.jpg"), "exclude_globs not saved (%s)", cfg_s("exclude_globs"));
    shot("sync_saved");

    /* validation error from the dock is shown in the field */
    tap("Library path template");
    clear_field();
    type("a/../b");
    tap("Save");
    run(300);
    CHECK(find("key_template: path segment '..' is not allowed"), "template validation error not shown");
    shot("editor_error");
    tap(ICON_CLOSE);
    run(200);

    /* Pairing */
    tap("Pair with cloud");
    run(400);
    tap("Start pairing");
    CHECK(textarea_text() && !strcmp(textarea_text(), "rrc.example.net"), "pairing URL prefilled from config");
    tap("Save");
    run(2600);
    CHECK(find("WDJB-MJHT"), "pairing code not shown");
    shot("pairing_code");
    tap("Cancel pairing");
    run(1200);
    tap(ICON_BACK);
    run(200);
    tap("Manual S3 settings");
    run(300);
    CHECK(find("Stored · tap to replace"), "secret shown as stored");
    shot("manual_s3");
    tap(ICON_BACK);

    /* Wi-Fi */
    nav(2);
    run(2600);
    CHECK(find("Connected · 192.168.4.37"), "association line");
    shot("wifi");
    lv_obj_t *nets = find("Networks in range");
    CHECK(nets, "scan list header");
    if (nets) { lv_obj_scroll_to_view_recursive(nets, LV_ANIM_OFF); run(100); }
    CHECK(find("FieldHotspot"), "scan results");
    shot("wifi_networks");
    tap("FieldHotspot");
    type("short");
    tap("Save");
    run(300);
    CHECK(find("wifi_password: WPA2 passphrases are 8–63 characters"), "short password rejected with the dock's message");
    shot("wifi_password_error");
    type("passphrase");
    tap("Save");
    run(300);
    CHECK(!strcmp(cfg_s("wifi_ssid"), "FieldHotspot"), "joined network saved (%s)", cfg_s("wifi_ssid"));
    shot("wifi_joining");
    tap("Forget network");
    run(200);
    shot("dialog_forget");
    tap("Cancel");
    run(200);
    CHECK(!strcmp(cfg_s("wifi_ssid"), "FieldHotspot"), "cancel must not forget");
    tap("Use Wi-Fi");
    run(2200);
    CHECK(!cfg_b("wifi_enabled"), "Wi-Fi switch did not save");
    shot("wifi_off");
    tap("Use Wi-Fi");
    run(1200);
    CHECK(cfg_b("wifi_enabled"), "Wi-Fi switch did not turn back on");

    /* Device */
    nav(3);
    run(400);
    shot("device");
    tap("Screen timeout");
    run(200);
    shot("dialog_timeout");
    tap("5 minutes");
    run(300);
    CHECK(cfg_n("display_timeout_s") == 300, "timeout not saved (%g)", cfg_n("display_timeout_s"));
    /* Orientation: turn the panel 90 degrees and back; the shell is rebuilt each time */
    tap("Orientation");
    run(200);
    shot("dialog_orientation");
    tap(native_landscape ? "Portrait, 90°" : "Landscape, 90°");
    run(600);
    CHECK(cfg_n("display_rotation") == 90, "rotation not saved (%g)", cfg_n("display_rotation"));
    CHECK((W > H) != native_landscape, "display not turned (now %dx%d)", W, H);
    CHECK(find("Orientation"), "Device page not rebuilt after the turn");
    shot("device_turned");
    nav(0);
    run(600);
    shot("status_turned");
    nav(3);
    run(300);
    tap("Orientation");
    run(200);
    tap(native_landscape ? "Landscape, 0°" : "Portrait, 0°");
    run(600);
    CHECK((W > H) == native_landscape, "display not turned back (now %dx%d)", W, H);
    tap("About this dock");
    run(300);
    shot("about");
    tap(ICON_BACK);
    tap("Log");
    run(2200);
    shot("log");
    tap(ICON_BACK);
    tap("Dark theme");
    run(600);
    CHECK(!cfg_b("display_dark"), "theme switch did not save");
    shot("device_light");
    nav(0);
    run(600);
    shot("status_light");
    nav(3);
    tap("Dark theme");
    run(600);

    /* Admin lock: set a password from the screen; it stays unlocked until sleep */
    tap("Admin password");
    type("hunter22");
    tap("Save");
    run(200);
    tap("Require a password");
    run(600);
    CHECK(cfg_b("admin_auth"), "admin_auth not saved");
    CHECK(find("Identity"), "the user who set the password stays unlocked");
    /* sleep → locked; the waking touch must not press anything */
    cJSON_ReplaceItemInObject(fake_config, "display_timeout_s", cJSON_CreateNumber(15));
    nav(0);
    run(33000);   /* ≤ 15 s until the periodic config refresh sees the new timeout, then 15 s idle */
    CHECK(rrc_ui_asleep(), "screen did not sleep after the timeout");
    CHECK(fake.backlight == 0, "backlight still on while asleep (%d)", fake.backlight);
    nav(3);      /* lands on the shield: wakes only */
    CHECK(!rrc_ui_asleep(), "touch did not wake the screen");
    CHECK(find("Uploaded by this dock"), "the waking touch navigated away");
    nav(3);
    run(300);
    CHECK(find("Settings are locked"), "settings not locked after sleep");
    shot("locked");
    tap("Unlock");
    type("nope");
    tap("Save");
    run(200);
    CHECK(find("Wrong password"), "wrong password accepted");
    shot("unlock_wrong");
    clear_field();
    type("hunter22");
    tap("Save");
    run(400);
    CHECK(find("Identity"), "correct password did not unlock");
    rrc_ui_user_key();
    CHECK(rrc_ui_asleep(), "user key did not sleep the screen");
    rrc_ui_user_key();
    CHECK(!rrc_ui_asleep(), "user key did not wake the screen");

    /* Leak check: walk every tab and sub-page a few times, come back to Status. */
    cJSON_ReplaceItemInObject(fake_config, "admin_auth", cJSON_CreateFalse());
    cJSON_ReplaceItemInObject(fake_config, "display_timeout_s", cJSON_CreateNumber(0));
    nav(0);
    run(16000);   /* the periodic config refresh picks up the changes made behind the UI's back */
    for (int i = 0; i < 4; i++) {
        nav(1); run(300); tap("Pair with cloud"); run(1200); tap(ICON_BACK);
        nav(2); run(2200); nav(3); run(300); tap("Log"); run(2100); tap(ICON_BACK);
        nav(0); run(1100);
    }
    run(4000);   /* let the snackbar timer expire */
    size_t after = lv_used();
    printf("lvgl heap in use: %zu bytes at first render, %zu after %d page cycles\n", baseline, after, 4);
    CHECK(after < baseline + 4096, "LVGL memory grew by %zd bytes across page cycles", (ssize_t)(after - baseline));

    printf("%d screenshots, %d failures\n", shots, failures);
    return failures ? 1 : 0;
}

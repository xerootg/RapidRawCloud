/*
 * Device — display, identity, security, Bluetooth, radio co-processor and
 * diagnostics (the web page's "Device" section plus the screen's own settings),
 * and the Log and About sub-pages.
 */
#include "ui_priv.h"
#include <stdio.h>
#include <string.h>

static const ui_setting_t s_dark = {.symbol = ICON_DARK_MODE, .headline = "Dark theme", .key = "display_dark", .kind = BIND_SWITCH};
static const ui_setting_t s_name = {.symbol = ICON_BADGE, .headline = "Device name", .key = "device_name", .kind = BIND_TEXT, .max_len = 47,
                                    .help = "How this dock appears in the library's device list."};
static const ui_setting_t s_host = {.symbol = ICON_LAN, .headline = "Hostname", .key = "hostname", .kind = BIND_TEXT, .max_len = 31,
                                    .help = "mDNS name: <hostname>.local, and the Bluetooth name rrcloud-<hostname>. Letters, digits and '-'."};
static const ui_setting_t s_auth = {.symbol = ICON_LOCK, .headline = "Require a password", .key = "admin_auth", .kind = BIND_SWITCH,
                                    .help = "For the web page (user admin), Bluetooth setup and this screen's settings"};
static const ui_setting_t s_apw = {.symbol = ICON_PASSWORD, .headline = "Admin password", .key = "admin_password", .has_key = "has_admin_password",
                                   .kind = BIND_SECRET, .max_len = 128, .help = "Write-only. Saving an empty value removes the password."};
static const ui_setting_t s_ble = {.symbol = ICON_BLUETOOTH, .headline = "Bluetooth setup", .key = "ble_enabled", .kind = BIND_SWITCH,
                                   .help = "Lets the RapidRAW app configure the dock without a network"};
static const ui_setting_t s_usbdbg = {.symbol = ICON_BUG, .headline = "Verbose USB logging", .key = "usb_debug", .kind = BIND_SWITCH,
                                      .help = "DEBUG-level USB host logs on the serial console (camera detection)"};

static struct {
    lv_obj_t *slider, *it_timeout, *it_blestate, *it_radio, *btn_radio, *it_update;
} w;

static const char *const timeout_names[] = {"15 seconds", "30 seconds", "1 minute", "2 minutes", "5 minutes", "10 minutes", "Never"};
static const int timeout_values[] = {15, 30, 60, 120, 300, 600, 0};
#define N_TIMEOUTS ((int)(sizeof timeout_values / sizeof timeout_values[0]))

static void saved_quiet(int status, const cJSON *json, void *ud)
{
    (void)ud;
    if (status != 200) ui_snackbar("Not saved: %s", ui_resp_error(status, json));
}

static void posted(int status, const cJSON *json, void *ud)
{
    if (status == 200) ui_snackbar("%s", (const char *)ud);
    else ui_snackbar("%s", ui_resp_error(status, json));
}

/* brightness: applied live while dragging, saved on release */
static void slider_cb(lv_event_t *e)
{
    lv_event_code_t code = lv_event_get_code(e);
    int v = (int)lv_slider_get_value(w.slider);
    ui_preview_backlight(v);
    if (code == LV_EVENT_RELEASED) {
        cJSON *p = cJSON_CreateObject();
        cJSON_AddNumberToObject(p, "display_brightness", v);
        ui_config_patch(p, saved_quiet, NULL);
    }
}

static void timeout_picked(int idx, void *ud)
{
    (void)ud;
    if (idx < 0 || idx >= N_TIMEOUTS) return;
    cJSON *p = cJSON_CreateObject();
    cJSON_AddNumberToObject(p, "display_timeout_s", timeout_values[idx]);
    ui_config_patch(p, saved_quiet, NULL);
}

static int timeout_index(double s)
{
    for (int i = 0; i < N_TIMEOUTS; i++) if (timeout_values[i] == (int)s) return i;
    return 2;
}

static void timeout_cb(lv_event_t *e)
{
    (void)e;
    ui_choose("Screen timeout", timeout_names, N_TIMEOUTS, timeout_index(jnum(ui_config(), "display_timeout_s")), timeout_picked, NULL);
}

static void forget_ok(void *ud) { (void)ud; ui_req_global("POST", "/api/ble/forget", "{}", posted, "Paired phones forgotten"); }
static void forget_cb(lv_event_t *e) { (void)e; ui_dialog("Forget paired phones?", "Every bonded phone has to pair again before it can configure the dock.", "Forget", true, forget_ok, NULL); }
static void radio_ok(void *ud) { (void)ud; ui_req_global("POST", "/api/coproc/update", "{}", posted, "Radio update started"); }
static void radio_cb(lv_event_t *e)
{
    (void)e;
    ui_dialog("Update radio firmware?", "Downloads the pinned ESP-Hosted image (SHA-256 verified) and installs it on the ESP32-C6. Wi-Fi and Bluetooth drop during the update and the dock restarts.",
              "Update", false, radio_ok, NULL);
}
static void usb_cb(lv_event_t *e) { (void)e; ui_req_global("POST", "/api/usb/reset", "{}", posted, "USB port power-cycled"); }
static void reboot_ok(void *ud) { (void)ud; ui_req_global("POST", "/api/reboot", "{}", posted, "Restarting…"); }
static void reboot_cb(lv_event_t *e) { (void)e; ui_dialog("Restart the dock?", "A sync in progress is interrupted and resumes after the restart.", "Restart", true, reboot_ok, NULL); }
static void log_cb(lv_event_t *e) { (void)e; ui_nav_push(&page_log); }
static void about_cb(lv_event_t *e) { (void)e; ui_nav_push(&page_about); }

static lv_obj_t *action_item(lv_obj_t *list, const char *sym, const char *head, const char *sup, lv_event_cb_t cb, bool chevron)
{
    lv_obj_t *it = ui_list_item(list, sym, head, sup);
    lv_obj_add_flag(it, LV_OBJ_FLAG_CLICKABLE);
    lv_obj_add_event_cb(it, cb, LV_EVENT_CLICKED, NULL);
    if (chevron) ui_item_trailing_text(it, ICON_CHEVRON);
    return it;
}

static void build(lv_obj_t *c)
{
    memset(&w, 0, sizeof w);
    lv_obj_t *l = ui_section(c, "Display");
    lv_obj_t *bi = ui_list_item(l, ICON_BRIGHTNESS, "Brightness", NULL);
    lv_obj_t *col = lv_obj_get_child(bi, 1);
    w.slider = lv_slider_create(col);
    lv_obj_set_width(w.slider, LV_PCT(96));
    lv_obj_set_height(w.slider, 10);
    lv_obj_set_style_margin_top(w.slider, 14, 0);
    lv_obj_set_style_margin_bottom(w.slider, 8, 0);
    lv_slider_set_range(w.slider, 5, 100);
    lv_obj_set_style_bg_color(w.slider, M.secondary_container, LV_PART_MAIN);
    lv_obj_set_style_bg_color(w.slider, M.primary, LV_PART_INDICATOR);
    lv_obj_set_style_bg_color(w.slider, M.primary, LV_PART_KNOB);
    lv_obj_set_style_pad_all(w.slider, 9, LV_PART_KNOB);
    lv_obj_set_ext_click_area(w.slider, 16);
    lv_obj_add_event_cb(w.slider, slider_cb, LV_EVENT_VALUE_CHANGED, NULL);
    lv_obj_add_event_cb(w.slider, slider_cb, LV_EVENT_RELEASED, NULL);
    w.it_timeout = action_item(l, ICON_TIMER, "Screen timeout", "", timeout_cb, false);
    ui_setting(l, &s_dark);
    lv_obj_t *hint = ui_label(c, UI_FONT_SMALL, M.on_surface_variant, "The BOOT key turns the screen off and on. The POWER key switches the whole board (hold 2 s to power off).");
    lv_obj_set_width(hint, LV_PCT(100));
    lv_label_set_long_mode(hint, LV_LABEL_LONG_MODE_WRAP);
    lv_obj_set_style_pad_hor(hint, 8, 0);

    l = ui_section(c, "Identity");
    ui_setting(l, &s_name);
    ui_setting(l, &s_host);

    l = ui_section(c, "Security");
    ui_setting(l, &s_auth);
    ui_setting(l, &s_apw);

    l = ui_section(c, "Bluetooth");
    ui_setting(l, &s_ble);
    w.it_blestate = ui_list_item(l, ICON_SMARTPHONE, "Phone link", "—");
    action_item(l, ICON_DELETE, "Forget paired phones", "Phones must pair again", forget_cb, false);

    l = ui_section(c, "Radio co-processor");
    w.it_radio = ui_list_item(l, ICON_MEMORY, "ESP32-C6", "—");
    w.it_update = action_item(l, ICON_SYSTEM_UPDATE, "Update radio firmware", "", radio_cb, false);

    l = ui_section(c, "Diagnostics");
    ui_setting(l, &s_usbdbg);
    action_item(l, ICON_USB, "Retry USB", "Power-cycle the camera port so it is detected again", usb_cb, false);
    action_item(l, ICON_ARTICLE, "Log", "Recent messages from the dock", log_cb, true);
    action_item(l, ICON_INFO, "About this dock", "Firmware, board, memory, storage, battery", about_cb, true);

    lv_obj_t *row = ui_row(c, 8);
    lv_obj_set_style_pad_top(row, 8, 0);
    ui_button(row, BTN_ERROR, ICON_RESTART, "Restart", reboot_cb, NULL);
}

static void on_config(const cJSON *cfg)
{
    double b = jnum(cfg, "display_brightness");
    if (w.slider && !lv_obj_has_state(w.slider, LV_STATE_PRESSED)) lv_slider_set_value(w.slider, b > 0 ? (int32_t)b : 80, LV_ANIM_OFF);
    if (w.it_timeout) ui_item_supporting(w.it_timeout, timeout_names[timeout_index(jnum(cfg, "display_timeout_s"))]);
}

static void on_status(const cJSON *s)
{
    char buf[200];
    const cJSON *b = jobj(s, "ble"), *r = jobj(s, "coproc"), *u = jobj(r, "update");
    if (!jbool(b, "enabled")) snprintf(buf, sizeof buf, "Off");
    else if (jbool(b, "connected")) snprintf(buf, sizeof buf, "App connected (%s, MTU %.0f)", jbool(b, "encrypted") ? "encrypted" : "not yet encrypted", jnum(b, "mtu"));
    else if (jbool(b, "advertising")) snprintf(buf, sizeof buf, "Advertising as %s", jstr(b, "name"));
    else if (jstr(b, "error")[0]) snprintf(buf, sizeof buf, "Unavailable: %s", jstr(b, "error"));
    else snprintf(buf, sizeof buf, "Starting…");
    ui_item_supporting(w.it_blestate, buf);

    if (jbool(r, "linked")) {
        if (jbool(r, "compatible")) snprintf(buf, sizeof buf, "ESP-Hosted %s · compatible", jstr(r, "version"));
        else snprintf(buf, sizeof buf, "ESP-Hosted %s · needs %s — Wi-Fi and Bluetooth stay off until updated", jstr(r, "version"), jstr(r, "host_lib"));
    } else if (jbool(r, "linking")) snprintf(buf, sizeof buf, "Linking over SDIO…");
    else snprintf(buf, sizeof buf, "Not linked%s%s", jstr(r, "error")[0] ? " — " : "", jstr(r, "error"));
    ui_item_supporting(w.it_radio, buf);
    ui_item_supporting_color(w.it_radio, jbool(r, "linked") && !jbool(r, "compatible") ? M.error : M.on_surface_variant);

    const char *st = jstr(u, "state");
    if (st[0] && strcmp(st, "idle")) {
        char a[24] = "", t[24] = "";
        if (jnum(u, "total") > 0) { ui_fmt_bytes(a, sizeof a, jnum(u, "done")); ui_fmt_bytes(t, sizeof t, jnum(u, "total")); }
        snprintf(buf, sizeof buf, "%s: %s%s%s%s", st, jstr(u, "message"), a[0] ? " · " : "", a, a[0] ? " / " : "");
        if (a[0]) strncat(buf, t, sizeof buf - strlen(buf) - 1);
    } else snprintf(buf, sizeof buf, "Installs the ESP-Hosted image this firmware expects");
    ui_item_supporting(w.it_update, buf);
    if (!strcmp(st, "running") || !jbool(r, "linked")) lv_obj_add_state(w.it_update, LV_STATE_DISABLED);
    else lv_obj_remove_state(w.it_update, LV_STATE_DISABLED);
}

static void destroy(void) { memset(&w, 0, sizeof w); }

const ui_page_t page_device = {.title = "Device", .build = build, .on_config = on_config, .on_status = on_status, .destroy = destroy, .locked = true};

/* ---- Log ---------------------------------------------------------------------------- */
#define LOG_LINES 60
static struct { lv_obj_t *label; bool inflight; char last[48]; int ticks; } lg;

static void log_loaded(int status, const cJSON *j, void *ud)
{
    (void)ud;
    lg.inflight = false;
    if (status != 200 || !cJSON_IsArray(j) || !lg.label) return;
    int n = cJSON_GetArraySize(j);
    const cJSON *lastl = cJSON_GetArrayItem(j, n - 1);
    const char *ls = cJSON_IsString(lastl) ? lastl->valuestring : "";
    if (!strncmp(ls, lg.last, sizeof lg.last - 1)) return;   /* nothing new */
    snprintf(lg.last, sizeof lg.last, "%s", ls);
    /* newest last, at most LOG_LINES lines (a label with the full 20 KB ring is slow to lay out) */
    size_t cap = 0;
    int first = n > LOG_LINES ? n - LOG_LINES : 0;
    for (int i = first; i < n; i++) { const cJSON *s = cJSON_GetArrayItem(j, i); if (cJSON_IsString(s)) cap += strlen(s->valuestring) + 1; }
    char *buf = lv_malloc(cap + 1);
    if (!buf) return;
    size_t off = 0;
    for (int i = first; i < n; i++) {
        const cJSON *s = cJSON_GetArrayItem(j, i);
        if (!cJSON_IsString(s)) continue;
        size_t l = strlen(s->valuestring);
        memcpy(buf + off, s->valuestring, l);
        off += l;
        buf[off++] = '\n';
    }
    buf[off ? off - 1 : 0] = 0;
    lv_label_set_text(lg.label, buf);
    lv_free(buf);
    lv_obj_t *c = lv_obj_get_parent(lv_obj_get_parent(lg.label));
    lv_obj_update_layout(c);
    lv_obj_scroll_to_y(c, LV_COORD_MAX, LV_ANIM_OFF);
}

static void log_fetch(void)
{
    if (lg.inflight) return;
    lg.inflight = true;
    ui_req("GET", "/api/log", NULL, log_loaded, NULL);
}

static void log_tick(void) { if (++lg.ticks % 2 == 0) log_fetch(); }

static void log_build(lv_obj_t *c)
{
    memset(&lg, 0, sizeof lg);
    lv_obj_t *card = ui_card(c);
    lv_obj_set_style_bg_color(card, M.surface_container_lowest, 0);
    lv_obj_set_style_pad_all(card, 12, 0);
    lg.label = ui_label(card, UI_FONT_SMALL, M.on_surface, "Loading…");
    lv_obj_set_width(lg.label, LV_PCT(100));
    lv_label_set_long_mode(lg.label, LV_LABEL_LONG_MODE_WRAP);
    ui_appbar_action(ICON_REFRESH, log_fetch);
    log_fetch();
}

static void log_destroy(void) { memset(&lg, 0, sizeof lg); }

const ui_page_t page_log = {.title = "Log", .build = log_build, .tick = log_tick, .destroy = log_destroy, .locked = true};

/* ---- About -------------------------------------------------------------------------- */
static struct { lv_obj_t *fw, *board, *id, *up, *mem, *store, *bat, *net; } ab;

static void about_build(lv_obj_t *c)
{
    memset(&ab, 0, sizeof ab);
    lv_obj_t *l = ui_section(c, "Firmware");
    ab.fw = ui_list_item(l, ICON_RELEASE, "Version", "—");
    ab.board = ui_list_item(l, ICON_BOARD, "Board", "—");
    ab.id = ui_list_item(l, ICON_FINGERPRINT, "Device ID", "—");
    l = ui_section(c, "Runtime");
    ab.up = ui_list_item(l, ICON_SCHEDULE, "Uptime", "—");
    ab.mem = ui_list_item(l, ICON_MEMORY, "Free memory", "—");
    ab.store = ui_list_item(l, ICON_SD_STORAGE, "Sync ledger storage", "—");
    ab.bat = ui_list_item(l, ICON_BATTERY, "Battery", "—");
    ab.net = ui_list_item(l, ICON_WIFI, "Network", "—");
}

static void about_status(const cJSON *s)
{
    char a[32], b[32], buf[160];
    ui_item_supporting(ab.fw, jstr(s, "version"));
    ui_item_supporting(ab.board, jstr(s, "board"));
    ui_item_supporting(ab.id, jstr(s, "device_id"));
    ui_fmt_duration(buf, sizeof buf, jnum(s, "uptime_s"));
    ui_item_supporting(ab.up, buf);
    ui_fmt_bytes(a, sizeof a, jnum(s, "heap_free"));
    ui_fmt_bytes(b, sizeof b, jnum(s, "psram_free"));
    snprintf(buf, sizeof buf, "Internal %s · PSRAM %s", a, b);
    ui_item_supporting(ab.mem, buf);
    ui_fmt_bytes(a, sizeof a, jnum(s, "storage_used"));
    ui_fmt_bytes(b, sizeof b, jnum(s, "storage_total"));
    snprintf(buf, sizeof buf, "%s of %s · %.0f records", a, b, jnum(jobj(s, "sync"), "ledger_records"));
    ui_item_supporting(ab.store, buf);
    const cJSON *p = jobj(s, "power");
    if (!jbool(p, "battery")) snprintf(buf, sizeof buf, "Not fitted on this board");
    else if (!jbool(p, "present")) snprintf(buf, sizeof buf, "No battery detected (%.2f V at the charger)", jnum(p, "mv") / 1000);
    else snprintf(buf, sizeof buf, "%.0f %% · %.2f V (charge state is not wired to the processor)", jnum(p, "pct"), jnum(p, "mv") / 1000);
    ui_item_supporting(ab.bat, buf);
    ui_item_supporting(ab.net, jstr(s, "network"));
}

static void about_destroy(void) { memset(&ab, 0, sizeof ab); }

const ui_page_t page_about = {.title = "About", .build = about_build, .on_status = about_status, .destroy = about_destroy};

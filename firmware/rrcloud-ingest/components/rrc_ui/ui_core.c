/*
 * rrc_ui core: the app shell (status bar, top app bar, navigation bar, content),
 * the request plumbing to the host, status/config polling, config bindings, the
 * admin lock and screen sleep.
 *
 * Requests carry a token (slot | sequence << 4). Page-scoped requests also
 * remember the page generation they were issued under: an answer that arrives
 * after the user navigated away is dropped instead of touching deleted widgets.
 * Polls never overlap themselves (one status poll in flight at most), so a slow
 * api_dispatch() — the sync task holding a lock, a config save writing NVS —
 * cannot pile requests up behind it.
 */
#include "ui_priv.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

static const rrc_ui_host_t *host;
static lv_display_t *disp;

int ui_w = 480, ui_h = 800;
bool ui_landscape;
static int applied_rotation = -1;   /* display_rotation the host was last asked for */

static void read_display_size(void)
{
    ui_w = lv_display_get_horizontal_resolution(disp);
    ui_h = lv_display_get_vertical_resolution(disp);
    ui_landscape = ui_w > ui_h;
}

/* ---- JSON helpers ----------------------------------------------------------------- */
const cJSON *jobj(const cJSON *o, const char *k) { return o ? cJSON_GetObjectItemCaseSensitive(o, k) : NULL; }
const char *jstr(const cJSON *o, const char *k)
{
    const cJSON *v = jobj(o, k);
    return cJSON_IsString(v) && v->valuestring ? v->valuestring : "";
}
double jnum(const cJSON *o, const char *k)
{
    const cJSON *v = jobj(o, k);
    return cJSON_IsNumber(v) ? v->valuedouble : 0;
}
bool jbool(const cJSON *o, const char *k) { return cJSON_IsTrue(jobj(o, k)); }

/* ---- requests --------------------------------------------------------------------- */
#define REQ_SLOTS 16
typedef struct {
    bool used, scoped;
    uint32_t seq, gen;
    ui_resp_fn fn;
    void *ud;
} req_t;
static req_t reqs[REQ_SLOTS];
static uint32_t req_seq, page_gen;

static void req_issue(const char *method, const char *path, const char *body, ui_resp_fn fn, void *ud, bool scoped)
{
    int slot = -1;
    for (int i = 0; i < REQ_SLOTS; i++) if (!reqs[i].used) { slot = i; break; }
    if (slot < 0) { LV_LOG_WARN("rrc_ui: request table full, dropping %s %s", method, path); if (fn) fn(503, NULL, ud); return; }
    req_seq = (req_seq + 1) & 0x0FFFFFFF;
    reqs[slot] = (req_t){.used = true, .scoped = scoped, .seq = req_seq, .gen = page_gen, .fn = fn, .ud = ud};
    host->request(method, path, body, (req_seq << 4) | (uint32_t)slot);
}

void ui_req(const char *method, const char *path, const char *body, ui_resp_fn fn, void *ud) { req_issue(method, path, body, fn, ud, true); }
void ui_req_global(const char *method, const char *path, const char *body, ui_resp_fn fn, void *ud) { req_issue(method, path, body, fn, ud, false); }

void rrc_ui_complete(uint32_t token, int status, const char *json, size_t len)
{
    req_t *r = &reqs[token & 0xF];
    if (!r->used || r->seq != token >> 4) return;
    req_t c = *r;
    r->used = false;
    if (!c.fn || (c.scoped && c.gen != page_gen)) return;
    cJSON *doc = json && len ? cJSON_ParseWithLength(json, len) : NULL;
    c.fn(status, doc, c.ud);
    cJSON_Delete(doc);
}

const char *ui_resp_error(int status, const cJSON *json)
{
    static char buf[64];
    const char *e = jstr(json, "error");
    if (e[0]) return e;
    if (status == 401) return "Not authorized";
    snprintf(buf, sizeof buf, "The dock answered HTTP %d", status);
    return buf;
}

/* ---- shell -------------------------------------------------------------------------- */
static lv_obj_t *statusbar, *sb_name, *sb_sync, *sb_cam, *sb_ble, *sb_sig, *sb_bat, *sb_pct;
static lv_obj_t *appbar, *ab_back, *ab_title, *ab_action;
static lv_obj_t *navbar, *nav_items[4];
static lv_obj_t *content;
static int cur_tab;
#define STACK_MAX 4
static const ui_page_t *stack[STACK_MAX];
static int depth;            /* 0 = a tab page is shown */
static const ui_page_t *const tabs[4] = {&page_home, &page_sync, &page_wifi, &page_device};
static bool showing_placeholder;

static cJSON *status_doc, *config_doc;
static bool status_inflight, config_inflight;
static bool unlocked, asleep;
static uint32_t auth_failures, auth_block_until;
static lv_obj_t *shield;

const cJSON *ui_status(void) { return status_doc; }
const cJSON *ui_config(void) { return config_doc; }
bool ui_lock_required(void) { return jbool(config_doc, "admin_auth") && jbool(config_doc, "has_admin_password"); }
bool ui_unlocked(void) { return unlocked || !ui_lock_required(); }
bool rrc_ui_asleep(void) { return asleep; }

static const ui_page_t *cur_page(void) { return depth ? stack[depth - 1] : tabs[cur_tab]; }

/* days since 1970-01-01 for a proleptic Gregorian date (H. Hinnant's algorithm) */
static long days_from_civil(long y, unsigned m, unsigned d)
{
    y -= m <= 2;
    long era = (y >= 0 ? y : y - 399) / 400;
    unsigned yoe = (unsigned)(y - era * 400);
    unsigned doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
    unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return era * 146097 + (long)doe - 719468;
}

double ui_now(void)
{
    const char *t = jstr(status_doc, "time");   /* "2026-10-16T12:34:56Z" or "" */
    int y, mo, d, h, mi, s;
    if (sscanf(t, "%4d-%2d-%2dT%2d:%2d:%2d", &y, &mo, &d, &h, &mi, &s) != 6) return 0;
    return (double)days_from_civil(y, (unsigned)mo, (unsigned)d) * 86400.0 + h * 3600 + mi * 60 + s;
}

static void style_bar(lv_obj_t *o, lv_color_t bg)
{
    lv_obj_remove_style_all(o);
    lv_obj_set_style_bg_color(o, bg, 0);
    lv_obj_set_style_bg_opa(o, LV_OPA_COVER, 0);
    lv_obj_remove_flag(o, LV_OBJ_FLAG_SCROLLABLE);
}

static void back_cb(lv_event_t *e) { (void)e; ui_nav_pop(); }

static void (*action_fn)(void);
static void action_clicked(lv_event_t *e) { (void)e; if (action_fn) action_fn(); }

static void nav_cb(lv_event_t *e) { ui_nav_tab((int)(intptr_t)lv_event_get_user_data(e)); }

static void build_statusbar(lv_obj_t *scr)
{
    statusbar = lv_obj_create(scr);
    style_bar(statusbar, M.surface);
    lv_obj_set_size(statusbar, UI_W, UI_STATUS_H);
    lv_obj_set_style_pad_hor(statusbar, 18, 0);
    lv_obj_set_flex_flow(statusbar, LV_FLEX_FLOW_ROW);
    lv_obj_set_flex_align(statusbar, LV_FLEX_ALIGN_START, LV_FLEX_ALIGN_CENTER, LV_FLEX_ALIGN_CENTER);
    lv_obj_set_style_pad_column(statusbar, 10, 0);
    sb_name = ui_label(statusbar, UI_FONT_LABEL, M.on_surface, "");
    lv_obj_set_flex_grow(sb_name, 1);
    lv_label_set_long_mode(sb_name, LV_LABEL_LONG_MODE_DOTS);
    sb_sync = ui_label(statusbar, UI_FONT_BODY, M.primary, ICON_UPLOAD);
    sb_cam = ui_label(statusbar, UI_FONT_BODY, M.on_surface, ICON_CAMERA);
    sb_ble = ui_label(statusbar, UI_FONT_BODY, M.on_surface, ICON_BLUETOOTH);
    sb_sig = ui_signal(statusbar, 16);
    sb_bat = ui_battery(statusbar);
    sb_pct = ui_label(statusbar, UI_FONT_LABEL, M.on_surface, "");
    lv_obj_add_flag(sb_sync, LV_OBJ_FLAG_HIDDEN);
    lv_obj_add_flag(sb_cam, LV_OBJ_FLAG_HIDDEN);
    lv_obj_add_flag(sb_ble, LV_OBJ_FLAG_HIDDEN);
    lv_obj_add_flag(sb_bat, LV_OBJ_FLAG_HIDDEN);
}

static void build_appbar(lv_obj_t *scr)
{
    appbar = lv_obj_create(scr);
    style_bar(appbar, M.surface);
    lv_obj_set_size(appbar, UI_CONTENT_W, UI_APPBAR_H);
    lv_obj_set_pos(appbar, UI_CONTENT_X, UI_STATUS_H);
    lv_obj_set_style_pad_hor(appbar, 8, 0);
    lv_obj_set_flex_flow(appbar, LV_FLEX_FLOW_ROW);
    lv_obj_set_flex_align(appbar, LV_FLEX_ALIGN_START, LV_FLEX_ALIGN_CENTER, LV_FLEX_ALIGN_CENTER);
    lv_obj_set_style_pad_column(appbar, 4, 0);
    ab_back = ui_button(appbar, BTN_TEXT, ICON_BACK, NULL, back_cb, NULL);
    lv_obj_set_style_text_font(lv_obj_get_child(ab_back, 0), UI_FONT_ICON, 0);
    lv_obj_set_style_text_color(ab_back, M.on_surface, 0);
    ab_title = ui_label(appbar, UI_FONT_HEADLINE, M.on_surface, "");
    lv_obj_set_style_pad_left(ab_title, 12, 0);
    lv_obj_set_flex_grow(ab_title, 1);
    lv_label_set_long_mode(ab_title, LV_LABEL_LONG_MODE_DOTS);
    ab_action = ui_button(appbar, BTN_TEXT, ICON_REFRESH, NULL, action_clicked, NULL);
    lv_obj_set_style_text_font(lv_obj_get_child(ab_action, 0), UI_FONT_ICON, 0);
    lv_obj_set_style_text_color(ab_action, M.on_surface_variant, 0);
}

/* Portrait: an M3 navigation bar along the bottom. Landscape: a navigation rail
 * down the left edge, destinations at a fixed pitch from the top. */
static void build_navbar(lv_obj_t *scr)
{
    static const char *const icons[4] = {ICON_HOME, ICON_UPLOAD, ICON_WIFI, ICON_SETTINGS};
    static const char *const names[4] = {"Status", "Sync", "Wi-Fi", "Device"};
    navbar = lv_obj_create(scr);
    style_bar(navbar, M.surface_container);
    if (ui_landscape) {
        lv_obj_set_size(navbar, UI_RAIL_W, UI_H - UI_STATUS_H);
        lv_obj_set_pos(navbar, 0, UI_STATUS_H);
        lv_obj_set_flex_flow(navbar, LV_FLEX_FLOW_COLUMN);
        lv_obj_set_flex_align(navbar, LV_FLEX_ALIGN_START, LV_FLEX_ALIGN_CENTER, LV_FLEX_ALIGN_CENTER);
        lv_obj_set_style_pad_top(navbar, 8, 0);
    } else {
        lv_obj_set_size(navbar, UI_W, UI_NAVBAR_H);
        lv_obj_align(navbar, LV_ALIGN_BOTTOM_MID, 0, 0);
        lv_obj_set_flex_flow(navbar, LV_FLEX_FLOW_ROW);
        lv_obj_set_flex_align(navbar, LV_FLEX_ALIGN_SPACE_EVENLY, LV_FLEX_ALIGN_CENTER, LV_FLEX_ALIGN_CENTER);
    }
    for (int i = 0; i < 4; i++) {
        lv_obj_t *it = ui_column(navbar, 6);
        if (ui_landscape) lv_obj_set_size(it, UI_RAIL_W, UI_RAIL_ITEM_H); else lv_obj_set_width(it, UI_W / 4);
        lv_obj_set_flex_align(it, LV_FLEX_ALIGN_CENTER, LV_FLEX_ALIGN_CENTER, LV_FLEX_ALIGN_CENTER);
        lv_obj_add_flag(it, LV_OBJ_FLAG_CLICKABLE);
        lv_obj_add_event_cb(it, nav_cb, LV_EVENT_CLICKED, (void *)(intptr_t)i);
        /* active indicator: 64x32 dp pill behind the icon */
        lv_obj_t *pill = lv_obj_create(it);
        lv_obj_remove_style_all(pill);
        lv_obj_set_size(pill, 80, 40);
        lv_obj_set_style_radius(pill, UI_RADIUS_FULL, 0);
        lv_obj_remove_flag(pill, LV_OBJ_FLAG_CLICKABLE);
        lv_obj_add_flag(pill, LV_OBJ_FLAG_EVENT_BUBBLE);
        lv_obj_t *ic = ui_label(pill, UI_FONT_ICON, M.on_surface_variant, icons[i]);
        lv_obj_center(ic);
        ui_label(it, UI_FONT_LABEL, M.on_surface_variant, names[i]);
        nav_items[i] = it;
    }
}

static void nav_highlight(void)
{
    for (int i = 0; i < 4; i++) {
        lv_obj_t *pill = lv_obj_get_child(nav_items[i], 0), *ic = lv_obj_get_child(pill, 0), *lbl = lv_obj_get_child(nav_items[i], 1);
        bool on = i == cur_tab;
        lv_obj_set_style_bg_color(pill, M.secondary_container, 0);
        lv_obj_set_style_bg_opa(pill, on ? LV_OPA_COVER : LV_OPA_TRANSP, 0);
        lv_obj_set_style_text_color(ic, on ? M.on_secondary_container : M.on_surface_variant, 0);
        lv_obj_set_style_text_color(lbl, on ? M.on_surface : M.on_surface_variant, 0);
    }
}

static void build_shell(void)
{
    lv_obj_t *scr = lv_display_get_screen_active(disp);
    lv_obj_clean(scr);
    lv_obj_set_style_bg_color(scr, M.surface, 0);
    lv_obj_set_style_bg_opa(scr, LV_OPA_COVER, 0);
    lv_obj_remove_flag(scr, LV_OBJ_FLAG_SCROLLABLE);
    build_statusbar(scr);
    build_appbar(scr);
    content = lv_obj_create(scr);
    lv_obj_remove_style_all(content);
    lv_obj_set_pos(content, UI_CONTENT_X, UI_CONTENT_Y);
    lv_obj_set_width(content, UI_CONTENT_W);
    lv_obj_set_flex_flow(content, LV_FLEX_FLOW_COLUMN);
    lv_obj_set_style_pad_hor(content, 16, 0);
    lv_obj_set_style_pad_top(content, 4, 0);
    lv_obj_set_style_pad_bottom(content, 24, 0);
    lv_obj_set_style_pad_row(content, UI_GAP, 0);
    lv_obj_set_scroll_dir(content, LV_DIR_VER);
    lv_obj_set_scrollbar_mode(content, LV_SCROLLBAR_MODE_ACTIVE);
    lv_obj_set_style_bg_color(content, M.on_surface_variant, LV_PART_SCROLLBAR);
    lv_obj_set_style_bg_opa(content, LV_OPA_40, LV_PART_SCROLLBAR);
    lv_obj_set_style_width(content, 4, LV_PART_SCROLLBAR);
    build_navbar(scr);
}

/* ---- bindings ---------------------------------------------------------------------- */
#define BIND_MAX 40
typedef struct { lv_obj_t *item, *sw; const ui_setting_t *s; } bind_t;
static bind_t binds[BIND_MAX];
static int nbinds;

static void bind_refresh(const bind_t *b)
{
    const cJSON *c = config_doc;
    const ui_setting_t *s = b->s;
    char buf[160];
    switch (s->kind) {
    case BIND_TEXT: {
        const char *v = jstr(c, s->key);
        ui_item_supporting(b->item, v[0] ? v : (s->placeholder ? s->placeholder : "Not set"));
        break;
    }
    case BIND_SECRET:
        ui_item_supporting(b->item, jbool(c, s->has_key) ? "Stored · tap to replace" : "Not set");
        break;
    case BIND_NUMBER:
        snprintf(buf, sizeof buf, "%.0f%s", jnum(c, s->key), s->suffix ? s->suffix : "");
        ui_item_supporting(b->item, buf);
        break;
    case BIND_SWITCH:
        if (jbool(c, s->key)) lv_obj_add_state(b->sw, LV_STATE_CHECKED); else lv_obj_remove_state(b->sw, LV_STATE_CHECKED);
        break;
    }
}

typedef struct { ui_resp_fn fn; void *ud; } patch_ctx_t;

static void config_loaded(int status, const cJSON *json, void *ud);

static void patch_done(int status, const cJSON *json, void *ud)
{
    patch_ctx_t c = *(patch_ctx_t *)ud;
    lv_free(ud);
    if (status == 200) ui_config_reload();
    if (c.fn) c.fn(status, json, c.ud);
}

void ui_config_patch(cJSON *patch, ui_resp_fn fn, void *ud)
{
    char *body = cJSON_PrintUnformatted(patch);
    cJSON_Delete(patch);
    if (!body) { if (fn) fn(500, NULL, ud); return; }
    patch_ctx_t *c = lv_malloc(sizeof *c);
    c->fn = fn;
    c->ud = ud;
    ui_req_global("POST", "/api/config", body, patch_done, c);
    cJSON_free(body);
}

static void editor_saved(int status, const cJSON *json, void *ud)
{
    (void)ud;
    if (status == 200) { ui_editor_done(NULL); ui_snackbar("Saved"); }
    else ui_editor_done(ui_resp_error(status, json));
}

static void setting_save(const char *value, void *ud)
{
    const ui_setting_t *s = ud;
    cJSON *p = cJSON_CreateObject();
    if (s->kind == BIND_NUMBER) cJSON_AddNumberToObject(p, s->key, strtod(value, NULL));
    else cJSON_AddStringToObject(p, s->key, value);
    ui_config_patch(p, editor_saved, NULL);
}

static void switch_saved(int status, const cJSON *json, void *ud)
{
    (void)ud;
    if (status != 200) { ui_snackbar("Not saved: %s", ui_resp_error(status, json)); ui_config_reload(); }
}

static void setting_switch_changed(lv_event_t *e)
{
    const ui_setting_t *s = lv_event_get_user_data(e);
    lv_obj_t *sw = lv_event_get_target_obj(e);
    cJSON *p = cJSON_CreateObject();
    cJSON_AddBoolToObject(p, s->key, lv_obj_has_state(sw, LV_STATE_CHECKED));
    ui_config_patch(p, switch_saved, NULL);
}

static void setting_clicked(lv_event_t *e)
{
    const ui_setting_t *s = lv_event_get_user_data(e);
    if (s->kind == BIND_SWITCH) {
        for (int i = 0; i < nbinds; i++) {
            if (binds[i].s != s) continue;
            lv_obj_t *sw = binds[i].sw;
            if (lv_obj_has_state(sw, LV_STATE_CHECKED)) lv_obj_remove_state(sw, LV_STATE_CHECKED); else lv_obj_add_state(sw, LV_STATE_CHECKED);
            lv_obj_send_event(sw, LV_EVENT_VALUE_CHANGED, NULL);
        }
        return;
    }
    char num[24];
    const char *initial = "";
    if (s->kind == BIND_TEXT) initial = jstr(config_doc, s->key);
    else if (s->kind == BIND_NUMBER) { snprintf(num, sizeof num, "%.0f", jnum(config_doc, s->key)); initial = num; }
    ui_editor_t ed = {
        .title = s->headline, .label = s->headline, .initial = initial, .help = s->help, .placeholder = s->placeholder,
        .mode = s->kind == BIND_SECRET ? EDIT_PASSWORD : s->kind == BIND_NUMBER ? EDIT_NUMBER : EDIT_TEXT,
        .max_len = s->max_len, .allow_empty = s->kind != BIND_NUMBER, .save = setting_save, .ud = (void *)s,
    };
    ui_editor_open(&ed);
}

lv_obj_t *ui_setting(lv_obj_t *list, const ui_setting_t *s)
{
    lv_obj_t *it = ui_list_item(list, s->symbol, s->headline, s->kind == BIND_SWITCH ? s->help : "");
    lv_obj_add_flag(it, LV_OBJ_FLAG_CLICKABLE);
    lv_obj_add_event_cb(it, setting_clicked, LV_EVENT_CLICKED, (void *)s);
    lv_obj_t *sw = NULL;
    if (s->kind == BIND_SWITCH) {
        sw = ui_item_switch(it, false);
        lv_obj_add_event_cb(sw, setting_switch_changed, LV_EVENT_VALUE_CHANGED, (void *)s);
    }
    if (nbinds < BIND_MAX) {
        binds[nbinds] = (bind_t){.item = it, .sw = sw, .s = s};
        if (config_doc) bind_refresh(&binds[nbinds]);
        nbinds++;
    }
    return it;
}

/* ---- polling ---------------------------------------------------------------------- */
static void apply_display_config(void);

static void config_loaded(int status, const cJSON *json, void *ud)
{
    (void)ud;
    config_inflight = false;
    if (status != 200 || !json) return;
    bool had = config_doc != NULL;
    bool was_required = had && ui_lock_required();
    bool was_locked = was_required && !unlocked;
    cJSON_Delete(config_doc);
    config_doc = cJSON_Duplicate(json, true);
    /* Whoever just set the password from this screen stays in until it sleeps. */
    if (had && !was_required && ui_lock_required()) unlocked = true;
    apply_display_config();
    bool now_locked = ui_lock_required() && !unlocked;
    if (showing_placeholder || (had && was_locked != now_locked && cur_page()->locked)) { ui_nav_rebuild(); return; }
    for (int i = 0; i < nbinds; i++) bind_refresh(&binds[i]);
    if (cur_page()->on_config) cur_page()->on_config(config_doc);
}

void ui_config_reload(void)
{
    if (config_inflight) return;
    config_inflight = true;
    ui_req_global("GET", "/api/config", NULL, config_loaded, NULL);
}

static void update_statusbar(void)
{
    const cJSON *s = status_doc, *y = jobj(s, "sync"), *pw = jobj(s, "power"), *ble = jobj(s, "ble");
    const char *name = jstr(config_doc, "device_name");
    lv_label_set_text(sb_name, name[0] ? name : jstr(s, "hostname"));
    const char *phase = jstr(y, "phase");
    bool busy = !strcmp(phase, "uploading") || !strcmp(phase, "enumerating") || !strcmp(phase, "publishing");
    if (busy) lv_obj_remove_flag(sb_sync, LV_OBJ_FLAG_HIDDEN); else lv_obj_add_flag(sb_sync, LV_OBJ_FLAG_HIDDEN);
    if (jbool(y, "camera_attached")) lv_obj_remove_flag(sb_cam, LV_OBJ_FLAG_HIDDEN); else lv_obj_add_flag(sb_cam, LV_OBJ_FLAG_HIDDEN);
    if (jbool(ble, "connected")) lv_obj_remove_flag(sb_ble, LV_OBJ_FLAG_HIDDEN); else lv_obj_add_flag(sb_ble, LV_OBJ_FLAG_HIDDEN);
    bool wifi_on = jbool(s, "wifi_enabled");
    ui_signal_set(sb_sig, (int)jnum(s, "wifi_rssi"), jbool(s, "wifi"), wifi_on ? M.on_surface : M.outline_variant);
    if (jbool(pw, "battery")) {
        int pct = (int)jnum(pw, "pct");
        bool present = jbool(pw, "present");
        lv_obj_remove_flag(sb_bat, LV_OBJ_FLAG_HIDDEN);
        ui_battery_set(sb_bat, pct, present);
        if (present && pct >= 0) lv_label_set_text_fmt(sb_pct, "%d%%", pct); else lv_label_set_text(sb_pct, "");
    }
}

static void status_loaded(int status, const cJSON *json, void *ud)
{
    (void)ud;
    status_inflight = false;
    if (status != 200 || !json) return;
    cJSON_Delete(status_doc);
    status_doc = cJSON_Duplicate(json, true);
    update_statusbar();
    if (!showing_placeholder && cur_page()->on_status) cur_page()->on_status(status_doc);
}

static void poll_cb(lv_timer_t *t)
{
    static uint32_t polls;
    (void)t;
    if (asleep) return;
    if (!status_inflight) { status_inflight = true; ui_req_global("GET", "/api/status", NULL, status_loaded, NULL); }
    /* The web page and the BLE app change settings too: re-read the config every
     * 15 s (an in-memory JSON encode on the dock) so the screen never shows stale values. */
    if (!config_doc || ++polls % 15 == 0) ui_config_reload();
    if (!showing_placeholder && cur_page()->tick) cur_page()->tick();
}

/* ---- navigation ------------------------------------------------------------------- */
static void unlock_cb(lv_event_t *e) { (void)e; ui_unlock_prompt(); }

static void show_page(void)
{
    page_gen++;
    nbinds = 0;
    lv_obj_clean(content);
    lv_obj_scroll_to_y(content, 0, LV_ANIM_OFF);
    ui_appbar_action(NULL, NULL);
    const ui_page_t *p = cur_page();
    lv_label_set_text(ab_title, p->title);
    if (depth) lv_obj_remove_flag(ab_back, LV_OBJ_FLAG_HIDDEN); else lv_obj_add_flag(ab_back, LV_OBJ_FLAG_HIDDEN);
    lv_obj_set_style_pad_left(ab_title, depth ? 0 : 12, 0);
    /* Sub-pages take the navigation bar's rows in portrait; the rail stays (width is
     * what landscape has plenty of, height is not). */
    int h = UI_H - UI_CONTENT_Y - (depth || ui_landscape ? 0 : UI_NAVBAR_H);
    lv_obj_set_height(content, h);
    if (depth && !ui_landscape) lv_obj_add_flag(navbar, LV_OBJ_FLAG_HIDDEN); else lv_obj_remove_flag(navbar, LV_OBJ_FLAG_HIDDEN);
    nav_highlight();
    showing_placeholder = false;
    if (p->locked && (!config_doc || (ui_lock_required() && !unlocked))) {
        showing_placeholder = true;
        lv_obj_t *col = ui_column(content, 16);
        lv_obj_set_flex_align(col, LV_FLEX_ALIGN_CENTER, LV_FLEX_ALIGN_CENTER, LV_FLEX_ALIGN_CENTER);
        lv_obj_set_style_pad_top(col, ui_landscape ? 24 : 120, 0);
        if (!config_doc) { ui_label(col, UI_FONT_BODY, M.on_surface_variant, "Loading…"); return; }
        ui_icon_circle(col, ICON_LOCK, TONE_PRIMARY);
        ui_label(col, UI_FONT_TITLE, M.on_surface, "Settings are locked");
        lv_obj_t *l = ui_label(col, UI_FONT_BODY, M.on_surface_variant, "Enter the admin password to change settings on this screen. Status stays visible.");
        lv_obj_set_width(l, UI_CONTENT_W - 80);
        lv_label_set_long_mode(l, LV_LABEL_LONG_MODE_WRAP);
        lv_obj_set_style_text_align(l, LV_TEXT_ALIGN_CENTER, 0);
        ui_button(col, BTN_FILLED, NULL, "Unlock", unlock_cb, NULL);
        return;
    }
    p->build(content);
    if (config_doc && p->on_config) p->on_config(config_doc);
    if (status_doc && p->on_status) p->on_status(status_doc);
}

static void leave_page(void)
{
    if (!showing_placeholder && cur_page()->destroy) cur_page()->destroy();
}

void ui_nav_tab(int index)
{
    if (index < 0 || index > 3) return;
    leave_page();
    depth = 0;
    cur_tab = index;
    show_page();
}

void ui_nav_push(const ui_page_t *page)
{
    if (depth >= STACK_MAX) return;
    leave_page();
    stack[depth++] = page;
    show_page();
}

void ui_nav_pop(void)
{
    if (!depth) return;
    leave_page();
    depth--;
    show_page();
}

void ui_nav_rebuild(void)
{
    leave_page();
    show_page();
}

void ui_appbar_action(const char *symbol, void (*fn)(void))
{
    action_fn = fn;
    if (!symbol || !fn) { lv_obj_add_flag(ab_action, LV_OBJ_FLAG_HIDDEN); return; }
    lv_obj_remove_flag(ab_action, LV_OBJ_FLAG_HIDDEN);
    lv_label_set_text(lv_obj_get_child(ab_action, 0), symbol);
}

/* ---- lock ----------------------------------------------------------------------------- */
static void auth_done(int status, const cJSON *json, void *ud)
{
    (void)json; (void)ud;
    if (status == 200) {
        unlocked = true;
        auth_failures = 0;
        ui_editor_done(NULL);
        ui_nav_rebuild();
        return;
    }
    auth_failures++;
    if (auth_failures >= 5) {
        /* 30 s after the fifth miss, doubling to a 15-minute ceiling: a passer-by
         * cannot walk the keyspace on the touchscreen. */
        uint32_t n = auth_failures - 5, secs = 30u << (n > 5 ? 5 : n);
        if (secs > 900) secs = 900;
        auth_block_until = lv_tick_get() + secs * 1000;
    }
    ui_editor_done("Wrong password");
}

static void auth_try(const char *pw, void *ud)
{
    (void)ud;
    if (auth_block_until && (int32_t)(auth_block_until - lv_tick_get()) > 0) {
        char msg[64];
        snprintf(msg, sizeof msg, "Too many attempts — try again in %u s", (unsigned)((auth_block_until - lv_tick_get()) / 1000 + 1));
        ui_editor_done(msg);
        return;
    }
    ui_req_global("AUTH", "", pw, auth_done, NULL);
}

void ui_unlock_prompt(void)
{
    ui_editor_t e = {.title = "Unlock", .label = "Admin password", .help = "The password set under Device › Security (the web page's login).",
                     .mode = EDIT_PASSWORD, .max_len = 128, .save = auth_try};
    ui_editor_open(&e);
}

/* ---- sleep ----------------------------------------------------------------------------- */
bool ui_can_rotate(void) { return host && host->set_rotation; }

static void apply_display_config(void)
{
    if (!config_doc) return;
    bool dark = jobj(config_doc, "display_dark") ? jbool(config_doc, "display_dark") : true;
    bool rebuild = dark != ui_dark;
    if (rebuild) ui_theme_init(disp, dark);
    /* Orientation: the host turns the panel, then the shell is laid out again for
     * whatever size the display reports (portrait bar or landscape rail). */
    int rot = jobj(config_doc, "display_rotation") ? (int)jnum(config_doc, "display_rotation") : 0;
    if (ui_can_rotate() && rot != applied_rotation) {
        applied_rotation = rot;
        host->set_rotation(rot);
        bool was = ui_landscape;
        read_display_size();
        if (was != ui_landscape) rebuild = true;
        if (ui_editor_is_open()) ui_editor_done(NULL);   /* laid out for the old size */
    }
    if (rebuild) {
        int tab = cur_tab, d = depth;
        const ui_page_t *st[STACK_MAX];
        memcpy(st, stack, sizeof st);
        leave_page();
        build_shell();
        cur_tab = tab; depth = d;
        memcpy(stack, st, sizeof st);
        if (status_doc) update_statusbar();
        show_page();
    }
    if (!asleep && host->set_backlight) {
        double b = jnum(config_doc, "display_brightness");
        host->set_backlight(b > 0 ? (int)b : 80);
    }
}

void ui_preview_backlight(int percent)
{
    if (!asleep && host->set_backlight) host->set_backlight(percent);
}

static void shield_cb(lv_event_t *e)
{
    lv_event_code_t code = lv_event_get_code(e);
    if (code == LV_EVENT_PRESSED && asleep) {
        asleep = false;
        apply_display_config();
        lv_display_trigger_activity(disp);
        poll_cb(NULL);
    } else if (code == LV_EVENT_RELEASED || code == LV_EVENT_PRESS_LOST) {
        /* the touch that woke the screen must not also press what is underneath */
        if (shield) { lv_obj_delete_async(shield); shield = NULL; }
    }
}

static void go_sleep(void)
{
    if (asleep) return;
    asleep = true;
    if (host->set_backlight) host->set_backlight(0);
    if (ui_lock_required() && unlocked) {
        unlocked = false;
        if (ui_editor_is_open()) ui_editor_done(NULL);
        ui_nav_rebuild();
    }
    if (!shield) {
        shield = lv_obj_create(lv_layer_sys());
        lv_obj_remove_style_all(shield);
        lv_obj_set_size(shield, LV_PCT(100), LV_PCT(100));
        lv_obj_add_flag(shield, LV_OBJ_FLAG_CLICKABLE);
        lv_obj_add_event_cb(shield, shield_cb, LV_EVENT_ALL, NULL);
    }
}

static void wake(void)
{
    if (!asleep) return;
    asleep = false;
    if (shield) { lv_obj_delete(shield); shield = NULL; }
    apply_display_config();
    lv_display_trigger_activity(disp);
    poll_cb(NULL);
}

void rrc_ui_user_key(void)
{
    if (asleep) wake(); else go_sleep();
}

static void idle_cb(lv_timer_t *t)
{
    (void)t;
    if (asleep || !config_doc) return;
    double timeout = jobj(config_doc, "display_timeout_s") ? jnum(config_doc, "display_timeout_s") : 60;
    if (timeout > 0 && lv_display_get_inactive_time(disp) > (uint32_t)(timeout * 1000)) go_sleep();
}

/* ---- start ----------------------------------------------------------------------------- */
void rrc_ui_start(const rrc_ui_host_t *h, lv_display_t *d)
{
    host = h;
    disp = d;
    read_display_size();
    /* The host already turned the panel the way the saved config says (so there is
     * no flash of the other layout at boot); the first config load confirms it. */
    switch (lv_display_get_rotation(disp)) {
    case LV_DISPLAY_ROTATION_90: applied_rotation = 90; break;
    case LV_DISPLAY_ROTATION_180: applied_rotation = 180; break;
    case LV_DISPLAY_ROTATION_270: applied_rotation = 270; break;
    default: applied_rotation = 0; break;
    }
    ui_theme_init(disp, true);
    build_shell();
    cur_tab = 0;
    depth = 0;
    show_page();
    ui_config_reload();
    poll_cb(NULL);
    lv_timer_create(poll_cb, 1000, NULL);
    lv_timer_create(idle_cb, 500, NULL);
}

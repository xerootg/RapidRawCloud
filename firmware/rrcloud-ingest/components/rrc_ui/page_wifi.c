/*
 * Wi-Fi — the radio switch, the current association in detail, and the
 * networks in range. Joining a network is a config patch {wifi_ssid,
 * wifi_password} like the web page's; the dock keeps a live association when
 * the same credentials are saved again (net.c), so nothing here can drop the
 * link by accident.
 *
 * The page is laid out so a list of known networks (field-cache roaming) can
 * replace the single "Current network" card without changing its structure.
 */
#include "ui_priv.h"
#include <stdio.h>
#include <string.h>

#define SCAN_ROWS 24

static struct {
    lv_obj_t *sw, *it_switch, *cur_card, *cur_sig, *cur_ssid, *cur_state, *btn_forget;
    lv_obj_t *details, *d_ip, *d_gw, *d_mask, *d_dns, *d_signal, *d_chan, *d_sec, *d_phy, *d_bssid, *d_mac, *d_since;
    lv_obj_t *scan_hdr, *scan_list, *scan_note;
    bool wifi_inflight, scan_inflight, scan_running, enabled;
    int ticks;
    char ssid[33];
    char rows[SCAN_ROWS][33];
    bool rows_open[SCAN_ROWS];
    char pending_ssid[33];
} w;

/* ---- actions --------------------------------------------------------------------- */
static void scan_started(int status, const cJSON *json, void *ud)
{
    (void)ud;
    if (status == 200) { w.scan_running = true; if (w.scan_note) lv_label_set_text(w.scan_note, "Scanning…"); }
    else if (status != 409 || strstr(ui_resp_error(status, json), "already") == NULL) ui_snackbar("%s", ui_resp_error(status, json));
    else w.scan_running = true;
}

static void start_scan(void) { if (w.enabled) ui_req("POST", "/api/wifi/scan", "{}", scan_started, NULL); }

static void switch_saved(int status, const cJSON *json, void *ud)
{
    (void)ud;
    if (status != 200) { ui_snackbar("%s", ui_resp_error(status, json)); ui_config_reload(); }
}

static void switch_cb(lv_event_t *e)
{
    (void)e;
    cJSON *p = cJSON_CreateObject();
    cJSON_AddBoolToObject(p, "wifi_enabled", lv_obj_has_state(w.sw, LV_STATE_CHECKED));
    ui_config_patch(p, switch_saved, NULL);
}

static void switch_row_cb(lv_event_t *e)
{
    (void)e;
    if (lv_obj_has_state(w.sw, LV_STATE_CHECKED)) lv_obj_remove_state(w.sw, LV_STATE_CHECKED); else lv_obj_add_state(w.sw, LV_STATE_CHECKED);
    lv_obj_send_event(w.sw, LV_EVENT_VALUE_CHANGED, NULL);
}

static void joined(int status, const cJSON *json, void *ud)
{
    (void)ud;
    if (status == 200) {
        ui_editor_done(NULL);
        ui_snackbar("Connecting to %s…", w.pending_ssid);
    } else {
        if (ui_editor_is_open()) ui_editor_done(ui_resp_error(status, json));
        else ui_snackbar("%s", ui_resp_error(status, json));
    }
}

static void join(const char *ssid, const char *password)
{
    if (ssid != w.pending_ssid) snprintf(w.pending_ssid, sizeof w.pending_ssid, "%s", ssid);
    cJSON *p = cJSON_CreateObject();
    cJSON_AddStringToObject(p, "wifi_ssid", ssid);
    cJSON_AddStringToObject(p, "wifi_password", password);
    ui_config_patch(p, joined, NULL);
}

static void password_entered(const char *pw, void *ud)
{
    (void)ud;
    if (!w.pending_ssid[0]) { ui_editor_done("Pick the network again"); return; }   /* never post an empty SSID: that is the forget path */
    join(w.pending_ssid, pw);
}

static void ask_password(const char *ssid, bool may_be_open)
{
    if (ssid != w.pending_ssid) snprintf(w.pending_ssid, sizeof w.pending_ssid, "%s", ssid);
    char title[64];
    snprintf(title, sizeof title, "%s", ssid);
    ui_editor_t ed = {.title = title, .label = "Password", .mode = EDIT_PASSWORD, .max_len = 63, .allow_empty = may_be_open,
                      .help = may_be_open ? "WPA2/WPA3 passphrase, 8–63 characters. Leave empty for an open network." : "WPA2/WPA3 passphrase, 8–63 characters.",
                      .save = password_entered};
    ui_editor_open(&ed);
}

static void join_open_confirmed(void *ud) { (void)ud; join(w.pending_ssid, ""); }

static void row_cb(lv_event_t *e)
{
    int i = (int)(intptr_t)lv_event_get_user_data(e);
    if (i < 0 || i >= SCAN_ROWS || !w.rows[i][0]) return;
    snprintf(w.pending_ssid, sizeof w.pending_ssid, "%s", w.rows[i]);
    if (w.rows_open[i]) {
        static char body[200];
        snprintf(body, sizeof body, "%s is an open network: traffic to the dock's web page is not encrypted on the air. Uploads stay TLS-protected.", w.rows[i]);
        ui_dialog("Join open network?", body, "Join", false, join_open_confirmed, NULL);
    } else ask_password(w.rows[i], false);
}

static void manual_ssid_entered(const char *ssid, void *ud)
{
    (void)ud;
    char s[33];
    snprintf(s, sizeof s, "%s", ssid);
    ui_editor_done(NULL);
    ask_password(s, true);
}

static void add_cb(lv_event_t *e)
{
    (void)e;
    ui_editor_t ed = {.title = "Add network", .label = "Network name (SSID)", .mode = EDIT_TEXT, .max_len = 32,
                      .help = "For a hidden network or one out of range right now.", .save = manual_ssid_entered};
    ui_editor_open(&ed);
}

static void forgot(int status, const cJSON *json, void *ud)
{
    (void)ud;
    if (status == 200) ui_snackbar("Network forgotten");
    else ui_snackbar("%s", ui_resp_error(status, json));
}

static void forget_confirmed(void *ud)
{
    (void)ud;
    cJSON *p = cJSON_CreateObject();
    cJSON_AddStringToObject(p, "wifi_ssid", "");
    cJSON_AddStringToObject(p, "wifi_password", "");
    ui_config_patch(p, forgot, NULL);
}

static void forget_cb(lv_event_t *e)
{
    (void)e;
    static char body[160];
    snprintf(body, sizeof body, "The dock disconnects from %s and deletes its password.%s", w.ssid,
             jbool(ui_status(), "has_ethernet") ? "" : " With no other network it goes offline until you join one here.");
    ui_dialog("Forget network?", body, "Forget", true, forget_confirmed, NULL);
}

/* ---- rendering ---------------------------------------------------------------------- */
static lv_obj_t *detail(lv_obj_t *list, const char *head) { return ui_list_item(list, NULL, head, "—"); }

static void build(lv_obj_t *c)
{
    memset(&w, 0, sizeof w);
    /* master switch, M3 "primary container" banner style */
    lv_obj_t *l = ui_column(c, 0);
    lv_obj_set_style_bg_color(l, M.primary_container, 0);
    lv_obj_set_style_bg_opa(l, LV_OPA_COVER, 0);
    lv_obj_set_style_radius(l, 28, 0);
    w.it_switch = ui_list_item(l, NULL, "Use Wi-Fi", "");
    lv_obj_set_style_min_height(w.it_switch, 84, 0);
    lv_obj_t *head = lv_obj_get_child(lv_obj_get_child(w.it_switch, 1), 0);
    lv_obj_set_style_text_font(head, UI_FONT_TITLE, 0);
    lv_obj_set_style_text_color(head, M.on_primary_container, 0);
    ui_item_supporting_color(w.it_switch, M.on_primary_container);
    lv_obj_add_flag(w.it_switch, LV_OBJ_FLAG_CLICKABLE);
    lv_obj_add_event_cb(w.it_switch, switch_row_cb, LV_EVENT_CLICKED, NULL);
    w.sw = ui_item_switch(w.it_switch, false);
    lv_obj_add_event_cb(w.sw, switch_cb, LV_EVENT_VALUE_CHANGED, NULL);

    /* current network */
    w.cur_card = ui_card(c);
    lv_obj_t *r = ui_row(w.cur_card, 16);
    w.cur_sig = ui_signal(r, 28);
    lv_obj_t *col = ui_column(r, 2);
    lv_obj_set_flex_grow(col, 1);
    lv_obj_set_width(col, 0);
    w.cur_ssid = ui_label(col, UI_FONT_TITLE, M.on_surface, "");
    lv_label_set_long_mode(w.cur_ssid, LV_LABEL_LONG_MODE_DOTS);
    lv_obj_set_width(w.cur_ssid, LV_PCT(100));
    w.cur_state = ui_label(col, UI_FONT_BODY, M.on_surface_variant, "");
    lv_obj_set_width(w.cur_state, LV_PCT(100));
    lv_label_set_long_mode(w.cur_state, LV_LABEL_LONG_MODE_WRAP);
    w.btn_forget = ui_button(w.cur_card, BTN_OUTLINED, NULL, "Forget network", forget_cb, NULL);

    w.details = ui_section(c, "Connection details");
    w.d_ip = detail(w.details, "IP address");
    w.d_gw = detail(w.details, "Gateway");
    w.d_mask = detail(w.details, "Subnet mask");
    w.d_dns = detail(w.details, "DNS");
    w.d_signal = detail(w.details, "Signal");
    w.d_chan = detail(w.details, "Channel");
    w.d_sec = detail(w.details, "Security");
    w.d_phy = detail(w.details, "Standard");
    w.d_bssid = detail(w.details, "Access point (BSSID)");
    w.d_mac = detail(w.details, "Dock MAC address");
    w.d_since = detail(w.details, "Connected for");

    w.scan_list = ui_section(c, "Networks in range");
    w.scan_hdr = lv_obj_get_child(c, (int32_t)lv_obj_get_index(w.scan_list) - 1);
    w.scan_note = ui_label(c, UI_FONT_SMALL, M.on_surface_variant, "");
    lv_obj_set_style_pad_left(w.scan_note, 8, 0);
    lv_obj_t *add = ui_list_item(ui_column(c, 0), ICON_ADD, "Add network", "Hidden or out-of-range network");
    lv_obj_t *addwrap = lv_obj_get_parent(add);
    lv_obj_set_style_bg_color(addwrap, M.surface_container, 0);
    lv_obj_set_style_bg_opa(addwrap, LV_OPA_COVER, 0);
    lv_obj_set_style_radius(addwrap, UI_RADIUS_CARD, 0);
    lv_obj_add_flag(add, LV_OBJ_FLAG_CLICKABLE);
    lv_obj_add_event_cb(add, add_cb, LV_EVENT_CLICKED, NULL);
    ui_appbar_action(ICON_REFRESH, start_scan);
}

static void set_detail(lv_obj_t *it, const char *v) { ui_item_supporting(it, v && v[0] ? v : "—"); }
static void show(lv_obj_t *o, bool on) { if (on) lv_obj_remove_flag(o, LV_OBJ_FLAG_HIDDEN); else lv_obj_add_flag(o, LV_OBJ_FLAG_HIDDEN); }

static const char *quality(int rssi)
{
    return rssi >= -55 ? "excellent" : rssi >= -67 ? "good" : rssi >= -75 ? "fair" : "weak";
}

static void wifi_loaded(int status, const cJSON *j, void *ud)
{
    (void)ud;
    w.wifi_inflight = false;
    if (status != 200 || !j || !w.cur_card) return;
    char buf[96];
    w.enabled = jbool(j, "enabled");
    snprintf(w.ssid, sizeof w.ssid, "%s", jstr(j, "ssid"));
    bool assoc = jbool(j, "associated"), has_ip = jbool(j, "has_ip");
    int rssi = (int)jnum(j, "rssi");
    if (w.enabled) lv_obj_add_state(w.sw, LV_STATE_CHECKED); else lv_obj_remove_state(w.sw, LV_STATE_CHECKED);
    ui_item_supporting(w.it_switch, !w.enabled ? "Off — the dock is offline unless Ethernet is connected"
                                    : jbool(j, "standby") ? "On — standby while Ethernet carries traffic"
                                    : assoc ? "On" : "On — not connected");
    show(w.cur_card, w.enabled && w.ssid[0]);
    show(w.details, w.enabled && assoc);
    show(lv_obj_get_child(lv_obj_get_parent(w.details), (int32_t)lv_obj_get_index(w.details) - 1), w.enabled && assoc);
    show(w.scan_list, w.enabled);
    show(w.scan_hdr, w.enabled);
    show(w.scan_note, w.enabled);
    if (!w.enabled) return;
    if (w.ssid[0]) {
        lv_label_set_text(w.cur_ssid, w.ssid);
        ui_signal_set(w.cur_sig, rssi, assoc, M.primary);
        if (assoc && has_ip) snprintf(buf, sizeof buf, "Connected · %s", jstr(j, "ip"));
        else if (assoc) snprintf(buf, sizeof buf, "Associated · getting an address…");
        else if (jbool(j, "standby")) snprintf(buf, sizeof buf, "Saved · Ethernet is in use");
        else if (jnum(j, "attempts") > 0 && jstr(j, "last_reason_text")[0]) snprintf(buf, sizeof buf, "Not connected: %s (attempt %.0f)", jstr(j, "last_reason_text"), jnum(j, "attempts"));
        else snprintf(buf, sizeof buf, "Connecting…");
        lv_label_set_text(w.cur_state, buf);
        lv_obj_set_style_text_color(w.cur_state, assoc ? M.on_surface_variant : jnum(j, "attempts") > 2 ? M.error : M.on_surface_variant, 0);
    }
    if (assoc) {
        set_detail(w.d_ip, jstr(j, "ip"));
        set_detail(w.d_gw, jstr(j, "gateway"));
        set_detail(w.d_mask, jstr(j, "netmask"));
        set_detail(w.d_dns, jstr(j, "dns"));
        snprintf(buf, sizeof buf, "%d dBm · %s", rssi, quality(rssi));
        set_detail(w.d_signal, buf);
        snprintf(buf, sizeof buf, "%.0f (2.4 GHz)", jnum(j, "channel"));
        set_detail(w.d_chan, buf);
        set_detail(w.d_sec, jstr(j, "security"));
        set_detail(w.d_phy, jstr(j, "phy"));
        set_detail(w.d_bssid, jstr(j, "bssid"));
        set_detail(w.d_mac, jstr(j, "mac"));
        char d[32];
        ui_fmt_duration(d, sizeof d, jnum(j, "connected_s"));
        set_detail(w.d_since, d);
    }
}

static void scan_loaded(int status, const cJSON *j, void *ud)
{
    (void)ud;
    w.scan_inflight = false;
    if (status != 200 || !j || !w.scan_list) return;
    const char *st = jstr(j, "state");
    if (!strcmp(st, "running")) { w.scan_running = true; lv_label_set_text(w.scan_note, "Scanning…"); return; }
    w.scan_running = false;
    if (!strcmp(st, "failed")) { lv_label_set_text(w.scan_note, jstr(j, "error")); return; }
    if (strcmp(st, "done")) { lv_label_set_text(w.scan_note, ""); return; }
    lv_obj_clean(w.scan_list);
    memset(w.rows, 0, sizeof w.rows);
    const cJSON *nets = jobj(j, "networks"), *n;
    int i = 0;
    cJSON_ArrayForEach(n, nets) {
        if (i >= SCAN_ROWS) break;
        const char *ssid = jstr(n, "ssid");
        snprintf(w.rows[i], sizeof w.rows[i], "%s", ssid);
        w.rows_open[i] = jbool(n, "open");
        char sup[96];
        int aps = (int)jnum(n, "aps");
        snprintf(sup, sizeof sup, "%s · ch %.0f%s%s", jstr(n, "security"), jnum(n, "channel"), jbool(n, "wifi6") ? " · Wi-Fi 6" : "",
                 aps > 1 ? " · roaming" : "");
        bool current = !strcmp(ssid, w.ssid);
        lv_obj_t *it = ui_list_item(w.scan_list, NULL, ssid, current ? "Saved network" : sup);
        lv_obj_t *sig = ui_signal(it, 20);
        lv_obj_move_to_index(sig, 0);
        lv_obj_set_style_margin_right(sig, 4, 0);
        ui_signal_set(sig, (int)jnum(n, "rssi"), true, current ? M.primary : M.on_surface_variant);
        if (!w.rows_open[i]) ui_item_trailing_text(it, ICON_LOCK);
        if (current) ui_item_supporting_color(it, M.primary);
        lv_obj_add_flag(it, LV_OBJ_FLAG_CLICKABLE);
        lv_obj_add_event_cb(it, row_cb, LV_EVENT_CLICKED, (void *)(intptr_t)i);
        i++;
    }
    lv_label_set_text(w.scan_note, i ? "" : "No networks found");
}

static void tick(void)
{
    w.ticks++;
    if (!w.wifi_inflight && (w.ticks % 2 == 1)) { w.wifi_inflight = true; ui_req("GET", "/api/wifi", NULL, wifi_loaded, NULL); }
    if (w.scan_running && !w.scan_inflight) { w.scan_inflight = true; ui_req("GET", "/api/wifi/scan", NULL, scan_loaded, NULL); }
    /* refresh the list every 30 s while the page is open */
    if (w.enabled && w.ticks % 30 == 2 && !w.scan_running) start_scan();
}

static void on_config(const cJSON *cfg)
{
    bool en = jbool(cfg, "wifi_enabled");
    if (en && !w.enabled) { w.enabled = true; start_scan(); }
    w.enabled = en;
    w.wifi_inflight = true;
    ui_req("GET", "/api/wifi", NULL, wifi_loaded, NULL);
}

static void destroy(void) { memset(&w, 0, sizeof w); }

const ui_page_t page_wifi = {.title = "Wi-Fi", .build = build, .on_config = on_config, .tick = tick, .destroy = destroy, .locked = true};

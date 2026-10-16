/*
 * Status — the screen the dock shows most of the time: what the sync is doing
 * (hero card with progress, rate and ETA), the camera, cloud and network at a
 * glance, and the library counters. Mirrors the web page's Status card.
 */
#include "ui_priv.h"
#include <stdio.h>
#include <string.h>

static struct {
    lv_obj_t *chip, *headline, *progress, *file, *stats, *stages, *error;
    lv_obj_t *btn_sync, *btn_cancel;
    lv_obj_t *it_camera, *it_cloud, *it_net, *it_clock;
    lv_obj_t *it_lifetime, *it_journal, *it_lastrun, *it_published;
} w;

static void posted(int status, const cJSON *json, void *ud)
{
    if (status == 200) ui_snackbar("%s", (const char *)ud);
    else ui_snackbar("%s", ui_resp_error(status, json));
}

static void sync_now_cb(lv_event_t *e) { (void)e; ui_req_global("POST", "/api/sync/now", "{}", posted, "Sync requested"); }
static void cancel_cb(lv_event_t *e) { (void)e; ui_req_global("POST", "/api/sync/cancel", "{}", posted, "Cancelling…"); }
static void usb_cb(lv_event_t *e) { (void)e; ui_req_global("POST", "/api/usb/reset", "{}", posted, "USB port power-cycled"); }
static void net_cb(lv_event_t *e) { (void)e; ui_nav_tab(2); }
static void cloud_cb(lv_event_t *e) { (void)e; ui_nav_tab(1); }

static lv_obj_t *nav_item(lv_obj_t *list, const char *sym, const char *head, lv_event_cb_t cb)
{
    lv_obj_t *it = ui_list_item(list, sym, head, "—");
    if (cb) {
        lv_obj_add_flag(it, LV_OBJ_FLAG_CLICKABLE);
        lv_obj_add_event_cb(it, cb, LV_EVENT_CLICKED, NULL);
        ui_item_trailing_text(it, ICON_CHEVRON);
    }
    return it;
}

static void build(lv_obj_t *c)
{
    memset(&w, 0, sizeof w);
    lv_obj_t *hero = ui_card(c);
    lv_obj_set_style_bg_color(hero, M.surface_container_high, 0);
    lv_obj_set_style_pad_row(hero, 12, 0);
    w.chip = ui_chip(hero, "…", TONE_NEUTRAL);
    w.headline = ui_label(hero, UI_FONT_TITLE, M.on_surface, "");
    lv_obj_set_width(w.headline, LV_PCT(100));
    lv_label_set_long_mode(w.headline, LV_LABEL_LONG_MODE_WRAP);
    w.progress = ui_progress(hero);
    w.file = ui_label(hero, UI_FONT_SMALL, M.on_surface_variant, "");
    lv_obj_set_width(w.file, LV_PCT(100));
    lv_label_set_long_mode(w.file, LV_LABEL_LONG_MODE_DOTS);
    w.stats = ui_label(hero, UI_FONT_BODY, M.on_surface, "");
    lv_obj_set_width(w.stats, LV_PCT(100));
    lv_label_set_long_mode(w.stats, LV_LABEL_LONG_MODE_WRAP);
    w.stages = ui_label(hero, UI_FONT_SMALL, M.on_surface_variant, "");
    lv_obj_set_width(w.stages, LV_PCT(100));
    lv_label_set_long_mode(w.stages, LV_LABEL_LONG_MODE_WRAP);
    w.error = ui_label(hero, UI_FONT_SMALL, M.error, "");
    lv_obj_set_width(w.error, LV_PCT(100));
    lv_label_set_long_mode(w.error, LV_LABEL_LONG_MODE_WRAP);
    lv_obj_t *row = ui_row(hero, 8);
    lv_obj_set_style_pad_top(row, 4, 0);
    w.btn_sync = ui_button(row, BTN_FILLED, ICON_SYNC, "Sync now", sync_now_cb, NULL);
    w.btn_cancel = ui_button(row, BTN_TONAL, NULL, "Cancel", cancel_cb, NULL);
    ui_button(row, BTN_TEXT, NULL, "Retry USB", usb_cb, NULL);

    lv_obj_t *dev = ui_section(c, "Connections");
    w.it_camera = nav_item(dev, ICON_CAMERA, "Camera", NULL);
    w.it_cloud = nav_item(dev, ICON_CLOUD, "Cloud", cloud_cb);
    w.it_net = nav_item(dev, ICON_WIFI, "Network", net_cb);
    w.it_clock = nav_item(dev, ICON_SCHEDULE, "Clock", NULL);

    lv_obj_t *lib = ui_section(c, "Library");
    w.it_lifetime = nav_item(lib, ICON_PHOTO_LIBRARY, "Uploaded by this dock", NULL);
    w.it_lastrun = nav_item(lib, ICON_HISTORY, "Last sync", NULL);
    w.it_journal = nav_item(lib, ICON_ARTICLE, "Journal", NULL);
    w.it_published = nav_item(lib, ICON_INVENTORY, "Manifest and heartbeat", NULL);
}

static const char *phase_text(const char *p, ui_tone_t *tone)
{
    *tone = TONE_PRIMARY;
    if (!strcmp(p, "idle")) { *tone = TONE_SUCCESS; return "Ready"; }
    if (!strcmp(p, "uploading")) return "Uploading";
    if (!strcmp(p, "enumerating")) return "Scanning camera";
    if (!strcmp(p, "publishing")) return "Publishing journal";
    if (!strcmp(p, "waiting_network")) { *tone = TONE_WARNING; return "Waiting for network"; }
    if (!strcmp(p, "unconfigured")) { *tone = TONE_WARNING; return "Cloud not set up"; }
    if (!strcmp(p, "error")) { *tone = TONE_ERROR; return "Error"; }
    *tone = TONE_NEUTRAL;
    return p[0] ? p : "…";
}

static void show(lv_obj_t *o, bool on) { if (on) lv_obj_remove_flag(o, LV_OBJ_FLAG_HIDDEN); else lv_obj_add_flag(o, LV_OBJ_FLAG_HIDDEN); }

static void on_status(const cJSON *s)
{
    const cJSON *y = jobj(s, "sync");
    char a[48], b[48], c2[48], buf[240];
    const char *phase = jstr(y, "phase");
    ui_tone_t tone;
    ui_chip_set(w.chip, phase_text(phase, &tone), tone);
    bool active = !strcmp(phase, "uploading") || !strcmp(phase, "enumerating") || !strcmp(phase, "publishing");
    bool cam = jbool(y, "camera_attached");
    double total = jnum(y, "run_total"), done = jnum(y, "run_done"), skipped = jnum(y, "run_skipped"), failed = jnum(y, "run_failed");
    double bytes = jnum(y, "run_bytes"), rd = jnum(y, "run_read_bytes"), tbytes = jnum(y, "run_total_bytes");
    double el = jnum(y, "run_elapsed_ms") / 1000.0;

    if (active) {
        if (!strcmp(phase, "enumerating")) lv_label_set_text(w.headline, "Looking for new photos…");
        else if (!strcmp(phase, "publishing")) lv_label_set_text(w.headline, "Publishing to the library…");
        else {
            /* snprintf, not lv_label_set_text_fmt: LVGL's built-in printf has no %f */
            snprintf(buf, sizeof buf, "%.0f of %.0f files", done + skipped + failed, total);
            lv_label_set_text(w.headline, buf);
        }
        double frac = tbytes > 0 ? (bytes + rd) / (2 * tbytes) : 0;
        lv_bar_set_value(w.progress, (int32_t)(frac > 1 ? 1000 : frac * 1000), LV_ANIM_ON);
        const char *cf = jstr(y, "current_file");
        if (cf[0]) {
            ui_fmt_bytes(a, sizeof a, jnum(y, "current_done"));
            ui_fmt_bytes(b, sizeof b, jnum(y, "current_size"));
            snprintf(buf, sizeof buf, "%s · %s / %s", cf, a, b);
            lv_label_set_text(w.file, buf);
        } else lv_label_set_text(w.file, "");
        double rate = el > 0 ? (bytes + rd) / 2 / el : 0;
        ui_fmt_bytes(a, sizeof a, bytes);
        ui_fmt_bytes(b, sizeof b, tbytes);
        if (rate > 0 && tbytes > 0) {
            ui_fmt_duration(c2, sizeof c2, (2 * tbytes - bytes - rd) / 2 / rate);
            snprintf(buf, sizeof buf, "%s of %s · %.1f MB/s · %s left", a, b, rate / 1e6, c2);
        } else snprintf(buf, sizeof buf, "%s of %s", a, b);
        lv_label_set_text(w.stats, buf);
        snprintf(buf, sizeof buf, "%.0f uploaded · %.0f already in library · %.0f failed · %.0f in flight", done, skipped, failed, jnum(y, "run_inflight"));
        lv_label_set_text(w.stages, buf);
    } else {
        if (!jbool(y, "s3_ready")) lv_label_set_text(w.headline, "Pair with your cloud to start");
        else if (cam) lv_label_set_text(w.headline, "Camera connected");
        else lv_label_set_text(w.headline, "Plug in a camera");
        lv_label_set_text(w.file, "");
        if (el > 0) {
            ui_fmt_bytes(a, sizeof a, bytes);
            ui_fmt_duration(b, sizeof b, el);
            snprintf(buf, sizeof buf, "Last run: %.0f uploaded, %.0f skipped, %.0f failed · %s in %s", done, skipped, failed, a, b);
            lv_label_set_text(w.stats, buf);
        } else lv_label_set_text(w.stats, "");
        lv_label_set_text(w.stages, "");
    }
    show(w.progress, active);
    show(w.file, active && lv_label_get_text(w.file)[0]);
    show(w.stats, lv_label_get_text(w.stats)[0]);
    show(w.stages, active);
    const char *err = jstr(y, "last_error");
    lv_label_set_text(w.error, err);
    show(w.error, err[0]);
    if (cam && !active && jbool(y, "s3_ready")) lv_obj_remove_state(w.btn_sync, LV_STATE_DISABLED); else lv_obj_add_state(w.btn_sync, LV_STATE_DISABLED);
    show(w.btn_cancel, active);

    /* connections */
    if (cam) {
        const char *ser = jstr(y, "camera_serial");
        snprintf(buf, sizeof buf, "%s%s%s via %s", jstr(y, "camera_model"), ser[0] ? " · sn " : "", ser, jstr(y, "camera_kind"));
        ui_item_supporting(w.it_camera, buf);
    } else if (jnum(s, "usb_devices") > 0) {
        ui_item_supporting(w.it_camera, "A USB device is attached but is not a camera — see the log");
    } else ui_item_supporting(w.it_camera, "None attached");
    if (jbool(y, "s3_ready")) ui_item_supporting(w.it_cloud, jbool(y, "digest_check_known") ? (jbool(y, "backend_rejects_bad_md5") ? "Configured · backend verifies checksums" : "Configured · read-back verification") : "Configured");
    else ui_item_supporting(w.it_cloud, "Not configured — pair or enter S3 settings");
    ui_item_supporting_color(w.it_cloud, jbool(y, "s3_ready") ? M.on_surface_variant : M.error);

    if (!jbool(s, "wifi_enabled") && !jbool(s, "has_ethernet")) snprintf(buf, sizeof buf, "Wi-Fi is off");
    else if (jbool(s, "wifi")) snprintf(buf, sizeof buf, "%s · %s · %.0f dBm", jstr(s, "wifi_ssid"), jstr(s, "network"), jnum(s, "wifi_rssi"));
    else snprintf(buf, sizeof buf, "%s", jstr(s, "network"));
    ui_item_supporting(w.it_net, buf);
    const char *t = jstr(s, "time");   /* 2026-10-16T14:00:01Z */
    if (jbool(s, "time_synced") && strlen(t) >= 16) snprintf(buf, sizeof buf, "%.10s %.5s UTC", t, t + 11);
    else snprintf(buf, sizeof buf, "Not synchronized (needs network)");
    ui_item_supporting(w.it_clock, buf);

    double now = ui_now();
    ui_fmt_bytes(a, sizeof a, jnum(y, "lifetime_bytes"));
    snprintf(buf, sizeof buf, "%.0f files · %s", jnum(y, "lifetime_uploaded"), a);
    ui_item_supporting(w.it_lifetime, buf);
    ui_fmt_ago(a, sizeof a, jnum(y, "last_run_ts"), now);
    ui_item_supporting(w.it_lastrun, a);
    snprintf(buf, sizeof buf, "Published up to #%.0f · %.0f pending", jnum(y, "published_cursor"), jnum(y, "pending_entries"));
    ui_item_supporting(w.it_journal, buf);
    ui_fmt_ago(a, sizeof a, jnum(y, "last_manifest_ts"), now);
    ui_fmt_ago(b, sizeof b, jnum(y, "last_heartbeat_ts"), now);
    snprintf(buf, sizeof buf, "Manifest %s · heartbeat %s", a, b);
    ui_item_supporting(w.it_published, buf);
}

static void destroy(void) { memset(&w, 0, sizeof w); }

const ui_page_t page_home = {.title = "Status", .build = build, .on_status = on_status, .destroy = destroy};

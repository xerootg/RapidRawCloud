/*
 * Sync — what to upload and where (the web page's "What to sync" and "Cloud"
 * sections), plus the two cloud sub-pages: pairing through the pairing service
 * (device-code flow; the code and a QR code of the verification link are shown
 * here, so a phone can approve it by scanning the screen) and manual S3.
 */
#include "ui_priv.h"
#include <stdio.h>
#include <string.h>

/* ---- Sync ---------------------------------------------------------------------------- */
static const ui_setting_t s_auto = {.symbol = ICON_USB, .headline = "Sync on plug-in", .key = "auto_sync", .kind = BIND_SWITCH,
                                    .help = "Start uploading as soon as a camera is connected"};
static const ui_setting_t s_include = {.symbol = ICON_FILTER, .headline = "Include patterns", .key = "include_globs", .kind = BIND_TEXT, .max_len = 255,
                                       .placeholder = "*.nef *.dng *.jpg",
                                       .help = "Space-separated, case-insensitive globs: * within a folder, ** across folders, ?, [abc]. A pattern with / matches the camera path (DCIM/100NIKON/*.NEF)."};
static const ui_setting_t s_exclude = {.symbol = ICON_FILTER_OFF, .headline = "Exclude patterns", .key = "exclude_globs", .kind = BIND_TEXT, .max_len = 255,
                                       .placeholder = "None", .help = "Files matching any of these are skipped, e.g. *_small.jpg"};
static const ui_setting_t s_videos = {.symbol = ICON_VIDEOCAM, .headline = "Upload videos", .key = "upload_videos", .kind = BIND_SWITCH,
                                      .help = "Adds *.mov and *.mp4"};
static const ui_setting_t s_minsize = {.symbol = ICON_SIZE, .headline = "Skip files smaller than", .key = "min_size_kb", .kind = BIND_NUMBER,
                                       .suffix = " KB", .max_len = 7, .help = "Keeps thumbnails and sidecar stubs out of the library"};
static const ui_setting_t s_root = {.symbol = ICON_FOLDER, .headline = "Card folder to scan", .key = "msc_root", .kind = BIND_TEXT, .max_len = 63,
                                    .placeholder = "Whole card", .help = "Mass-storage cameras only (e.g. Sigma fp). Usually DCIM; empty scans the whole card."};
static const ui_setting_t s_template = {.symbol = ICON_ROUTE, .headline = "Library path template", .key = "key_template", .kind = BIND_TEXT, .max_len = 159,
                                        .help = "Where files land under library/. {name} {stem} {ext} {path} {model} {serial} {yyyy} {mm} {dd} {hh}"};

static lv_obj_t *it_pair, *it_manual;

static void pair_cb(lv_event_t *e) { (void)e; ui_nav_push(&page_pairing); }
static void manual_cb(lv_event_t *e) { (void)e; ui_nav_push(&page_cloud); }

static void sync_build(lv_obj_t *c)
{
    lv_obj_t *l = ui_section(c, "Automation");
    ui_setting(l, &s_auto);
    l = ui_section(c, "Files");
    ui_setting(l, &s_include);
    ui_setting(l, &s_exclude);
    ui_setting(l, &s_videos);
    ui_setting(l, &s_minsize);
    ui_setting(l, &s_root);
    l = ui_section(c, "Destination");
    ui_setting(l, &s_template);
    l = ui_section(c, "Cloud");
    it_pair = ui_list_item(l, ICON_LINK, "Pair with cloud", "Sign in with a code — same account as the RapidRAW app");
    lv_obj_add_flag(it_pair, LV_OBJ_FLAG_CLICKABLE);
    lv_obj_add_event_cb(it_pair, pair_cb, LV_EVENT_CLICKED, NULL);
    ui_item_trailing_text(it_pair, ICON_CHEVRON);
    it_manual = ui_list_item(l, ICON_STORAGE, "Manual S3 settings", "");
    lv_obj_add_flag(it_manual, LV_OBJ_FLAG_CLICKABLE);
    lv_obj_add_event_cb(it_manual, manual_cb, LV_EVENT_CLICKED, NULL);
    ui_item_trailing_text(it_manual, ICON_CHEVRON);
}

static void sync_config(const cJSON *cfg)
{
    char buf[200];
    const char *ep = jstr(cfg, "s3_endpoint"), *bk = jstr(cfg, "s3_bucket");
    if (ep[0]) snprintf(buf, sizeof buf, "%s · %s", ep, bk[0] ? bk : "no bucket");
    else snprintf(buf, sizeof buf, "Not configured");
    ui_item_supporting(it_manual, buf);
}

static void sync_destroy(void) { it_pair = it_manual = NULL; }

const ui_page_t page_sync = {.title = "Sync", .build = sync_build, .on_config = sync_config, .destroy = sync_destroy, .locked = true};

/* ---- Manual S3 ----------------------------------------------------------------------- */
static const ui_setting_t s_ep = {.symbol = ICON_DNS, .headline = "Endpoint", .key = "s3_endpoint", .kind = BIND_TEXT, .max_len = 159,
                                  .placeholder = "https://s3.example.com", .help = "Base URL of the S3 API (path-style)."};
static const ui_setting_t s_bucket = {.symbol = ICON_STORAGE, .headline = "Bucket", .key = "s3_bucket", .kind = BIND_TEXT, .max_len = 63};
static const ui_setting_t s_region = {.symbol = ICON_PUBLIC, .headline = "Region", .key = "s3_region", .kind = BIND_TEXT, .max_len = 31,
                                      .placeholder = "garage"};
static const ui_setting_t s_ak = {.symbol = ICON_KEY, .headline = "Access key ID", .key = "s3_access_key", .kind = BIND_TEXT, .max_len = 79};
static const ui_setting_t s_sk = {.symbol = ICON_PASSWORD, .headline = "Secret access key", .key = "s3_secret_key", .has_key = "has_s3_secret",
                                  .kind = BIND_SECRET, .max_len = 128,
                                  .help = "Write-only: stored on the dock and never shown again. Saving an empty value clears it."};
static const ui_setting_t s_tls = {.symbol = ICON_WARNING, .headline = "Skip TLS verification", .key = "s3_tls_insecure", .kind = BIND_SWITCH,
                                   .help = "Only for a self-signed endpoint on a network you trust"};

static void cloud_build(lv_obj_t *c)
{
    lv_obj_t *n = ui_label(c, UI_FONT_SMALL, M.on_surface_variant, "Pairing fills these in. Edit them only for a bucket without a pairing service.");
    lv_obj_set_width(n, LV_PCT(100));
    lv_label_set_long_mode(n, LV_LABEL_LONG_MODE_WRAP);
    lv_obj_set_style_pad_hor(n, 8, 0);
    lv_obj_t *l = ui_section(c, "Bucket");
    ui_setting(l, &s_ep);
    ui_setting(l, &s_bucket);
    ui_setting(l, &s_region);
    l = ui_section(c, "Credentials");
    ui_setting(l, &s_ak);
    ui_setting(l, &s_sk);
    ui_setting(l, &s_tls);
}

const ui_page_t page_cloud = {.title = "Manual S3", .build = cloud_build, .locked = true};

/* ---- Pairing ------------------------------------------------------------------------- */
static struct {
    lv_obj_t *it_url, *card, *chip, *msg, *code, *uri, *qr, *btn_cancel;
    char url[160];
    char qr_data[400];
    bool polling, inflight;
} pr;

static void pair_begun(int status, const cJSON *json, void *ud)
{
    (void)ud;
    if (status == 200) { ui_editor_done(NULL); pr.polling = true; }
    else ui_editor_done(ui_resp_error(status, json));
}

static void pair_url_saved(const char *value, void *ud)
{
    (void)ud;
    snprintf(pr.url, sizeof pr.url, "%s", value);
    if (pr.it_url) ui_item_supporting(pr.it_url, pr.url);
    cJSON *b = cJSON_CreateObject();
    cJSON_AddStringToObject(b, "url", pr.url);
    char *body = cJSON_PrintUnformatted(b);
    cJSON_Delete(b);
    ui_req_global("POST", "/api/pair/begin", body, pair_begun, NULL);
    cJSON_free(body);
}

static void pair_start_cb(lv_event_t *e)
{
    (void)e;
    ui_editor_t ed = {.title = "Pair with cloud", .label = "Pairing service", .initial = pr.url, .placeholder = "rrc.example.com",
                      .help = "The address of your RapidRawCloud pairing service. The dock then shows a code to approve.",
                      .mode = EDIT_TEXT, .max_len = 159, .save = pair_url_saved};
    ui_editor_open(&ed);
}

static void pair_cancelled(int status, const cJSON *json, void *ud) { (void)status; (void)json; (void)ud; }
static void pair_cancel_cb(lv_event_t *e) { (void)e; ui_req_global("POST", "/api/pair/cancel", "{}", pair_cancelled, NULL); }

static void pairing_status(int status, const cJSON *p, void *ud)
{
    (void)ud;
    pr.inflight = false;
    if (status != 200 || !p || !pr.card) return;
    const char *st = jstr(p, "state"), *msg = jstr(p, "message");
    bool waiting = !strcmp(st, "waiting_user") || !strcmp(st, "polling");
    bool idle = !strcmp(st, "idle");
    if (idle) { lv_obj_add_flag(pr.card, LV_OBJ_FLAG_HIDDEN); pr.polling = false; return; }
    lv_obj_remove_flag(pr.card, LV_OBJ_FLAG_HIDDEN);
    ui_tone_t tone = !strcmp(st, "done") ? TONE_SUCCESS : !strcmp(st, "failed") ? TONE_ERROR : TONE_PRIMARY;
    const char *label = !strcmp(st, "discovering") ? "Contacting the service" : waiting ? "Waiting for approval" : !strcmp(st, "done") ? "Paired" : !strcmp(st, "failed") ? "Failed" : st;
    ui_chip_set(pr.chip, label, tone);
    lv_label_set_text(pr.msg, msg);
    const char *code = jstr(p, "user_code"), *uri = jstr(p, "verification_uri"), *full = jstr(p, "verification_uri_complete");
    bool show = waiting && code[0];
    if (show) {
        lv_label_set_text(pr.code, code);
        lv_label_set_text_fmt(pr.uri, "Open %s and enter the code, or scan:", uri);
        const char *q = full[0] ? full : uri;
        if (strcmp(q, pr.qr_data)) {
            snprintf(pr.qr_data, sizeof pr.qr_data, "%s", q);
            lv_qrcode_update(pr.qr, pr.qr_data, (uint32_t)strlen(pr.qr_data));
        }
    }
    if (show) { lv_obj_remove_flag(pr.code, LV_OBJ_FLAG_HIDDEN); lv_obj_remove_flag(pr.uri, LV_OBJ_FLAG_HIDDEN); lv_obj_remove_flag(pr.qr, LV_OBJ_FLAG_HIDDEN); }
    else { lv_obj_add_flag(pr.code, LV_OBJ_FLAG_HIDDEN); lv_obj_add_flag(pr.uri, LV_OBJ_FLAG_HIDDEN); lv_obj_add_flag(pr.qr, LV_OBJ_FLAG_HIDDEN); }
    bool running = waiting || !strcmp(st, "discovering");
    if (running) lv_obj_remove_flag(pr.btn_cancel, LV_OBJ_FLAG_HIDDEN); else lv_obj_add_flag(pr.btn_cancel, LV_OBJ_FLAG_HIDDEN);
    if (!strcmp(st, "done") && pr.polling) { ui_snackbar("Paired — cloud settings applied"); ui_config_reload(); }
    pr.polling = running;
}

static void pairing_tick(void)
{
    if (pr.inflight) return;
    pr.inflight = true;
    ui_req("GET", "/api/pair/status", NULL, pairing_status, NULL);
}

static void pairing_build(lv_obj_t *c)
{
    memset(&pr, 0, sizeof pr);
    snprintf(pr.url, sizeof pr.url, "%s", jstr(ui_config(), "pairing_url"));
    lv_obj_t *n = ui_label(c, UI_FONT_BODY, M.on_surface_variant,
                           "The dock signs in with a device code and configures its bucket itself — the same account as your RapidRAW app.");
    lv_obj_set_width(n, LV_PCT(100));
    lv_label_set_long_mode(n, LV_LABEL_LONG_MODE_WRAP);
    lv_obj_set_style_pad_hor(n, 8, 0);
    lv_obj_t *l = ui_section(c, "Service");
    pr.it_url = ui_list_item(l, ICON_DNS, "Pairing service", pr.url[0] ? pr.url : "Not set");
    lv_obj_add_flag(pr.it_url, LV_OBJ_FLAG_CLICKABLE);
    lv_obj_add_event_cb(pr.it_url, pair_start_cb, LV_EVENT_CLICKED, NULL);
    lv_obj_t *row = ui_row(c, 8);
    ui_button(row, BTN_FILLED, ICON_QR, "Start pairing", pair_start_cb, NULL);

    pr.card = ui_card(c);
    lv_obj_set_flex_align(pr.card, LV_FLEX_ALIGN_START, LV_FLEX_ALIGN_CENTER, LV_FLEX_ALIGN_CENTER);
    pr.chip = ui_chip(pr.card, "…", TONE_PRIMARY);
    pr.msg = ui_label(pr.card, UI_FONT_SMALL, M.on_surface_variant, "");
    lv_obj_set_width(pr.msg, LV_PCT(100));
    lv_label_set_long_mode(pr.msg, LV_LABEL_LONG_MODE_WRAP);
    pr.uri = ui_label(pr.card, UI_FONT_BODY, M.on_surface, "");
    lv_obj_set_width(pr.uri, LV_PCT(100));
    lv_label_set_long_mode(pr.uri, LV_LABEL_LONG_MODE_WRAP);
    pr.code = ui_label(pr.card, UI_FONT_DISPLAY, M.primary, "");
    lv_obj_set_style_text_letter_space(pr.code, 4, 0);
    pr.qr = lv_qrcode_create(pr.card);
    lv_qrcode_set_size(pr.qr, 220);
    lv_qrcode_set_dark_color(pr.qr, lv_color_hex(0x000000));
    lv_qrcode_set_light_color(pr.qr, lv_color_hex(0xFFFFFF));
    lv_obj_set_style_border_color(pr.qr, lv_color_hex(0xFFFFFF), 0);
    lv_obj_set_style_border_width(pr.qr, 10, 0);
    pr.btn_cancel = ui_button(pr.card, BTN_TONAL, NULL, "Cancel pairing", pair_cancel_cb, NULL);
    lv_obj_add_flag(pr.card, LV_OBJ_FLAG_HIDDEN);
    pairing_tick();
}

static void pairing_destroy(void) { memset(&pr, 0, sizeof pr); }

const ui_page_t page_pairing = {.title = "Pair with cloud", .build = pairing_build, .tick = pairing_tick, .destroy = pairing_destroy, .locked = true};

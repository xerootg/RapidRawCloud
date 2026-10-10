#include "app_config.h"
#include <string.h>
#include <stdio.h>
#include <time.h>
#include "nvs_flash.h"
#include "nvs.h"
#include "esp_log.h"
#include "esp_random.h"
#include "cJSON.h"
#include "rrc_proto.h"
#include "board.h"

static const char *TAG = "cfg";
#define NS "rrc"

static app_config_t cur;
static char device_id[RRC_DEVICE_ID_LEN + 1];
static int64_t device_created;

static void defaults(app_config_t *c)
{
    memset(c, 0, sizeof *c);
    strcpy(c->s3_region, "garage");
    strcpy(c->include_globs, "*.nef *.nrw *.dng *.jpg *.jpeg *.heif *.hif *.tif *.tiff");
    strcpy(c->exclude_globs, "");
    strcpy(c->key_template, "Camera Import/{model}/{yyyy}/{mm}/{dd}/{name}");
    strcpy(c->msc_root, "DCIM");
    c->auto_sync = true;
    c->min_size_kb = 64;
    strcpy(c->device_name, "Camera dock");
    strcpy(c->hostname, BOARD_HOSTNAME_DEFAULT);
    c->admin_auth = false;
    c->usb_debug = true;   /* pre-release default: camera bring-up is the open question */
}

static esp_err_t nvs_get_string(nvs_handle_t h, const char *key, char *out, size_t cap)
{
    size_t len = cap;
    esp_err_t e = nvs_get_str(h, key, out, &len);
    if (e != ESP_OK) out[0] = 0;
    return e;
}

static void load(void)
{
    defaults(&cur);
    nvs_handle_t h;
    if (nvs_open(NS, NVS_READONLY, &h) != ESP_OK) return;
    char *blob = NULL;
    size_t len = 0;
    if (nvs_get_str(h, "cfg", NULL, &len) == ESP_OK && len > 0 && len < 8192) {
        blob = malloc(len);
        if (blob && nvs_get_str(h, "cfg", blob, &len) == ESP_OK) {
            char err[64];
            app_config_apply_json(blob, strlen(blob), err, sizeof err);
        }
        free(blob);
    }
    nvs_close(h);
}

static esp_err_t load_or_mint_identity(void)
{
    nvs_handle_t h;
    esp_err_t e = nvs_open(NS, NVS_READWRITE, &h);
    if (e != ESP_OK) return e;
    if (nvs_get_string(h, "device_id", device_id, sizeof device_id) != ESP_OK || !rrc_device_id_valid(device_id)) {
        uint8_t rnd[16];
        esp_fill_random(rnd, sizeof rnd);
        rrc_uuid4_format(rnd, device_id);
        device_created = (int64_t)time(NULL);
        if (device_created < 1600000000) device_created = 0; /* fixed up on first heartbeat if clock unset */
        nvs_set_str(h, "device_id", device_id);
        nvs_set_i64(h, "created", device_created);
        nvs_commit(h);
        ESP_LOGI(TAG, "minted device id %s", device_id);
    } else {
        if (nvs_get_i64(h, "created", &device_created) != ESP_OK) device_created = 0;
    }
    nvs_close(h);
    return ESP_OK;
}

esp_err_t app_config_init(void)
{
    esp_err_t e = nvs_flash_init();
    if (e == ESP_ERR_NVS_NO_FREE_PAGES || e == ESP_ERR_NVS_NEW_VERSION_FOUND) {
        ESP_ERROR_CHECK(nvs_flash_erase());
        e = nvs_flash_init();
    }
    if (e != ESP_OK) return e;
    load();
    return load_or_mint_identity();
}

const app_config_t *app_config_get(void) { return &cur; }
const char *app_device_id(void) { return device_id; }

int64_t app_device_created(void)
{
    if (device_created == 0) {
        int64_t now = (int64_t)time(NULL);
        if (now > 1600000000) {
            device_created = now;
            nvs_handle_t h;
            if (nvs_open(NS, NVS_READWRITE, &h) == ESP_OK) { nvs_set_i64(h, "created", now); nvs_commit(h); nvs_close(h); }
        }
    }
    return device_created;
}

static void put_str(rrc_jsonw *w, const char *k, const char *v, bool first) { rrc_jsonw_key(w, k, first); rrc_jsonw_str(w, v); }
static void put_bool(rrc_jsonw *w, const char *k, bool v) { rrc_jsonw_key(w, k, false); rrc_jsonw_raw(w, v ? "true" : "false"); }

static int to_json(const app_config_t *c, char *out, size_t cap, bool for_ui)
{
    rrc_jsonw w;
    rrc_jsonw_init(&w, out, cap);
    rrc_jsonw_raw(&w, "{");
    put_str(&w, "s3_endpoint", c->s3_endpoint, true);
    put_str(&w, "s3_bucket", c->s3_bucket, false);
    put_str(&w, "s3_region", c->s3_region, false);
    put_str(&w, "s3_access_key", c->s3_access_key, false);
    put_bool(&w, "s3_tls_insecure", c->s3_tls_insecure);
    put_str(&w, "pairing_url", c->pairing_url, false);
    put_str(&w, "include_globs", c->include_globs, false);
    put_str(&w, "exclude_globs", c->exclude_globs, false);
    put_str(&w, "key_template", c->key_template, false);
    put_str(&w, "msc_root", c->msc_root, false);
    put_bool(&w, "auto_sync", c->auto_sync);
    put_bool(&w, "upload_videos", c->upload_videos);
    rrc_jsonw_key(&w, "min_size_kb", false); rrc_jsonw_u64(&w, c->min_size_kb);
    put_str(&w, "device_name", c->device_name, false);
    put_str(&w, "wifi_ssid", c->wifi_ssid, false);
    put_str(&w, "hostname", c->hostname, false);
    put_bool(&w, "admin_auth", c->admin_auth);
    put_bool(&w, "usb_debug", c->usb_debug);
    if (for_ui) {
        char tmp[APP_SECRET_MAX + 1];
        put_bool(&w, "has_s3_secret", app_config_get_secret(tmp, sizeof tmp) == ESP_OK && tmp[0]);
        put_bool(&w, "has_wifi_password", app_config_get_wifi_password(tmp, sizeof tmp) == ESP_OK && tmp[0]);
        put_bool(&w, "has_admin_password", app_config_get_admin_password(tmp, sizeof tmp) == ESP_OK && tmp[0]);
        put_str(&w, "device_id", device_id, false);
    }
    rrc_jsonw_raw(&w, "}");
    return rrc_jsonw_finish(&w);
}

int app_config_to_json(char *out, size_t cap) { return to_json(&cur, out, cap, true); }

esp_err_t app_config_save(const app_config_t *cfg)
{
    char *blob = malloc(4096);
    if (!blob) return ESP_ERR_NO_MEM;
    int n = to_json(cfg, blob, 4096, false);
    if (n < 0) { free(blob); return ESP_ERR_NO_MEM; }
    nvs_handle_t h;
    esp_err_t e = nvs_open(NS, NVS_READWRITE, &h);
    if (e == ESP_OK) {
        e = nvs_set_str(h, "cfg", blob);
        if (e == ESP_OK) e = nvs_commit(h);
        nvs_close(h);
    }
    free(blob);
    if (e == ESP_OK) cur = *cfg;
    return e;
}

static esp_err_t set_secret_key(const char *key, const char *val)
{
    nvs_handle_t h;
    esp_err_t e = nvs_open(NS, NVS_READWRITE, &h);
    if (e != ESP_OK) return e;
    e = (val && *val) ? nvs_set_str(h, key, val) : nvs_erase_key(h, key);
    if (e == ESP_ERR_NVS_NOT_FOUND) e = ESP_OK;
    if (e == ESP_OK) e = nvs_commit(h);
    nvs_close(h);
    return e;
}

/* "" when unset. A stored value that does not fit `cap` is an error (never
 * silently reported as unset — callers such as Basic auth must fail closed). */
static esp_err_t get_secret_key(const char *key, char *out, size_t cap)
{
    out[0] = 0;
    nvs_handle_t h;
    esp_err_t e = nvs_open(NS, NVS_READONLY, &h);
    if (e != ESP_OK) return ESP_OK;
    e = nvs_get_string(h, key, out, cap);
    nvs_close(h);
    if (e == ESP_ERR_NVS_NOT_FOUND) return ESP_OK;
    return e;
}

static esp_err_t set_secret_checked(const char *key, const char *val)
{
    if (val && strlen(val) > APP_SECRET_MAX) return ESP_ERR_INVALID_SIZE;
    return set_secret_key(key, val);
}

esp_err_t app_config_set_secret(const char *s) { return set_secret_checked("s3_secret", s); }
esp_err_t app_config_get_secret(char *out, size_t cap) { return get_secret_key("s3_secret", out, cap); }
esp_err_t app_config_set_wifi_password(const char *s) { return set_secret_checked("wifi_pw", s); }
esp_err_t app_config_get_wifi_password(char *out, size_t cap) { return get_secret_key("wifi_pw", out, cap); }
esp_err_t app_config_set_admin_password(const char *s) { return set_secret_checked("admin_pw", s); }
esp_err_t app_config_get_admin_password(char *out, size_t cap) { return get_secret_key("admin_pw", out, cap); }

static void copy_str(cJSON *o, const char *k, char *dst, size_t cap)
{
    cJSON *v = cJSON_GetObjectItemCaseSensitive(o, k);
    if (cJSON_IsString(v)) { strncpy(dst, v->valuestring, cap - 1); dst[cap - 1] = 0; }
}
static void copy_bool(cJSON *o, const char *k, bool *dst)
{
    cJSON *v = cJSON_GetObjectItemCaseSensitive(o, k);
    if (cJSON_IsBool(v)) *dst = cJSON_IsTrue(v);
}

esp_err_t app_config_apply_json(const char *json, size_t len, char *err, size_t err_cap)
{
    cJSON *o = cJSON_ParseWithLength(json, len);
    if (!o) { snprintf(err, err_cap, "invalid JSON"); return ESP_ERR_INVALID_ARG; }
    app_config_t c = cur;
    copy_str(o, "s3_endpoint", c.s3_endpoint, sizeof c.s3_endpoint);
    copy_str(o, "s3_bucket", c.s3_bucket, sizeof c.s3_bucket);
    copy_str(o, "s3_region", c.s3_region, sizeof c.s3_region);
    copy_str(o, "s3_access_key", c.s3_access_key, sizeof c.s3_access_key);
    copy_bool(o, "s3_tls_insecure", &c.s3_tls_insecure);
    copy_str(o, "pairing_url", c.pairing_url, sizeof c.pairing_url);
    copy_str(o, "include_globs", c.include_globs, sizeof c.include_globs);
    copy_str(o, "exclude_globs", c.exclude_globs, sizeof c.exclude_globs);
    copy_str(o, "key_template", c.key_template, sizeof c.key_template);
    copy_str(o, "msc_root", c.msc_root, sizeof c.msc_root);
    copy_bool(o, "auto_sync", &c.auto_sync);
    copy_bool(o, "upload_videos", &c.upload_videos);
    cJSON *ms = cJSON_GetObjectItemCaseSensitive(o, "min_size_kb");
    if (cJSON_IsNumber(ms) && ms->valuedouble >= 0) c.min_size_kb = (uint32_t)ms->valuedouble;
    copy_str(o, "device_name", c.device_name, sizeof c.device_name);
    copy_str(o, "wifi_ssid", c.wifi_ssid, sizeof c.wifi_ssid);
    copy_str(o, "hostname", c.hostname, sizeof c.hostname);
    copy_bool(o, "admin_auth", &c.admin_auth);
    copy_bool(o, "usb_debug", &c.usb_debug);
    /* trim trailing slash on endpoint */
    size_t n = strlen(c.s3_endpoint);
    while (n && c.s3_endpoint[n - 1] == '/') c.s3_endpoint[--n] = 0;
    /* validate the template against a representative object */
    if (c.key_template[0]) {
        char probe[RRC_RELKEY_MAX];
        rrc_template_vars v = {.name = "DSC_0001.NEF", .path = "DCIM/100NIKON", .model = "Camera", .serial = "0", .when = 1700000000};
        rrc_relkey_err e = rrc_template_expand(c.key_template, &v, probe, sizeof probe);
        if (e != RRC_RELKEY_OK) { snprintf(err, err_cap, "key_template: %s", rrc_relkey_err_str(e)); cJSON_Delete(o); return ESP_ERR_INVALID_ARG; }
    } else {
        strcpy(c.key_template, "Camera Import/{model}/{yyyy}/{mm}/{dd}/{name}");
    }
    if (!c.hostname[0]) strcpy(c.hostname, BOARD_HOSTNAME_DEFAULT);
    /* secrets: present → set; absent → unchanged; empty string → cleared */
    cJSON *sec = cJSON_GetObjectItemCaseSensitive(o, "s3_secret_key");
    cJSON *wpw = cJSON_GetObjectItemCaseSensitive(o, "wifi_password");
    cJSON *apw = cJSON_GetObjectItemCaseSensitive(o, "admin_password");
    for (int i = 0; i < 3; i++) {
        cJSON *v = i == 0 ? sec : i == 1 ? wpw : apw;
        if (cJSON_IsString(v) && strlen(v->valuestring) > APP_SECRET_MAX) {
            snprintf(err, err_cap, "%s: longer than %d characters", i == 0 ? "s3_secret_key" : i == 1 ? "wifi_password" : "admin_password", APP_SECRET_MAX);
            cJSON_Delete(o);
            return ESP_ERR_INVALID_ARG;
        }
    }
    if (cJSON_IsString(wpw) && wpw->valuestring[0] && strlen(wpw->valuestring) < 8) { snprintf(err, err_cap, "wifi_password: WPA2 passphrases are 8–63 characters"); cJSON_Delete(o); return ESP_ERR_INVALID_ARG; }
    if (cJSON_IsString(sec)) app_config_set_secret(sec->valuestring);
    if (cJSON_IsString(wpw)) app_config_set_wifi_password(wpw->valuestring);
    if (cJSON_IsString(apw)) app_config_set_admin_password(apw->valuestring);
    cJSON_Delete(o);
    cur = c;
    err[0] = 0;
    return ESP_OK;
}

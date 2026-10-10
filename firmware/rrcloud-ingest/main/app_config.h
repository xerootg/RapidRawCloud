/*
 * Persistent configuration (NVS). The S3 secret and the admin password live in
 * their own NVS keys so the JSON document handed to the web UI never contains them.
 */
#pragma once
#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>
#include "esp_err.h"

#define CFG_STR 160
/* Longest secret (S3 secret key / Wi-Fi / admin password) the device stores. */
#define APP_SECRET_MAX 128

typedef struct {
    /* Cloud */
    char s3_endpoint[CFG_STR];
    char s3_bucket[64];
    char s3_region[32];
    char s3_access_key[80];
    bool s3_tls_insecure;
    char pairing_url[CFG_STR];        /* e.g. https://rrc.themissing.xyz — informational after pairing */
    /* What to sync */
    char include_globs[256];          /* "*.nef *.nrw *.dng *.jpg *.jpeg *.heif *.hif *.tif *.tiff" */
    char exclude_globs[256];
    char key_template[CFG_STR];       /* relkey template under library/ */
    char msc_root[64];                /* e.g. "DCIM" ("" = whole card) */
    bool auto_sync;                   /* sync when a camera is attached */
    bool upload_videos;               /* convenience toggle appending *.mov *.mp4 */
    uint32_t min_size_kb;             /* skip tiny files (thumbnails) */
    /* Device identity shown in the registry */
    char device_name[48];
    /* Network */
    char wifi_ssid[33];
    char hostname[32];
    /* Admin UI */
    bool admin_auth;                  /* require HTTP basic auth (user "admin") */
} app_config_t;

esp_err_t app_config_init(void);                     /* nvs_flash_init + load (defaults when empty) */
const app_config_t *app_config_get(void);            /* current in-memory copy */
esp_err_t app_config_save(const app_config_t *cfg);  /* persists + replaces in-memory copy */

/* Secrets: write-only from the UI's point of view. */
esp_err_t app_config_set_secret(const char *s3_secret);
esp_err_t app_config_get_secret(char *out, size_t cap);   /* "" when unset */
esp_err_t app_config_set_wifi_password(const char *pw);
esp_err_t app_config_get_wifi_password(char *out, size_t cap);
esp_err_t app_config_set_admin_password(const char *pw);
esp_err_t app_config_get_admin_password(char *out, size_t cap);

/* Device identity (UUIDv4 minted once, persisted outside the config so a settings
 * reset never forks the identity — §1.2). */
const char *app_device_id(void);
int64_t app_device_created(void);

/* JSON for the web UI (no secrets; includes has_secret flags). */
int app_config_to_json(char *out, size_t cap);
/* Applies a JSON patch from the web UI; unknown keys ignored, secrets handled if present. */
esp_err_t app_config_apply_json(const char *json, size_t len, char *err, size_t err_cap);

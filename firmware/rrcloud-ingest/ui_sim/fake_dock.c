#include "fake_dock.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

fake_state_t fake;
cJSON *fake_config;
static bool has_s3_secret = true, has_wifi_pw = true, has_admin_pw;
static const char *admin_pw = "hunter22";
static uint32_t scan_done_at, pair_ready_at;
static int scan_state; /* 0 idle 1 running 2 done */

void fake_dock_init(void)
{
    fake = (fake_state_t){.phase = "idle", .camera = true, .s3_ready = true, .wifi_assoc = true, .rssi = -58, .pair_state = "idle", .backlight = -1};
    cJSON_Delete(fake_config);
    fake_config = cJSON_Parse(
        "{\"s3_endpoint\":\"https://garage.example.net\",\"s3_bucket\":\"photos\",\"s3_region\":\"garage\",\"s3_access_key\":\"GK31c2f218a2e44f485b94239e\","
        "\"s3_tls_insecure\":false,\"pairing_url\":\"rrc.example.net\",\"include_globs\":\"*.nef *.nrw *.dng *.jpg *.jpeg *.heif *.hif *.tif *.tiff\","
        "\"exclude_globs\":\"\",\"key_template\":\"Camera Import/{model}/{yyyy}/{mm}/{dd}/{name}\",\"msc_root\":\"DCIM\",\"auto_sync\":true,"
        "\"upload_videos\":false,\"min_size_kb\":64,\"device_name\":\"Field dock\",\"wifi_enabled\":true,\"wifi_ssid\":\"Basecamp\",\"hostname\":\"rrcloud-ingest\","
        "\"admin_auth\":false,\"usb_debug\":true,\"ble_enabled\":true,\"display_brightness\":80,\"display_timeout_s\":60,\"display_dark\":true,\"display_rotation\":0,\"ota_dev_channel\":false}");
    has_admin_pw = false;
    scan_state = 0;
}

static char *dup(const char *s) { char *d = malloc(strlen(s) + 1); strcpy(d, s); return d; }
static char *ok(int *st) { *st = 200; return dup("{\"ok\":true}"); }
static char *err(int *st, int code, const char *msg)
{
    char *b = malloc(strlen(msg) + 32);
    sprintf(b, "{\"ok\":false,\"error\":\"%s\"}", msg);
    *st = code;
    return b;
}

static bool cfg_bool(const char *k) { return cJSON_IsTrue(cJSON_GetObjectItem(fake_config, k)); }
static const char *cfg_str(const char *k) { const cJSON *v = cJSON_GetObjectItem(fake_config, k); return cJSON_IsString(v) ? v->valuestring : ""; }

static char *status_json(uint32_t now)
{
    bool up = !strcmp(fake.phase, "uploading");
    char sync[1400];
    snprintf(sync, sizeof sync,
             "{\"phase\":\"%s\",\"s3_ready\":%s,\"camera_attached\":%s,\"camera_kind\":\"ptp\",\"camera_model\":\"Z 7II\",\"camera_serial\":\"6012345\","
             "\"current_file\":\"%s\",\"current_size\":%u,\"current_done\":%u,\"run_total\":%d,\"run_done\":%d,\"run_skipped\":%d,\"run_failed\":%d,"
             "\"run_bytes\":%llu,\"run_total_bytes\":%llu,\"run_read_bytes\":%llu,\"run_inflight\":%d,\"run_elapsed_ms\":%u,"
             "\"stage_read_ms\":2500,\"stage_hash_ms\":5400,\"stage_sum_ms\":5300,\"stage_upload_ms\":9100,"
             "\"last_run_ts\":1792141200,\"last_heartbeat_ts\":1792144500,\"last_manifest_ts\":1792141260,\"last_error\":\"%s\","
             "\"digest_check_known\":true,\"backend_rejects_bad_md5\":true,\"lifetime_uploaded\":4312,\"lifetime_bytes\":231874123776,"
             "\"pending_entries\":%d,\"published_cursor\":4290,\"ledger_records\":4398}",
             fake.phase, fake.s3_ready ? "true" : "false", fake.camera ? "true" : "false",
             up ? "DCIM/100NZ7_2/DSC_4821.NEF" : "", up ? 58123264u : 0u, up ? 31457280u : 0u,
             up ? 40 : 12, up ? 17 : 11, up ? 3 : 1, 0,
             up ? 1046478848ULL : 642000000ULL, up ? 2324922368ULL : 642000000ULL, up ? 1132462080ULL : 642000000ULL, up ? 2 : 0,
             up ? 98000u : 121000u, "", up ? 17 : 0);
    char *b = malloc(4096);
    snprintf(b, 4096,
             "{\"sync\":%s,\"network\":\"%s\",\"eth_link\":false,\"wifi\":%s,\"time_synced\":true,\"time\":\"2026-10-16T14:%02u:%02uZ\",\"version\":\"0.9.0\","
             "\"hostname\":\"%s\",\"device_id\":\"6f1c2a4e-8b0d-4c57-9a63-2f4e1d9b7c05\",\"storage_total\":8388608,\"storage_used\":1048576,"
             "\"heap_free\":93184,\"psram_free\":27262976,\"uptime_s\":%u,\"usb_devices\":1,"
             "\"coproc\":{\"linked\":true,\"linking\":false,\"version\":\"3.0.9\",\"host_lib\":\"3.0.9\",\"compatible\":true,\"error\":\"\",\"update\":{\"state\":\"idle\",\"done\":0,\"total\":0,\"message\":\"\"}},"
             "\"ble\":{\"enabled\":true,\"up\":true,\"advertising\":true,\"connected\":false,\"encrypted\":false,\"name\":\"rrcloud-%s\",\"address\":\"aa:bb:cc:dd:ee:ff\",\"mtu\":23,\"error\":\"\"},"
             "\"board\":\"ESP32-P4-WIFI6-Touch-LCD-4.3\",\"has_ethernet\":false,\"wifi_enabled\":%s,\"wifi_ssid\":\"%s\",\"wifi_rssi\":%d,"
             "\"power\":{\"battery\":true,\"mv\":3912,\"pct\":71,\"present\":true}}",
             sync, fake.wifi_assoc && cfg_bool("wifi_enabled") ? "wifi 192.168.4.37" : "offline",
             fake.wifi_assoc && cfg_bool("wifi_enabled") ? "true" : "false", (now / 60000) % 60, (now / 1000) % 60, cfg_str("hostname"),
             3600 * 5 + now / 1000, cfg_str("hostname"), cfg_bool("wifi_enabled") ? "true" : "false", cfg_str("wifi_ssid"),
             fake.wifi_assoc ? fake.rssi : 0);
    return b;
}

static char *config_json(void)
{
    cJSON *c = cJSON_Duplicate(fake_config, true);
    cJSON_AddBoolToObject(c, "has_s3_secret", has_s3_secret);
    cJSON_AddBoolToObject(c, "has_wifi_password", has_wifi_pw);
    cJSON_AddBoolToObject(c, "has_admin_password", has_admin_pw);
    cJSON_AddStringToObject(c, "device_id", "6f1c2a4e-8b0d-4c57-9a63-2f4e1d9b7c05");
    char *s = cJSON_PrintUnformatted(c);
    cJSON_Delete(c);
    return s;
}

/* The subset of app_config_apply_json()'s validation the UI can trigger. */
static char *config_post(const char *body, int *st)
{
    cJSON *p = cJSON_Parse(body ? body : "");
    if (!p) return err(st, 400, "invalid JSON");
    const cJSON *wpw = cJSON_GetObjectItem(p, "wifi_password");
    if (cJSON_IsString(wpw) && wpw->valuestring[0] && strlen(wpw->valuestring) < 8) {
        cJSON_Delete(p);
        return err(st, 400, "wifi_password: WPA2 passphrases are 8–63 characters");
    }
    const cJSON *kt = cJSON_GetObjectItem(p, "key_template");
    if (cJSON_IsString(kt) && strstr(kt->valuestring, "..")) { cJSON_Delete(p); return err(st, 400, "key_template: path segment '..' is not allowed"); }
    cJSON *it;
    cJSON_ArrayForEach(it, p) {
        if (!strcmp(it->string, "s3_secret_key")) { has_s3_secret = it->valuestring && it->valuestring[0]; continue; }
        if (!strcmp(it->string, "wifi_password")) { has_wifi_pw = it->valuestring && it->valuestring[0]; continue; }
        if (!strcmp(it->string, "admin_password")) { has_admin_pw = it->valuestring && it->valuestring[0]; continue; }
        cJSON_ReplaceItemInObject(fake_config, it->string, cJSON_Duplicate(it, true));
        if (!cJSON_GetObjectItem(fake_config, it->string)) cJSON_AddItemToObject(fake_config, it->string, cJSON_Duplicate(it, true));
    }
    cJSON_Delete(p);
    return ok(st);
}

static char *wifi_json(void)
{
    char *b = malloc(1024);
    bool en = cfg_bool("wifi_enabled"), assoc = en && fake.wifi_assoc && cfg_str("wifi_ssid")[0];
    if (assoc)
        snprintf(b, 1024, "{\"enabled\":true,\"ssid\":\"%s\",\"standby\":false,\"started\":true,\"associated\":true,\"has_ip\":true,\"attempts\":0,"
                          "\"last_reason\":0,\"last_reason_text\":\"\",\"connected_s\":7385,\"bssid\":\"74:ac:b9:12:4e:01\",\"rssi\":%d,\"channel\":6,"
                          "\"security\":\"WPA2/WPA3\",\"phy\":\"802.11ax (Wi-Fi 6)\",\"mac\":\"30:30:f9:7a:11:c2\",\"ip\":\"192.168.4.37\","
                          "\"netmask\":\"255.255.255.0\",\"gateway\":\"192.168.4.1\",\"dns\":\"192.168.4.1\"}", cfg_str("wifi_ssid"), fake.rssi);
    else
        snprintf(b, 1024, "{\"enabled\":%s,\"ssid\":\"%s\",\"standby\":false,\"started\":%s,\"associated\":false,\"has_ip\":false,\"attempts\":%d,"
                          "\"last_reason\":%d,\"last_reason_text\":\"%s\",\"mac\":\"30:30:f9:7a:11:c2\"}",
                 en ? "true" : "false", cfg_str("wifi_ssid"), en ? "true" : "false", en ? 4 : 0, en ? 201 : 0, en ? "network not found" : "");
    return b;
}

static const char *scan_result =
    "{\"state\":\"done\",\"error\":\"\",\"networks\":["
    "{\"ssid\":\"Basecamp\",\"bssid\":\"74:ac:b9:12:4e:01\",\"rssi\":-58,\"channel\":6,\"security\":\"WPA2/WPA3\",\"open\":false,\"wifi6\":true,\"aps\":3},"
    "{\"ssid\":\"Studio North\",\"bssid\":\"f0:9f:c2:0a:11:20\",\"rssi\":-64,\"channel\":1,\"security\":\"WPA2-PSK\",\"open\":false,\"wifi6\":false,\"aps\":1},"
    "{\"ssid\":\"FieldHotspot\",\"bssid\":\"de:ad:be:ef:00:01\",\"rssi\":-71,\"channel\":11,\"security\":\"WPA3-SAE\",\"open\":false,\"wifi6\":true,\"aps\":1},"
    "{\"ssid\":\"Cafe Guest\",\"bssid\":\"10:20:30:40:50:60\",\"rssi\":-79,\"channel\":6,\"security\":\"open\",\"open\":true,\"wifi6\":false,\"aps\":2},"
    "{\"ssid\":\"DIRECT-7Q-Printer\",\"bssid\":\"aa:aa:aa:aa:aa:aa\",\"rssi\":-86,\"channel\":11,\"security\":\"WPA2-PSK\",\"open\":false,\"wifi6\":false,\"aps\":1}]}";

static char *pair_json(uint32_t now)
{
    if (!strcmp(fake.pair_state, "discovering") && now >= pair_ready_at) fake.pair_state = "waiting_user";
    char *b = malloc(1024);
    bool code = !strcmp(fake.pair_state, "waiting_user");
    snprintf(b, 1024, "{\"state\":\"%s\",\"user_code\":\"%s\",\"verification_uri\":\"%s\",\"verification_uri_complete\":\"%s\",\"expires_in\":600,\"message\":\"%s\"}",
             fake.pair_state, code ? "WDJB-MJHT" : "", code ? "https://auth.example.net/device" : "",
             code ? "https://auth.example.net/device?code=WDJB-MJHT" : "",
             code ? "Approve the dock in your browser; this page updates by itself." : "");
    return b;
}

char *fake_dock_handle(const char *method, const char *path, const char *body, int *st, uint32_t now)
{
    bool get = !strcmp(method, "GET");
    *st = 200;
    if (!strcmp(method, "AUTH")) return body && !strcmp(body, admin_pw) ? ok(st) : err(st, 401, "wrong password");
    if (get && !strcmp(path, "/api/status")) return status_json(now);
    if (get && !strcmp(path, "/api/config")) return config_json();
    if (!get && !strcmp(path, "/api/config")) return config_post(body, st);
    if (get && !strcmp(path, "/api/wifi")) return wifi_json();
    if (!get && !strcmp(path, "/api/wifi/scan")) {
        if (!cfg_bool("wifi_enabled")) return err(st, 409, "Wi-Fi is turned off");
        if (scan_state == 1) return err(st, 409, "a scan is already running");
        scan_state = 1; scan_done_at = now + 1500;
        return ok(st);
    }
    if (get && !strcmp(path, "/api/wifi/scan")) {
        if (scan_state == 1 && now >= scan_done_at) scan_state = 2;
        if (scan_state == 2) return dup(scan_result);
        return dup(scan_state == 1 ? "{\"state\":\"running\",\"error\":\"\",\"networks\":[]}" : "{\"state\":\"idle\",\"error\":\"\",\"networks\":[]}");
    }
    if (!get && !strcmp(path, "/api/pair/begin")) {
        if (!body || !strstr(body, "\"url\"")) return err(st, 400, "url required");
        fake.pair_state = "discovering"; pair_ready_at = now + 800;
        return ok(st);
    }
    if (get && !strcmp(path, "/api/pair/status")) return pair_json(now);
    if (!get && !strcmp(path, "/api/pair/cancel")) { fake.pair_state = "idle"; return ok(st); }
    if (get && !strcmp(path, "/api/log")) {
        return dup("[\"2026-10-16T14:02:11Z I rrc: rrcloud-ingest starting; device 6f1c2a4e-8b0d-4c57-9a63-2f4e1d9b7c05; psram free 31744 KiB\","
                   "\"2026-10-16T14:02:12Z I rrc: display: 480x800 MIPI-DSI panel up, touch ok\","
                   "\"2026-10-16T14:02:14Z I rrc: wifi: connecting to Basecamp\",\"2026-10-16T14:02:15Z I rrc: wifi associated\","
                   "\"2026-10-16T14:02:16Z I rrc: wifi ip 192.168.4.37 — web ui: http://192.168.4.37/ (http://rrcloud-ingest.local/)\","
                   "\"2026-10-16T14:02:17Z I rrc: time synchronized (sntp)\",\"2026-10-16T14:05:40Z I rrc: camera attached: Z 7II via PTP\","
                   "\"2026-10-16T14:05:41Z I rrc: sync: 40 objects match, 23 new\"]");
    }
    if (get && !strcmp(path, "/api/ota")) {
        char *b = malloc(640);
        snprintf(b, 640, "{\"running\":\"0.4.0\",\"built\":\"Oct 26 2026 10:00:00\",\"hardware\":\"touch-lcd-43\",\"variant\":\"touch-lcd-43\",\"chip_rev\":301,"
                 "\"channel\":\"%s\",\"state\":\"%s\",\"message\":\"%s\",\"done\":0,\"total\":0,\"trial\":false,\"rolled_back\":\"\",\"checked\":%s,\"available\":%s}",
                 cfg_bool("ota_dev_channel") ? "dev" : "release", fake.ota_checked ? "idle" : "idle",
                 fake.ota_checked ? "release 0.5.0 is available (running 0.4.0)" : "", fake.ota_checked ? "true" : "false",
                 fake.ota_checked ? "{\"version\":\"0.5.0\",\"tag\":\"firmware-v0.5.0\",\"commit\":\"68e11af\",\"channel\":\"release\",\"size\":2859008,\"newer\":true}" : "null");
        return b;
    }
    if (!get && !strcmp(path, "/api/ota/check")) { fake.ota_checked = true; return ok(st); }
    if (!get) return ok(st);   /* sync/now, sync/cancel, usb/reset, reboot, ble/forget, coproc/update, ota/install */
    return err(st, 404, "unknown endpoint");
}

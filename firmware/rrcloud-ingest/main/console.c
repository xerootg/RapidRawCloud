#include "console.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "esp_console.h"
#include "esp_log.h"
#include "esp_system.h"
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "lwip/stats.h"
#include "app_config.h"
#include "sync.h"
#include "net.h"
#include "pairing.h"
#include "usb_diag.h"
#include "coproc.h"
#include "ble.h"
#include "log_ring.h"
#include "rrc_proto.h"

static const char *TAG = "console";

static int cmd_status(int argc, char **argv)
{
    (void)argc; (void)argv;
    char *buf = malloc(2048);
    if (!buf) return 1;
    sync_status_json(buf, 2048);
    char netd[64];
    net_describe(netd, sizeof netd);
    printf("network: %s (link %s, time %s)\nusb devices enumerated: %d\nsync: %s\n", netd, net_eth_link() ? "up" : "down",
           net_time_synced() ? "synced" : "not synced", usb_diag_device_count(), buf);
    free(buf);
    return 0;
}

static int cmd_config(int argc, char **argv)
{
    (void)argc; (void)argv;
    char *buf = malloc(4096);
    if (!buf) return 1;
    int n = app_config_to_json(buf, 4096);
    if (n > 0) printf("%.*s\n", n, buf);
    free(buf);
    return n > 0 ? 0 : 1;
}

/* Keys whose value is a JSON bool / number; everything else is a string.
 * Secrets (s3_secret_key, wifi_password, admin_password) go through the same
 * JSON patch the web UI sends, so one code path validates and persists. */
static const char *const BOOL_KEYS[] = {"s3_tls_insecure", "auto_sync", "upload_videos", "admin_auth", "usb_debug", "ble_enabled"};
static const char *const NUM_KEYS[] = {"min_size_kb"};

static bool in_list(const char *k, const char *const *list, size_t n)
{
    for (size_t i = 0; i < n; i++) if (!strcmp(k, list[i])) return true;
    return false;
}

static int cmd_set(int argc, char **argv)
{
    if (argc < 3) {
        printf("usage: set <key> <value...>   (keys: see `config`; also s3_secret_key, wifi_password, admin_password)\n");
        return 1;
    }
    const char *key = argv[1];
    /* Values may contain spaces (globs, device name): rejoin the remaining args.
     * Both buffers live on the heap: the REPL task stack is small and the JSON
     * patch below goes through cJSON and NVS on top of them. */
    enum { VALUE_CAP = 1024, DOC_CAP = 1400 };
    char *value = calloc(1, VALUE_CAP + DOC_CAP);
    if (!value) { printf("out of memory\n"); return 1; }
    char *doc = value + VALUE_CAP;
    size_t o = 0;
    for (int i = 2; i < argc; i++) {
        int w = snprintf(value + o, VALUE_CAP - o, "%s%s", i > 2 ? " " : "", argv[i]);
        if (w < 0 || (size_t)w >= VALUE_CAP - o) { printf("value too long\n"); free(value); return 1; }
        o += (size_t)w;
    }
    rrc_jsonw w;
    rrc_jsonw_init(&w, doc, DOC_CAP);
    rrc_jsonw_raw(&w, "{");
    rrc_jsonw_key(&w, key, true); /* first key: no leading comma */
    if (in_list(key, BOOL_KEYS, sizeof BOOL_KEYS / sizeof BOOL_KEYS[0])) {
        bool b = !strcmp(value, "1") || !strcasecmp(value, "true") || !strcasecmp(value, "on") || !strcasecmp(value, "yes");
        rrc_jsonw_raw(&w, b ? "true" : "false");
    } else if (in_list(key, NUM_KEYS, sizeof NUM_KEYS / sizeof NUM_KEYS[0])) {
        rrc_jsonw_u64(&w, strtoull(value, NULL, 10));
    } else {
        rrc_jsonw_str(&w, value);
    }
    rrc_jsonw_raw(&w, "}");
    int n = rrc_jsonw_finish(&w);
    if (n < 0) { printf("could not encode\n"); free(value); return 1; }
    char err[96] = "";
    esp_err_t e = app_config_apply_json(doc, (size_t)n, err, sizeof err);
    free(value);
    if (e != ESP_OK) { printf("rejected: %s\n", err[0] ? err : esp_err_to_name(e)); return 1; }
    e = app_config_save(app_config_get());
    if (e != ESP_OK) { printf("could not persist: %s\n", esp_err_to_name(e)); return 1; }
    sync_config_changed();
    net_wifi_reconfigure();
    usb_diag_set_verbose(app_config_get()->usb_debug);
    log_ring_printf("configuration changed from the console (%s)", key);
    printf("ok\n");
    return 0;
}

static int cmd_sync(int argc, char **argv) { (void)argc; (void)argv; sync_request_now(); printf("sync requested\n"); return 0; }
static int cmd_sync_cancel(int argc, char **argv) { (void)argc; (void)argv; sync_cancel(); printf("cancelling\n"); return 0; }
static int cmd_usb_reset(int argc, char **argv) { (void)argc; (void)argv; return usb_diag_power_cycle() == ESP_OK ? 0 : 1; }
static int cmd_reboot(int argc, char **argv) { (void)argc; (void)argv; printf("rebooting\n"); vTaskDelay(pdMS_TO_TICKS(200)); esp_restart(); return 0; }

static int cmd_pair(int argc, char **argv)
{
    if (argc < 2) { printf("usage: pair <pairing service url>\n"); return 1; }
    esp_err_t e = pairing_begin(argv[1]);
    if (e != ESP_OK) { printf("pairing could not start: %s\n", esp_err_to_name(e)); return 1; }
    printf("pairing started; watch `pair-status` for the code and URL\n");
    return 0;
}

static int cmd_pair_status(int argc, char **argv)
{
    (void)argc; (void)argv;
    char buf[600];
    pairing_status_json(buf, sizeof buf);
    printf("%s\n", buf);
    return 0;
}

static int cmd_c6(int argc, char **argv)
{
    (void)argc; (void)argv;
    char buf[640];
    int n = coproc_status_json(buf, sizeof buf);
    if (n > 0) printf("%.*s\n", n, buf);
    return n > 0 ? 0 : 1;
}

static int cmd_c6_update(int argc, char **argv)
{
    const char *url = argc > 1 ? argv[1] : NULL;
    const char *sha = argc > 2 ? argv[2] : NULL;
    esp_err_t e = coproc_update_start(url, sha);
    if (e == ESP_ERR_INVALID_STATE) { printf("an update is already running\n"); return 1; }
    if (e != ESP_OK) { printf("rejected: %s (need an http(s) url and an optional 64-hex sha256)\n", esp_err_to_name(e)); return 1; }
    printf("started: %s%s\n", url ? url : COPROC_IMAGE_URL_DEFAULT, url ? "" : " (pinned sha256)");
    return 0;
}

static int cmd_cam_ls(int argc, char **argv) { (void)argc; (void)argv; sync_debug_list(); return 0; }
static int cmd_cam_hash(int argc, char **argv)
{
    if (argc < 2) { printf("usage: cam-hash <hex handle | path> [repeat]\n"); return 1; }
    sync_debug_hash(argv[1], argc > 2 ? atoi(argv[2]) : 1);
    return 0;
}

static int cmd_ble(int argc, char **argv)
{
    (void)argc; (void)argv;
    char buf[400];
    int n = ble_status_json(buf, sizeof buf);
    if (n > 0) printf("%.*s\n", n, buf);
    return n > 0 ? 0 : 1;
}

static int cmd_ble_forget(int argc, char **argv) { (void)argc; (void)argv; ble_forget_bonds(); printf("ok\n"); return 0; }

static int cmd_net(int argc, char **argv)
{
    (void)argc; (void)argv;
    char netd[64];
    net_describe(netd, sizeof netd);
    printf("network: %s\n", netd);
#if LWIP_STATS
    printf("lwip: link rx=%u tx=%u drop=%u | ip rx=%u tx=%u drop=%u | tcp rx=%u tx=%u drop=%u err=%u | udp rx=%u tx=%u | arp rx=%u tx=%u\n",
           (unsigned)lwip_stats.link.recv, (unsigned)lwip_stats.link.xmit, (unsigned)lwip_stats.link.drop,
           (unsigned)lwip_stats.ip.recv, (unsigned)lwip_stats.ip.xmit, (unsigned)lwip_stats.ip.drop,
           (unsigned)lwip_stats.tcp.recv, (unsigned)lwip_stats.tcp.xmit, (unsigned)lwip_stats.tcp.drop, (unsigned)lwip_stats.tcp.err,
           (unsigned)lwip_stats.udp.recv, (unsigned)lwip_stats.udp.xmit,
           (unsigned)lwip_stats.etharp.recv, (unsigned)lwip_stats.etharp.xmit);
#endif
    return 0;
}

static int cmd_log(int argc, char **argv)
{
    (void)argc; (void)argv;
    char *buf = malloc(24576);
    if (!buf) return 1;
    int n = log_ring_to_json(buf, 24576);
    if (n > 0) printf("%.*s\n", n, buf);
    free(buf);
    return 0;
}

esp_err_t console_start(void)
{
    esp_console_repl_t *repl = NULL;
    esp_console_repl_config_t rc = ESP_CONSOLE_REPL_CONFIG_DEFAULT();
    rc.prompt = "rrc> ";
    rc.max_cmdline_length = 1200;
    /* The default 4 KiB REPL stack overflows (stack protection fault) as soon as
     * a command formats a status document or runs a config patch through cJSON
     * and NVS; `status` and `set` both rebooted the dock on 0.1.4. */
    rc.task_stack_size = 16384;
    esp_console_dev_uart_config_t uc = ESP_CONSOLE_DEV_UART_CONFIG_DEFAULT();
    esp_err_t e = esp_console_new_repl_uart(&uc, &rc, &repl);
    if (e != ESP_OK) { ESP_LOGW(TAG, "console unavailable: %s", esp_err_to_name(e)); return e; }
    esp_console_register_help_command();
    const esp_console_cmd_t cmds[] = {
        {.command = "status", .help = "Sync, network and USB state", .func = cmd_status},
        {.command = "config", .help = "Print the configuration (no secrets)", .func = cmd_config},
        {.command = "set", .help = "set <key> <value>: change one setting and persist it (s3_endpoint, s3_bucket, s3_region, s3_access_key, s3_secret_key, s3_tls_insecure, include_globs, key_template, auto_sync, wifi_ssid, wifi_password, admin_auth, admin_password, usb_debug, ...)", .func = cmd_set},
        {.command = "sync", .help = "Start a sync now", .func = cmd_sync},
        {.command = "sync-cancel", .help = "Cancel the running sync", .func = cmd_sync_cancel},
        {.command = "usb-reset", .help = "Power-cycle the USB-A port", .func = cmd_usb_reset},
        {.command = "pair", .help = "pair <url>: start device-flow pairing against the pairing service", .func = cmd_pair},
        {.command = "pair-status", .help = "Pairing progress (user code and verification URL)", .func = cmd_pair_status},
        {.command = "net", .help = "Addresses and lwIP packet counters (did anything reach us?)", .func = cmd_net},
        {.command = "cam-ls", .help = "List the attached camera's objects (handle, size, path)", .func = cmd_cam_ls},
        {.command = "cam-hash", .help = "cam-hash <hex handle | path> [repeat]: read one object through the sync path and print its BLAKE3 (repeat to test determinism)", .func = cmd_cam_hash},
        {.command = "ble", .help = "Bluetooth LE admin service: state, name, address, link", .func = cmd_ble},
        {.command = "ble-forget", .help = "Forget every paired phone (they pair again on next use)", .func = cmd_ble_forget},
        {.command = "c6", .help = "ESP32-C6 radio co-processor: link state, firmware version, update progress", .func = cmd_c6},
        {.command = "c6-update", .help = "c6-update [url [sha256]]: install an ESP-Hosted co-processor image over SDIO (default: the pinned " COPROC_HOST_LIB_VERSION " image); the dock restarts afterwards", .func = cmd_c6_update},
        {.command = "log", .help = "The web UI's log ring", .func = cmd_log},
        {.command = "reboot", .help = "Restart the dock", .func = cmd_reboot},
    };
    for (size_t i = 0; i < sizeof cmds / sizeof cmds[0]; i++) esp_console_cmd_register(&cmds[i]);
    e = esp_console_start_repl(repl);
    if (e == ESP_OK) log_ring_printf("serial console ready: type `help` on the USB-C console port");
    return e;
}

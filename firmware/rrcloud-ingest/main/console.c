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
#include "rrc_ptp.h"
#include "log_ring.h"
#include "rrc_proto.h"
#include "esp_timer.h"
#include "esp_heap_caps.h"
#include "esp_memory_utils.h"
#include "mbedtls/gcm.h"
#include "mbedtls/sha256.h"
#include "mbedtls/md5.h"
#include "mbedtls/aes.h"
#include "rrc_hash.h"
#include "board.h"
#include "display.h"
#include "ota.h"
#ifdef BOARD_I2C_SDA_GPIO
#include "driver/i2c_master.h"
#endif

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
#if CONFIG_RRC_DISPLAY_RPI_DSI
static int cmd_dsi_timing(int argc, char **argv)
{
    display_dsi_timing_t t;
    display_dsi_get_timing(&t);
    if (argc == 11) {
        char *end[8];
        t.lane_mbps = strtof(argv[1], &end[0]); t.dpi_mhz = strtof(argv[2], &end[1]);
        t.hsw = (int)strtol(argv[3], &end[2], 10); t.hbp = (int)strtol(argv[4], &end[3], 10); t.hfp = (int)strtol(argv[5], &end[4], 10);
        t.vsw = (int)strtol(argv[6], &end[5], 10); t.vbp = (int)strtol(argv[7], &end[6], 10); t.vfp = (int)strtol(argv[8], &end[7], 10);
        bool numeric = true;
        for (int i = 0; i < 8; i++) if (*end[i] || !argv[i + 1][0]) numeric = false;
        /* the D-PHY's range, a pixel clock the panel could take, and porches that
         * keep a frame a frame: checked before the link is torn down */
        if (!numeric || t.lane_mbps < 80 || t.lane_mbps > 1500 || t.dpi_mhz < 5 || t.dpi_mhz > 120
            || t.hsw < 1 || t.hsw > 255 || t.hbp < 1 || t.hbp > 1023 || t.hfp < 1 || t.hfp > 1023
            || t.vsw < 1 || t.vsw > 255 || t.vbp < 0 || t.vbp > 1023 || t.vfp < 0 || t.vfp > 1023
            || (strcmp(argv[9], "burst") && strcmp(argv[9], "nonburst")) || (strcmp(argv[10], "lp") && strcmp(argv[10], "nolp"))) {
            printf("out of range: lane 80..1500 Mbit/s, dpi 5..120 MHz, hsw/vsw 1..255, porches up to 1023, burst|nonburst, lp|nolp\n");
            return 1;
        }
        t.non_burst = !strcmp(argv[9], "nonburst");
        t.disable_lp = !strcmp(argv[10], "nolp");
        esp_err_t e = display_dsi_retime(&t);
        printf("retime: %s\n", esp_err_to_name(e));
        display_dsi_get_timing(&t);
    } else if (argc != 1) {
        printf("usage: dsi-timing lane_mbps dpi_mhz hsw hbp hfp vsw vbp vfp burst|nonburst lp|nolp\n");
        return 1;
    }
    int ht = 800 + t.hsw + t.hbp + t.hfp, vt = 480 + t.vsw + t.vbp + t.vfp;
    printf("lane %.1f Mbit/s, dpi %.3f MHz (lane/dpi = %.2f), h %d/%d/%d (%d), v %d/%d/%d (%d), %s, %s, %.1f Hz\n",
           t.lane_mbps, t.dpi_mhz, t.lane_mbps / t.dpi_mhz, t.hsw, t.hbp, t.hfp, ht, t.vsw, t.vbp, t.vfp, vt,
           t.non_burst ? "non-burst" : "burst", t.disable_lp ? "no LP" : "LP in blanking", t.dpi_mhz * 1e6f / ht / vt);
    return 0;
}
static int cmd_dsi_reg(int argc, char **argv)
{
    if (argc != 3) { printf("usage: dsi-reg <reg hex> <value hex>\n"); return 1; }
    char *e1, *e2;
    unsigned long r = strtoul(argv[1], &e1, 16), v = strtoul(argv[2], &e2, 16);
    if (!argv[1][0] || !argv[2][0] || *e1 || *e2 || r > 0xFFFF || v > 0xFFFFFFFFul) {
        printf("dsi-reg: register 0000..ffff and a 32-bit value, both hex\n");   /* a typo must not write register 0 */
        return 1;
    }
    uint16_t reg = (uint16_t)r;
    uint32_t val = (uint32_t)v;
    esp_err_t e = display_dsi_bridge_write(reg, val);
    printf("bridge 0x%04x <= 0x%08lx: %s\n", reg, (unsigned long)val, esp_err_to_name(e));
    return e == ESP_OK ? 0 : 1;
}
static int cmd_fb_pattern(int argc, char **argv) { display_dsi_test_pattern(!(argc > 1 && !strcmp(argv[1], "off"))); return 0; }
static int cmd_fb_dump(int argc, char **argv) { (void)argc; (void)argv; display_dsi_dump_framebuffer(); return 0; }
static int cmd_touch_poll(int argc, char **argv) { display_touch_poll(!(argc > 1 && !strcmp(argv[1], "off"))); printf("touch polling %s\n", argc > 1 && !strcmp(argv[1], "off") ? "off" : "on"); return 0; }
#endif
#ifdef BOARD_I2C_SDA_GPIO
/* Lists every 7-bit address that ACKs on the board's internal I2C bus (the one
 * the DSI/CSI connectors carry). A temporary bus on the same pins: run it while
 * nothing else owns them (on a display build, only when the display was not found). */
static int cmd_i2c_scan(int argc, char **argv)
{
    (void)argc; (void)argv;
    i2c_master_bus_handle_t bus;
    i2c_master_bus_config_t bc = {
        .i2c_port = -1, .sda_io_num = BOARD_I2C_SDA_GPIO, .scl_io_num = BOARD_I2C_SCL_GPIO,
        .clk_source = I2C_CLK_SRC_DEFAULT, .glitch_ignore_cnt = 7, .flags.enable_internal_pullup = true,
    };
    esp_err_t e = i2c_new_master_bus(&bc, &bus);
    if (e != ESP_OK) { printf("i2c bus on SDA %d SCL %d: %s\n", BOARD_I2C_SDA_GPIO, BOARD_I2C_SCL_GPIO, esp_err_to_name(e)); return 1; }
    int found = 0;
    printf("scanning I2C on SDA %d SCL %d\n", BOARD_I2C_SDA_GPIO, BOARD_I2C_SCL_GPIO);
    for (uint16_t a = 0x08; a < 0x78; a++) {
        if (i2c_master_probe(bus, a, 20) == ESP_OK) { printf("  0x%02x\n", a); found++; }
    }
    printf("%d device(s)\n", found);
    i2c_del_master_bus(bus);
    return 0;
}
#endif

static int cmd_ota(int argc, char **argv)
{
    esp_err_t e = ESP_OK;
    if (argc >= 2 && !strcmp(argv[1], "check")) e = ota_check();
    else if (argc >= 2 && !strcmp(argv[1], "install")) e = argc >= 3 ? ota_install_url(argv[2], argc >= 4 ? argv[3] : "") : ota_install_available();
    else if (argc >= 2 && !strcmp(argv[1], "cancel")) ota_cancel();
    else if (argc >= 2) { printf("usage: ota [check | install [url [sha256]] | cancel]\n"); return 1; }
    if (e != ESP_OK) { printf("ota: %s\n", esp_err_to_name(e)); return 1; }
    char buf[1024];
    if (ota_status_json(buf, sizeof buf) >= 0) printf("%s\n", buf);
    return 0;
}

static int cmd_eth_rx_stall_test(int argc, char **argv)
{
    (void)argc; (void)argv;
    net_eth_rx_drop_test(true);
    printf("dropping every received ethernet frame; the receive watchdog should restart the dock in about 70 s\n");
    return 0;
}

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

/* crypto-bench: throughput of the primitives the upload path leans on, so a slow
 * upload can be blamed on the right stage. Internal-RAM vs PSRAM matters: the AES
 * engine only takes DMA-capable internal buffers in one shot. */
static double mbps(size_t bytes, int64_t us) { return us > 0 ? (double)bytes / 1048576.0 / ((double)us / 1e6) : 0.0; }
static int cmd_crypto_bench(int argc, char **argv)
{
    size_t psz = 1024u * 1024u, isz = 16u * 1024u;
    printf("internal heap: %u free, largest block %u; psram: %u free\n",
           (unsigned)heap_caps_get_free_size(MALLOC_CAP_INTERNAL), (unsigned)heap_caps_get_largest_free_block(MALLOC_CAP_INTERNAL),
           (unsigned)heap_caps_get_free_size(MALLOC_CAP_SPIRAM));
    uint8_t *ps = heap_caps_malloc(psz, MALLOC_CAP_SPIRAM | MALLOC_CAP_8BIT);
    uint8_t *in = heap_caps_aligned_alloc(64, isz, MALLOC_CAP_INTERNAL | MALLOC_CAP_DMA | MALLOC_CAP_8BIT);
    uint8_t *out = heap_caps_aligned_alloc(64, isz + 64, MALLOC_CAP_INTERNAL | MALLOC_CAP_DMA | MALLOC_CAP_8BIT);
    if (!ps || !in || !out) { printf("alloc failed (psram %p, in %p, out %p)\n", ps, in, out); goto done; }
    for (size_t i = 0; i < psz; i++) ps[i] = (uint8_t)(i * 7);
    for (size_t i = 0; i < isz; i++) in[i] = (uint8_t)(i * 3);
    printf("internal buf %p dma-capable=%d, psram buf %p\n", in, (int)esp_ptr_dma_capable(in), ps);
    int64_t t0, t1;
    uint8_t d[32];

    t0 = esp_timer_get_time(); memcpy(in, ps, isz); for (size_t o = isz; o < psz; o += isz) memcpy(in, ps + o, isz); t1 = esp_timer_get_time();
    printf("memcpy psram->internal 1 MiB: %.1f MB/s\n", mbps(psz, t1 - t0));

    t0 = esp_timer_get_time(); rrc_sha256(ps, psz, d); t1 = esp_timer_get_time();
    printf("rrc_sha256 (software) 1 MiB psram: %.1f MB/s\n", mbps(psz, t1 - t0));
    t0 = esp_timer_get_time(); rrc_md5(ps, psz, d); t1 = esp_timer_get_time();
    printf("rrc_md5 (software) 1 MiB psram: %.1f MB/s\n", mbps(psz, t1 - t0));
    t0 = esp_timer_get_time(); mbedtls_sha256(ps, psz, d, 0); t1 = esp_timer_get_time();
    printf("mbedtls_sha256 (hw) 1 MiB psram: %.1f MB/s\n", mbps(psz, t1 - t0));
    t0 = esp_timer_get_time(); for (int r = 0; r < 64; r++) mbedtls_sha256(in, isz, d, 0); t1 = esp_timer_get_time();
    printf("mbedtls_sha256 (hw) 64 x 16 KiB internal: %.1f MB/s\n", mbps(isz * 64, t1 - t0));

    mbedtls_gcm_context g; mbedtls_gcm_init(&g);
    static const uint8_t key[16] = {1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16};
    static const uint8_t iv[12] = {0};
    /* The AES driver takes the one-shot hardware GCM path only when AAD, input and
     * output are all DMA-capable internal RAM; otherwise GHASH runs in software. */
    uint8_t *aad = heap_caps_calloc(16, 1, MALLOC_CAP_INTERNAL | MALLOC_CAP_DMA | MALLOC_CAP_8BIT);
    uint8_t *aad_ps = heap_caps_calloc(16, 1, MALLOC_CAP_SPIRAM | MALLOC_CAP_8BIT);
    uint8_t stack_probe[16] = {0};
    printf("aad internal %p dma=%d, psram %p dma=%d, this task's stack %p dma=%d\n", aad, (int)esp_ptr_dma_capable(aad),
           aad_ps, (int)esp_ptr_dma_capable(aad_ps), stack_probe, (int)esp_ptr_dma_capable(stack_probe));
    uint8_t tag[16];
    if (mbedtls_gcm_setkey(&g, MBEDTLS_CIPHER_ID_AES, key, 128) != 0) { printf("gcm setkey failed\n"); goto done; }
    const size_t rec = 16384; int n = 64;
    for (size_t sz = 1024; sz <= rec; sz *= 4) {
        int reps = (int)(rec * n / sz);
        t0 = esp_timer_get_time();
        for (int r = 0; r < reps; r++) if (mbedtls_gcm_crypt_and_tag(&g, MBEDTLS_GCM_ENCRYPT, sz, iv, 12, aad, 13, in, out, 16, tag) != 0) { printf("gcm failed\n"); break; }
        t1 = esp_timer_get_time();
        printf("aes-128-gcm %u B records, internal->internal (one-shot, as TLS does): %.1f MB/s, %lld us/call\n", (unsigned)sz, mbps(sz * reps, t1 - t0), (long long)((t1 - t0) / reps));
    }
    t0 = esp_timer_get_time();
    for (int r = 0; r < n; r++) if (mbedtls_gcm_crypt_and_tag(&g, MBEDTLS_GCM_ENCRYPT, rec, iv, 12, aad_ps, 13, in, out, 16, tag) != 0) { printf("gcm failed\n"); break; }
    t1 = esp_timer_get_time();
    printf("aes-128-gcm 16 KiB records, internal buffers but AAD in psram (software GHASH fallback): %.1f MB/s\n", mbps(rec * n, t1 - t0));
    free(aad); free(aad_ps);
    {
        mbedtls_aes_context a; mbedtls_aes_init(&a); mbedtls_aes_setkey_enc(&a, key, 128);
        uint8_t cbc_iv[16] = {0};
        t0 = esp_timer_get_time();
        for (int r = 0; r < n; r++) mbedtls_aes_crypt_cbc(&a, MBEDTLS_AES_ENCRYPT, rec, cbc_iv, in, out);
        t1 = esp_timer_get_time();
        printf("aes-128-cbc 16 KiB, internal->internal: %.1f MB/s, %lld us/call\n", mbps(rec * n, t1 - t0), (long long)((t1 - t0) / n));
        mbedtls_aes_free(&a);
    }
    t0 = esp_timer_get_time(); mbedtls_md5(ps, psz, d); t1 = esp_timer_get_time();
    printf("mbedtls_md5 (software) 1 MiB psram: %.1f MB/s\n", mbps(psz, t1 - t0));
    t0 = esp_timer_get_time();
    for (int r = 0; r < n; r++) if (mbedtls_gcm_crypt_and_tag(&g, MBEDTLS_GCM_ENCRYPT, rec, iv, 12, aad, 13, ps + (size_t)r * rec, out, 16, tag) != 0) { printf("gcm failed\n"); break; }
    t1 = esp_timer_get_time();
    printf("aes-128-gcm 16 KiB records, psram->internal: %.1f MB/s\n", mbps(rec * n, t1 - t0));
    t0 = esp_timer_get_time();
    for (int r = 0; r < n; r++) if (mbedtls_gcm_crypt_and_tag(&g, MBEDTLS_GCM_ENCRYPT, rec, iv, 12, aad, 13, ps + (size_t)r * rec, ps + (size_t)r * rec, 16, tag) != 0) { printf("gcm failed\n"); break; }
    t1 = esp_timer_get_time();
    printf("aes-128-gcm 16 KiB records, psram in place: %.1f MB/s\n", mbps(rec * n, t1 - t0));
    mbedtls_gcm_free(&g);
done:
    free(ps); free(in); free(out);
    return 0;
}

static int cmd_tcp_speed(int argc, char **argv)
{
    if (argc < 3) { printf("usage: tcp-speed <host> <port> [MiB]\n"); return 1; }
    sync_debug_tcp_speed(argv[1], atoi(argv[2]), argc > 3 ? atoi(argv[3]) : 8);
    return 0;
}

static int cmd_net_speed(int argc, char **argv) { sync_debug_speed(argc > 1 ? atoi(argv[1]) : 8); return 0; }
static int cmd_cam_read(int argc, char **argv)
{
    if (argc < 2) { printf("usage: cam-read <handle|path> [MiB] [chunk KiB]\n"); return 1; }
    sync_debug_read(argv[1], argc > 2 ? atoi(argv[2]) : 16, argc > 3 ? atoi(argv[3]) : 0);
    return 0;
}
static int cmd_tasks(int argc, char **argv)
{
    char *buf = heap_caps_malloc(4096, MALLOC_CAP_SPIRAM | MALLOC_CAP_8BIT);
    if (!buf) return 1;
    vTaskList(buf);
    printf("name             state prio  stack-free(words) #\n%s", buf);
    printf("internal heap: %u free, largest block %u, lowest ever %u\n", (unsigned)heap_caps_get_free_size(MALLOC_CAP_INTERNAL),
           (unsigned)heap_caps_get_largest_free_block(MALLOC_CAP_INTERNAL), (unsigned)heap_caps_get_minimum_free_size(MALLOC_CAP_INTERNAL));
    free(buf);
    return 0;
}
static int cmd_cam_multi(int argc, char **argv)
{
    char list[256] = "";
    for (int i = 1; i < argc; i++) { strlcat(list, argv[i], sizeof list); strlcat(list, " ", sizeof list); }
    if (!list[0]) { printf("usage: cam-multi <handle:size> [<handle:size> ...]   (see cam-ls)\n"); return 1; }
    sync_debug_multi(list);
    return 0;
}
static int cmd_pipe_multi(int argc, char **argv) { sync_debug_pipemulti(argc > 1 ? atoi(argv[1]) : 12, argc > 2 ? atoi(argv[2]) : 2); return 0; }
static int cmd_cam_pipe(int argc, char **argv)
{
    if (argc < 3) { printf("usage: cam-pipe <handle> <size bytes>   (see cam-ls)\n"); return 1; }
    sync_debug_campipe(argv[1], atoi(argv[2]));
    return 0;
}
static int cmd_usb_direct(int argc, char **argv)
{
    if (argc > 1) {
        bool on = !strcmp(argv[1], "on") || !strcmp(argv[1], "1");
        rrc_ptp_set_direct(on);
        if (on) sync_usb_direct_state_set(0);   /* explicit override of a failed check; the next attach re-verifies */
    }
    printf("usb direct dma reads: %s\n", rrc_ptp_direct_enabled() ? "on" : "off");
    return 0;
}
static int cmd_pipe_test(int argc, char **argv) { sync_debug_pipe(argc > 1 ? atoi(argv[1]) : 24, argc > 2 ? atoi(argv[2]) : 0); return 0; }

static int cmd_net(int argc, char **argv)
{
    (void)argc; (void)argv;
    char netd[64];
    net_describe(netd, sizeof netd);
    printf("network: %s\n", netd);
#if LWIP_STATS
    { int sp = 0; bool fd = false; net_eth_link_info(&sp, &fd); printf("ethernet: %s, %d Mbit/s %s-duplex\n", net_eth_link() ? "link up" : "link down", sp, fd ? "full" : "half"); }
#if MIB2_STATS
    printf("tcp mib2: out=%u retrans=%u in=%u inerr=%u | ip out=%u in=%u discards=%u\n", (unsigned)lwip_stats.mib2.tcpoutsegs, (unsigned)lwip_stats.mib2.tcpretranssegs, (unsigned)lwip_stats.mib2.tcpinsegs, (unsigned)lwip_stats.mib2.tcpinerrs, (unsigned)lwip_stats.mib2.ipoutrequests, (unsigned)lwip_stats.mib2.ipinreceives, (unsigned)lwip_stats.mib2.ipoutdiscards);
#endif
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
    rc.task_stack_size = 8192;   /* measured use: ~1 KB; was 16 KiB before the buffers moved to the heap */
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
        {.command = "crypto-bench", .help = "crypto-bench: throughput of SHA-256, MD5 and AES-GCM on this board (internal RAM vs PSRAM)", .func = cmd_crypto_bench},
        {.command = "cam-read", .help = "cam-read <handle|path> [MiB] [chunk KiB]: raw camera read rate (no hash, no upload), USB DMA straight into PSRAM vs bounce+copy", .func = cmd_cam_read},
        {.command = "tasks", .help = "tasks: every task with its stack headroom (words), and the internal heap low-water mark", .func = cmd_tasks},
        {.command = "cam-multi", .help = "cam-multi <handle:size>...: several real camera objects through the upload engine at once to scratch keys (not the library), verified and deleted", .func = cmd_cam_multi},
        {.command = "pipe-multi", .help = "pipe-multi [files] [MiB each]: synthetic files through the upload engine with the sync window (concurrent uploads), no camera", .func = cmd_pipe_multi},
        {.command = "cam-pipe", .help = "cam-pipe <handle> <size>: one real camera object through the whole pipeline to a scratch key (not the library), verified, then deleted", .func = cmd_cam_pipe},
        {.command = "usb-direct", .help = "usb-direct [on|off]: zero-copy USB DMA reads on/off (A/B for diagnostics)", .func = cmd_usb_direct},
        {.command = "pipe-test", .help = "pipe-test [MiB] [MB/s]: push a synthetic object through the real read-ahead/upload pipeline at an emulated camera rate (0 = unthrottled), verify by read-back", .func = cmd_pipe_test},
        {.command = "tcp-speed", .help = "tcp-speed <host> <port> [MiB]: raw TCP send test (no HTTP, no TLS) to isolate lwIP + Ethernet", .func = cmd_tcp_speed},
        {.command = "net-speed", .help = "net-speed [MiB]: upload a scratch object to the bucket and print the cloud-path throughput (no camera involved)", .func = cmd_net_speed},
        {.command = "c6", .help = "ESP32-C6 radio co-processor: link state, firmware version, update progress", .func = cmd_c6},
        {.command = "c6-update", .help = "c6-update [url [sha256]]: install an ESP-Hosted co-processor image over SDIO (default: the pinned " COPROC_HOST_LIB_VERSION " image); the dock restarts afterwards", .func = cmd_c6_update},
        {.command = "log", .help = "The web UI's log ring", .func = cmd_log},
        {.command = "reboot", .help = "Restart the dock", .func = cmd_reboot},
#ifdef BOARD_I2C_SDA_GPIO
        {.command = "i2c-scan", .help = "List the devices that answer on the internal I2C bus (DSI/CSI connectors)", .func = cmd_i2c_scan},
#endif
#if CONFIG_RRC_DISPLAY_RPI_DSI
        {.command = "dsi-reg", .help = "dsi-reg <reg> <value>: write a TC358762 bridge register (hex) over DSI (bring-up tool)", .func = cmd_dsi_reg},
        {.command = "fb-pattern", .help = "fb-pattern [off]: hold a test pattern in the Pi DSI framebuffer, bypassing LVGL, until `fb-pattern off` (bring-up tool)", .func = cmd_fb_pattern},
        {.command = "fb-dump", .help = "Print the Pi DSI framebuffer as base64 rows (bring-up tool, ~1.5 MB on the console)", .func = cmd_fb_dump},
        {.command = "touch-poll", .help = "touch-poll [off]: stop or resume polling the touch controller over I2C (bring-up tool)", .func = cmd_touch_poll},
        {.command = "dsi-timing", .help = "dsi-timing [lane_mbps dpi_mhz hsw hbp hfp vsw vbp vfp burst|nonburst lp|nolp]: show or change the Pi DSI panel's link timing live (bring-up tool)", .func = cmd_dsi_timing},
#endif
        {.command = "ota", .help = "ota [check | install [url [sha256]] | cancel]: firmware update state; check the release (or test-build) manifest; install what it found, or any image by URL", .func = cmd_ota},
        {.command = "eth-rx-stall-test", .help = "Drop every received ethernet frame to test the receive watchdog (the dock restarts itself)", .func = cmd_eth_rx_stall_test},
    };
    for (size_t i = 0; i < sizeof cmds / sizeof cmds[0]; i++) esp_console_cmd_register(&cmds[i]);
    e = esp_console_start_repl(repl);
    if (e == ESP_OK) log_ring_printf("serial console ready: type `help` on the USB-C console port");
    return e;
}

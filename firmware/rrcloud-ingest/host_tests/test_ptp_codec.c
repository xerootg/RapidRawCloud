#include <stdio.h>
#include <string.h>
#include "rrc_ptp_codec.h"
static int fails = 0;
#define CHECK(c, ...) do { if (!(c)) { fails++; printf("FAIL %s:%d ", __FILE__, __LINE__); printf(__VA_ARGS__); printf("\n"); } } while (0)
static size_t put_str(uint8_t *p, const uint16_t *u16, size_t n_incl_nul) { p[0] = (uint8_t)n_incl_nul; for (size_t i = 0; i < n_incl_nul; i++) { p[1 + 2 * i] = (uint8_t)u16[i]; p[2 + 2 * i] = (uint8_t)(u16[i] >> 8); } return 1 + 2 * n_incl_nul; }
static void w32(uint8_t *p, uint32_t v) { p[0] = v; p[1] = v >> 8; p[2] = v >> 16; p[3] = v >> 24; }
static void w16(uint8_t *p, uint16_t v) { p[0] = v; p[1] = v >> 8; }
int main(void) {
    uint8_t buf[512];
    uint32_t params[2] = {0x10001, 0x20002};
    size_t n = ptp_encode_command(buf, PTP_OC_GetObjectInfo, 7, params, 2);
    CHECK(n == 20, "cmd len %zu", n);
    ptp_container_hdr h; CHECK(ptp_decode_hdr(buf, n, &h), "hdr");
    CHECK(h.length == 20 && h.type == PTP_CT_COMMAND && h.code == 0x1008 && h.transaction_id == 7, "hdr fields");
    CHECK(!ptp_decode_hdr(buf, 5, &h), "short hdr");
    /* string decode incl. surrogate pair (U+1F4F7 camera emoji) and empty */
    uint16_t s1[] = {'D', 'S', 'C', 0xD83D, 0xDCF7, '.', 'N', 'E', 'F', 0};
    size_t used = put_str(buf, s1, 10);
    char out[64]; size_t c = ptp_decode_string(buf, used, out, sizeof out);
    CHECK(c == used, "string consumed %zu/%zu", c, used);
    CHECK(!strcmp(out, "DSC\xf0\x9f\x93\xb7.NEF"), "string utf8 '%s'", out);
    buf[0] = 0; CHECK(ptp_decode_string(buf, 1, out, sizeof out) == 1 && out[0] == 0, "empty string");
    buf[0] = 5; CHECK(ptp_decode_string(buf, 4, out, sizeof out) == 0, "truncated string");
    /* u32 array */
    w32(buf, 3); w32(buf + 4, 1); w32(buf + 8, 2); w32(buf + 12, 0x80000001u);
    uint32_t ids[8]; int cnt = ptp_decode_u32_array(buf, 16, ids, 8);
    CHECK(cnt == 3 && ids[2] == 0x80000001u, "u32 array");
    CHECK(ptp_decode_u32_array(buf, 10, ids, 8) == -1, "u32 array short");
    cnt = ptp_decode_u32_array(buf, 16, ids, 2); CHECK(cnt == 3, "u32 array cap reports full count");
    /* ObjectInfo */
    memset(buf, 0, sizeof buf);
    w32(buf, 0x10001); w16(buf + 4, PTP_OFC_Undefined); w16(buf + 6, 0); w32(buf + 8, 52345678); w16(buf + 12, PTP_OFC_EXIF_JPEG); w32(buf + 14, 9000);
    w32(buf + 18, 160); w32(buf + 22, 120); w32(buf + 26, 8256); w32(buf + 30, 5504); w32(buf + 34, 14); w32(buf + 38, 0x20000); w16(buf + 42, 0); w32(buf + 44, 0); w32(buf + 48, 42);
    size_t pos = 52;
    uint16_t fn[] = {'D','S','C','_','0','0','0','1','.','N','E','F',0}; pos += put_str(buf + pos, fn, 13);
    uint16_t cd[] = {'2','0','2','6','1','0','1','0','T','1','2','3','4','5','6',0}; pos += put_str(buf + pos, cd, 16);
    uint16_t md[] = {'2','0','2','6','1','0','1','0','T','1','2','3','4','5','7',0}; pos += put_str(buf + pos, md, 16);
    buf[pos++] = 0; /* keywords */
    ptp_object_info oi; CHECK(ptp_decode_object_info(buf, pos, &oi), "objectinfo decode");
    CHECK(oi.storage_id == 0x10001 && oi.object_format == 0x3000 && oi.compressed_size == 52345678 && oi.parent_object == 0x20000 && oi.sequence_number == 42, "oi fields");
    CHECK(!strcmp(oi.filename, "DSC_0001.NEF") && !strcmp(oi.capture_date, "20261010T123456") && !strcmp(oi.modification_date, "20261010T123457"), "oi strings %s %s", oi.filename, oi.capture_date);
    CHECK(!ptp_decode_object_info(buf, 60, &oi), "oi truncated");
    /* DeviceInfo */
    memset(buf, 0, sizeof buf); pos = 0;
    w16(buf, 100); w32(buf + 2, 0x0000000A); w16(buf + 6, 100); pos = 8;
    uint16_t vd[] = {'m','i','c','r','o','s','o','f','t','.','c','o','m',':',' ','1','.','0',0}; pos += put_str(buf + pos, vd, 19);
    w16(buf + pos, 0); pos += 2;
    w32(buf + pos, 4); pos += 4; w16(buf + pos, 0x1001); w16(buf + pos + 2, 0x1002); w16(buf + pos + 4, 0x101B); w16(buf + pos + 6, 0x1009); pos += 8;
    w32(buf + pos, 1); pos += 4; w16(buf + pos, 0x4002); pos += 2;
    w32(buf + pos, 0); pos += 4;
    w32(buf + pos, 0); pos += 4;
    w32(buf + pos, 2); pos += 4; w16(buf + pos, 0x3801); w16(buf + pos + 2, 0x3000); pos += 4;
    uint16_t mf[] = {'N','i','k','o','n',' ','C','o','r','p','o','r','a','t','i','o','n',0}; pos += put_str(buf + pos, mf, 18);
    uint16_t mo[] = {'Z',' ','7','_','2',0}; pos += put_str(buf + pos, mo, 6);
    uint16_t ve[] = {'V','1','.','6','0',0}; pos += put_str(buf + pos, ve, 6);
    uint16_t sn[] = {'0','0','0','3','0','1','2','3','4','5',0}; pos += put_str(buf + pos, sn, 11);
    ptp_device_info di; CHECK(ptp_decode_device_info(buf, pos, &di), "deviceinfo decode");
    CHECK(!strcmp(di.manufacturer, "Nikon Corporation") && !strcmp(di.model, "Z 7_2") && !strcmp(di.serial_number, "0003012345") && !strcmp(di.device_version, "V1.60"), "di strings %s|%s|%s", di.manufacturer, di.model, di.serial_number);
    CHECK(di.supports_partial_object && di.supports_get_object && di.vendor_extension_id == 10, "di caps");
    CHECK(!ptp_decode_device_info(buf, pos - 5, &di), "di truncated");
    CHECK(ptp_format_is_file(0x3000) && !ptp_format_is_file(PTP_OFC_Association), "format filter");
    printf(fails ? "FAILED (%d)\n" : "ALL OK\n", fails);
    return fails ? 1 : 0;
}

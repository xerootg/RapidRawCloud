#include "rrc_ptp_codec.h"
#include <string.h>

static inline uint16_t rd16(const uint8_t *p) { return (uint16_t)(p[0] | (p[1] << 8)); }
static inline uint32_t rd32(const uint8_t *p) { return (uint32_t)p[0] | ((uint32_t)p[1] << 8) | ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24); }
static inline void wr16(uint8_t *p, uint16_t v) { p[0] = (uint8_t)v; p[1] = (uint8_t)(v >> 8); }
static inline void wr32(uint8_t *p, uint32_t v) { p[0] = (uint8_t)v; p[1] = (uint8_t)(v >> 8); p[2] = (uint8_t)(v >> 16); p[3] = (uint8_t)(v >> 24); }

size_t ptp_encode_command(uint8_t *out, uint16_t code, uint32_t tid, const uint32_t *params, size_t nparams)
{
    if (nparams > 5) nparams = 5;
    uint32_t len = PTP_HDR_LEN + 4 * (uint32_t)nparams;
    wr32(out, len); wr16(out + 4, PTP_CT_COMMAND); wr16(out + 6, code); wr32(out + 8, tid);
    for (size_t i = 0; i < nparams; i++) wr32(out + 12 + 4 * i, params[i]);
    return len;
}

bool ptp_decode_hdr(const uint8_t *in, size_t len, ptp_container_hdr *h)
{
    if (len < PTP_HDR_LEN) return false;
    h->length = rd32(in); h->type = rd16(in + 4); h->code = rd16(in + 6); h->transaction_id = rd32(in + 8);
    return true;
}

static size_t utf8_put(char *out, size_t cap, size_t o, uint32_t cp)
{
    char tmp[4]; int n;
    if (cp < 0x80) { tmp[0] = (char)cp; n = 1; }
    else if (cp < 0x800) { tmp[0] = (char)(0xc0 | (cp >> 6)); tmp[1] = (char)(0x80 | (cp & 0x3f)); n = 2; }
    else if (cp < 0x10000) { tmp[0] = (char)(0xe0 | (cp >> 12)); tmp[1] = (char)(0x80 | ((cp >> 6) & 0x3f)); tmp[2] = (char)(0x80 | (cp & 0x3f)); n = 3; }
    else { tmp[0] = (char)(0xf0 | (cp >> 18)); tmp[1] = (char)(0x80 | ((cp >> 12) & 0x3f)); tmp[2] = (char)(0x80 | ((cp >> 6) & 0x3f)); tmp[3] = (char)(0x80 | (cp & 0x3f)); n = 4; }
    if (o + (size_t)n + 1 > cap) return o; /* truncate silently */
    memcpy(out + o, tmp, (size_t)n);
    return o + (size_t)n;
}

size_t ptp_decode_string(const uint8_t *in, size_t len, char *out, size_t cap)
{
    if (len < 1 || cap == 0) return 0;
    uint8_t n = in[0];
    if (n == 0) { out[0] = 0; return 1; }
    size_t need = 1 + 2u * n;
    if (need > len) return 0;
    size_t o = 0;
    for (size_t i = 0; i < n; i++) {
        uint32_t cp = rd16(in + 1 + 2 * i);
        if (cp == 0) break; /* terminator (the count includes it) */
        if (cp >= 0xd800 && cp <= 0xdbff && i + 1 < n) {
            uint32_t lo = rd16(in + 1 + 2 * (i + 1));
            if (lo >= 0xdc00 && lo <= 0xdfff) { cp = 0x10000 + ((cp - 0xd800) << 10) + (lo - 0xdc00); i++; }
        }
        o = utf8_put(out, cap, o, cp);
    }
    out[o] = 0;
    return need;
}

int ptp_decode_u32_array(const uint8_t *in, size_t len, uint32_t *out, size_t cap)
{
    if (len < 4) return -1;
    uint32_t n = rd32(in);
    if ((size_t)n * 4 + 4 > len) return -1;
    for (size_t i = 0; i < n && i < cap; i++) out[i] = rd32(in + 4 + 4 * i);
    return (int)n;
}

static bool skip_u16_array(const uint8_t *in, size_t len, size_t *pos)
{
    if (*pos + 4 > len) return false;
    uint32_t n = rd32(in + *pos);
    *pos += 4;
    if (*pos + (size_t)n * 2 > len) return false;
    *pos += (size_t)n * 2;
    return true;
}

static bool scan_u16_array(const uint8_t *in, size_t len, size_t *pos, uint16_t needle_a, bool *found_a, uint16_t needle_b, bool *found_b)
{
    if (*pos + 4 > len) return false;
    uint32_t n = rd32(in + *pos);
    *pos += 4;
    if (*pos + (size_t)n * 2 > len) return false;
    for (uint32_t i = 0; i < n; i++) {
        uint16_t v = rd16(in + *pos + 2 * i);
        if (v == needle_a) *found_a = true;
        if (v == needle_b) *found_b = true;
    }
    *pos += (size_t)n * 2;
    return true;
}

static bool take_string(const uint8_t *in, size_t len, size_t *pos, char *out, size_t cap)
{
    if (*pos >= len) return false;
    size_t used = ptp_decode_string(in + *pos, len - *pos, out, cap);
    if (!used) return false;
    *pos += used;
    return true;
}

bool ptp_decode_object_info(const uint8_t *in, size_t len, ptp_object_info *oi)
{
    if (len < 52) return false;
    memset(oi, 0, sizeof *oi);
    oi->storage_id = rd32(in + 0);
    oi->object_format = rd16(in + 4);
    oi->protection_status = rd16(in + 6);
    oi->compressed_size = rd32(in + 8);
    oi->thumb_format = rd16(in + 12);
    oi->thumb_size = rd32(in + 14);
    /* 18: ThumbPixWidth, 22: ThumbPixHeight, 26: ImagePixWidth, 30: ImagePixHeight, 34: ImageBitDepth */
    oi->parent_object = rd32(in + 38);
    oi->association_type = rd16(in + 42);
    /* 44: AssociationDesc */
    oi->sequence_number = rd32(in + 48);
    size_t pos = 52;
    if (!take_string(in, len, &pos, oi->filename, sizeof oi->filename)) return false;
    if (!take_string(in, len, &pos, oi->capture_date, sizeof oi->capture_date)) return false;
    if (!take_string(in, len, &pos, oi->modification_date, sizeof oi->modification_date)) return false;
    /* Keywords string follows; not needed. */
    return true;
}

bool ptp_decode_device_info(const uint8_t *in, size_t len, ptp_device_info *di)
{
    memset(di, 0, sizeof *di);
    if (len < 8) return false;
    di->standard_version = rd16(in);
    di->vendor_extension_id = rd32(in + 2);
    size_t pos = 8; /* VendorExtensionVersion u16 at 6 */
    char scratch[256];
    if (!take_string(in, len, &pos, scratch, sizeof scratch)) return false; /* VendorExtensionDesc */
    if (pos + 2 > len) return false;
    pos += 2; /* FunctionalMode */
    if (!scan_u16_array(in, len, &pos, PTP_OC_GetPartialObject, &di->supports_partial_object, PTP_OC_GetObject, &di->supports_get_object)) return false;
    if (!skip_u16_array(in, len, &pos)) return false; /* EventsSupported */
    if (!skip_u16_array(in, len, &pos)) return false; /* DevicePropertiesSupported */
    if (!skip_u16_array(in, len, &pos)) return false; /* CaptureFormats */
    if (!skip_u16_array(in, len, &pos)) return false; /* ImageFormats */
    if (!take_string(in, len, &pos, di->manufacturer, sizeof di->manufacturer)) return false;
    if (!take_string(in, len, &pos, di->model, sizeof di->model)) return false;
    if (!take_string(in, len, &pos, di->device_version, sizeof di->device_version)) return false;
    if (!take_string(in, len, &pos, di->serial_number, sizeof di->serial_number)) return false;
    return true;
}

bool ptp_format_is_file(uint16_t ofc)
{
    return ofc != PTP_OFC_Association;
}

#include "rrc_proto.h"
#include <string.h>
#include <ctype.h>
#include <stdio.h>

static const char HEXL[] = "0123456789abcdef";

void rrc_uuid4_format(const uint8_t in[16], char out[RRC_DEVICE_ID_LEN + 1])
{
    uint8_t b[16];
    memcpy(b, in, 16);
    b[6] = (uint8_t)((b[6] & 0x0f) | 0x40); /* version 4 */
    b[8] = (uint8_t)((b[8] & 0x3f) | 0x80); /* RFC 4122 variant */
    size_t o = 0;
    for (int i = 0; i < 16; i++) {
        if (i == 4 || i == 6 || i == 8 || i == 10) out[o++] = '-';
        out[o++] = HEXL[b[i] >> 4];
        out[o++] = HEXL[b[i] & 15];
    }
    out[o] = 0;
}

bool rrc_device_id_valid(const char *s)
{
    if (!s || strlen(s) != 36) return false;
    for (int i = 0; i < 36; i++) {
        char c = s[i];
        switch (i) {
        case 8: case 13: case 18: case 23: if (c != '-') return false; break;
        case 14: if (c != '4') return false; break;
        case 19: if (!(c == '8' || c == '9' || c == 'a' || c == 'b')) return false; break;
        default: if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))) return false;
        }
    }
    return true;
}

const char *rrc_relkey_err_str(rrc_relkey_err e)
{
    switch (e) {
    case RRC_RELKEY_OK: return "ok";
    case RRC_RELKEY_EMPTY: return "empty relkey";
    case RRC_RELKEY_BACKSLASH: return "backslash in relkey";
    case RRC_RELKEY_COLON: return "colon in relkey";
    case RRC_RELKEY_CONTROL: return "control character in relkey";
    case RRC_RELKEY_LEADING_SLASH: return "leading slash in relkey";
    case RRC_RELKEY_BAD_SEGMENT: return "dot, dot-dot, or empty segment in relkey";
    case RRC_RELKEY_TRAILING_DOT_SPACE: return "segment ends with dot or space";
    case RRC_RELKEY_WINDOWS_RESERVED: return "Windows-reserved device name segment";
    case RRC_RELKEY_ENGINE_RESERVED: return "engine-reserved `.rr.` segment";
    case RRC_RELKEY_INVALID_UTF8: return "invalid UTF-8";
    case RRC_RELKEY_NOT_NFC: return "relkey is not NFC-normalized";
    case RRC_RELKEY_TOO_LONG: return "relkey too long";
    }
    return "?";
}

/* Decode one UTF-8 scalar; returns byte length or 0 on malformed input. */
static int utf8_decode(const unsigned char *p, uint32_t *cp)
{
    if (p[0] < 0x80) { *cp = p[0]; return 1; }
    if ((p[0] & 0xe0) == 0xc0) {
        if ((p[1] & 0xc0) != 0x80) return 0;
        *cp = ((uint32_t)(p[0] & 0x1f) << 6) | (p[1] & 0x3f);
        return *cp >= 0x80 ? 2 : 0;
    }
    if ((p[0] & 0xf0) == 0xe0) {
        if ((p[1] & 0xc0) != 0x80 || (p[2] & 0xc0) != 0x80) return 0;
        *cp = ((uint32_t)(p[0] & 0x0f) << 12) | ((uint32_t)(p[1] & 0x3f) << 6) | (p[2] & 0x3f);
        return (*cp >= 0x800 && !(*cp >= 0xd800 && *cp <= 0xdfff)) ? 3 : 0;
    }
    if ((p[0] & 0xf8) == 0xf0) {
        if ((p[1] & 0xc0) != 0x80 || (p[2] & 0xc0) != 0x80 || (p[3] & 0xc0) != 0x80) return 0;
        *cp = ((uint32_t)(p[0] & 0x07) << 18) | ((uint32_t)(p[1] & 0x3f) << 12) | ((uint32_t)(p[2] & 0x3f) << 6) | (p[3] & 0x3f);
        return (*cp >= 0x10000 && *cp <= 0x10ffff) ? 4 : 0;
    }
    return 0;
}

/* Code points whose presence means the text is (almost certainly) not NFC:
 * combining marks and conjoining Hangul jamo. Without full normalization tables
 * this is the conservative check — the receivers' wire decoders fail closed on
 * non-NFC relkeys (§1.1), so we must never publish one. Camera filesystems are
 * ASCII, so this only bites user-typed template text. */
static bool is_decomposition_mark(uint32_t cp)
{
    return (cp >= 0x0300 && cp <= 0x036f) || (cp >= 0x1ab0 && cp <= 0x1aff) || (cp >= 0x1dc0 && cp <= 0x1dff) ||
           (cp >= 0x20d0 && cp <= 0x20ff) || (cp >= 0xfe20 && cp <= 0xfe2f) || (cp >= 0x1100 && cp <= 0x11ff) ||
           (cp >= 0x3099 && cp <= 0x309a) || (cp >= 0xf900 && cp <= 0xfaff) || (cp >= 0x2f800 && cp <= 0x2fa1f);
}

static bool seg_is_windows_reserved(const char *seg, size_t len)
{
    /* base name = before the first '.', trailing spaces stripped */
    size_t base = 0;
    while (base < len && seg[base] != '.') base++;
    while (base > 0 && seg[base - 1] == ' ') base--;
    if (base == 3) {
        static const char *r3[] = {"con", "prn", "aux", "nul"};
        for (int i = 0; i < 4; i++) {
            if (tolower((unsigned char)seg[0]) == r3[i][0] && tolower((unsigned char)seg[1]) == r3[i][1] &&
                tolower((unsigned char)seg[2]) == r3[i][2]) return true;
        }
        return false;
    }
    if (base > 3 && ((tolower((unsigned char)seg[0]) == 'c' && tolower((unsigned char)seg[1]) == 'o' && tolower((unsigned char)seg[2]) == 'm') ||
                     (tolower((unsigned char)seg[0]) == 'l' && tolower((unsigned char)seg[1]) == 'p' && tolower((unsigned char)seg[2]) == 't'))) {
        /* exactly one char after the prefix: ASCII 1-9 or U+00B9/U+00B2/U+00B3 (2-byte UTF-8: C2 B9 / C2 B2 / C2 B3) */
        size_t rest = base - 3;
        const unsigned char *q = (const unsigned char *)seg + 3;
        if (rest == 1) return q[0] >= '1' && q[0] <= '9';
        if (rest == 2) return q[0] == 0xc2 && (q[1] == 0xb9 || q[1] == 0xb2 || q[1] == 0xb3);
    }
    return false;
}

rrc_relkey_err rrc_relkey_validate(const char *s)
{
    if (!s || !*s) return RRC_RELKEY_EMPTY;
    size_t n = strlen(s);
    if (n > RRC_RELKEY_MAX) return RRC_RELKEY_TOO_LONG;
    for (const unsigned char *p = (const unsigned char *)s; *p;) {
        uint32_t cp;
        int l = utf8_decode(p, &cp);
        if (!l) return RRC_RELKEY_INVALID_UTF8;
        if (cp == '\\') return RRC_RELKEY_BACKSLASH;
        if (cp == ':') return RRC_RELKEY_COLON;
        if (cp < 0x20 || cp == 0x7f || (cp >= 0x80 && cp < 0xa0)) return RRC_RELKEY_CONTROL;
        if (is_decomposition_mark(cp)) return RRC_RELKEY_NOT_NFC;
        p += l;
    }
    if (s[0] == '/') return RRC_RELKEY_LEADING_SLASH;
    const char *seg = s;
    for (;;) {
        const char *end = strchr(seg, '/');
        size_t len = end ? (size_t)(end - seg) : strlen(seg);
        if (len == 0 || (len == 1 && seg[0] == '.') || (len == 2 && seg[0] == '.' && seg[1] == '.')) return RRC_RELKEY_BAD_SEGMENT;
        if (seg[len - 1] == '.' || seg[len - 1] == ' ') return RRC_RELKEY_TRAILING_DOT_SPACE;
        if (len >= 4 && !memcmp(seg, ".rr.", 4)) return RRC_RELKEY_ENGINE_RESERVED;
        if (seg_is_windows_reserved(seg, len)) return RRC_RELKEY_WINDOWS_RESERVED;
        if (!end) break;
        seg = end + 1;
    }
    return RRC_RELKEY_OK;
}

rrc_relkey_err rrc_relkey_sanitize(const char *in, char *out, size_t cap)
{
    if (!in || cap < 2) { if (cap) out[0] = 0; return RRC_RELKEY_EMPTY; }
    /* Pass 1: byte-level repairs into `out`. */
    size_t o = 0;
    for (const unsigned char *p = (const unsigned char *)in; *p; p++) {
        unsigned char c = *p;
        if (c == '\\' || c == ':' || c < 0x20 || c == 0x7f) c = '_';
        if (c == '/' && (o == 0 || out[o - 1] == '/')) continue; /* leading / repeated slashes */
        if (o + 1 >= cap) { out[0] = 0; return RRC_RELKEY_TOO_LONG; }
        out[o++] = (char)c;
    }
    out[o] = 0;
    /* Pass 2: per-segment repairs, rebuilt into a scratch buffer. */
    char tmp[RRC_RELKEY_MAX + 8];
    size_t t = 0;
    char *seg = out;
    while (*seg) {
        char *end = strchr(seg, '/');
        size_t len = end ? (size_t)(end - seg) : strlen(seg);
        /* trim trailing dots/spaces */
        while (len > 0 && (seg[len - 1] == '.' || seg[len - 1] == ' ')) len--;
        bool drop = (len == 0);
        char fixed[RRC_RELKEY_MAX + 4];
        size_t fl = 0;
        if (!drop) {
            if (len >= 4 && !memcmp(seg, ".rr.", 4)) { fixed[fl++] = '_'; }
            memcpy(fixed + fl, seg, len); fl += len;
            if (seg_is_windows_reserved(seg, len)) { fixed[fl++] = '_'; }
            fixed[fl] = 0;
        }
        if (!drop) {
            if (t + fl + 2 > sizeof tmp) { out[0] = 0; return RRC_RELKEY_TOO_LONG; }
            if (t) tmp[t++] = '/';
            memcpy(tmp + t, fixed, fl); t += fl;
        }
        if (!end) break;
        seg = end + 1;
    }
    tmp[t] = 0;
    if (t + 1 > cap) { out[0] = 0; return RRC_RELKEY_TOO_LONG; }
    memcpy(out, tmp, t + 1);
    return rrc_relkey_validate(out);
}

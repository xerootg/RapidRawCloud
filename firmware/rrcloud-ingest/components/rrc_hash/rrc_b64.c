#include "rrc_hash.h"

char *rrc_hex_lower(const uint8_t *in, size_t len, char *out)
{
    static const char hx[] = "0123456789abcdef";
    for (size_t i = 0; i < len; i++) { out[2 * i] = hx[in[i] >> 4]; out[2 * i + 1] = hx[in[i] & 15]; }
    out[2 * len] = 0;
    return out;
}

static size_t b64_generic(const uint8_t *in, size_t len, char *out, const char *alphabet, bool pad)
{
    size_t o = 0;
    size_t i = 0;
    while (i + 3 <= len) {
        uint32_t v = ((uint32_t)in[i] << 16) | ((uint32_t)in[i + 1] << 8) | in[i + 2];
        out[o++] = alphabet[(v >> 18) & 63]; out[o++] = alphabet[(v >> 12) & 63];
        out[o++] = alphabet[(v >> 6) & 63]; out[o++] = alphabet[v & 63];
        i += 3;
    }
    size_t rem = len - i;
    if (rem == 1) {
        uint32_t v = (uint32_t)in[i] << 16;
        out[o++] = alphabet[(v >> 18) & 63]; out[o++] = alphabet[(v >> 12) & 63];
        if (pad) { out[o++] = '='; out[o++] = '='; }
    } else if (rem == 2) {
        uint32_t v = ((uint32_t)in[i] << 16) | ((uint32_t)in[i + 1] << 8);
        out[o++] = alphabet[(v >> 18) & 63]; out[o++] = alphabet[(v >> 12) & 63]; out[o++] = alphabet[(v >> 6) & 63];
        if (pad) out[o++] = '=';
    }
    out[o] = 0;
    return o;
}

size_t rrc_base64_encode(const uint8_t *in, size_t len, char *out)
{
    return b64_generic(in, len, out, "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/", true);
}

size_t rrc_base64url_encode_nopad(const uint8_t *in, size_t len, char *out)
{
    return b64_generic(in, len, out, "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_", false);
}

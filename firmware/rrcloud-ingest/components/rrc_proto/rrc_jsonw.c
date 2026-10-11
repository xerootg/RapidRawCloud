#include "rrc_proto.h"
#include <stdio.h>
#include <string.h>

void rrc_jsonw_init(rrc_jsonw *w, char *buf, size_t cap)
{
    w->buf = buf; w->cap = cap; w->len = 0; w->overflow = (cap == 0);
    if (cap) buf[0] = 0;
}

static void putc_(rrc_jsonw *w, char c)
{
    if (w->overflow) return;
    if (w->len + 1 >= w->cap) { w->overflow = true; return; }
    w->buf[w->len++] = c;
    w->buf[w->len] = 0;
}

void rrc_jsonw_raw(rrc_jsonw *w, const char *s) { while (*s) putc_(w, *s++); }

static void escape_into(rrc_jsonw *w, const char *s)
{
    static const char HEX[] = "0123456789abcdef";
    putc_(w, '"');
    for (const unsigned char *p = (const unsigned char *)s; *p; p++) {
        unsigned char c = *p;
        switch (c) {
        case '"': rrc_jsonw_raw(w, "\\\""); break;
        case '\\': rrc_jsonw_raw(w, "\\\\"); break;
        case '\b': rrc_jsonw_raw(w, "\\b"); break;
        case '\f': rrc_jsonw_raw(w, "\\f"); break;
        case '\n': rrc_jsonw_raw(w, "\\n"); break;
        case '\r': rrc_jsonw_raw(w, "\\r"); break;
        case '\t': rrc_jsonw_raw(w, "\\t"); break;
        default:
            if (c < 0x20) {
                char e[7] = {'\\', 'u', '0', '0', HEX[c >> 4], HEX[c & 15], 0};
                rrc_jsonw_raw(w, e);
            } else {
                putc_(w, (char)c);
            }
        }
    }
    putc_(w, '"');
}

void rrc_jsonw_str(rrc_jsonw *w, const char *s) { escape_into(w, s ? s : ""); }

void rrc_jsonw_u64(rrc_jsonw *w, uint64_t v)
{
    char b[24];
    snprintf(b, sizeof b, "%llu", (unsigned long long)v);
    rrc_jsonw_raw(w, b);
}

void rrc_jsonw_i64(rrc_jsonw *w, int64_t v)
{
    char b[24];
    snprintf(b, sizeof b, "%lld", (long long)v);
    rrc_jsonw_raw(w, b);
}

void rrc_jsonw_key(rrc_jsonw *w, const char *key, bool first)
{
    if (!first) putc_(w, ',');
    escape_into(w, key);
    putc_(w, ':');
}

int rrc_jsonw_finish(const rrc_jsonw *w) { return w->overflow ? -1 : (int)w->len; }

int rrc_json_quote(const char *s, char *out, size_t cap)
{
    rrc_jsonw w;
    rrc_jsonw_init(&w, out, cap);
    rrc_jsonw_str(&w, s);
    return rrc_jsonw_finish(&w);
}

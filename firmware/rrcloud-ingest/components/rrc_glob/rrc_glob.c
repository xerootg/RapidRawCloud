#include "rrc_glob.h"
#include <ctype.h>
#include <string.h>

static int lc(int c) { return tolower((unsigned char)c); }

/* Match `p` against `s`; `p` may not contain `/` unless `segment_only` is false. */
static bool match_here(const char *p, const char *s, bool whole_path)
{
    while (*p) {
        if (*p == '*') {
            bool dstar = (p[1] == '*');
            const char *np = p + (dstar ? 2 : 1);
            if (dstar && *np == '/') {
                /* "**\/" matches zero or more complete segments */
                if (match_here(np + 1, s, whole_path)) return true;
                for (const char *t = s; *t; t++) {
                    if (*t == '/' && match_here(np + 1, t + 1, whole_path)) return true;
                }
                return false;
            }
            for (const char *t = s;; t++) {
                if (match_here(np, t, whole_path)) return true;
                if (!*t) return false;
                if (!dstar && whole_path && *t == '/') return false;
            }
        }
        if (!*s) return false;
        if (*p == '?') {
            if (whole_path && *s == '/') return false;
            p++; s++; continue;
        }
        if (*p == '[') {
            const char *q = p + 1;
            bool neg = false, hit = false;
            if (*q == '!' || *q == '^') { neg = true; q++; }
            bool first = true;
            while (*q && (*q != ']' || first)) {
                first = false;
                int lo = lc(*q);
                if (q[1] == '-' && q[2] && q[2] != ']') {
                    int hi = lc(q[2]);
                    if (lc(*s) >= lo && lc(*s) <= hi) hit = true;
                    q += 3;
                } else {
                    if (lc(*s) == lo) hit = true;
                    q++;
                }
            }
            if (*q != ']') return false; /* malformed class: no match */
            if (hit == neg) return false;
            if (whole_path && *s == '/') return false;
            p = q + 1; s++; continue;
        }
        char pc = *p;
        if (pc == '\\' && p[1]) { pc = p[1]; p++; }
        if (lc(pc) != lc(*s)) return false;
        p++; s++;
    }
    return *s == 0;
}

bool rrc_glob_match(const char *pattern, const char *path)
{
    if (!pattern || !path) return false;
    while (*pattern == '/') pattern++;
    while (*path == '/') path++;
    if (!*pattern) return false;
    if (strchr(pattern, '/')) return match_here(pattern, path, true);
    const char *base = strrchr(path, '/');
    base = base ? base + 1 : path;
    return match_here(pattern, base, false);
}

bool rrc_glob_match_any(const char *patterns, const char *path)
{
    if (!patterns) return false;
    const char *p = patterns;
    while (*p) {
        while (*p && (isspace((unsigned char)*p) || *p == ',' || *p == ';')) p++;
        if (!*p) break;
        const char *e = p;
        while (*e && !isspace((unsigned char)*e) && *e != ',' && *e != ';') e++;
        char one[256];
        size_t n = (size_t)(e - p);
        if (n < sizeof one) {
            memcpy(one, p, n); one[n] = 0;
            if (rrc_glob_match(one, path)) return true;
        }
        p = e;
    }
    return false;
}

bool rrc_glob_selected(const char *include, const char *exclude, const char *path)
{
    if (!rrc_glob_match_any(include, path)) return false;
    if (exclude && *exclude && rrc_glob_match_any(exclude, path)) return false;
    return true;
}

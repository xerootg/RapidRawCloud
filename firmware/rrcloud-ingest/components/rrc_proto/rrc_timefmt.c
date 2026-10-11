#include "rrc_proto.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>

int64_t rrc_timegm(const struct tm *tm)
{
    /* days from civil (Howard Hinnant) */
    int64_t y = tm->tm_year + 1900;
    int64_t m = tm->tm_mon + 1;
    int64_t d = tm->tm_mday;
    y -= m <= 2;
    int64_t era = (y >= 0 ? y : y - 399) / 400;
    int64_t yoe = y - era * 400;
    int64_t doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
    int64_t doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    int64_t days = era * 146097 + doe - 719468;
    return days * 86400 + tm->tm_hour * 3600 + tm->tm_min * 60 + tm->tm_sec;
}

static int month_from_abbrev(const char *s)
{
    static const char *m[] = {"jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"};
    for (int i = 0; i < 12; i++) {
        if (tolower((unsigned char)s[0]) == m[i][0] && tolower((unsigned char)s[1]) == m[i][1] && tolower((unsigned char)s[2]) == m[i][2]) return i;
    }
    return -1;
}

int64_t rrc_parse_http_date(const char *s)
{
    if (!s) return -1;
    /* "Fri, 10 Oct 2026 12:34:56 GMT" — skip weekday + comma */
    const char *p = strchr(s, ',');
    p = p ? p + 1 : s;
    while (*p == ' ') p++;
    int day, year, hh, mm, ss;
    char mon[4] = {0};
    if (sscanf(p, "%2d %3s %4d %2d:%2d:%2d", &day, mon, &year, &hh, &mm, &ss) != 6) return -1;
    int mo = month_from_abbrev(mon);
    if (mo < 0 || day < 1 || day > 31 || hh > 23 || mm > 59 || ss > 60) return -1;
    struct tm tm = {0};
    tm.tm_year = year - 1900; tm.tm_mon = mo; tm.tm_mday = day; tm.tm_hour = hh; tm.tm_min = mm; tm.tm_sec = ss;
    return rrc_timegm(&tm);
}

int64_t rrc_parse_ptp_datetime(const char *s)
{
    /* YYYYMMDDThhmmss[.s][Z|+hhmm|-hhmm] */
    if (!s || strlen(s) < 15 || s[8] != 'T') return -1;
    for (int i = 0; i < 15; i++) if (i != 8 && !isdigit((unsigned char)s[i])) return -1;
    struct tm tm = {0};
    tm.tm_year = atoi((char[5]){s[0], s[1], s[2], s[3], 0}) - 1900;
    tm.tm_mon = atoi((char[3]){s[4], s[5], 0}) - 1;
    tm.tm_mday = atoi((char[3]){s[6], s[7], 0});
    tm.tm_hour = atoi((char[3]){s[9], s[10], 0});
    tm.tm_min = atoi((char[3]){s[11], s[12], 0});
    tm.tm_sec = atoi((char[3]){s[13], s[14], 0});
    if (tm.tm_mon < 0 || tm.tm_mon > 11 || tm.tm_mday < 1 || tm.tm_mday > 31 || tm.tm_hour > 23 || tm.tm_min > 59 || tm.tm_sec > 60) return -1;
    int64_t t = rrc_timegm(&tm);
    const char *p = s + 15;
    if (*p == '.') { p++; while (isdigit((unsigned char)*p)) p++; }
    if (*p == '+' || *p == '-') {
        int sign = *p == '+' ? 1 : -1;
        if (strlen(p) >= 5 && isdigit((unsigned char)p[1]) && isdigit((unsigned char)p[2]) && isdigit((unsigned char)p[3]) && isdigit((unsigned char)p[4])) {
            int oh = (p[1] - '0') * 10 + (p[2] - '0'), om = (p[3] - '0') * 10 + (p[4] - '0');
            t -= sign * (oh * 3600 + om * 60);
        }
    }
    return t;
}

void rrc_format_iso8601(int64_t t, char out[21])
{
    time_t tt = (time_t)t;
    struct tm tm;
    gmtime_r(&tt, &tm);
    strftime(out, 21, "%Y-%m-%dT%H:%M:%SZ", &tm);
}

/* ---- key template ------------------------------------------------------- */

static void append(char *out, size_t cap, size_t *o, const char *s)
{
    while (*s && *o + 1 < cap) out[(*o)++] = *s++;
    out[*o] = 0;
}

rrc_relkey_err rrc_template_expand(const char *tpl, const rrc_template_vars *v, char *out, size_t cap)
{
    char buf[RRC_RELKEY_MAX * 2];
    size_t o = 0;
    buf[0] = 0;
    struct tm tm = {0};
    if (v->when > 0) { time_t t = (time_t)v->when; gmtime_r(&t, &tm); }
    const char *name = v->name ? v->name : "";
    const char *dot = strrchr(name, '.');
    char stem[RRC_RELKEY_MAX];
    if (dot && dot != name) { size_t n = (size_t)(dot - name); if (n >= sizeof stem) n = sizeof stem - 1; memcpy(stem, name, n); stem[n] = 0; }
    else { strncpy(stem, name, sizeof stem - 1); stem[sizeof stem - 1] = 0; }
    for (const char *p = tpl ? tpl : ""; *p;) {
        if (*p == '{') {
            const char *e = strchr(p, '}');
            if (!e) { append(buf, sizeof buf, &o, p); break; }
            size_t n = (size_t)(e - p - 1);
            char tag[16] = {0};
            if (n < sizeof tag) memcpy(tag, p + 1, n);
            char num[16];
            if (!strcmp(tag, "name")) append(buf, sizeof buf, &o, name);
            else if (!strcmp(tag, "stem")) append(buf, sizeof buf, &o, stem);
            else if (!strcmp(tag, "ext")) append(buf, sizeof buf, &o, (dot && dot != name) ? dot + 1 : "");
            else if (!strcmp(tag, "path")) append(buf, sizeof buf, &o, v->path ? v->path : "");
            else if (!strcmp(tag, "model")) append(buf, sizeof buf, &o, v->model ? v->model : "camera");
            else if (!strcmp(tag, "serial")) append(buf, sizeof buf, &o, v->serial ? v->serial : "");
            else if (!strcmp(tag, "yyyy")) { snprintf(num, sizeof num, "%04d", v->when > 0 ? tm.tm_year + 1900 : 0); append(buf, sizeof buf, &o, num); }
            else if (!strcmp(tag, "mm")) { snprintf(num, sizeof num, "%02d", v->when > 0 ? tm.tm_mon + 1 : 0); append(buf, sizeof buf, &o, num); }
            else if (!strcmp(tag, "dd")) { snprintf(num, sizeof num, "%02d", v->when > 0 ? tm.tm_mday : 0); append(buf, sizeof buf, &o, num); }
            else if (!strcmp(tag, "hh")) { snprintf(num, sizeof num, "%02d", v->when > 0 ? tm.tm_hour : 0); append(buf, sizeof buf, &o, num); }
            else { append(buf, sizeof buf, &o, "{"); append(buf, sizeof buf, &o, tag); append(buf, sizeof buf, &o, "}"); }
            p = e + 1;
        } else {
            char c[2] = {*p++, 0};
            append(buf, sizeof buf, &o, c);
        }
    }
    return rrc_relkey_sanitize(buf, out, cap);
}

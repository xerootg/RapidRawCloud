#include "rrc_proto.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

int rrc_ledger_format(const rrc_ledger_rec *r, char *out, size_t cap)
{
    const char *sid = r->source_id ? r->source_id : "", *sp = r->source_path ? r->source_path : "";
    const char *rk = r->relkey ? r->relkey : "", *b3 = r->blake3_hex ? r->blake3_hex : "";
    if (strpbrk(sid, "\t\n") || strpbrk(sp, "\t\n") || strpbrk(rk, "\t\n")) return -1;
    int n = snprintf(out, cap, "L1\t%c\t%s\t%s\t%llu\t%lld\t%s\t%s\t%llu\t%lld\n", r->status, sid, sp,
                     (unsigned long long)r->size, (long long)r->mtime, rk, b3, (unsigned long long)r->seq, (long long)r->ts);
    return (n < 0 || (size_t)n >= cap) ? -1 : n;
}

int rrc_ledger_parse(char *line, rrc_ledger_rec *r)
{
    char *f[10];
    int n = 0;
    char *p = line;
    line[strcspn(line, "\r\n")] = 0;
    while (n < 10) {
        f[n++] = p;
        char *t = strchr(p, '\t');
        if (!t) break;
        *t = 0;
        p = t + 1;
    }
    if (n != 10 || strcmp(f[0], "L1") || strlen(f[1]) != 1) return -1;
    memset(r, 0, sizeof *r);
    r->status = f[1][0];
    r->source_id = f[2];
    r->source_path = f[3];
    r->size = strtoull(f[4], NULL, 10);
    r->mtime = strtoll(f[5], NULL, 10);
    r->relkey = f[6];
    r->blake3_hex = f[7];
    r->seq = strtoull(f[8], NULL, 10);
    r->ts = strtoll(f[9], NULL, 10);
    return 0;
}

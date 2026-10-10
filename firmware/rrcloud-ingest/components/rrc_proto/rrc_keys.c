#include "rrc_proto.h"
#include <stdarg.h>
#include <stdio.h>
#include <string.h>

static int fmt(char *out, size_t cap, const char *f, ...) __attribute__((format(printf, 3, 4)));
static int fmt(char *out, size_t cap, const char *f, ...)
{
    va_list ap;
    va_start(ap, f);
    int n = vsnprintf(out, cap, f, ap);
    va_end(ap);
    return (n < 0 || (size_t)n >= cap) ? -1 : n;
}

int rrc_key_library(const char *relkey, char *out, size_t cap) { return fmt(out, cap, RRC_LIBRARY_PREFIX "%s", relkey); }

int rrc_segment_filename(uint64_t seq, char out[32]) { return fmt(out, 32, "%016llx.v%d.ndjson", (unsigned long long)seq, RRC_JOURNAL_VERSION); }

int rrc_key_journal_segment(const char *device, uint64_t seq, char *out, size_t cap)
{
    char fn[32];
    rrc_segment_filename(seq, fn);
    return fmt(out, cap, RRC_CONTROL_PREFIX "journal/%s/%s", device, fn);
}

int rrc_key_manifest(const char *device, char *out, size_t cap) { return fmt(out, cap, RRC_CONTROL_PREFIX "manifests/%s.json.gz", device); }

int rrc_key_device_registry(const char *device, char *out, size_t cap) { return fmt(out, cap, RRC_CONTROL_PREFIX "devices/%s.json", device); }

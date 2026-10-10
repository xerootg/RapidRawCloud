/* Key schema: delegates to the generated SDK so templates live in one place. */
#include "rrc_proto.h"
#include "rrcloud_proto.h"
#include <stdio.h>
#include <string.h>

static int ret(int n) { return n < 0 ? -1 : n; }

int rrc_key_library(const char *relkey, char *out, size_t cap) { return ret(rrcp_key_library_original(relkey, out, cap)); }

int rrc_segment_filename(uint64_t seq, char out[32])
{
    int n = snprintf(out, 32, "%016llx.v%d.ndjson", (unsigned long long)seq, RRCP_JOURNAL_VERSION);
    return (n < 0 || n >= 32) ? -1 : n;
}

int rrc_key_journal_segment(const char *device, uint64_t seq, char *out, size_t cap) { return ret(rrcp_key_journal_segment(device, seq, out, cap)); }
int rrc_key_manifest(const char *device, char *out, size_t cap) { return ret(rrcp_key_manifest(device, out, cap)); }
int rrc_key_device_registry(const char *device, char *out, size_t cap) { return ret(rrcp_key_device_registry(device, out, cap)); }

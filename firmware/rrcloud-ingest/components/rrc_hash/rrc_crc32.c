#include "rrc_hash.h"

/* Table-less bitwise CRC-32 (zlib polynomial). The manifest is small and
 * written rarely, so the ~8x slowdown versus a table is irrelevant; the
 * 1 KiB table saved matters more on a device that also runs TLS + USB. */
uint32_t rrc_crc32_update(uint32_t crc, const void *data, size_t len)
{
    const uint8_t *p = (const uint8_t *)data;
    crc = ~crc;
    while (len--) {
        crc ^= *p++;
        for (int k = 0; k < 8; k++) crc = (crc >> 1) ^ (0xEDB88320u & (0u - (crc & 1u)));
    }
    return ~crc;
}

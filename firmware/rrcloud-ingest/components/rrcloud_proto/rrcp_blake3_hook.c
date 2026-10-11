/* The generated SDK hashes some key parts (tombstones, thumbpacks); supply BLAKE3. */
#include "rrcloud_proto.h"
#include "rrc_blake3.h"

void rrcp_blake3_hook(const uint8_t *data, size_t len, uint8_t out[32]) { rrc_blake3(data, len, out); }

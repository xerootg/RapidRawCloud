#include "rrc_hash.h"
#include <string.h>

void rrc_hmac_sha256(const void *key, size_t key_len, const void *data, size_t data_len, uint8_t out[32])
{
    uint8_t k[64] = {0};
    if (key_len > 64) {
        rrc_sha256(key, key_len, k);
    } else {
        memcpy(k, key, key_len);
    }
    uint8_t ipad[64], opad[64];
    for (int i = 0; i < 64; i++) { ipad[i] = k[i] ^ 0x36; opad[i] = k[i] ^ 0x5c; }
    uint8_t inner[32];
    rrc_sha256_ctx c;
    rrc_sha256_init(&c);
    rrc_sha256_update(&c, ipad, 64);
    rrc_sha256_update(&c, data, data_len);
    rrc_sha256_final(&c, inner);
    rrc_sha256_init(&c);
    rrc_sha256_update(&c, opad, 64);
    rrc_sha256_update(&c, inner, 32);
    rrc_sha256_final(&c, out);
}

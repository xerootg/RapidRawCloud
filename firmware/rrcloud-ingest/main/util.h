#pragma once
#include <string.h>
#include <stddef.h>
/* Bounded string copy that always NUL-terminates (truncating silently). */
static inline void scpy(char *dst, size_t cap, const char *src)
{
    if (!cap) return;
    size_t n = src ? strlen(src) : 0;
    if (n >= cap) n = cap - 1;
    if (n) memcpy(dst, src, n);
    dst[n] = 0;
}

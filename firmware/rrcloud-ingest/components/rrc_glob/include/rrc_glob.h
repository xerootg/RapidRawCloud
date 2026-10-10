/*
 * rrc_glob — case-insensitive glob matching for the "what to sync" filter.
 *
 * Syntax: `*` (any run within one path segment), `**` (any run across `/`),
 * `?` (one char), `[abc]` / `[a-z]` / `[!x]` classes, `\` escapes. Matching is
 * ASCII case-insensitive (camera filesystems are FAT; "DSC_0001.NEF" and
 * ".nef" should both match "*.nef"). A pattern with no `/` matches against the
 * path's final segment only (so "*.NEF" matches "DCIM/100NIKON/DSC_0001.NEF");
 * a pattern containing `/` matches the whole relative path.
 */
#pragma once
#include <stdbool.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

bool rrc_glob_match(const char *pattern, const char *path);

/* `patterns` is a whitespace/comma/semicolon separated list, e.g. "*.nef, *.dng *.jpg".
 * Returns true if any pattern matches. An empty list matches nothing. */
bool rrc_glob_match_any(const char *patterns, const char *path);

/* Returns true when `path` is selected: matches `include` and does not match `exclude`
 * (exclude may be NULL/empty). */
bool rrc_glob_selected(const char *include, const char *exclude, const char *path);

#ifdef __cplusplus
}
#endif

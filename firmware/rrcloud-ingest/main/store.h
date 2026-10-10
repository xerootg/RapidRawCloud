/*
 * store — the device's durable sync state on LittleFS (the firmware's
 * equivalent of the engine's redb state DB, §1.2 / §2.1.5):
 *
 *   /lfs/ledger.tsv        one line per source object seen (rrc_ledger_rec)
 *   /lfs/journal/pending   NDJSON entries not yet frozen into a segment
 *   /lfs/journal/<seq16>.v1.ndjson   frozen segment bytes awaiting PUT (crash replay re-PUTs byte-identical)
 *   /lfs/segments.tsv      "<first_seq>\t<max_seq>\t<published_server_ts>\n" per published segment (for §2.10 compaction)
 *   /lfs/state.kv          small key=value state (last_seq, published_cursor, manifest etag/ts, probe result)
 *
 * Durability order for the journal (§2.1.5): entry bytes are appended to
 * `pending` (fsync) BEFORE the caller treats the upload as journaled; freezing
 * renames `pending` to the segment file; publishing PUTs the file bytes, then
 * records the segment in segments.tsv and deletes the file.
 */
#pragma once
#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>
#include "esp_err.h"
#include "rrc_proto.h"

#define STORE_BASE "/lfs"

esp_err_t store_init(void);     /* mounts (formats on first boot) */

/* --- small kv ------------------------------------------------------------ */
esp_err_t store_kv_set_u64(const char *key, uint64_t v);
uint64_t store_kv_get_u64(const char *key, uint64_t def);
esp_err_t store_kv_set_i64(const char *key, int64_t v);
int64_t store_kv_get_i64(const char *key, int64_t def);
esp_err_t store_kv_set_str(const char *key, const char *v);
bool store_kv_get_str(const char *key, char *out, size_t cap);

/* --- ledger -------------------------------------------------------------- */
/* Loads ledger into the in-RAM index (PSRAM). Call once at boot. */
esp_err_t store_ledger_load(void);
/* Lookup by (source_id, source_path, size). Returns the record status char or 0 when absent. */
char store_ledger_lookup(const char *source_id, const char *source_path, uint64_t size);
/* Appends a record (fsync) and indexes it. */
esp_err_t store_ledger_append(const rrc_ledger_rec *r);
/* Iterates uploaded ('U') records — used to build the manifest. Return non-zero from cb to stop. */
typedef int (*store_ledger_cb)(void *ctx, const rrc_ledger_rec *r);
esp_err_t store_ledger_foreach_uploaded(store_ledger_cb cb, void *ctx);
size_t store_ledger_count(void);
size_t store_ledger_uploaded_count(void);
uint64_t store_ledger_uploaded_bytes(void);

/* --- journal ------------------------------------------------------------- */
/* The seq the next entry must carry (gap-free, never reused). Peeking costs no
 * flash write: a seq is COMMITTED by store_journal_append_pending() succeeding
 * with a line that carries it. Recovery at boot derives the high-water mark
 * from what is durable — kv `last_seq` (written at freeze), the published
 * cursor, the frozen segment files and `pending` — so a crash between peek and
 * append simply hands the same seq out again, and a crash after append sees
 * it in `pending`. */
uint64_t store_journal_next_seq(void);
/* Appends one encoded entry line (no trailing newline needed) to `pending`
 * (fsync) and advances the in-RAM seq high-water mark to the line's seq. */
esp_err_t store_journal_append_pending(const char *json_line);
size_t store_journal_pending_entries(void);
size_t store_journal_pending_bytes(void);
/* Freezes `pending` into `<first_seq>.v1.ndjson`. first_seq is parsed from the first line. Returns ESP_ERR_NOT_FOUND when nothing pending. */
esp_err_t store_journal_freeze(uint64_t *first_seq_out);
/* Lists frozen (unpublished) segment first-seqs ascending. */
int store_journal_frozen_list(uint64_t *seqs, size_t cap);
/* Reads a frozen segment's bytes (malloc'd; caller frees). */
esp_err_t store_journal_frozen_read(uint64_t first_seq, uint8_t **bytes, size_t *len, uint64_t *max_seq);
/* Marks a frozen segment published: appends to segments.tsv, deletes the file, advances published_cursor. */
esp_err_t store_journal_mark_published(uint64_t first_seq, uint64_t max_seq, int64_t server_ts);
uint64_t store_journal_published_cursor(void);

typedef struct { uint64_t first_seq, max_seq; int64_t published_server_ts; } store_segment_t;
/* Lists published segments (ascending). */
int store_segments_list(store_segment_t *out, size_t cap);
/* Removes a segment row after its bucket object was deleted by compaction. */
esp_err_t store_segments_remove(uint64_t first_seq);

/* Filesystem usage for the UI. */
void store_usage(size_t *total, size_t *used);

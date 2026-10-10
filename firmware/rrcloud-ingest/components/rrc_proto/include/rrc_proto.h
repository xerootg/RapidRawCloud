/*
 * rrc_proto — the RapidRawCloud sync protocol as seen by a write-mostly
 * ingest participant (docs/ARCHITECTURE.md §1–§2, mirrored from
 * src-tauri/crates/rrcloud-core: keys.rs, journal.rs, publisher.rs,
 * manifest.rs, clock.rs).
 *
 * Everything here is pure (bytes in, bytes out) and host-testable.
 */
#pragma once
#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>
#include <time.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ------------------------------------------------------------------------ */
/* Identity                                                                  */
/* ------------------------------------------------------------------------ */
#define RRC_DEVICE_ID_LEN 36
/* Canonical lowercase hyphenated UUIDv4 from 16 random bytes (clock.rs DeviceId). */
void rrc_uuid4_format(const uint8_t rnd[16], char out[RRC_DEVICE_ID_LEN + 1]);
bool rrc_device_id_valid(const char *s);

/* ------------------------------------------------------------------------ */
/* RelKey (keys.rs §1.1)                                                     */
/* ------------------------------------------------------------------------ */
typedef enum {
    RRC_RELKEY_OK = 0,
    RRC_RELKEY_EMPTY,
    RRC_RELKEY_BACKSLASH,
    RRC_RELKEY_COLON,
    RRC_RELKEY_CONTROL,
    RRC_RELKEY_LEADING_SLASH,
    RRC_RELKEY_BAD_SEGMENT,          /* "", "." or ".." segment */
    RRC_RELKEY_TRAILING_DOT_SPACE,
    RRC_RELKEY_WINDOWS_RESERVED,
    RRC_RELKEY_ENGINE_RESERVED,      /* segment starts with ".rr." */
    RRC_RELKEY_INVALID_UTF8,
    RRC_RELKEY_NOT_NFC,              /* contains combining marks: would be rejected by wire decoders */
    RRC_RELKEY_TOO_LONG,
} rrc_relkey_err;

#define RRC_RELKEY_MAX 768

/* Validates `s` under the full RelKey::new rule set (without normalizing). */
rrc_relkey_err rrc_relkey_validate(const char *s);
const char *rrc_relkey_err_str(rrc_relkey_err e);

/* Best-effort repair of a candidate relkey (template output / camera path) so it
 * passes rrc_relkey_validate: `\` and `:` and control chars -> `_`, repeated or
 * leading slashes dropped, trailing dots/spaces trimmed per segment, `.`/`..`
 * segments dropped, reserved names suffixed with `_`, `.rr.` prefix broken with `_`.
 * Returns the validation result of the output (OK unless it is empty/too long/non-NFC). */
rrc_relkey_err rrc_relkey_sanitize(const char *in, char *out, size_t cap);

/* ------------------------------------------------------------------------ */
/* Bucket key schema (keys.rs §1.2)                                          */
/* ------------------------------------------------------------------------ */
#define RRC_LIBRARY_PREFIX "library/"
#define RRC_CONTROL_PREFIX ".rrcloud/v1/"
#define RRC_JOURNAL_VERSION 1
#define RRC_MANIFEST_PROTO 1
#define RRC_SEGMENT_MAX_ENTRIES 1000
#define RRC_SEGMENT_MAX_BYTES (1024 * 1024)

int rrc_key_library(const char *relkey, char *out, size_t cap);
int rrc_segment_filename(uint64_t seq, char out[32]);                 /* <seq:016x>.v1.ndjson */
int rrc_key_journal_segment(const char *device, uint64_t seq, char *out, size_t cap);
int rrc_key_manifest(const char *device, char *out, size_t cap);
int rrc_key_device_registry(const char *device, char *out, size_t cap);

/* ------------------------------------------------------------------------ */
/* Minimal JSON writer (serde_json-compatible string escaping)               */
/* ------------------------------------------------------------------------ */
typedef struct {
    char *buf;
    size_t cap;
    size_t len;
    bool overflow;
} rrc_jsonw;

void rrc_jsonw_init(rrc_jsonw *w, char *buf, size_t cap);
void rrc_jsonw_raw(rrc_jsonw *w, const char *s);
void rrc_jsonw_str(rrc_jsonw *w, const char *s);        /* quoted + escaped */
void rrc_jsonw_u64(rrc_jsonw *w, uint64_t v);
void rrc_jsonw_i64(rrc_jsonw *w, int64_t v);
/* "key": — appends a comma first unless `first`. */
void rrc_jsonw_key(rrc_jsonw *w, const char *key, bool first);
/* Returns length, or -1 on overflow. */
int rrc_jsonw_finish(const rrc_jsonw *w);
/* Escapes `s` into a quoted JSON string; returns length or -1. */
int rrc_json_quote(const char *s, char *out, size_t cap);

/* ------------------------------------------------------------------------ */
/* Journal entries (journal.rs §2.2)                                         */
/* ------------------------------------------------------------------------ */
typedef struct {
    uint64_t seq;
    int64_t ts;                 /* server-time estimate, unix seconds */
    const char *device;         /* authoring device id */
    const char *bucket_key;     /* "library/<relkey>" */
    uint32_t vv_self;           /* this device's version-vector component (1 for a first upload) */
    uint64_t size;
    const char *blake3_hex;     /* 64 lowercase hex */
    const char *content_id_hex; /* = blake3 of the bytes for originals */
    bool has_mtime;
    int64_t mtime;              /* unix seconds */
} rrc_journal_put_original;

/* One NDJSON line WITHOUT trailing newline. Field order matches the Rust struct
 * (v, seq, ts, device, op, kind, key, vv, size, blake3, content_id, mtime). */
int rrc_journal_encode_put_original(const rrc_journal_put_original *e, char *out, size_t cap);

/* ------------------------------------------------------------------------ */
/* Device registry entry (publisher.rs §1.2 / §2.2)                          */
/* ------------------------------------------------------------------------ */
typedef struct {
    const char *name;
    const char *platform;      /* "esp32" */
    int64_t created;
    int64_t last_seen_server_ts;
    /* applied cursors: this participant applies no peer journals, so the map is
     * normally empty (compaction then relies on the §2.10 14-day cap). */
    const char *const *applied_devices;
    const uint64_t *applied_seqs;
    size_t n_applied;
} rrc_device_entry;

int rrc_device_entry_encode(const rrc_device_entry *d, char *out, size_t cap);

/* ------------------------------------------------------------------------ */
/* Per-writer manifest (manifest.rs §2.3) — NDJSON lines, gzip'd by caller    */
/* ------------------------------------------------------------------------ */
/* Header: {"written_server_ts":T,"cursors":{"<self>":published_cursor},"proto":1}
 * (cursor row omitted when published_cursor == 0, like build_manifest). */
int rrc_manifest_header_encode(int64_t written_server_ts, const char *self_device, uint64_t published_cursor,
                               char *out, size_t cap);

typedef struct {
    const char *relkey;
    uint64_t size;
    const char *blake3_hex;
    const char *device;        /* authoring device */
    uint32_t vv_self;
    const char *content_id_hex;
    bool has_mtime;
    int64_t mtime;
    int64_t ts;                /* the head version's journal ts */
} rrc_manifest_row_original;

int rrc_manifest_row_encode(const rrc_manifest_row_original *r, char *out, size_t cap);

/* ------------------------------------------------------------------------ */
/* gzip (RFC 1952) writer using stored deflate blocks — zero dependencies     */
/* ------------------------------------------------------------------------ */
typedef int (*rrc_gzip_sink)(void *ctx, const uint8_t *data, size_t len);

typedef struct {
    rrc_gzip_sink sink;
    void *sink_ctx;
    uint32_t crc;
    uint32_t isize;
    int err;
} rrc_gzip_store;

int rrc_gzip_store_begin(rrc_gzip_store *g, rrc_gzip_sink sink, void *ctx);
/* Emits one or more non-final stored blocks (<= 65535 bytes each). */
int rrc_gzip_store_write(rrc_gzip_store *g, const void *data, size_t len);
/* Emits the empty final block + trailer. */
int rrc_gzip_store_end(rrc_gzip_store *g);

/* ------------------------------------------------------------------------ */
/* Time helpers                                                              */
/* ------------------------------------------------------------------------ */
/* RFC 1123 HTTP-date ("Fri, 10 Oct 2026 12:34:56 GMT") -> unix seconds, or -1. */
int64_t rrc_parse_http_date(const char *s);
/* PTP DateTime string "YYYYMMDDThhmmss[.s][Z|+hhmm]" -> unix seconds treating the
 * camera clock as UTC when no zone is given; -1 on parse failure. */
int64_t rrc_parse_ptp_datetime(const char *s);
/* Portable timegm. */
int64_t rrc_timegm(const struct tm *tm);
/* ISO-8601 "YYYY-MM-DDTHH:MM:SSZ" (21-byte buffer). */
void rrc_format_iso8601(int64_t t, char out[21]);

/* ------------------------------------------------------------------------ */
/* Key template: "{model}/{yyyy}/{mm}/{dd}/{name}" etc.                       */
/* ------------------------------------------------------------------------ */
typedef struct {
    const char *name;    /* file name with extension */
    const char *path;    /* camera-relative directory path, no leading slash, may be "" */
    const char *model;   /* camera model */
    const char *serial;  /* camera serial */
    int64_t when;        /* capture/mtime unix seconds for {yyyy}{mm}{dd}{hh}; <=0 -> "0000" etc. */
} rrc_template_vars;

/* Expands the template, then sanitizes into a relkey. Returns the validation result. */
rrc_relkey_err rrc_template_expand(const char *tpl, const rrc_template_vars *v, char *out, size_t cap);

/* ------------------------------------------------------------------------ */
/* Ledger line codec (what the device persists per source object)            */
/* ------------------------------------------------------------------------ */
typedef enum { RRC_LEDGER_UPLOADED = 'U', RRC_LEDGER_REMOTE_EXISTS = 'R', RRC_LEDGER_COLLISION = 'C' } rrc_ledger_status;

typedef struct {
    char status;
    const char *source_id;   /* "ptp:<model>:<serial>" / "msc:<vid>:<pid>:<serial>" */
    const char *source_path; /* camera-relative path incl. name */
    uint64_t size;
    int64_t mtime;
    const char *relkey;      /* "" when not uploaded */
    const char *blake3_hex;  /* "" when unknown */
    uint64_t seq;            /* journal seq of the put entry, 0 if none */
    int64_t ts;              /* journal ts */
} rrc_ledger_rec;

/* Tab-separated, newline-terminated: "L1\t<status>\t<source_id>\t<path>\t<size>\t<mtime>\t<relkey>\t<blake3>\t<seq>\t<ts>\n" */
int rrc_ledger_format(const rrc_ledger_rec *r, char *out, size_t cap);
/* Parses one line in place (modifies `line`, pointers in `r` point into it). Returns 0 or -1. */
int rrc_ledger_parse(char *line, rrc_ledger_rec *r);

#ifdef __cplusplus
}
#endif

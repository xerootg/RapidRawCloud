/*
 * sync — the ingest engine: camera → S3 (library/<relkey>) → journal → device
 * registry heartbeat → per-writer manifest → §2.10 segment compaction.
 */
#pragma once
#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>
#include "esp_err.h"
#include "camera_source.h"

typedef enum {
    SYNC_UNCONFIGURED, SYNC_IDLE, SYNC_WAITING_NETWORK, SYNC_ENUMERATING, SYNC_UPLOADING, SYNC_PUBLISHING, SYNC_ERROR
} sync_phase_t;

typedef struct {
    sync_phase_t phase;
    bool s3_ready;
    bool camera_attached;
    char camera_kind[8];
    char camera_model[64];
    char camera_serial[64];
    char current_file[256];
    uint64_t current_size, current_done;
    uint32_t run_total, run_done, run_skipped, run_failed;
    uint64_t run_bytes;
    int64_t last_run_ts;
    int64_t last_heartbeat_ts;
    int64_t last_manifest_ts;
    char last_error[160];
    bool digest_check_known, backend_rejects_bad_md5;
    uint32_t lifetime_uploaded;
    uint64_t lifetime_bytes;
    size_t pending_entries;
} sync_status_t;

esp_err_t sync_init(void);
void sync_get_status(sync_status_t *out);
int sync_status_json(char *out, size_t cap);
void sync_request_now(void);
void sync_cancel(void);
void sync_config_changed(void);
/* Console diagnostics, executed on the sync task (output goes to the console UART):
 * list the attached camera's objects; hash one object `repeat` times. */
void sync_debug_list(void);
void sync_debug_hash(const char *handle_or_path, int repeat);
/* Camera event hook (registered with the source installers). */
void sync_on_camera_event(cam_event_t ev, cam_source_t *src, void *arg);

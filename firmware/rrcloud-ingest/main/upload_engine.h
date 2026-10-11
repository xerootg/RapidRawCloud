#pragma once
/*
 * Upload engine: a persistent pipeline that moves camera objects to the bucket, with several files in
 * flight at once.
 *
 *   ue_submit() -> reader -> BLAKE3 -> checksum -> two uploaders -> ue_wait() -> ue_finish()
 *
 * One reader task fills 6 MiB part buffers from the camera (zero-copy USB DMA), a hash task runs BLAKE3
 * on both cores, a checksum task computes the per-part SHA-256 (hardware) or MD5, and two uploader tasks,
 * each with its own TLS connection, pull parts of ANY file off one shared queue. So the tail of one file
 * overlaps the head of the next, small files upload two at a time, and a big file uses both connections.
 *
 * The caller (the sync task) owns the control connection: it is the only user of `ctl`, for the multipart
 * create in ue_submit and the complete/verify in ue_finish.
 */
#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>
#include "esp_err.h"
#include "camera_source.h"
#include "rrc_s3_client.h"

#define UE_PART_BYTES (6u * 1024u * 1024u)   /* one multipart part, and the largest single PUT */
#define UE_SLOTS 4                           /* part buffers (24 MiB of PSRAM) */

typedef struct ue_job ue_job_t;

typedef struct {
    rrc_s3_t *ctl;          /* control connection: multipart create / complete / verify (caller's thread only) */
    rrc_s3_t *conn[2];      /* one per uploader task */
    const char *device_id;  /* x-amz-meta-rrc-device */
    /* called from the reader task as bytes arrive; may be NULL */
    void (*progress)(void *ctx, const char *path, uint64_t size, uint64_t done);
    void *progress_ctx;
} ue_config_t;

esp_err_t ue_init(const ue_config_t *cfg);
/* The first part buffer, for the console diagnostics (valid only while no job is in flight). The payload
 * starts 12 bytes into a cache-line-aligned block, see rrc_ptp_read_partial_direct. */
uint8_t *ue_slot_buf(int i);

/* A newly attached body gets its own zero-copy USB self-check before its first upload. */
void ue_camera_attached(void);

/* Queues one object. Creates the multipart upload first when the object needs one. Blocks while the reader
 * queue is full. Returns NULL (and fills `err`) on failure. `user` comes back from ue_job_user(). */
ue_job_t *ue_submit(cam_source_t *src, const cam_object_t *o, const char *key, void *user, char *err, size_t errcap);
/* Next job whose parts have all been uploaded (or failed), in completion order; NULL on timeout. */
ue_job_t *ue_wait(uint32_t timeout_ms);
void *ue_job_user(const ue_job_t *j);
/* Completes the multipart upload, verifies the object in the bucket, and frees the job. On failure aborts
 * the multipart upload. */
esp_err_t ue_finish(ue_job_t *j, char b3hex[65], uint64_t *size, char *err, size_t errcap);

/* Fails every queued and running job quickly (cancel, camera gone); jobs still come out of ue_wait. */
void ue_set_abort(bool on);

typedef struct { int64_t read_us, hash_us, md5_us, upload_us; } ue_stats_t;
ue_stats_t *ue_stats(void);

/* Persistent state of the zero-copy USB read check: 0 ok/unknown, 1 running, 2 disabled. */
void sync_usb_direct_state_set(uint8_t v);

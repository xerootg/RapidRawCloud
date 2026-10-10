/*
 * camera_source — one abstraction over the two ways a camera shows up on the
 * USB port: as a PTP/MTP device (Nikon Z f, Z 7II in "MTP/PTP" mode) or as a
 * USB mass-storage disk (Sigma fp). The sync engine only sees this interface.
 */
#pragma once
#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>
#include "esp_err.h"

typedef struct {
    char name[256];     /* file name with extension */
    char path[512];     /* camera-relative path including name, '/'-separated, no leading slash */
    uint64_t size;
    int64_t mtime;      /* capture / modification time, unix seconds, 0 if unknown */
    uint32_t handle;    /* PTP object handle; 0 for MSC */
} cam_object_t;

typedef struct cam_source cam_source_t;

/* Return non-zero from the enumeration callback to stop early. */
typedef int (*cam_enum_cb_t)(void *ctx, const cam_object_t *obj);

struct cam_source {
    const char *kind;          /* "ptp" | "msc" */
    char source_id[200];       /* stable per camera: "ptp:<model>:<serial>" / "msc:<vid>:<pid>:<serial>" */
    char model[64];
    char serial[64];
    void *impl;
    esp_err_t (*enumerate)(cam_source_t *s, cam_enum_cb_t cb, void *ctx);
    esp_err_t (*open)(cam_source_t *s, const cam_object_t *obj, void **fh);
    esp_err_t (*read)(cam_source_t *s, void *fh, uint64_t offset, uint8_t *buf, size_t want, size_t *got);
    void (*close)(cam_source_t *s, void *fh);
    bool (*connected)(cam_source_t *s);
    void (*release)(cam_source_t *s);   /* free impl after detach */
};

typedef enum { CAM_EV_ATTACHED, CAM_EV_DETACHED } cam_event_t;
typedef void (*cam_event_cb_t)(cam_event_t ev, cam_source_t *src, void *arg);

/* Both installers require usb_host_install() to have run. The callback is invoked
 * from USB driver tasks and must only post a message (no camera I/O). */
esp_err_t source_msc_install(cam_event_cb_t cb, void *arg);
esp_err_t source_ptp_install(cam_event_cb_t cb, void *arg);

/* PTP only: open the session and resolve model/serial (blocking; sync task). */
esp_err_t source_ptp_identify(cam_source_t *s);

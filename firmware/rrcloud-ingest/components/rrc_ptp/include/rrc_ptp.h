/*
 * rrc_ptp — USB Still Image Capture class (PTP) host driver on top of the
 * ESP-IDF USB Host Library. One camera at a time (the dock has one port).
 *
 * The driver owns a usb_host client + task. The application registers a
 * callback for connect/disconnect and then drives the camera synchronously
 * from its own task (the sync task): the calls below block until the PTP
 * transaction completes.
 */
#pragma once
#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>
#include "esp_err.h"
#include "rrc_ptp_codec.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct rrc_ptp_dev rrc_ptp_dev_t;

typedef enum { RRC_PTP_EV_CONNECTED, RRC_PTP_EV_DISCONNECTED } rrc_ptp_event_t;
typedef void (*rrc_ptp_event_cb_t)(rrc_ptp_event_t ev, rrc_ptp_dev_t *dev, void *arg);

/* usb_host_install() must already have been called by the application. */
esp_err_t rrc_ptp_host_install(rrc_ptp_event_cb_t cb, void *arg);

/* Opens a PTP session and reads DeviceInfo (call once after CONNECTED, from the sync task). */
esp_err_t rrc_ptp_open(rrc_ptp_dev_t *dev);
esp_err_t rrc_ptp_close(rrc_ptp_dev_t *dev);
bool rrc_ptp_is_connected(const rrc_ptp_dev_t *dev);
const ptp_device_info *rrc_ptp_device_info(const rrc_ptp_dev_t *dev);
uint16_t rrc_ptp_vid(const rrc_ptp_dev_t *dev);
uint16_t rrc_ptp_pid(const rrc_ptp_dev_t *dev);

esp_err_t rrc_ptp_get_storage_ids(rrc_ptp_dev_t *dev, uint32_t *ids, size_t cap, size_t *count);
/* Returns a heap array (free() it) of every object handle on `storage_id`. */
esp_err_t rrc_ptp_get_object_handles(rrc_ptp_dev_t *dev, uint32_t storage_id, uint32_t **handles, size_t *count);
esp_err_t rrc_ptp_get_object_info(rrc_ptp_dev_t *dev, uint32_t handle, ptp_object_info *oi);
/* GetPartialObject: reads up to `want` bytes at `offset` into `out`. */
esp_err_t rrc_ptp_read_partial(rrc_ptp_dev_t *dev, uint32_t handle, uint32_t offset, uint8_t *out, size_t want, size_t *got);

/* Last PTP response code of a failed call (e.g. 0x2019 DeviceBusy). */
uint16_t rrc_ptp_last_response(const rrc_ptp_dev_t *dev);

#ifdef __cplusplus
}
#endif

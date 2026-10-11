/*
 * rrc_ptp_codec — PTP (ISO 15740 / PIMA 15740) container and dataset codec.
 * Pure C, transport-agnostic: the USB still-image-class driver (rrc_ptp.c)
 * frames these over bulk pipes; the same codec would frame PTP/IP packets.
 *
 * Nikon Z bodies in "MTP/PTP" USB mode speak standard PTP for everything this
 * firmware needs (sessions, storage enumeration, object info, partial reads).
 */
#pragma once
#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Container types */
#define PTP_CT_COMMAND  1
#define PTP_CT_DATA     2
#define PTP_CT_RESPONSE 3
#define PTP_CT_EVENT    4

/* Operation codes */
#define PTP_OC_GetDeviceInfo      0x1001
#define PTP_OC_OpenSession        0x1002
#define PTP_OC_CloseSession       0x1003
#define PTP_OC_GetStorageIDs      0x1004
#define PTP_OC_GetStorageInfo     0x1005
#define PTP_OC_GetNumObjects      0x1006
#define PTP_OC_GetObjectHandles   0x1007
#define PTP_OC_GetObjectInfo      0x1008
#define PTP_OC_GetObject          0x1009
#define PTP_OC_GetPartialObject   0x101B

/* Response codes */
#define PTP_RC_OK                      0x2001
#define PTP_RC_GeneralError            0x2002
#define PTP_RC_SessionNotOpen          0x2003
#define PTP_RC_InvalidTransactionID    0x2004
#define PTP_RC_OperationNotSupported   0x2005
#define PTP_RC_ParameterNotSupported   0x2006
#define PTP_RC_IncompleteTransfer      0x2007
#define PTP_RC_InvalidStorageId        0x2008
#define PTP_RC_InvalidObjectHandle     0x2009
#define PTP_RC_StoreNotAvailable       0x2013
#define PTP_RC_SessionAlreadyOpen      0x201E
#define PTP_RC_DeviceBusy              0x2019

/* Object format codes we care about */
#define PTP_OFC_Association   0x3001
#define PTP_OFC_EXIF_JPEG     0x3801
#define PTP_OFC_TIFF          0x380D
#define PTP_OFC_Undefined     0x3000
#define PTP_OFC_DNG           0x3811
#define PTP_OFC_NikonNEF      0xB103  /* Nikon vendor: NEF (often reported as Undefined/0x3000 on newer bodies) */

#define PTP_HANDLE_ALL   0xFFFFFFFFu
#define PTP_STORAGE_ALL  0xFFFFFFFFu

#define PTP_HDR_LEN 12

typedef struct {
    uint32_t length;
    uint16_t type;
    uint16_t code;
    uint32_t transaction_id;
} ptp_container_hdr;

/* Serialize a command container: 12-byte header + up to 5 u32 params. Returns bytes written. */
size_t ptp_encode_command(uint8_t *out, uint16_t code, uint32_t tid, const uint32_t *params, size_t nparams);
/* Parse a container header (little-endian). Returns false if len < 12. */
bool ptp_decode_hdr(const uint8_t *in, size_t len, ptp_container_hdr *h);

/* Datasets ---------------------------------------------------------------- */
typedef struct {
    uint32_t storage_id;
    uint16_t object_format;
    uint16_t protection_status;
    uint32_t compressed_size;      /* 0xFFFFFFFF when >= 4 GiB */
    uint16_t thumb_format;
    uint32_t thumb_size;
    uint32_t parent_object;        /* 0 = storage root */
    uint16_t association_type;
    uint32_t sequence_number;
    char filename[256];            /* UTF-8 */
    char capture_date[40];         /* raw PTP DateTime string */
    char modification_date[40];
} ptp_object_info;

/* Parses an ObjectInfo dataset. Returns true on success. */
bool ptp_decode_object_info(const uint8_t *in, size_t len, ptp_object_info *oi);

typedef struct {
    uint16_t standard_version;
    uint32_t vendor_extension_id;
    char manufacturer[64];
    char model[64];
    char device_version[64];
    char serial_number[64];
    bool supports_partial_object;
    bool supports_get_object;
} ptp_device_info;

bool ptp_decode_device_info(const uint8_t *in, size_t len, ptp_device_info *di);

/* Array of u32 (StorageIDs / ObjectHandles): returns count, fills up to `cap` ids.
 * Returns -1 on malformed input. */
int ptp_decode_u32_array(const uint8_t *in, size_t len, uint32_t *out, size_t cap);

/* PTP string (u8 count incl. NUL, UTF-16LE) → UTF-8. Returns bytes consumed or 0 on error. */
size_t ptp_decode_string(const uint8_t *in, size_t len, char *out, size_t cap);

/* True for object formats this firmware treats as image files (JPEG, TIFF, DNG, NEF,
 * and Undefined — Nikon reports NEF as 0x3000 on Z bodies; filename globs decide). */
bool ptp_format_is_file(uint16_t ofc);

#ifdef __cplusplus
}
#endif

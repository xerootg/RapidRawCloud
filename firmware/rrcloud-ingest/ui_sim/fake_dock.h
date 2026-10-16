#pragma once
#include <stdbool.h>
#include <stdint.h>
#include "cJSON.h"

/* A stand-in for api_dispatch(): the same routes and document shapes as main/api.c,
 * backed by state the scenarios set. POST /api/config really patches the config and
 * applies the firmware's validation for the fields the UI edits. */
typedef struct {
    const char *phase;          /* sync phase */
    bool camera, s3_ready, uploading_numbers;
    bool wifi_assoc;
    int rssi;
    const char *pair_state;     /* idle | discovering | waiting_user | done | failed */
    int backlight;              /* last value set through the host hook */
} fake_state_t;

extern fake_state_t fake;
extern cJSON *fake_config;

void fake_dock_init(void);
/* Handles one request synchronously; returns malloc'd JSON (caller frees) and the status. */
char *fake_dock_handle(const char *method, const char *path, const char *body, int *status, uint32_t now_ms);

/*
 * pairing — zero-typing cloud setup against the RapidRawCloud pairing service
 * (docs/CLOUD_SETUP.md §6), adapted for a headless device:
 *
 *   1. GET <service>/api/pairing-info          → OIDC issuer + public client id
 *   2. GET <issuer>/.well-known/openid-configuration → device_authorization_endpoint, token_endpoint
 *   3. POST device_authorization_endpoint (RFC 8628) → user_code + verification URL shown in the web UI
 *   4. poll token_endpoint (grant urn:ietf:params:oauth:grant-type:device_code) until the user approves
 *   5. GET <service>/api/config with the bearer → {sync, credentials} applied to this device
 *
 * The phone app uses the browser redirect flow (rapidraw://auth-callback); a
 * dock has no browser, so it uses the device-code grant against the same
 * Authentik provider (requires a device-code flow on the Authentik brand).
 */
#pragma once
#include <stdbool.h>
#include <stddef.h>
#include "esp_err.h"

typedef enum { PAIR_IDLE, PAIR_DISCOVERING, PAIR_WAITING_USER, PAIR_POLLING, PAIR_DONE, PAIR_FAILED } pair_state_t;

typedef struct {
    pair_state_t state;
    char user_code[32];
    char verification_uri[256];
    char verification_uri_complete[384];
    int expires_in;
    char message[200];
} pair_status_t;

esp_err_t pairing_begin(const char *service_url);
void pairing_cancel(void);
void pairing_get_status(pair_status_t *out);
int pairing_status_json(char *out, size_t cap);

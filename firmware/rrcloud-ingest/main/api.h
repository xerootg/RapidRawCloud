#pragma once
#include <stdbool.h>
#include <stddef.h>

/* Transport-neutral admin API: the operations behind the web UI (web.c over
 * HTTP) and the BLE RPC characteristic (ble.c). Routes mirror the HTTP paths
 * (`/api/status`, `/api/config`, …) so one client library serves both. */

typedef struct {
    int status;   /* HTTP-style: 200, 400, 401, 404, 409, 500 */
    char *json;   /* malloc'd JSON body; NULL only when out of memory */
    size_t len;
} api_resp_t;

/* method: "GET" | "POST"; path: "/api/…" (no query string); body: JSON or NULL.
 * Always produces a document (errors are {"ok":false,"error":"…"}). */
void api_dispatch(const char *method, const char *path, const char *body, size_t body_len, api_resp_t *out);
void api_resp_free(api_resp_t *r);

/* The admin-password gate every transport applies: true when no password is
 * required or `password` matches the stored admin password. */
bool api_password_ok(const char *password);
/* Reason string for the HTTP status line / BLE reply ("OK", "Bad Request", …). */
const char *api_status_text(int status);

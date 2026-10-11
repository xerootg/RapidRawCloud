#pragma once
#include <stdbool.h>
#include <stddef.h>
#include "esp_err.h"

/* Bluetooth LE admin transport: a GATT service (DOCK_BLE_SERVICE_UUID in the
 * protocol definition) that carries the api.c operations as fragmented JSON
 * messages, so the RapidRAW app can configure the dock with no network. The
 * NimBLE host runs on the P4; the controller is the ESP32-C6 over Hosted-HCI. */

/* Spawns the BLE bring-up task (waits for the co-processor link); never blocks. */
esp_err_t ble_start(void);
/* Applies `ble_enabled` from the configuration: advertise or go silent. */
void ble_reconfigure(void);
/* {"enabled":…,"up":…,"advertising":…,"connected":…,"encrypted":…,"name":…,"address":…,"mtu":…,"error":…} */
int ble_status_json(char *out, size_t cap);
/* Drops every stored bond (and the current connection): the next phone pairs afresh. */
void ble_forget_bonds(void);

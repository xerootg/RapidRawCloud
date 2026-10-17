/*
 * USB channel fault recorder. The DWC host controller reports a failed
 * transfer through the channel interrupt register (HCINT). IDF's HAL decodes
 * and clears it inside the interrupt, and the class driver only ever sees a
 * coarse pipe error, so when a transfer died mid-sync there was nothing in the
 * log that said which error the controller raised or whether the channel had
 * halted. IDF asserts that it has (usb_dwc_hal.c, usb_dwc_hal_chan_decode_intr)
 * and that assert rebooted the dock once; CONFIG_HAL_ASSERTION_DISABLE turns
 * it off, and this records what the assert would have fired on.
 *
 * The HAL decoder is wrapped at link time (-Wl,--wrap) so the raw HCINT bits
 * of every error-class interrupt are kept in a small ring. The interrupt
 * cannot log, so the USB watchdog task drains the ring into the log.
 */
#pragma once
#include <stdint.h>

/* Writes every fault recorded since the last call to the log ring. */
void usb_fault_drain(void);

/* Faults recorded since boot, and channel interrupts decoded since boot (the
 * latter proves the wrapper is in the build: it counts every transfer). */
uint32_t usb_fault_count(void);
uint32_t usb_fault_decode_calls(void);

#include "usb_fault.h"
#include <stdio.h>
#include <string.h>
#include "esp_timer.h"
#include "esp_attr.h"
#include "esp_system.h"
#include "esp_rom_sys.h"
#include "hal/usb_dwc_hal.h"
#include "hal/usb_dwc_ll.h"
#include "soc/usb_dwc_struct.h"
#include "log_ring.h"

/* Mirrors CHAN_INTRS_ERROR_MSK in usb_dwc_hal.c: the bits the HAL treats as a
 * transfer error and asserts CHHLTD alongside. */
#define ERROR_MSK (USB_DWC_LL_INTR_CHAN_STALL | USB_DWC_LL_INTR_CHAN_BBLEER | \
                   USB_DWC_LL_INTR_CHAN_BNAINTR | USB_DWC_LL_INTR_CHAN_XCS_XACT_ERR)

enum { KIND_ERROR, KIND_STRAY_HALT, KIND_BNA };

typedef struct {
    int64_t t_us;
    uint32_t hcint, hcchar, hctsiz;
    uint8_t kind, event, error, active;
    uint16_t wait_us;      /* KIND_BNA: how long the channel took to halt */
} fault_t;

/* Longest wait for a channel to report halted after it was disabled. The
 * core finishes the transaction in flight first, so up to a high-speed
 * microframe (125 us) is ordinary. */
#define HALT_WAIT_US 400

/* The ring lives in retained RAM so faults recorded in the seconds before a
 * crash reset are still there to be logged at the next boot: a transfer error
 * that brings the stack down would otherwise be the one fault never seen. */
#define RING 8
#define RING_MAGIC 0x55464c54u   /* "UFLT" */
static RTC_NOINIT_ATTR fault_t ring[RING];
static RTC_NOINIT_ATTR volatile uint32_t head;   /* written in the interrupt */
static RTC_NOINIT_ATTR uint32_t ring_magic;
static uint32_t tail;              /* advanced by usb_fault_drain */
static volatile uint32_t decode_calls;

usb_dwc_hal_chan_event_t __real_usb_dwc_hal_chan_decode_intr(usb_dwc_hal_chan_t *chan_obj);

/* Runs in the USB host interrupt, from flash (no IRAM_ATTR), as does IDF's own handler
 * that calls it (hcd_dwc.c intr_hdlr_main). That is safe only because app_main installs
 * the USB host without ESP_INTR_FLAG_IRAM: a non-IRAM interrupt is masked while the
 * flash cache is off (esp_intr_noniram_disable), so neither runs then. Adding the flag
 * would need this function, everything it calls (esp_timer_get_time, esp_rom_delay_us)
 * and the HCD's handler in IRAM. */

usb_dwc_hal_chan_event_t __wrap_usb_dwc_hal_chan_decode_intr(usb_dwc_hal_chan_t *chan_obj)
{
    /* The decoder reads HCINT and clears it, so copy it first. */
    uint32_t hcint = chan_obj->regs->hcint_reg.val;
    uint32_t active = chan_obj->flags.active, halt_req = chan_obj->flags.halt_requested;
    decode_calls++;
    usb_dwc_hal_chan_event_t ev = __real_usb_dwc_hal_chan_decode_intr(chan_obj);

    int kind = -1;
    uint16_t wait_us = 0;
    if (ev == USB_DWC_HAL_CHAN_EVENT_ERROR && !(hcint & USB_DWC_LL_INTR_CHAN_CHHLTD)) {
        /* An error on a channel that has not halted. Per the DWC2 databook
         * that is Buffer Not Available: the core fetched a transfer descriptor
         * it did not find ready, and unlike every other error it leaves the
         * channel running. IDF's HAL asserted on this until
         * CONFIG_HAL_ASSERTION_DISABLE, and its HCD still cannot finish a
         * transfer with that error (it aborts in _buffer_parse_error on
         * URB_NOT_AVAIL). Halt the channel here and wait for it, then hand the
         * HCD a transaction error on a halted channel: it completes the URB as
         * a failed transfer, which the mass-storage layer recovers from with a
         * reset (see xread in source_msc.c). */
        usb_dwc_ll_hcchar_disable_chan(chan_obj->regs);
        while (!(chan_obj->regs->hcint_reg.val & USB_DWC_LL_INTR_CHAN_CHHLTD) && wait_us < HALT_WAIT_US) {
            esp_rom_delay_us(1);
            wait_us++;
        }
        /* Take the halt interrupt here so it does not come back as a second
         * channel interrupt the HAL would read as a completed transfer. */
        usb_dwc_ll_hcint_read_and_clear_intrs(chan_obj->regs);
        chan_obj->error = USB_DWC_HAL_CHAN_ERROR_XCS_XACT;
        kind = KIND_BNA;
    } else if (hcint & ERROR_MSK) {
        kind = KIND_ERROR;
    } else if ((hcint & USB_DWC_LL_INTR_CHAN_CHHLTD) && !active && !halt_req) {
        /* A halt on a channel the HAL thinks is idle cannot be a completed
         * transfer; the HAL has just reported it as one. Swallow it rather
         * than let the HCD complete a buffer it does not have. */
        kind = KIND_STRAY_HALT;
        ev = USB_DWC_HAL_CHAN_EVENT_NONE;
    }
    if (kind < 0) return ev;

    fault_t *f = &ring[head % RING];
    f->t_us = esp_timer_get_time();
    f->hcint = hcint;
    f->hcchar = chan_obj->regs->hcchar_reg.val;
    f->hctsiz = chan_obj->regs->hctsiz_reg.val;
    f->kind = kind;
    f->event = ev;
    f->error = chan_obj->error;
    f->active = active;
    f->wait_us = wait_us;
    head++;
    return ev;
}

static const char *const bit_names[] = {
    "xfercompl", "chhltd", "ahberr", "stall", "nak", "ack", "nyet", "xacterr",
    "bblerr", "frmovrun", "datatglerr", "bnaintr", "xcs_xact_err", "desc_ls_roll",
};

static void bits_to_str(uint32_t v, char *out, size_t cap)
{
    out[0] = 0;
    for (unsigned i = 0; i < sizeof bit_names / sizeof *bit_names; i++) {
        if (!(v & (1u << i))) continue;
        if (out[0]) strlcat(out, " ", cap);
        strlcat(out, bit_names[i], cap);
    }
}

static void drain(const char *when)
{
    uint32_t h = head;
    if (h - tail > RING) {
        log_ring_printf("usb: %u channel fault(s) lost (ring overflow)", (unsigned)(h - tail - RING));
        tail = h - RING;
    }
    while (tail != h) {
        fault_t f = ring[tail % RING];
        tail++;
        char bits[128];
        bits_to_str(f.hcint, bits, sizeof bits);
        usb_dwc_hcchar_reg_t c = {.val = f.hcchar};
        usb_dwc_hctsiz_reg_t t = {.val = f.hctsiz};
        if (f.kind == KIND_BNA) {
            log_ring_printf("usb: channel fault%s at %lld ms: hcint 0x%04x [%s] without a halt (buffer not available); halted it in %u us, failed the transfer; dev %u ep %u %s type %u; hctsiz 0x%08x",
                            when, (long long)(f.t_us / 1000), (unsigned)f.hcint, bits, (unsigned)f.wait_us,
                            (unsigned)c.devaddr, (unsigned)c.epnum, c.epdir ? "in" : "out", (unsigned)c.eptype, (unsigned)f.hctsiz);
        } else if (f.kind == KIND_ERROR) {
            log_ring_printf("usb: channel fault%s at %lld ms: hcint 0x%04x [%s], channel halted; dev %u ep %u %s type %u mps %u; hctsiz 0x%08x pid %u; hal event %u error %u",
                            when, (long long)(f.t_us / 1000), (unsigned)f.hcint, bits,
                            (unsigned)c.devaddr, (unsigned)c.epnum, c.epdir ? "in" : "out", (unsigned)c.eptype, (unsigned)c.mps,
                            (unsigned)f.hctsiz, (unsigned)t.pid, (unsigned)f.event, (unsigned)f.error);
        } else {
            log_ring_printf("usb: stray channel halt%s at %lld ms: hcint 0x%04x [%s] on an idle channel, ignored; dev %u ep %u %s",
                            when, (long long)(f.t_us / 1000), (unsigned)f.hcint, bits,
                            (unsigned)c.devaddr, (unsigned)c.epnum, c.epdir ? "in" : "out");
        }
    }
}

void usb_fault_drain(void) { drain(""); }

void usb_fault_init(void)
{
    esp_reset_reason_t r = esp_reset_reason();
    bool crash = r == ESP_RST_PANIC || r == ESP_RST_INT_WDT || r == ESP_RST_TASK_WDT || r == ESP_RST_WDT;
    if (crash && ring_magic == RING_MAGIC && head != 0) {
        /* Only the entries still in the ring can be shown; older ones were
         * drained into the previous boot's log already. */
        tail = head > RING ? head - RING : 0;
        log_ring_printf("usb: %u channel fault(s) recorded before the crash reset:", (unsigned)(head - tail));
        drain(" (before the reset)");
    }
    head = 0;
    tail = 0;
    ring_magic = RING_MAGIC;
}

uint32_t usb_fault_count(void) { return head; }
uint32_t usb_fault_decode_calls(void) { return decode_calls; }

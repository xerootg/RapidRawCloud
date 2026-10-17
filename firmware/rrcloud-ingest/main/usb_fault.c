#include "usb_fault.h"
#include <stdio.h>
#include <string.h>
#include "esp_timer.h"
#include "hal/usb_dwc_hal.h"
#include "hal/usb_dwc_ll.h"
#include "soc/usb_dwc_struct.h"
#include "log_ring.h"

/* Mirrors CHAN_INTRS_ERROR_MSK in usb_dwc_hal.c: the bits the HAL treats as a
 * transfer error and asserts CHHLTD alongside. */
#define ERROR_MSK (USB_DWC_LL_INTR_CHAN_STALL | USB_DWC_LL_INTR_CHAN_BBLEER | \
                   USB_DWC_LL_INTR_CHAN_BNAINTR | USB_DWC_LL_INTR_CHAN_XCS_XACT_ERR)

enum { KIND_ERROR, KIND_STRAY_HALT };

typedef struct {
    int64_t t_us;
    uint32_t hcint, hcchar, hctsiz;
    uint8_t kind, event, error, active;
} fault_t;

#define RING 8
static fault_t ring[RING];
static volatile uint32_t head;     /* written in the interrupt */
static uint32_t tail;              /* advanced by usb_fault_drain */
static volatile uint32_t decode_calls;

usb_dwc_hal_chan_event_t __real_usb_dwc_hal_chan_decode_intr(usb_dwc_hal_chan_t *chan_obj);

usb_dwc_hal_chan_event_t __wrap_usb_dwc_hal_chan_decode_intr(usb_dwc_hal_chan_t *chan_obj)
{
    /* The decoder reads HCINT and clears it, so copy it first. */
    uint32_t hcint = chan_obj->regs->hcint_reg.val;
    uint32_t active = chan_obj->flags.active, halt_req = chan_obj->flags.halt_requested;
    decode_calls++;
    usb_dwc_hal_chan_event_t ev = __real_usb_dwc_hal_chan_decode_intr(chan_obj);

    int kind = -1;
    if (hcint & ERROR_MSK) kind = KIND_ERROR;
    /* A halt on a channel the HAL thinks is idle: the hardware caught up with
     * an earlier error that arrived without CHHLTD, which the HAL will have
     * mistaken for a completed transfer. */
    else if ((hcint & USB_DWC_LL_INTR_CHAN_CHHLTD) && !active && !halt_req) kind = KIND_STRAY_HALT;
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

void usb_fault_drain(void)
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
        if (f.kind == KIND_ERROR) {
            log_ring_printf("usb: channel fault at %lld ms: hcint 0x%04x [%s], %s; dev %u ep %u %s type %u mps %u; hctsiz 0x%08x pid %u; hal event %u error %u",
                            (long long)(f.t_us / 1000), (unsigned)f.hcint, bits,
                            (f.hcint & USB_DWC_LL_INTR_CHAN_CHHLTD) ? "channel halted" : "channel NOT halted (IDF asserts on this)",
                            (unsigned)c.devaddr, (unsigned)c.epnum, c.epdir ? "in" : "out", (unsigned)c.eptype, (unsigned)c.mps,
                            (unsigned)f.hctsiz, (unsigned)t.pid, (unsigned)f.event, (unsigned)f.error);
        } else {
            log_ring_printf("usb: stray channel halt at %lld ms: hcint 0x%04x [%s] on an idle channel; dev %u ep %u %s; hal event %u",
                            (long long)(f.t_us / 1000), (unsigned)f.hcint, bits,
                            (unsigned)c.devaddr, (unsigned)c.epnum, c.epdir ? "in" : "out", (unsigned)f.event);
        }
    }
}

uint32_t usb_fault_count(void) { return head; }
uint32_t usb_fault_decode_calls(void) { return decode_calls; }

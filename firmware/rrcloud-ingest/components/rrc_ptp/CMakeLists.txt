# rrc_ptp_codec.c is pure (host-testable); rrc_ptp.c is the USB still-image-class host driver.
idf_component_register(
    SRCS "rrc_ptp_codec.c" "rrc_ptp.c"
    INCLUDE_DIRS "include"
    PRIV_REQUIRES usb)

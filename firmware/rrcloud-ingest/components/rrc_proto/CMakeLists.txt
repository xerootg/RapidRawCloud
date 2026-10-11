# Pure C: the rrcloud wire protocol (docs/ARCHITECTURE.md §1–§2) as the firmware speaks it.
idf_component_register(
    SRCS "rrc_relkey.c" "rrc_keys.c" "rrc_jsonw.c" "rrc_journal.c" "rrc_manifest.c" "rrc_gzip_store.c" "rrc_timefmt.c" "rrc_ledger.c"
    INCLUDE_DIRS "include"
    REQUIRES rrc_hash rrc_blake3 rrcloud_proto)

# firmware/rrcloud-ingest — ESP32-P4 camera ingest dock

Also read the root `CLAUDE.md` and this directory's `README.md`.

- C on ESP-IDF v5.5.5, target `esp32p4`. Full builds need `idf.py` (CI uses the `espressif/idf`
  container via `.github/workflows/firmware-build.yml`); you usually don't have it locally.
- What you can always run — host tests for the pure components (gcc + cmake only):
  ```sh
  cd firmware/rrcloud-ingest/host_tests
  cmake -S . -B build && cmake --build build && ctest --test-dir build --output-on-failure
  ./build/test_proto /tmp/manifest_test.gz fixtures && git diff --exit-code -- fixtures
  ```
  If `fixtures/` changed, commit it and run
  `cargo test --test firmware_interop` in `src-tauri/crates/rrcloud-core`.
- Keep pure logic (hashing, SigV4, codecs, glob, PTP codec) in `components/` free of ESP-IDF calls so it
  stays host-testable. Hardware/OS glue goes in `main/`.
- Two boards (`main/Kconfig.projbuild`, pins in `main/board.h`): the PoE-ETH board is the default; the
  Touch-LCD-4.3 builds with `-DSDKCONFIG_DEFAULTS="sdkconfig.defaults;sdkconfig.board.touch_lcd_43"`.
  Guard board-specific code with `CONFIG_RRC_HAS_ETHERNET` / `_DISPLAY` / `_BATTERY`, not board names.
  Use a separate `-B`/`-DSDKCONFIG=` per board, or the old board's `sdkconfig` is reused.
- The on-screen UI (`components/rrc_ui`) is pure LVGL + cJSON and talks to the dock only through
  `/api/…` (the host hooks in `main/display.c`); add features as API routes first. Check UI changes with
  the simulator, which also fails on missing glyphs, leaks and asserts:
  ```sh
  cmake -S firmware/rrcloud-ingest/ui_sim -B /tmp/ui_sim && cmake --build /tmp/ui_sim -j && /tmp/ui_sim/rrc_ui_sim /tmp/ui_sim/shots
  ```
  Look at the PNGs. Icons are `ICON_*` (Material Symbols) from `ui_icons.h`; new icons or glyph ranges go
  through `components/rrc_ui/fonts/gen_fonts.py` (never hand-edit `ui_font_*.c`). LVGL's built-in printf has
  no `%f`: format with `snprintf`.
- Protocol encoding delegates to the generated C SDK: `components/rrcloud_proto` compiles
  `protocol/gen/c` in place. Change the protocol in `protocol/`, not here.
- **Do not edit `version.txt` unless asked**: pushing a new version on any branch publishes a
  firmware GitHub release automatically.
- Memory is tight and inputs come from cameras, USB and the network: bound every buffer and length,
  check every return code.

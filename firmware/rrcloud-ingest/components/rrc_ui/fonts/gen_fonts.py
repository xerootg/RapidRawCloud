#!/usr/bin/env python3
"""Regenerates the UI's LVGL fonts (ui_font_*.c) and icon macros (ui_icons.h).

Material 3's typeface is Roboto and its icon set is Material Symbols; LVGL's built-in
Montserrat fonts cover ASCII and a FontAwesome subset only, so SSIDs, camera names and the
dock's own messages ("8–63 characters", "…") would render as boxes. These fonts cover
Latin-1, Latin Extended-A and the common punctuation, with the icons merged into the sizes
that show icons next to text.

Bitmaps are uncompressed: LVGL decodes RLE-compressed glyphs only with
LV_USE_FONT_COMPRESSED, and plain 4 bpp bitmaps cost no CPU per glyph on cores the upload
pipeline is using; flash is not the constraint.

Inputs are pinned by URL and SHA-256; the converter is lv_font_conv 1.5.3 (npx). Needs
python3 with fontTools (pip install fonttools) and node/npx. Run from anywhere:

    python3 firmware/rrcloud-ingest/components/rrc_ui/fonts/gen_fonts.py

and commit the regenerated files. Both fonts are Apache-2.0 (see README.md here).
"""
import hashlib
import io
import os
import subprocess
import sys
import tempfile
import urllib.request
import zipfile

HERE = os.path.dirname(os.path.abspath(__file__))
OUT_HEADER = os.path.join(HERE, "..", "ui_icons.h")

ROBOTO_URL = "https://github.com/googlefonts/roboto/releases/download/v2.138/roboto-unhinted.zip"
ROBOTO_SHA256 = "70f64c718510a601fbcf752aafe644314dacaeb85474dc689c89787c4a72a728"
# Static instance opsz 24 / wght 400 / FILL 0 / GRAD 0 served by the Google Fonts API (v377).
SYMBOLS_URL = ("https://fonts.gstatic.com/s/materialsymbolsoutlined/v377/kJF1BvYX7BgnkSrUwT8OhrdQw4oELdPIeeII9v6oDMzBy"
               "HX9rA6RzaxHMPdY43zj-jCxv3fzvRNU22ZXGJpEpjC_1v-p_4MrImHCIJIZrDCvHOel.woff")
SYMBOLS_SHA256 = "995caf1fef804a9b578cf10552369e819a776d32e210cce307e28d55516edff6"
LV_FONT_CONV = "lv_font_conv@1.5.3"

# Latin-1, Latin Extended-A, dashes/quotes/bullet/ellipsis, per-mille/angle quotes, euro,
# trade mark, arrows, minus, ≤ ≥.
TEXT_RANGES = "0x20-0x7E,0xA0-0x17F,0x2010-0x2027,0x2030-0x203A,0x20AC,0x2122,0x2190-0x2193,0x2212,0x2264-0x2265"

# Icon macro → Material Symbols name. Add an icon here, rerun, and use ICON_<NAME>.
ICONS = {
    "HOME": "home", "UPLOAD": "cloud_upload", "WIFI": "wifi", "WIFI_OFF": "wifi_off", "SETTINGS": "settings", "USB": "usb",
    "SCREEN_ROTATION": "screen_rotation",
    "BLUETOOTH": "bluetooth", "BACK": "arrow_back", "CHEVRON": "chevron_right", "REFRESH": "refresh", "CLOSE": "close",
    "VISIBILITY": "visibility", "VISIBILITY_OFF": "visibility_off", "BACKSPACE": "backspace", "SHIFT": "shift", "CHECK": "check",
    "SCHEDULE": "schedule", "PHOTO_LIBRARY": "photo_library", "HISTORY": "history", "INVENTORY": "inventory_2", "ADD": "add",
    "FILTER": "filter_alt", "FILTER_OFF": "filter_alt_off", "VIDEOCAM": "videocam", "SIZE": "photo_size_select_small",
    "FOLDER": "folder", "ROUTE": "route", "LINK": "link", "STORAGE": "storage", "PUBLIC": "public", "KEY": "key",
    "PASSWORD": "password", "WARNING": "warning", "DNS": "dns", "DARK_MODE": "dark_mode", "BRIGHTNESS": "brightness_6",
    "TIMER": "timer", "BADGE": "badge", "LAN": "lan", "LOCK": "lock", "DELETE": "delete", "SMARTPHONE": "smartphone",
    "MEMORY": "memory", "SYSTEM_UPDATE": "system_update", "BUG": "bug_report", "RESTART": "restart_alt", "ARTICLE": "article",
    "INFO": "info", "POWER": "power_settings_new", "RELEASE": "new_releases", "BOARD": "developer_board",
    "FINGERPRINT": "fingerprint", "SD_STORAGE": "sd_storage", "BATTERY": "battery_5_bar", "CAMERA": "photo_camera",
    "CLOUD": "cloud", "CLOUD_OFF": "cloud_off", "SYNC": "sync", "QR": "qr_code_2", "ERROR": "error",
}

# name, TTF (regular|medium), size, text ranges, merge icons
FONTS = [
    ("ui_font_r16", "regular", 16, TEXT_RANGES, False),   # supporting text, log
    ("ui_font_r20", "regular", 20, TEXT_RANGES, True),    # body / list headlines, status bar and inline icons
    ("ui_font_r24", "regular", 24, TEXT_RANGES, True),    # titles, keyboard keys (with their icons)
    ("ui_font_r30", "regular", 30, TEXT_RANGES, False),   # top app bar headline
    ("ui_font_r44", "regular", 44, "0x20-0x7E", False),   # display: the pairing code
    ("ui_font_m16", "medium", 16, TEXT_RANGES, False),    # labels: section headers, chips, navigation
    ("ui_font_m18", "medium", 18, TEXT_RANGES, False),    # buttons
    ("ui_font_icon28", None, 28, None, True),             # navigation, list leading icons, app bar actions
]


def fetch(url, sha):
    data = urllib.request.urlopen(url).read()
    got = hashlib.sha256(data).hexdigest()
    if got != sha:
        sys.exit(f"{url}: sha256 {got} != pinned {sha}")
    return data


def symbols_codepoints(ttf_path):
    from fontTools.ttLib import TTFont
    f = TTFont(ttf_path)
    cmap = f.getBestCmap()
    rev = {}
    for cp, g in cmap.items():
        rev.setdefault(g, cp)
    ascii_glyph = {g: chr(cp) for cp, g in cmap.items() if cp < 128}
    names = {}
    for lookup in f["GSUB"].table.LookupList.Lookup:
        for st in lookup.SubTable:
            if lookup.LookupType == 7:
                st = st.ExtSubTable
            for first, ligs in getattr(st, "ligatures", {}).items():
                for lig in ligs:
                    try:
                        name = ascii_glyph[first] + "".join(ascii_glyph[c] for c in lig.Component)
                    except KeyError:
                        continue
                    cp = rev.get(lig.LigGlyph, 0)
                    if cp >= 0xE000:
                        names[name] = cp
    return names


def main():
    from fontTools.ttLib import TTFont
    with tempfile.TemporaryDirectory() as tmp:
        z = zipfile.ZipFile(io.BytesIO(fetch(ROBOTO_URL, ROBOTO_SHA256)))
        ttf = {}
        for weight, member in (("regular", "Roboto-Regular.ttf"), ("medium", "Roboto-Medium.ttf")):
            name = next(n for n in z.namelist() if n.endswith("/" + member) or n == member)
            ttf[weight] = os.path.join(tmp, member)
            with open(ttf[weight], "wb") as o:
                o.write(z.read(name))
        woff = os.path.join(tmp, "symbols.woff")
        with open(woff, "wb") as o:
            o.write(fetch(SYMBOLS_URL, SYMBOLS_SHA256))
        sym = TTFont(woff)
        sym.flavor = None
        sym_ttf = os.path.join(tmp, "MaterialSymbolsOutlined.ttf")
        sym.save(sym_ttf)
        cps = symbols_codepoints(sym_ttf)
        missing = [n for n in ICONS.values() if n not in cps]
        if missing:
            sys.exit(f"icons not in Material Symbols: {missing}")
        icon_range = ",".join(hex(cps[n]) for n in sorted(set(ICONS.values())))

        for name, weight, size, ranges, icons in FONTS:
            cmd = ["npx", "-y", LV_FONT_CONV, "--bpp", "4", "--size", str(size), "--format", "lvgl", "--lv-include", "lvgl.h",
                   "--lv-font-name", name, "--no-compress", "-o", os.path.join(HERE, name + ".c")]
            if weight:
                cmd += ["--font", ttf[weight], "-r", ranges]
            if icons:
                cmd += ["--font", sym_ttf, "-r", icon_range]
            print(" ".join(cmd[:8]), name)
            subprocess.run(cmd, check=True)

    with open(OUT_HEADER, "w") as h:
        h.write("/* Generated by fonts/gen_fonts.py — Material Symbols (Outlined, 400, opsz 24) as UTF-8.\n"
                " * Rendered by the fonts that merge the icon set: UI_FONT_BODY, UI_FONT_TITLE, UI_FONT_ICON. */\n")
        h.write("#pragma once\n\n")
        for macro, n in ICONS.items():
            esc = "".join("\\x%02X" % b for b in chr(cps[n]).encode("utf-8"))
            h.write('#define ICON_%-15s "%s"  /* %s U+%04X */\n' % (macro, esc, n, cps[n]))
    print("wrote", OUT_HEADER)


if __name__ == "__main__":
    main()

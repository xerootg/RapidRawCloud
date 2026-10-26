#!/usr/bin/env python3
"""Collects the dock's app images for over-the-air updates and writes their manifest.

    ota_manifest.py <builds-dir> <out-dir> <version> <tag> <commit>

<builds-dir> holds one directory per board as the build artifacts unpack
(<builds-dir>/<board>/rrcloud-ingest.bin, board = the firmware-build.yml input:
poe-eth, poe-eth-dsi5, touch-lcd-43, touch-lcd-43-rev3). For each, the app image is
copied to <out-dir>/rrcloud-ingest-<version>-<board>.bin, and
<out-dir>/rrcloud-ingest-ota.json lists them:

    {"version": "...", "tag": "...", "commit": "...",
     "images": [{"variant": "poe-eth", "board": "poe-eth", "file": "...bin", "size": n,
                 "sha256": "...", "chip_rev_min": 0, "chip_rev_max": 199}, ...]}

The dock (main/ota.c) picks the entry whose variant is its own and whose chip range
holds its chip revision. The variant is what the image says about itself (the tag
after esp_app_desc_t, main/ota.c), and the chip range comes from the ESP image
header, so the manifest cannot disagree with the images it lists.
"""
import hashlib
import json
import os
import shutil
import struct
import sys

TAG_MAGIC = 0x47544652          # main/ota.c FW_TAG_MAGIC
TAG_OFFSET = 24 + 8 + 256       # image header + first segment header + esp_app_desc_t


def describe(path):
    data = open(path, "rb").read()
    if len(data) < TAG_OFFSET + 44 or data[0] != 0xE9:
        sys.exit(f"{path}: not an ESP application image")
    chip_id, = struct.unpack_from("<H", data, 12)
    if chip_id != 0x12:
        sys.exit(f"{path}: chip id {chip_id:#x} is not the ESP32-P4")
    rev_min, rev_max = struct.unpack_from("<HH", data, 15)
    magic, = struct.unpack_from("<I", data, TAG_OFFSET)
    if magic != TAG_MAGIC:
        sys.exit(f"{path}: no firmware tag at {TAG_OFFSET} (main/ota.c) — the dock would refuse it")
    hardware = data[TAG_OFFSET + 4:TAG_OFFSET + 20].split(b"\0")[0].decode()
    variant = data[TAG_OFFSET + 20:TAG_OFFSET + 44].split(b"\0")[0].decode()
    version = data[32 + 16:32 + 48].split(b"\0")[0].decode()
    return data, hardware, variant, rev_min, rev_max, version


def main():
    builds, out, version, tag, commit = sys.argv[1:6]
    os.makedirs(out, exist_ok=True)
    images = []
    for board in sorted(os.listdir(builds)):
        src = os.path.join(builds, board, "rrcloud-ingest.bin")
        if not os.path.isfile(src):
            continue
        data, hardware, variant, rev_min, rev_max, built_version = describe(src)
        if built_version != version:
            sys.exit(f"{src}: built as {built_version}, publishing as {version}")
        name = f"rrcloud-ingest-{version}-{board}.bin"
        shutil.copyfile(src, os.path.join(out, name))
        images.append({
            "variant": variant, "hardware": hardware, "board": board, "file": name, "size": len(data),
            "sha256": hashlib.sha256(data).hexdigest(), "chip_rev_min": rev_min,
            "chip_rev_max": None if rev_max == 0xFFFF else rev_max,
        })
        print(f"{board}: {name} {len(data)} bytes, variant {variant}, chip v{rev_min // 100}.{rev_min % 100}..{rev_max}")
    if not images:
        sys.exit(f"no images under {builds}")
    for i in images:
        if i["chip_rev_max"] is None:
            del i["chip_rev_max"]
    manifest = {"version": version, "tag": tag, "commit": commit, "images": images}
    with open(os.path.join(out, "rrcloud-ingest-ota.json"), "w") as f:
        json.dump(manifest, f, indent=1)
        f.write("\n")


if __name__ == "__main__":
    main()

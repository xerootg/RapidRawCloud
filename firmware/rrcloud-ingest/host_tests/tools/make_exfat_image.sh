#!/bin/bash
# Build an exFAT test image (bare volume, or MBR + exFAT partition with `mbr`) populated from a
# directory tree, for test_exfat. Needs root for the loop mount.
#   make_exfat_image.sh <image> <tree> [mbr]
set -euo pipefail
img=$1 tree=$2 mbr=${3:-}
size_mb=${SIZE_MB:-64}
mnt=$(mktemp -d)
cleanup() { sudo umount "$mnt" 2>/dev/null || true; [ -n "${loop:-}" ] && sudo losetup -d "$loop" 2>/dev/null || true; rmdir "$mnt"; }
trap cleanup EXIT
truncate -s ${size_mb}M "$img"
if [ -n "$mbr" ]; then
    echo 'start=2048, type=7' | sfdisk -q "$img"
    loop=$(sudo losetup --find --show -P "$img")
    sudo mkfs.exfat -q "${loop}p1"
    sudo mount -o uid=$(id -u),gid=$(id -g) "${loop}p1" "$mnt"
else
    loop=$(sudo losetup --find --show "$img")
    sudo mkfs.exfat -q "$loop"
    sudo mount -o uid=$(id -u),gid=$(id -g) "$loop" "$mnt"
fi
cp -r "$tree"/. "$mnt"/
sync

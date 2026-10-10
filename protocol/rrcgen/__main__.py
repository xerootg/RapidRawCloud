"""rrcgen — generate SDK sources from protocol/rrcloud.protocol.toml.

    python3 protocol/rrcgen [--check] [--idl FILE] [--out DIR]

--check regenerates into a temp dir and fails if it differs from the committed output.
"""
from __future__ import annotations

import argparse
import filecmp
import os
import shutil
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import model  # noqa: E402
import emit_rust, emit_c, emit_cpp, emit_schema, emit_md  # noqa: E402

EMITTERS = {"rust": emit_rust.emit, "c": emit_c.emit, "cpp": emit_cpp.emit, "schema": emit_schema.emit, "md": emit_md.emit}


def generate(idl: Path, out: Path, langs: list[str]) -> None:
    m = model.load(idl)
    for lang in langs:
        EMITTERS[lang](m, out)


def tree_diff(a: Path, b: Path) -> list[str]:
    diffs: list[str] = []
    for root, _, files in os.walk(a):
        for f in files:
            pa = Path(root) / f
            rel = pa.relative_to(a)
            pb = b / rel
            if not pb.exists():
                diffs.append(f"missing in committed output: {rel}")
            elif not filecmp.cmp(pa, pb, shallow=False):
                diffs.append(f"differs: {rel}")
    return diffs


def main() -> int:
    here = Path(__file__).resolve().parent.parent
    ap = argparse.ArgumentParser(prog="rrcgen")
    ap.add_argument("--idl", type=Path, default=here / "rrcloud.protocol.toml")
    ap.add_argument("--out", type=Path, default=here / "gen")
    ap.add_argument("--lang", action="append", choices=sorted(EMITTERS), help="restrict to these emitters (default: all)")
    ap.add_argument("--check", action="store_true", help="verify committed output is up to date")
    args = ap.parse_args()
    langs = args.lang or list(EMITTERS)
    if args.check:
        with tempfile.TemporaryDirectory() as tmp:
            generate(args.idl, Path(tmp), langs)
            diffs = tree_diff(Path(tmp), args.out)
        if diffs:
            print("generated output is stale:\n  " + "\n  ".join(diffs), file=sys.stderr)
            return 1
        print("generated output is up to date")
        return 0
    generate(args.idl, args.out, langs)
    print(f"generated {', '.join(langs)} into {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

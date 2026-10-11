"""JSON Schema (draft 2020-12) emitter — one file per record plus a bundle with $defs.

This is the hand-off format for further languages (e.g. Java via jsonschema2pojo) and
for validating fixtures with any JSON Schema validator.
"""
from __future__ import annotations

import json
from pathlib import Path

from model import Model, Record, Field, TypeRef

PATTERNS = {
    "uuid4-lower": "^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$",
}


def scalar_schema(m: Model, name: str) -> dict:
    s = m.scalars[name]
    if s.kind != "string":
        return {"type": "integer", "minimum": 0} if s.kind == "u64" else {"type": "integer"}
    sch: dict = {"type": "string"}
    if s.format == "lower-hex":
        sch["pattern"] = f"^[0-9a-f]{{{s.length}}}$"
    elif s.format in PATTERNS:
        sch["pattern"] = PATTERNS[s.format]
    elif s.format == "relkey":
        sch["minLength"] = 1
        sch["pattern"] = "^(?!/)(?!.*(^|/)\\.\\.?(/|$))[^\\\\:\\u0000-\\u001f]+$"
        sch["x-rrcloud-rules"] = "NFC; segments not '.'/'..'; no trailing dot/space; not Windows-reserved; no '.rr.' prefix (full rules in generated code)"
    if s.max_length:
        sch["maxLength"] = s.max_length
    return sch


def type_schema(m: Model, t: TypeRef, f: Field | None = None) -> dict:
    n = t.name
    if t.array:
        inner = type_schema(m, TypeRef(n), f)
        sch = {"type": "array", "items": inner}
        if f and f.max_items:
            sch["maxItems"] = f.max_items
        return sch
    if n in ("u8", "u32", "u64", "usize"):
        sch = {"type": "integer", "minimum": 0}
        if n == "u8":
            sch["maximum"] = 255
        elif n == "u32":
            sch["maximum"] = 4294967295
        return sch
    if n == "i64":
        return {"type": "integer"}
    if n == "bool":
        return {"type": "boolean"}
    if n == "string":
        sch = {"type": "string"}
        if f and f.max_length:
            sch["maxLength"] = f.max_length
        return sch
    return {"$ref": f"#/$defs/{n}"}


def defs(m: Model) -> dict:
    d: dict = {}
    for s in m.scalars.values():
        sch = scalar_schema(m, s.name)
        if s.doc:
            sch["description"] = s.doc
        d[s.name] = sch
    for e in m.enums.values():
        d[e.name] = {"type": "string", "enum": [v.wire for v in e.values], "description": e.doc}
    for mp in m.maps.values():
        val = {"type": "integer", "minimum": 0}
        d[mp.name] = {"type": "object", "propertyNames": {"$ref": f"#/$defs/{mp.key}"}, "additionalProperties": val, "description": mp.doc,
                      "x-rrcloud-drop-zero": mp.drop_zero}
    for r in m.records.values():
        props = {}
        required = []
        for f in r.fields:
            sch = type_schema(m, f.type, f)
            if f.doc:
                sch = dict(sch, description=f.doc)
            if f.has_default:
                sch = dict(sch, default=f.default)
            props[f.wire_name(r.naming)] = sch
            if f.required:
                required.append(f.wire_name(r.naming))
        rec = {"type": "object", "description": r.doc, "properties": props, "required": required,
               "additionalProperties": True, "x-rrcloud-field-order": [f.wire_name(r.naming) for f in r.fields]}
        if r.version_field:
            rec["x-rrcloud-version-field"] = r.version_field
            rec["x-rrcloud-supported-versions"] = r.supported_versions
            props[r.version_field] = dict(props[r.version_field], enum=r.supported_versions)
        d[r.name] = rec
    return d


def emit(m: Model, out_dir: Path) -> None:
    d = out_dir / "schema"
    d.mkdir(parents=True, exist_ok=True)
    all_defs = defs(m)
    bundle = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": f"https://rapidrawcloud.dev/schema/{m.protocol['name']}-v{m.protocol['wire_version']}.json",
        "title": m.protocol["description"],
        "x-rrcloud-constants": {k: c.value for k, c in m.constants.items()},
        "x-rrcloud-keys": {k: v.template for k, v in m.keys.items()},
        "$defs": all_defs,
    }
    (d / "rrcloud.bundle.schema.json").write_text(json.dumps(bundle, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    for r in m.records.values():
        one = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$id": f"https://rapidrawcloud.dev/schema/{r.name}.json",
            "$ref": f"#/$defs/{r.name}",
            "$defs": all_defs,
        }
        (d / f"{r.name}.schema.json").write_text(json.dumps(one, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

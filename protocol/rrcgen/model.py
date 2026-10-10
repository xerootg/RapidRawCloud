"""IDL model: loads protocol/rrcloud.protocol.toml and validates cross-references.

Every emitter works from this model only, so adding a language is one new
emit_<lang>.py that walks `Model`.
"""
from __future__ import annotations

import re
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

PRIMITIVES = {"u8", "u32", "u64", "i64", "usize", "bool", "string"}


@dataclass
class TypeRef:
    name: str              # primitive name or named type
    array: bool = False    # [T]

    @property
    def is_primitive(self) -> bool:
        return self.name in PRIMITIVES

    def __str__(self) -> str:
        return f"[{self.name}]" if self.array else self.name


def parse_type(s: str) -> TypeRef:
    s = s.strip()
    if s.startswith("[") and s.endswith("]"):
        return TypeRef(s[1:-1].strip(), array=True)
    return TypeRef(s)


@dataclass
class Const:
    name: str
    type: str
    value: Any
    doc: str = ""


@dataclass
class Scalar:
    name: str
    kind: str                 # string | i64 | u64
    format: str = "any"       # uuid4-lower | relkey | lower-hex | any
    length: int | None = None
    max_length: int | None = None
    doc: str = ""
    derived_from: str | None = None   # another scalar this one relabels (same format/length); emits a conversion

    @property
    def buf_len(self) -> int:
        return self.length if self.length is not None else (self.max_length or 0)


@dataclass
class EnumValue:
    name: str
    wire: str
    doc: str = ""


@dataclass
class Enum:
    name: str
    values: list[EnumValue]
    doc: str = ""


@dataclass
class Map:
    name: str
    key: str
    value: str
    drop_zero: bool
    max_items: int
    doc: str = ""


@dataclass
class Field:
    name: str
    type: TypeRef
    optional: bool = False
    default: Any = None       # None = no default (required unless optional)
    has_default: bool = False
    doc: str = ""
    max_length: int | None = None
    max_items: int | None = None

    @property
    def required(self) -> bool:
        return not self.optional and not self.has_default

    def wire_name(self, naming: str) -> str:
        if naming == "camel":
            parts = self.name.split("_")
            return parts[0] + "".join(p[:1].upper() + p[1:] for p in parts[1:])
        return self.name


@dataclass
class Record:
    name: str
    fields: list[Field]
    doc: str = ""
    naming: str = "snake"
    version_field: str | None = None
    supported_versions: list[int] = field(default_factory=list)


@dataclass
class KeyPart:
    literal: str | None = None
    param: str | None = None
    fmt: str | None = None    # None | hex16 | blake3_32


@dataclass
class Key:
    name: str
    template: str
    params: dict[str, str]
    parts: list[KeyPart]
    doc: str = ""


@dataclass
class Document:
    name: str
    data: dict[str, Any]


@dataclass
class Model:
    protocol: dict[str, Any]
    constants: dict[str, Const]
    scalars: dict[str, Scalar]
    enums: dict[str, Enum]
    maps: dict[str, Map]
    records: dict[str, Record]
    keys: dict[str, Key]
    documents: dict[str, Document]
    operations: list[dict[str, Any]]

    def kind_of(self, name: str) -> str:
        if name in PRIMITIVES:
            return "primitive"
        if name in self.scalars:
            return "scalar"
        if name in self.enums:
            return "enum"
        if name in self.maps:
            return "map"
        if name in self.records:
            return "record"
        raise KeyError(f"unknown type {name!r}")

    def string_buf_len(self, f: Field) -> int:
        """Byte capacity (excluding NUL) a fixed-buffer target needs for a string-ish field."""
        t = f.type.name
        if t == "string":
            if f.max_length is None:
                raise ValueError(f"field {f.name}: string fields need max_length")
            return f.max_length
        if t in self.scalars and self.scalars[t].kind == "string":
            return self.scalars[t].buf_len
        raise ValueError(f"{f.name} is not a string field")


_TEMPLATE_RE = re.compile(r"\{([A-Za-z_][A-Za-z0-9_]*)(?::([a-z0-9]+))?\}|\{blake3_32\(([A-Za-z_][A-Za-z0-9_]*)\)\}")


def parse_template(tpl: str, params: dict[str, str]) -> list[KeyPart]:
    parts: list[KeyPart] = []
    pos = 0
    for m in _TEMPLATE_RE.finditer(tpl):
        if m.start() > pos:
            parts.append(KeyPart(literal=tpl[pos:m.start()]))
        if m.group(3):
            name, fmt = m.group(3), "blake3_32"
        else:
            name, fmt = m.group(1), m.group(2)
        if name not in params:
            raise ValueError(f"template {tpl!r}: parameter {name!r} not declared")
        parts.append(KeyPart(param=name, fmt=fmt))
        pos = m.end()
    if pos < len(tpl):
        parts.append(KeyPart(literal=tpl[pos:]))
    return parts


def load(path: Path) -> Model:
    raw = tomllib.loads(path.read_text(encoding="utf-8"))
    constants = {k: Const(k, v["type"], v["value"], v.get("doc", "")) for k, v in raw.get("constants", {}).items()}
    scalars = {k: Scalar(k, v["kind"], v.get("format", "any"), v.get("length"), v.get("max_length"), v.get("doc", ""), v.get("derived_from"))
               for k, v in raw.get("scalars", {}).items()}
    enums = {k: Enum(k, [EnumValue(e["name"], e["wire"], e.get("doc", "")) for e in v["values"]], v.get("doc", ""))
             for k, v in raw.get("enums", {}).items()}
    maps = {k: Map(k, v["key"], v["value"], bool(v.get("drop_zero", False)), int(v.get("max_items", 32)), v.get("doc", ""))
            for k, v in raw.get("maps", {}).items()}
    records: dict[str, Record] = {}
    for k, v in raw.get("records", {}).items():
        fields = []
        for f in v["fields"]:
            fields.append(Field(
                name=f["name"], type=parse_type(f["type"]), optional=bool(f.get("optional", False)),
                default=f.get("default"), has_default="default" in f, doc=f.get("doc", ""),
                max_length=f.get("max_length"), max_items=f.get("max_items")))
        records[k] = Record(k, fields, v.get("doc", ""), v.get("naming", "snake"), v.get("version_field"),
                            list(v.get("supported_versions", [])))
    keys = {}
    for k, v in raw.get("keys", {}).items():
        params = dict(v.get("params", {}))
        keys[k] = Key(k, v["template"], params, parse_template(v["template"], params), v.get("doc", ""))
    documents = {k: Document(k, v) for k, v in raw.get("documents", {}).items()}
    model = Model(raw["protocol"], constants, scalars, enums, maps, records, keys, documents, raw.get("operations", []))
    validate(model)
    return model


def validate(m: Model) -> None:
    for s in m.scalars.values():
        if s.kind == "string" and s.buf_len == 0:
            raise ValueError(f"scalar {s.name}: string scalars need length or max_length")
        if s.format == "lower-hex" and s.length is None:
            raise ValueError(f"scalar {s.name}: lower-hex needs length")
        if s.derived_from is not None:
            base = m.scalars.get(s.derived_from)
            if base is None or (base.kind, base.format, base.length, base.max_length) != (s.kind, s.format, s.length, s.max_length):
                raise ValueError(f"scalar {s.name}: derived_from must name a scalar of identical shape")
    for mp in m.maps.values():
        if m.kind_of(mp.key) != "scalar" or m.scalars[mp.key].kind != "string":
            raise ValueError(f"map {mp.name}: key must be a string scalar")
        if mp.value not in {"u32", "u64", "i64"}:
            raise ValueError(f"map {mp.name}: value must be an integer primitive")
    for r in m.records.values():
        names = set()
        for f in r.fields:
            if f.name in names:
                raise ValueError(f"record {r.name}: duplicate field {f.name}")
            names.add(f.name)
            m.kind_of(f.type.name)
            if f.type.name == "string" and f.max_length is None:
                raise ValueError(f"record {r.name}.{f.name}: string needs max_length")
            if f.type.array and f.max_items is None:
                raise ValueError(f"record {r.name}.{f.name}: arrays need max_items")
            if f.optional and f.has_default:
                raise ValueError(f"record {r.name}.{f.name}: optional fields cannot have defaults")
        if r.version_field:
            if r.version_field not in names:
                raise ValueError(f"record {r.name}: version_field {r.version_field} is not a field")
            if not r.supported_versions:
                raise ValueError(f"record {r.name}: supported_versions required with version_field")
    for k in m.keys.values():
        for pname, ptype in k.params.items():
            if ptype not in {"hex6", "username"} and m.kind_of(ptype) not in {"scalar", "enum"}:
                raise ValueError(f"key {k.name}: param {pname} has unsupported type {ptype}")
    for d in m.documents.values():
        for ref in ("record", "header"):
            if ref in d.data and d.data[ref] not in m.records:
                raise ValueError(f"document {d.name}: unknown record {d.data[ref]}")
        for ref in d.data.get("rows", []):
            if ref not in m.records:
                raise ValueError(f"document {d.name}: unknown row record {ref}")
        if "key" in d.data and d.data["key"] not in m.keys:
            raise ValueError(f"document {d.name}: unknown key {d.data['key']}")
        for lim in d.data.get("limits", []):
            if lim not in m.constants:
                raise ValueError(f"document {d.name}: unknown limit constant {lim}")


def snake(name: str) -> str:
    """CamelCase -> snake_case (Blake3Hex -> blake3_hex, DeviceId -> device_id)."""
    out = re.sub(r"(?<=[a-z0-9])([A-Z])", r"_\1", name)
    return out.lower()


def upper_snake(name: str) -> str:
    return snake(name).upper()

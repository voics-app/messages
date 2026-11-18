#!/usr/bin/env python3
"""Import/check scheduled wire types generated from an exact canonical contracts bundle."""
import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess

ROOT = Path(__file__).resolve().parents[1]
DEST = ROOT / "crates/contracts/src/scheduled"
ROOTS = ("CreateScheduledMessageRequest", "EditScheduledMessageRequest", "ScheduledMessageList")
FILES = ("schema.json", "types.rs")


def digest(data):
    return hashlib.sha256(data.replace(b"\r\n", b"\n")).hexdigest()


def projected(bundle):
    selected = set()
    pending = list(ROOTS)
    while pending:
        name = pending.pop()
        if name in selected:
            continue
        selected.add(name)
        text = json.dumps(bundle["$defs"][name])
        pending.extend(re.findall(r'"\$ref": "#/\$defs/([^"]+)"', text))
    return {"$schema": bundle["$schema"], "$defs": {name: bundle["$defs"][name] for name in sorted(selected)}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--canonical", type=Path)
    parser.add_argument("--import-revision")
    parser.add_argument("--generator", type=Path)
    args = parser.parse_args()
    if args.import_revision:
        if not args.canonical or not args.generator or not re.fullmatch(r"[0-9a-f]{40}", args.import_revision):
            parser.error("import requires canonical checkout, full commit SHA and compiled canonical typify generator")
        raw = subprocess.check_output(["git", "-C", str(args.canonical), "show", f"{args.import_revision}:dist/schemas.bundle.json"])
        schema = projected(json.loads(raw))
        DEST.mkdir(parents=True, exist_ok=True)
        (DEST / "schema.json").write_text(json.dumps(schema, indent=2) + "\n", encoding="utf-8")
        subprocess.run([str(args.generator), str(DEST / "schema.json"), str(DEST / "types.rs")], check=True)
        record = {"repository": "https://github.com/discordy-voics/contracts", "revision": args.import_revision,
                  "source": "dist/schemas.bundle.json", "roots": ROOTS,
                  "sha256_lf": {name: digest((DEST / name).read_bytes()) for name in FILES}}
        (DEST / "UPSTREAM.json").write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")
    record = json.loads((DEST / "UPSTREAM.json").read_text(encoding="utf-8"))
    if record["repository"] != "https://github.com/discordy-voics/contracts" or not re.fullmatch(r"[0-9a-f]{40}", record["revision"]) or tuple(record["roots"]) != ROOTS or set(record["sha256_lf"]) != set(FILES):
        raise ValueError("invalid scheduled contract provenance")
    for name in FILES:
        if digest((DEST / name).read_bytes()) != record["sha256_lf"][name]:
            raise ValueError(f"scheduled contract drift: {name}; regenerate from the canonical bundle")
    if args.canonical:
        raw = subprocess.check_output(["git", "-C", str(args.canonical), "show", f"{record['revision']}:dist/schemas.bundle.json"])
        if projected(json.loads(raw)) != json.loads((DEST / "schema.json").read_text(encoding="utf-8")):
            raise ValueError("scheduled schema differs from its canonical commit")
    print(f"Scheduled contracts match {record['revision']}: {len(FILES)} digests verified")


if __name__ == "__main__":
    main()

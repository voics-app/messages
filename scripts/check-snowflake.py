#!/usr/bin/env python3
"""Verify the pinned shared issuer; import updates only from an exact canonical commit."""
import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
DEST = ROOT / "crates" / "snowflake"
REPOSITORY = "https://github.com/discordy-voics/contracts"
SOURCE = "packages/snowflake"
FILES = ("Cargo.toml", "README.md", "rustfmt.toml", "src/lib.rs")


def git_bytes(canonical, *args):
    return subprocess.check_output(["git", "-C", str(canonical), *args])


def digest(data):
    # Git's canonical text is LF; tolerate a Windows checkout's CRLF conversion.
    return hashlib.sha256(data.replace(b"\r\n", b"\n")).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--canonical", type=Path, help="local canonical contracts Git checkout")
    parser.add_argument("--import-revision", help="replace snapshot from this full canonical commit SHA")
    args = parser.parse_args()
    if args.import_revision:
        if not args.canonical or not re.fullmatch(r"[0-9a-f]{40}", args.import_revision):
            parser.error("import requires --canonical and a full 40-character commit SHA")
        revision = args.import_revision
        resolved = git_bytes(args.canonical, "rev-parse", "--verify", revision + "^{commit}").decode().strip()
        if resolved != revision:
            raise ValueError("canonical commit mismatch")
        contents = {name: git_bytes(args.canonical, "show", f"{revision}:{SOURCE}/{name}") for name in FILES}
        for name, data in contents.items():
            target = DEST / name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(data)
        record = {"repository": REPOSITORY, "revision": revision, "source": SOURCE,
                  "sha256_lf": {name: digest(data) for name, data in contents.items()}}
        (DEST / "UPSTREAM.json").write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")

    record = json.loads((DEST / "UPSTREAM.json").read_text(encoding="utf-8"))
    revision = record["revision"]
    if (record["repository"] != REPOSITORY or record["source"] != SOURCE
            or not re.fullmatch(r"[0-9a-f]{40}", revision)
            or set(record["sha256_lf"]) != set(FILES)):
        raise ValueError("invalid shared-issuer provenance")
    if args.canonical:
        resolved = git_bytes(args.canonical, "rev-parse", "--verify", revision + "^{commit}").decode().strip()
        if resolved != revision:
            raise ValueError("canonical commit mismatch")
    for name in FILES:
        expected = record["sha256_lf"][name]
        if digest((DEST / name).read_bytes()) != expected:
            raise ValueError(f"shared issuer drift: {name}; import canonical changes instead of editing the copy")
        if args.canonical and digest(git_bytes(args.canonical, "show", f"{revision}:{SOURCE}/{name}")) != expected:
            raise ValueError(f"canonical {revision} does not match {name}")
    print(f"Shared issuer matches {revision}: {len(FILES)} digests verified"
          + (" against canonical Git objects" if args.canonical else " against pinned provenance"))


if __name__ == "__main__":
    try:
        main()
    except (ValueError, KeyError, OSError, subprocess.CalledProcessError) as error:
        print(f"Shared issuer check failed: {error}", file=sys.stderr)
        sys.exit(1)

#!/usr/bin/env bash
# Rebrand check (discordy-voics/docs#69): the product name and the brand domain must not appear in
# user-visible string literals in Rust sources. They come from config (`BRAND_NAME`, `BRAND_URL`).
# Test code under tests/ is exempt; a line may opt out with the marker `brand-check: allow`.
# Internal identifiers (crate names, image names, env var prefixes) are out of scope on purpose.
set -euo pipefail
cd "$(dirname "$0")/.."
name="${BRAND_NAME:-Discordy}"
domain="${BRAND_DOMAIN:-voics.nl}"
hits=$(grep -rnE --include='*.rs' "\"[^\"]*(${name}|${domain//./\.})" crates bin 2>/dev/null \
  | grep -v '/tests/' | grep -v 'brand-check: allow' || true)
if [ -n "$hits" ]; then
  echo "Hard-coded brand name or domain in user-visible strings (use config instead):" >&2
  echo "$hits" >&2
  exit 1
fi
echo "brand check ok"

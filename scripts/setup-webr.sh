#!/usr/bin/env bash
# Idempotently copy the webR distribution (R.js + R.wasm + virtual filesystem +
# JS shims) from node_modules into static/webr/ so SvelteKit serves it under
# /webr/. Self-hosting (instead of using the CDN) keeps COOP/COEP simple — the
# Cross-Origin-Embedder-Policy: require-corp header forbids opaque resources, so
# every script/wasm a webR boot fetches must come from our origin (or send the
# matching CORP header).
#
# Re-run when the webr npm package is bumped. Idempotent: wipes the destination
# and copies fresh, so removed/renamed source files don't linger. Uses only
# POSIX tools so it works on CI images (e.g. node:22) that lack rsync.

set -euo pipefail

ROOT="$(git rev-parse --show-toplevel 2>/dev/null)" || exit 0
cd "$ROOT"

SRC="node_modules/webr/dist"
DST="static/webr"

if [ ! -d "$SRC" ]; then
  echo "[setup-webr] $SRC missing — run \`npm install\` first" >&2
  exit 1
fi

rm -rf "$DST"
mkdir -p "$DST"

cp -R "$SRC/." "$DST/"

# Prune dev-only assets: the demo REPL, the test fixtures, and the TypeScript
# declaration directory (.d.ts files are already bundled into webr.mjs's types).
rm -rf "$DST/repl" "$DST/tests" "$DST/webR"
find "$DST" -type f \( -name '*.d.ts' -o -name '*.cjs.map' -o -name '*.mjs.map' \) -delete

echo "[setup-webr] synced webR -> $DST ($(du -sh "$DST" | cut -f1))" >&2

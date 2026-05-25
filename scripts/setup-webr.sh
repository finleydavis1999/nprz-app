#!/usr/bin/env bash
# Idempotently copy the webR distribution (R.js + R.wasm + virtual filesystem +
# JS shims) from node_modules into static/webr/ so SvelteKit serves it under
# /webr/. Self-hosting (instead of using the CDN) keeps COOP/COEP simple — the
# Cross-Origin-Embedder-Policy: require-corp header forbids opaque resources, so
# every script/wasm a webR boot fetches must come from our origin (or send the
# matching CORP header).
#
# Re-run when the webr npm package is bumped: the source-file mtimes change and
# rsync notices. No-op when up-to-date.

set -euo pipefail

ROOT="$(git rev-parse --show-toplevel 2>/dev/null)" || exit 0
cd "$ROOT"

SRC="node_modules/webr/dist"
DST="static/webr"

if [ ! -d "$SRC" ]; then
  echo "[setup-webr] $SRC missing — run \`npm install\` first" >&2
  exit 1
fi

mkdir -p "$DST"

# Exclude dev-only assets: the demo REPL, the test fixtures, and the TypeScript
# declaration directory (.d.ts files are already bundled into webr.mjs's types).
rsync -a --delete \
  --exclude='repl/' \
  --exclude='tests/' \
  --exclude='webR/' \
  --exclude='*.d.ts' \
  --exclude='*.cjs.map' \
  --exclude='*.mjs.map' \
  "$SRC/" "$DST/"

echo "[setup-webr] synced webR -> $DST ($(du -sh "$DST" | cut -f1))" >&2

#!/usr/bin/env bash
# Speeds up fresh git worktrees: links node_modules to the main worktree so we
# skip a multi-minute `npm install` (and reuse its warm Vite prebundle cache).
# Idempotent + fast: no-ops when node_modules already exists. Run automatically
# by the SessionStart hook in .claude/settings.json.
set -euo pipefail

ROOT="$(git rev-parse --show-toplevel 2>/dev/null)" || exit 0
cd "$ROOT"

# Already set up (real dir or symlink) -> nothing to do.
[ -e node_modules ] && exit 0

# Main worktree is always the first entry of `git worktree list`.
MAIN="$(git worktree list --porcelain | sed -n '1s/^worktree //p')"

if [ -z "$MAIN" ] || [ "$ROOT" = "$MAIN" ]; then
  echo "[setup-worktree] node_modules missing — run \`npm install\`" >&2
  exit 0
fi

if [ ! -d "$MAIN/node_modules" ]; then
  echo "[setup-worktree] main repo has no node_modules — \`npm install\` there first" >&2
  exit 0
fi

# Branch changed dependencies -> a shared node_modules would be wrong; install for real.
if ! cmp -s package-lock.json "$MAIN/package-lock.json"; then
  echo "[setup-worktree] package-lock.json differs from main — running npm install" >&2
  npm install
  exit 0
fi

ln -s "$MAIN/node_modules" node_modules
echo "[setup-worktree] linked node_modules -> $MAIN/node_modules" >&2

# .svelte-kit is gitignored/generated; create it so types resolve immediately.
npx --no-install svelte-kit sync >/dev/null 2>&1 || true

# webR runtime is gitignored (~46MB); sync from node_modules.
bash scripts/setup-webr.sh >/dev/null 2>&1 || true

# Claude project notes

See `README.md` for the user-facing run/architecture docs.

## House rules

- **Svelte 5 runes only.** Never use `export let`, `$:`, or `<script context="module">`. Use `$state`, `$derived`, `$derived.by(() => ...)`, `$effect`, `$props()`, snippets via `{@render children?.()}`. Field declarations on classes for singleton state.
- **Map context** is Symbol-keyed via `src/lib/map/context.js`. Use `setMapContext()` / `getMapContext()` — never `getContext('map')`.
- **Field rows** use `<Field label="...">{control}</Field>` (`src/lib/ui/Field.svelte`). Don't recreate the `.row` grid.
- **Browser-only deps** (`maplibre-gl`, `pmtiles`, `@duckdb/duckdb-wasm`) must be dynamically imported inside `onMount`. They touch `window` at module-eval time and break SSR otherwise.
- **No name shadowing of JS globals** in component imports. `import Map from '$lib/map/Map.svelte'` shadows the JS `Map` class — always rename to `MapView`, `MapSet`, etc.
- **CSS tokens only.** Every `<style>` block consumes `var(--text-sm)`, `var(--color-muted)`, `var(--spacing-2)` etc. defined in `src/routes/layout.css` `@theme`. No raw `#rrggbb` / `0.85rem` / `8px` literals.
- **State module suffix `.svelte.js`** for runes-using files (the compiler needs the suffix).
- **Reactivity for object members**: assign new objects (`s.filters = { ...s.filters, [k]: v }`); deep mutation isn't tracked.
- **Layer calculator state** lives in `src/lib/state/layers.svelte.js` (singleton `layers` + read-only `displayed` facade), persisted to `localStorage` under `nprz.layers.v1`. The map/legend/print read `displayed.data` so an active saved layer overrides the live `queryResult.data`.

## Tooling

- `npm run dev` for the dev server (Vite default port 5173). The e2e dev server binds a **per-worktree** port hashed from the worktree path (see `playwright.config.js`), so parallel worktrees never collide on a shared port — don't hardcode a test port.
- `npm run test:e2e` runs Playwright against an auto-started dev server. Don't write throwaway `smoke.mjs` scripts — extend `tests/e2e/app.e2e.js` instead. The suite disables the app's background parquet prefetch (`globalThis.__E2E_NO_PREFETCH__`, set via `addInitScript`) so tests only fetch the datasets they query.
- `npm run data` rebuilds parquet + geo + manifest. Required after editing R pipeline.

## Worktrees

Fresh git worktrees have no `node_modules` (it's gitignored). Don't `npm install`
per worktree — that wastes minutes and ~530 MB each. Instead `node_modules` is
symlinked to the main repo's copy, which also shares its warm Vite prebundle cache.

- Run `bash scripts/setup-worktree.sh` once in a fresh worktree (it's idempotent
  and fast — no-ops when `node_modules` already exists).
- If a branch changes dependencies (`package-lock.json` differs from main), the
  script runs a real `npm install` in the worktree instead of symlinking.
- When you bump dependencies, `npm install` in the **main** repo so every symlinked
  worktree picks them up.
- The symlinked `node_modules` resolves to a path outside the worktree, so
  `vite.config.js` widens `server.fs.allow` to the main repo root. **If Vite
  throws a `403` / `server.fs.allow` error, that config is the fix — do NOT run
  `npm ci` / `npm install` to "get a real node_modules"; that just discards the
  speedup. The symlink is intentional and correct.**

## Svelte MCP

The Svelte MCP server is available with these tools:

1. `list-sections` — call first to discover docs sections.
2. `get-documentation` — fetch full docs for relevant sections (use after list-sections).
3. `svelte-autofixer` — run on every Svelte snippet you write before sending; loop until clean.
4. `playground-link` — only on user request, only for snippets not written to files.

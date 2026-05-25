// Declarative R-package install manifest per model family.
//
// Lazy: `ensurePackagesFor(family)` only installs what's missing from the
// session's running tally. webR also persists packages in IndexedDB across
// reloads, so a "cold" first run on a new browser is the only slow case.
//
// Verified live in https://repo.r-wasm.org/bin/emscripten/contrib/4.6/PACKAGES
// (and 4.5/) as of 2025-Q4 — sizes are the compiled `.tgz` Content-Length:
//   speedglm  0.10 MB  CG solver, much faster than base glm() once N > a few hundred
//   MASS      0.83 MB  glm.nb for negative-binomial
//   Matrix    2.80 MB  sparse model matrices for constrained SIM
//
// data.table is intentionally *not* installed — ETL happens in DuckDB-WASM
// (multi-threaded), not in webR (single-threaded, OpenMP off).

import { ensurePackages } from './webr-client.js';

/** @typedef {'nlm' | 'sim' | 'gwr'} ModelFamily */

/** Packages required for each model family. NLM is the superset for phase 1;
 *  SIM and GWR add nothing because the pure-R fitting loops only need base. */
const PACKAGES_BY_FAMILY = /** @type {const} */ ({
	nlm: ['speedglm', 'MASS', 'Matrix'],
	sim: ['speedglm', 'Matrix'],
	gwr: [] // pure R using lm.wfit / glm.fit from base — no installs needed
});

/** @param {ModelFamily} family */
export function packagesFor(family) {
	return PACKAGES_BY_FAMILY[family] ?? [];
}

/** Ensure all packages needed by the given model family are installed. Safe
 *  to call repeatedly — cached at the webR-client layer.
 *  @param {ModelFamily} family */
export async function ensurePackagesFor(family) {
	const pkgs = packagesFor(family);
	if (pkgs.length === 0) return;
	await ensurePackages(pkgs);
}

// Lazy-loaded singleton wrapping the webR (R-on-WebAssembly) runtime.
//
// webR ships its own dedicated `Worker` thread for R execution; we don't wrap
// it in another Worker. `getWebR()` returns a Promise<WebR>, lazily initialised
// on first call. The runtime weighs ~46 MB (R interpreter + base + libRblas/
// libRlapack), so we never touch it on app boot — only when a model run is
// requested or when `prefetchWebR()` is fired-and-forgotten from a dock that
// the user just opened.
//
// Cross-origin isolation: COOP/COEP are set in src/hooks.server.js. With them
// in place, webR auto-selects the SharedArrayBuffer channel — gives us a faster
// main↔worker bridge and, crucially, `interrupt()` support. Without them webR
// silently falls back to PostMessage (slower, can't cancel).
//
// All `import('webr')` calls live in this file. The webr package touches
// `window` at module-eval time, so it must never be imported statically.

let webRPromise = null;
/** @type {Set<string>} */
const installedPackages = new Set();

async function bootWebR() {
	const { WebR } = await import('webr');
	const webR = new WebR({
		// Self-hosted under static/webr/ (see scripts/setup-webr.sh) — keeps every
		// fetch same-origin, so CORP requirements impose no extra audit on us.
		baseUrl: '/webr/',
		// r-wasm.org serves package binaries with `Cross-Origin-Resource-Policy:
		// cross-origin`, so installPackages() works under our require-corp policy.
		repoUrl: 'https://repo.r-wasm.org/'
	});
	await webR.init();
	return webR;
}

/** Lazy singleton accessor. Boots webR on first call; subsequent calls share
 *  the same instance.
 *  @returns {Promise<import('webr').WebR>} */
export function getWebR() {
	if (!webRPromise) webRPromise = bootWebR();
	return webRPromise;
}

/** Fire-and-forget warmup — call from dock-open handlers so the user doesn't
 *  wait for the boot on first "Run". Errors are swallowed; the next real call
 *  will surface them. */
export function prefetchWebR() {
	getWebR().catch(() => {
		webRPromise = null; // allow retry on next real call
	});
}

/** Install a set of R packages from the webR repo. Caches install state in
 *  module scope (within a session) and across reloads (webR persists packages
 *  in IndexedDB). Safe to call many times — only the first call per package
 *  does the network fetch.
 *  @param {string[]} packages */
export async function ensurePackages(packages) {
	const missing = packages.filter((p) => !installedPackages.has(p));
	if (missing.length === 0) return;
	const webR = await getWebR();
	await webR.installPackages(missing);
	for (const p of missing) installedPackages.add(p);
}

/** Attempt to interrupt an in-flight R computation. Requires SharedArrayBuffer
 *  (i.e. COOP/COEP). No-op if webR hasn't booted yet. */
export function interruptR() {
	if (!webRPromise) return;
	webRPromise
		.then((webR) => {
			try {
				webR.interrupt();
			} catch {
				// interrupt() throws when SAB unavailable — nothing actionable here.
			}
		})
		.catch(() => {});
}

/** Run R code under a fresh evaluation environment populated with the given
 *  JS bindings. Wraps in `tryCatch(...)` so R-side errors come back as a
 *  rejected promise instead of an unhandled R condition.
 *
 *  @param {string} code  R expression(s) to evaluate. The *last* expression's
 *    value is what comes back.
 *  @param {Record<string, unknown>} [env]  JS values bound as R variables of
 *    the same name. Float64Array → numeric, string[] → character, etc.
 *  @returns {Promise<import('webr').RObject>} the R-side result object;
 *    caller is responsible for converting it (e.g. `.toArray()`, `.toJs()`).
 */
export async function runR(code, env = {}) {
	const webR = await getWebR();
	// Wrap user code so R errors surface as JS rejections instead of resolved
	// `RObject`s that happen to be a `condition`. throwJsException is on by
	// default in webR but we belt-and-brace with an inner tryCatch that yields
	// a stable error shape for `__err` detection if a caller turns it off.
	const wrapped = `tryCatch({\n${code}\n}, error = function(e) stop(conditionMessage(e)))`;
	return webR.evalR(wrapped, { env });
}

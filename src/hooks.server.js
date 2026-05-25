import { building } from '$app/environment';
import { sequence } from '@sveltejs/kit/hooks';
import { auth } from '$lib/server/auth';
import { svelteKitHandler } from 'better-auth/svelte-kit';

/** @type {import('@sveltejs/kit').Handle} */
const handleBetterAuth = async ({ event, resolve }) => {
	const session = await auth.api.getSession({ headers: event.request.headers });

	if (session) {
		event.locals.session = session.session;
		event.locals.user = session.user;
	}

	return svelteKitHandler({ event, resolve, auth, building });
};

// Cross-origin isolation — required for `SharedArrayBuffer`, which webR uses
// for its fast main↔worker channel and for `interrupt()` support, and which
// DuckDB-WASM uses for its multi-threaded pthread workers.
//
// COEP: require-corp blocks any cross-origin resource that doesn't explicitly
// opt in via `Cross-Origin-Resource-Policy`. We self-host all heavy assets
// (webR under /webr/, DuckDB-WASM under node_modules, parquet under /data/),
// and a Vite dev-server middleware (see vite.config.js) stamps CORP on every
// asset so this is safe in development.
//
// In production (adapter-static) these headers must be set by the hosting
// layer (Nginx/Cloudflare/etc.) because the static build has no SvelteKit
// runtime — this handler only fires on a non-prerendered request path.
/** @type {import('@sveltejs/kit').Handle} */
const handleCrossOriginIsolation = async ({ event, resolve }) => {
	const response = await resolve(event);
	response.headers.set('Cross-Origin-Opener-Policy', 'same-origin');
	response.headers.set('Cross-Origin-Embedder-Policy', 'require-corp');
	return response;
};

export const handle = sequence(handleBetterAuth, handleCrossOriginIsolation);

<script>
	import { onMount, onDestroy } from 'svelte';
	import { setMapContext } from './context.js';
	import { installLayerAnchors } from './layer-order.js';
	import { mapView } from '$lib/state/map-view.svelte.js';

	// `center` / `zoom` are intentionally NOT props anymore — the live-map
	// view is owned by the `mapView` singleton so it can survive navigations
	// and reloads. The persisted defaults inside `map-view.svelte.js` use
	// `MAP_DEFAULTS` from `./defaults.js` as their seed — single source of
	// truth shared with the reset-all affordance in `+page.svelte`.
	let { apiKey = null, pmtilesUrl = null, theme = 'white', children } = $props();

	let container;
	let viewSync = /** @type {(() => void) | null} */ (null);
	const ctx = $state({ map: null, ready: false });
	setMapContext(ctx);

	onMount(async () => {
		const [{ default: maplibregl }, basemap] = await Promise.all([
			import('maplibre-gl'),
			import('./basemap.js')
		]);
		await import('maplibre-gl/dist/maplibre-gl.css');

		basemap.registerPmtilesProtocol();
		const style = apiKey
			? basemap.protomapsApiStyle({ apiKey, theme })
			: pmtilesUrl
				? basemap.pmtilesStyle({ url: pmtilesUrl, theme })
				: basemap.emptyStyle();

		// Hydrate the initial camera from the persisted singleton so the user
		// returns to the same view after every navigation.
		const map = new maplibregl.Map({
			container,
			style,
			center: mapView.center,
			zoom: mapView.zoom,
			dragRotate: false,
			touchPitch: false,
			pitchWithRotate: false
		});
		// Expose for e2e tests; harmless in production.
		if (typeof window !== 'undefined') window.__map = map;
		map.addControl(new maplibregl.NavigationControl(), 'bottom-right');
		// Bottom-right scale bar — bottom-left is reserved for the dock
		// toggle strip and would intercept clicks on its controls.
		map.addControl(new maplibregl.ScaleControl({ maxWidth: 120, unit: 'metric' }), 'bottom-right');

		// Persist the camera on every gesture end. Coalesce with rAF so a
		// burst of `moveend` events (e.g. during an `easeTo` animation) only
		// produces one state write per frame — same defence used in
		// PrintFrameOverlay against Svelte 5's effect-update-depth heuristic.
		let raf = 0;
		const syncView = () => {
			if (raf) return;
			raf = requestAnimationFrame(() => {
				raf = 0;
				const c = map.getCenter();
				const z = map.getZoom();
				// Assign fresh array — deep mutation isn't tracked.
				if (mapView.center[0] !== c.lng || mapView.center[1] !== c.lat) {
					mapView.center = [c.lng, c.lat];
				}
				if (mapView.zoom !== z) mapView.zoom = z;
			});
		};
		map.on('moveend', syncView);
		map.on('zoomend', syncView);
		viewSync = () => {
			if (raf) cancelAnimationFrame(raf);
			map.off('moveend', syncView);
			map.off('zoomend', syncView);
		};
		// Resolve readiness whether `load` fires now or has already fired during
		// the awaits above (race when the protomaps style resolves quickly).
		let resolved = false;
		const ready = () => {
			if (resolved) return;
			resolved = true;
			// Install z-order anchors before any child layer component mounts.
			installLayerAnchors(map);
			ctx.map = map;
			ctx.ready = true;
		};
		if (map.loaded()) ready();
		else {
			map.on('load', ready);
			map.once('idle', ready);
		}
	});

	onDestroy(() => {
		viewSync?.();
		ctx.map?.remove();
	});
</script>

<div bind:this={container} style="width: 100%; height: 100%;"></div>
{#if ctx.ready}
	{@render children?.()}
{/if}

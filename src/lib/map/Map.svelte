<script>
	import { onMount, onDestroy } from 'svelte';
	import { setMapContext } from './context.js';
	import { installLayerAnchors } from './layer-order.js';
	import { MAP_DEFAULTS } from './defaults.js';

	let {
		center = MAP_DEFAULTS.center,
		zoom = MAP_DEFAULTS.zoom,
		apiKey = null,
		pmtilesUrl = null,
		theme = 'white',
		children
	} = $props();

	let container;
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

		const map = new maplibregl.Map({ container, style, center, zoom });
		// Expose for e2e tests; harmless in production.
		if (typeof window !== 'undefined') window.__map = map;
		map.addControl(new maplibregl.NavigationControl(), 'top-right');
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
		ctx.map?.remove();
	});
</script>

<div bind:this={container} style="width: 100%; height: 100%;"></div>
{#if ctx.ready}
	{@render children?.()}
{/if}

<script>
	import { onMount, onDestroy } from 'svelte';
	import { setMapContext } from './context.js';

	let {
		center = [5.3, 52.1],
		zoom = 7,
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

		// Esri World Light Gray Canvas as raster base — clean greyscale with
		// clear urban fabric. Protomaps vector labels added on top after load.
		const style = {
			version: 8,
			sources: {
				esri: {
					type: 'raster',
					tiles: [
						'https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/World_Light_Gray_Base/MapServer/tile/{z}/{y}/{x}'
					],
					tileSize: 256,
					maxzoom: 16,
					attribution: 'Tiles &copy; Esri &mdash; Esri, DeLorme, NAVTEQ'
				}
			},
			layers: [{ id: 'esri-base', type: 'raster', source: 'esri' }]
		};

		const map = new maplibregl.Map({ container, style, center, zoom });
		// Expose for e2e tests; harmless in production.
		if (typeof window !== 'undefined') window.__map = map;
		map.addControl(new maplibregl.NavigationControl(), 'bottom-right');

		// Resolve readiness whether `load` fires now or has already fired during
		// the awaits above (race when the protomaps style resolves quickly).
		let resolved = false;
		const ready = () => {
			if (resolved) return;
			resolved = true;
			// Add Protomaps symbol (label) layers on top of the raster base.
			// Only symbol layers are added — no fills or lines from Protomaps.
			if (apiKey) {
				map.addSource('protomaps-labels', {
					type: 'vector',
					tiles: [`https://api.protomaps.com/tiles/v4/{z}/{x}/{y}.mvt?key=${apiKey}`],
					minzoom: 0,
					maxzoom: 15
				});
				const labelLayers = basemap
					.protomapsApiStyle({ apiKey, theme })
					.layers.filter((l) => l.type === 'symbol');
				labelLayers.forEach((l) => {
					map.addLayer({ ...l, source: 'protomaps-labels' });
				});
			}
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

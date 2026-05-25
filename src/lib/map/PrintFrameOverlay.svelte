<script>
	// Renders an A4-aspect rectangle on top of the live map. Whatever the user
	// has framed inside this rectangle is what `/print` will render. Updates
	// `printView.rdExtent` reactively as the map moves/resizes/orientation
	// flips, so the print viewport stays in sync.
	//
	// The overlay must live inside a `<MapView>` so it can pull the maplibre
	// instance via the map context (Symbol-keyed, per house rules). The DOM
	// element is absolutely positioned and fills the nearest positioned
	// ancestor — in `/+page.svelte` that's the `position: fixed; inset: 0;`
	// wrapper, which means the overlay covers the full map area.
	import { untrack } from 'svelte';
	import { getMapContext } from './context.js';
	import { placeLabelLayerIds } from './layer-order.js';
	import { printView } from '$lib/state/print-view.svelte.js';
	import { wgs84ToRd } from '$lib/print/rd.js';

	const ctx = getMapContext();

	let host;
	let hostSize = $state({ w: 0, h: 0 });
	let mapTick = $state(0);

	// A4 paper aspect, with width/height keyed off orientation.
	const aspect = $derived(printView.orientation === 'landscape' ? 297 / 210 : 210 / 297);

	// Compute the rectangle in screen pixels — centred, sized to fit ~85% of
	// the smaller map axis without exceeding 85% of the other axis.
	const rectPx = $derived.by(() => {
		const W = hostSize.w;
		const H = hostSize.h;
		if (W <= 0 || H <= 0) return null;
		const maxW = W * 0.85;
		const maxH = H * 0.85;
		// Try fitting to height first, then clamp to width if needed.
		let h = maxH;
		let w = h * aspect;
		if (w > maxW) {
			w = maxW;
			h = w / aspect;
		}
		const x0 = (W - w) / 2;
		const y0 = (H - h) / 2;
		return { x0, y0, x1: x0 + w, y1: y0 + h, w, h };
	});

	// Watch the map container size. ResizeObserver fires synchronously after
	// any layout change so the overlay tracks both window resizes and
	// sidebar collapses.
	$effect(() => {
		if (!host) return;
		const ro = new ResizeObserver((entries) => {
			const e = entries[0];
			hostSize = { w: e.contentRect.width, h: e.contentRect.height };
		});
		ro.observe(host);
		return () => ro.disconnect();
	});

	// Re-derive rdExtent on every map gesture end. `mapTick` increments
	// asynchronously (rAF-debounced) so even if maplibre fires the event
	// burstily during a single tick, we only schedule one state write per
	// frame — Svelte 5's effect-update-depth heuristic would otherwise see
	// the burst as a runaway loop and abort.
	//
	// `moveend`/`zoomend` are the right granularity for a print-framing
	// affordance: we don't need 60 Hz updates while the user is dragging,
	// only when they release.
	$effect(() => {
		const map = ctx.map;
		if (!map) return;
		let raf = 0;
		const bump = () => {
			if (raf) return;
			raf = requestAnimationFrame(() => {
				raf = 0;
				mapTick++;
			});
		};
		map.on('moveend', bump);
		map.on('zoomend', bump);
		map.on('resize', bump);
		// Initial tick once the listeners are attached.
		bump();
		return () => {
			if (raf) cancelAnimationFrame(raf);
			map.off('moveend', bump);
			map.off('zoomend', bump);
			map.off('resize', bump);
		};
	});

	// Project the rectangle's four screen-space corners → WGS84 → RD, then
	// take the axis-aligned bbox. Writes back to the shared singleton so
	// `/print` can pick it up.
	//
	// IMPORTANT: do not call `getBoundingClientRect()` here. The map's
	// internal ResizeObserver fires a 'resize' event when a forced layout
	// detects a size change, which calls `bump`, which increments mapTick,
	// which re-runs this effect — an infinite cycle that trips Svelte 5's
	// effect-update-depth heuristic. Our host element is `position: absolute;
	// inset: 0;` inside the same `position: fixed; inset: 0;` wrapper as the
	// map canvas, so host-relative pixel coordinates equal map-relative
	// pixel coordinates and no offset measurement is needed.
	$effect(() => {
		const map = ctx.map;
		if (!map || !rectPx) return;
		void mapTick; // dep on every move/resize

		const { x0, y0, x1, y1 } = rectPx;
		const cornersPx = [
			[x0, y0],
			[x1, y0],
			[x1, y1],
			[x0, y1]
		];
		let minX = Infinity,
			minY = Infinity,
			maxX = -Infinity,
			maxY = -Infinity;
		for (const p of cornersPx) {
			const ll = map.unproject(p);
			const [rx, ry] = wgs84ToRd([ll.lng, ll.lat]);
			if (rx < minX) minX = rx;
			if (ry < minY) minY = ry;
			if (rx > maxX) maxX = rx;
			if (ry > maxY) maxY = ry;
		}
		// Round to whole metres so we don't churn localStorage on sub-px jitter.
		const next = [
			[Math.round(minX), Math.round(minY)],
			[Math.round(maxX), Math.round(maxY)]
		];

		// Snapshot the visible Protomaps place labels inside the rectangle and
		// project them to RD so the print SVG can render the same names. The
		// query is scoped to a pixel-space bounding box matching the frame, and
		// to the `places_*` symbol layers only (no roads / POIs).
		const placeIds = placeLabelLayerIds(map);
		const bbox = [
			[x0, y0],
			[x1, y1]
		];
		/** @type {{text:string, x:number, y:number, kind:string, kindDetail:string, populationRank:number}[]} */
		const labels = [];
		// eslint-disable-next-line svelte/prefer-svelte-reactivity -- transient local Set, never escapes this effect
		const seen = new Set();
		if (placeIds.length) {
			let rawFeatures;
			try {
				rawFeatures = map.queryRenderedFeatures(bbox, { layers: placeIds });
			} catch {
				// Style may still be loading or a layer id may have moved between
				// theme versions — just skip the snapshot this tick.
				rawFeatures = [];
			}
			for (const f of rawFeatures) {
				const props = f.properties ?? {};
				// Per Protomaps v4 schema (docs.protomaps.com/basemaps/layers#places):
				// `name`, `name:<lang>`, `kind`, `kind_detail`, `population_rank`
				// (int, **higher = bigger**), `population`. The `pmap:` prefix used
				// elsewhere does NOT apply here — we tried it and got nothing back.
				const text =
					props['name:nl'] ?? props['name:latin'] ?? props['name'] ?? props['name_en'] ?? null;
				if (!text || typeof text !== 'string') continue;
				const geom = f.geometry;
				if (!geom || geom.type !== 'Point') continue;
				const [lng, lat] = geom.coordinates;
				if (!Number.isFinite(lng) || !Number.isFinite(lat)) continue;
				// Fall back to the layer-id class if the feature doesn't carry `kind`.
				// `places_locality` → 'locality', `places_country` → 'country', etc.
				const kind =
					(typeof props.kind === 'string' && props.kind) ||
					f.layer?.id?.replace(/^places_/, '') ||
					'place';
				const kindDetail = typeof props.kind_detail === 'string' ? props.kind_detail : '';
				const populationRank = Number.isFinite(props.population_rank)
					? Number(props.population_rank)
					: 0;
				const key = `${text}|${kind}`;
				if (seen.has(key)) continue;
				seen.add(key);
				const [rx, ry] = wgs84ToRd([lng, lat]);
				labels.push({
					text,
					x: Math.round(rx),
					y: Math.round(ry),
					kind,
					kindDetail,
					populationRank
				});
			}
			// Sort by class importance first (country > region > locality > subplace),
			// then by population_rank descending (bigger first), then alpha.
			const kindOrder = { country: 0, region: 1, subregion: 2, locality: 3, neighbourhood: 4 };
			labels.sort(
				(a, b) =>
					(kindOrder[a.kind] ?? 9) - (kindOrder[b.kind] ?? 9) ||
					b.populationRank - a.populationRank ||
					a.text.localeCompare(b.text)
			);
		}

		// Read prev via untrack so we don't depend on these fields — otherwise
		// our writes below would re-trigger this same effect.
		untrack(() => {
			const prev = printView.rdExtent;
			if (
				!prev ||
				prev[0][0] !== next[0][0] ||
				prev[0][1] !== next[0][1] ||
				prev[1][0] !== next[1][0] ||
				prev[1][1] !== next[1][1]
			) {
				printView.rdExtent = next;
			}
			if (!labelsEqual(printView.placeLabels, labels)) {
				printView.placeLabels = labels;
			}
		});
	});

	function labelsEqual(a, b) {
		if (a === b) return true;
		if (!a || !b || a.length !== b.length) return false;
		for (let i = 0; i < a.length; i++) {
			const p = a[i],
				q = b[i];
			if (
				p.text !== q.text ||
				p.kind !== q.kind ||
				p.kindDetail !== q.kindDetail ||
				p.populationRank !== q.populationRank ||
				p.x !== q.x ||
				p.y !== q.y
			) {
				return false;
			}
		}
		return true;
	}
</script>

<div class="frame-host" bind:this={host}>
	{#if rectPx}
		<svg
			class="frame-svg"
			viewBox="0 0 {hostSize.w} {hostSize.h}"
			preserveAspectRatio="none"
			aria-hidden="true"
		>
			<!-- Dim the area outside the print rectangle with a single
			     even-odd-filled path: outer rect minus inner rect. -->
			<path
				class="mask"
				d={`M0 0H${hostSize.w}V${hostSize.h}H0Z M${rectPx.x0} ${rectPx.y0}H${rectPx.x1}V${rectPx.y1}H${rectPx.x0}Z`}
				fill-rule="evenodd"
			/>
			<!-- Print rectangle outline and rule-of-thirds guides. -->
			<rect class="frame" x={rectPx.x0} y={rectPx.y0} width={rectPx.w} height={rectPx.h} />
			<g class="thirds">
				<line
					x1={rectPx.x0 + rectPx.w / 3}
					y1={rectPx.y0}
					x2={rectPx.x0 + rectPx.w / 3}
					y2={rectPx.y1}
				/>
				<line
					x1={rectPx.x0 + (2 * rectPx.w) / 3}
					y1={rectPx.y0}
					x2={rectPx.x0 + (2 * rectPx.w) / 3}
					y2={rectPx.y1}
				/>
				<line
					x1={rectPx.x0}
					y1={rectPx.y0 + rectPx.h / 3}
					x2={rectPx.x1}
					y2={rectPx.y0 + rectPx.h / 3}
				/>
				<line
					x1={rectPx.x0}
					y1={rectPx.y0 + (2 * rectPx.h) / 3}
					x2={rectPx.x1}
					y2={rectPx.y0 + (2 * rectPx.h) / 3}
				/>
			</g>
		</svg>
		<div class="badge">A4 {printView.orientation}</div>
	{/if}
</div>

<style>
	.frame-host {
		position: absolute;
		inset: 0;
		pointer-events: none;
		z-index: 5;
	}
	.frame-svg {
		width: 100%;
		height: 100%;
		display: block;
	}
	.mask {
		fill: rgba(15, 23, 42, 0.18);
	}
	.frame {
		fill: none;
		stroke: var(--color-accent, #2563eb);
		stroke-width: 1.5;
		stroke-dasharray: 6 4;
		vector-effect: non-scaling-stroke;
	}
	.thirds line {
		stroke: rgba(255, 255, 255, 0.55);
		stroke-width: 1;
		stroke-dasharray: 2 4;
		vector-effect: non-scaling-stroke;
	}
	.badge {
		position: absolute;
		top: var(--spacing-2, 8px);
		left: 50%;
		transform: translateX(-50%);
		padding: 2px 8px;
		background: var(--color-accent, #2563eb);
		color: var(--color-accent-fg, #fff);
		font-size: var(--text-xs, 11px);
		font-weight: 600;
		letter-spacing: 0.04em;
		text-transform: uppercase;
		border-radius: var(--radius, 4px);
	}
</style>

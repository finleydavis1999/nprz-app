<script>
	import { onMount } from 'svelte';
	import { resolve } from '$app/paths';
	import { dataUrl } from '$lib/data/url.js';
	import PrintMap from '$lib/print/PrintMap.svelte';
	import Legend from '$lib/cartography/Legend.svelte';
	import { downloadSvg, downloadPng, composePrintSheet } from '$lib/print/export.js';
	import { classify } from '$lib/cartography/classify.js';
	import { paletteColors } from '$lib/cartography/palettes.js';
	import { selection } from '$lib/state/selection.svelte.js';
	import { cartography } from '$lib/state/cartography.svelte.js';
	import { manifestState } from '$lib/state/manifest.svelte.js';
	import { displayed } from '$lib/state/layers.svelte.js';
	import { flow, flowCartography } from '$lib/state/flow.svelte.js';
	import { ui } from '$lib/state/ui.svelte.js';
	import { mapLayers } from '$lib/state/map-layers.svelte.js';
	import { geoNames } from '$lib/state/geo-names.svelte.js';
	import { printView } from '$lib/state/print-view.svelte.js';
	import { runFlows } from '$lib/data/flowQuery.js';
	import { scaleLabel } from '$lib/scales.js';

	let title = $state('');
	let mapWrap;

	const manifest = $derived(manifestState.data);
	const dataset = $derived(manifest?.datasets?.[selection.dataset]);
	const geo = $derived(manifest?.geo?.[selection.scale]);
	const overlays = $derived(manifest?.overlays);
	const geoBoundary = $derived(manifest?.geo?.[mapLayers.boundaryScale]);
	const yearLabel = $derived(
		dataset?.fields?.year?.values?.find((y) => y.id === selection.year)?.label ?? selection.year
	);

	// Mirrors `/`: keep finite values regardless of sign so model residuals and
	// other non-positive analytical layers render in print previews too. Same
	// auto-diverging detection as the live page — keeps the print map visually
	// consistent with what the user previewed before opening this route.
	const sortedValues = $derived([...displayed.data.values()].filter((v) => Number.isFinite(v)));
	const hasBothSigns = $derived.by(() => {
		if (sortedValues.length === 0) return false;
		let neg = false;
		let pos = false;
		for (const v of sortedValues) {
			if (v < 0) neg = true;
			else if (v > 0) pos = true;
			if (neg && pos) return true;
		}
		return false;
	});
	const useDiverging = $derived(hasBothSigns && !cartography.forceSequential);
	const breaks = $derived.by(() => {
		if (sortedValues.length === 0) return null;
		return useDiverging
			? classify(sortedValues, {
					method: 'diverging',
					n: cartography.n,
					pivot: 0,
					subMethod: cartography.method
				})
			: classify(sortedValues, { method: cartography.method, n: cartography.n });
	});
	const colors = $derived(
		breaks
			? paletteColors(
					useDiverging ? cartography.divergingPalette : cartography.palette,
					cartography.n,
					{
						kind: useDiverging ? 'diverging' : 'sequential'
					}
				)
			: []
	);

	// --- Flow side mirrors `/` route. Re-runs whenever flow state changes.
	let flowResult = $state(
		/** @type {{flows:{o:string,d:string,value:number,count?:number}[], min:number, max:number, weighted?:boolean} | null} */ (
			null
		)
	);
	let centroids = $state(/** @type {Record<string, [number,number]> | null} */ (null));

	onMount(() => {
		ui.load();
		manifestState.ensureLoaded();
	});

	$effect(() => {
		if (!manifest) return;
		geoNames.ensureLoaded(selection.scale);
		if (flow.enabled && flow.scale !== selection.scale) geoNames.ensureLoaded(flow.scale);
	});

	// Load RD-projected centroids when flows are enabled. The print map's
	// projection is `geoIdentity` over RD topojson — passing WGS84 lng/lat
	// would drop the lines outside the fitted bbox.
	$effect(() => {
		if (!manifest || !flow.enabled || centroids) return;
		const path = manifest?.geo?.[flow.scale]?.centroidsRd;
		const version = manifest?.version;
		if (!path || !version) return;
		fetch(dataUrl(path, version))
			.then((r) => (r.ok ? r.json() : null))
			.then((json) => {
				centroids = json;
			})
			.catch(() => {
				centroids = null;
			});
	});

	$effect(() => {
		if (!manifest || !flow.enabled) {
			flowResult = null;
			return;
		}
		const args = {
			dataset: flow.dataset,
			scale: flow.scale,
			yearMin: flow.yearMin,
			yearMax: flow.yearMax,
			ageMin: flow.ageMin,
			ageMax: flow.ageMax,
			filters: flow.filters,
			includeSelfLoops: flow.includeSelfLoops
		};
		runFlows(args)
			.then((res) => {
				flowResult = res;
			})
			.catch(() => {
				flowResult = null;
			});
	});

	const filteredFlows = $derived(
		flowResult
			? flowResult.flows.filter(
					(f) =>
						Math.abs(f.value) >= flow.minWeight && (f.count == null || f.count >= flow.minCount)
				)
			: []
	);

	const flowValues = $derived(filteredFlows.map((f) => f.value).filter((v) => Number.isFinite(v)));
	// Same auto-diverging detection as the live page — keeps the print map
	// visually consistent with what the user saw before opening this route.
	const flowHasBothSigns = $derived.by(() => {
		if (flowValues.length === 0) return false;
		let neg = false;
		let pos = false;
		for (const v of flowValues) {
			if (v < 0) neg = true;
			else if (v > 0) pos = true;
			if (neg && pos) return true;
		}
		return false;
	});
	const flowUseDiverging = $derived(flowHasBothSigns && !flowCartography.forceSequential);
	const flowBreaks = $derived.by(() => {
		if (flowValues.length === 0) return null;
		return flowUseDiverging
			? classify(flowValues, {
					method: 'diverging',
					n: flowCartography.n,
					pivot: 0,
					subMethod: flowCartography.method
				})
			: classify(flowValues, { method: flowCartography.method, n: flowCartography.n });
	});
	const flowColors = $derived(
		flowBreaks
			? paletteColors(
					flowUseDiverging ? flowCartography.divergingPalette : flowCartography.palette,
					flowCartography.n,
					{ kind: flowUseDiverging ? 'diverging' : 'sequential' }
				)
			: []
	);

	// Name lookup for pie labels + optional label layer.
	const nameByCode = $derived(geoNames.byScale.get(selection.scale) ?? null);

	const defaultTitle = $derived.by(() => {
		const active = displayed.activeLayer;
		if (active) return `${active.name} — ${scaleLabel(selection.scale)}`;
		return dataset && geo ? `${dataset.name} — ${scaleLabel(selection.scale)} — ${yearLabel}` : '';
	});
	const effectiveTitle = $derived(title.trim() || defaultTitle);

	// SVG viewport dimensions. A4 portrait usable area is ~186×273mm with our
	// 12mm @page margin; subtract title/legend/footer (~50mm) for the map
	// area. We render at 96dpi (1mm ≈ 3.78px) and let the SVG viewBox handle
	// any final scaling.
	const mapDims = $derived(
		printView.orientation === 'landscape' ? { w: 1000, h: 600 } : { w: 700, h: 900 }
	);

	function setOrientation(o) {
		printView.orientation = o;
	}

	// --- Label controls (toolbar popover). The popover trigger lives in the
	// toolbar; the popover itself is a separate <div popover> sibling.

	let labelFilter = $state('');

	const disabledSet = $derived(new Set(printView.disabledLabels));

	// Group + sort captured labels by `kind`, applying the search filter.
	const groupedLabels = $derived.by(() => {
		const q = labelFilter.trim().toLowerCase();
		/** @type {Map<string, {text:string, kind:string, kindDetail:string, populationRank:number, key:string}[]>} */
		// eslint-disable-next-line svelte/prefer-svelte-reactivity -- local accumulator, never escapes
		const groups = new Map();
		for (const l of printView.placeLabels ?? []) {
			if (q && !l.text.toLowerCase().includes(q)) continue;
			const arr = groups.get(l.kind) ?? [];
			arr.push({
				text: l.text,
				kind: l.kind,
				kindDetail: l.kindDetail ?? '',
				populationRank: l.populationRank ?? 0,
				key: `${l.text}|${l.kind}`
			});
			groups.set(l.kind, arr);
		}
		// Bigger first, then alpha.
		for (const arr of groups.values()) {
			arr.sort((a, b) => b.populationRank - a.populationRank || a.text.localeCompare(b.text));
		}
		const order = ['country', 'region', 'subregion', 'locality', 'neighbourhood', 'subplace'];
		return [...groups.entries()].sort(
			([a], [b]) =>
				(order.indexOf(a) === -1 ? 99 : order.indexOf(a)) -
				(order.indexOf(b) === -1 ? 99 : order.indexOf(b))
		);
	});

	// Visible count for the toolbar trigger.
	const visibleLabelCount = $derived(
		(printView.placeLabels ?? []).filter(
			(l) =>
				!disabledSet.has(`${l.text}|${l.kind}`) &&
				(l.populationRank ?? 0) >= printView.minPopulationRank
		).length
	);
	const totalLabelCount = $derived(printView.placeLabels?.length ?? 0);

	function toggleLabel(key) {
		// eslint-disable-next-line svelte/prefer-svelte-reactivity -- transient dedupe set, written back as array
		const next = new Set(printView.disabledLabels);
		if (next.has(key)) next.delete(key);
		else next.add(key);
		printView.disabledLabels = [...next];
	}

	function setKindEnabled(kind, enabled) {
		// eslint-disable-next-line svelte/prefer-svelte-reactivity -- transient dedupe set, written back as array
		const next = new Set(printView.disabledLabels);
		for (const l of printView.placeLabels ?? []) {
			if (l.kind !== kind) continue;
			const key = `${l.text}|${l.kind}`;
			if (enabled) next.delete(key);
			else next.add(key);
		}
		printView.disabledLabels = [...next];
	}

	function resetLabels() {
		printView.disabledLabels = [];
		printView.minPopulationRank = 0;
		labelFilter = '';
	}

	// Native `popovertargetaction="hide"` doesn't reliably trigger when the
	// button lives inside the same popover it targets — invoking the API
	// imperatively is robust across browsers.
	function closeLabelsPopover() {
		/** @type {HTMLElement | null} */
		const el = typeof document !== 'undefined' ? document.getElementById('labels-popover') : null;
		// `hidePopover()` exists on any element with a `popover` attribute.
		if (el && typeof (/** @type {any} */ (el).hidePopover) === 'function') {
			/** @type {any} */ (el).hidePopover();
		}
	}

	// Population-rank slider: Protomaps `population_rank` is higher = bigger.
	// The slider ranges 0..maxRank so dragging right keeps fewer, more
	// important labels. At 0 there's no threshold.
	const maxRankFound = $derived(
		Math.max(0, ...(printView.placeLabels ?? []).map((l) => l.populationRank ?? 0))
	);
	function onSliderInput(e) {
		printView.minPopulationRank = Number(e.currentTarget.value);
	}

	function safeFilename() {
		return effectiveTitle.replace(/[^\w-]+/g, '-').replace(/^-+|-+$/g, '') || 'map';
	}

	// Build the export SVG from the same data the preview uses so the
	// downloaded file mirrors the on-screen sheet — title, framed map,
	// legend, source footer.
	function buildExportSvg() {
		const mapSvgEl = mapWrap?.querySelector('svg');
		if (!mapSvgEl || !breaks) return null;
		return composePrintSheet({
			titleText: effectiveTitle,
			mapSvgEl,
			legend: { breaks, colors },
			footnoteText: 'Source: CBS microdata. Cells < 10 suppressed for privacy.',
			mapWidth: mapDims.w,
			mapHeight: mapDims.h
		});
	}

	function onDownload() {
		const svg = buildExportSvg();
		if (!svg) return;
		downloadSvg(svg, `${safeFilename()}.svg`);
	}

	let exporting = $state(false);
	async function onDownloadPng() {
		if (exporting) return;
		const svg = buildExportSvg();
		if (!svg) return;
		exporting = true;
		try {
			await downloadPng(svg, `${safeFilename()}.png`, { dpi: 300 });
		} finally {
			exporting = false;
		}
	}

	function onPrint() {
		window.print();
	}

	// `@page` can't live in a scoped <style>; we inject it via a real <style>
	// element in <head> and update its textContent reactively. Safer than
	// {@html} (no XSS surface even though our value is a closed enum).
	$effect(() => {
		if (typeof document === 'undefined') return;
		let el = document.getElementById('print-page-rule');
		if (!el) {
			el = document.createElement('style');
			el.id = 'print-page-rule';
			document.head.appendChild(el);
		}
		el.textContent = `@page { size: A4 ${printView.orientation}; margin: 0; }`;
		return () => {
			el?.remove();
		};
	});
</script>

<div class="page">
	<header class="toolbar">
		<a class="back" href={resolve('/')}>← Back to map</a>
		<input class="title-input" type="text" placeholder={defaultTitle} bind:value={title} />
		<div class="orient" role="group" aria-label="Page orientation">
			<button
				type="button"
				class="seg"
				class:active={printView.orientation === 'portrait'}
				onclick={() => setOrientation('portrait')}
				title="Portrait A4"
				aria-pressed={printView.orientation === 'portrait'}
			>
				<span class="seg-glyph">▯</span>
				<span class="seg-label">Portrait</span>
			</button>
			<button
				type="button"
				class="seg"
				class:active={printView.orientation === 'landscape'}
				onclick={() => setOrientation('landscape')}
				title="Landscape A4"
				aria-pressed={printView.orientation === 'landscape'}
			>
				<span class="seg-glyph">▭</span>
				<span class="seg-label">Landscape</span>
			</button>
		</div>
		<div class="grow"></div>
		<button
			type="button"
			class="ghost"
			popovertarget="labels-popover"
			disabled={totalLabelCount === 0}
			title={totalLabelCount === 0
				? 'No labels captured yet — enable Frame on the live map and pan to a city.'
				: 'Choose which place labels to show'}
		>
			Labels
			<span class="ghost-count">{visibleLabelCount}/{totalLabelCount}</span>
		</button>
		<button type="button" onclick={onDownload}>Download SVG</button>
		<button type="button" onclick={onDownloadPng} disabled={exporting}>
			{exporting ? 'Rendering…' : 'Download PNG'}
		</button>
		<button type="button" onclick={onPrint}>Print / PDF</button>
	</header>

	<article class="sheet" class:landscape={printView.orientation === 'landscape'}>
		<h1 class="title">{effectiveTitle}</h1>
		<div class="map" bind:this={mapWrap}>
			{#if geo && breaks}
				<PrintMap
					topojsonUrl={dataUrl(geo.topojson, manifest.version)}
					valueByArea={displayed.data}
					{breaks}
					{colors}
					idProp={geo.idProp}
					width={mapDims.w}
					height={mapDims.h}
					extent={printView.rdExtent}
					placeLabels={printView.placeLabels}
					disabledLabelKeys={printView.disabledLabels}
					minPopulationRank={printView.minPopulationRank}
					flows={flow.enabled ? filteredFlows : []}
					centroids={centroids ?? {}}
					{flowBreaks}
					{flowColors}
					widthMin={flowCartography.widthMin}
					widthMax={flowCartography.widthMax}
					opacity={flowCartography.opacity}
					curvature={flowCartography.curvature}
					selectedFlowNode={ui.selectedFlowNode}
					flowMode={ui.flowMode}
					{nameByCode}
					labelLayer={ui.showLabels}
					boundaryUrl={mapLayers.boundary && geoBoundary?.topojson
						? dataUrl(geoBoundary.topojson, manifest.version)
						: null}
					boundaryColor={mapLayers.boundaryColor}
					boundaryWidth={mapLayers.boundaryWidth}
					boundaryOpacity={mapLayers.boundaryOpacity}
					builtupUrl={mapLayers.builtup && overlays?.builtup?.topojson
						? dataUrl(overlays.builtup.topojson, manifest.version)
						: null}
					builtupColor={mapLayers.builtupColor}
					builtupOpacity={mapLayers.builtupOpacity}
					provinceUrl={mapLayers.provinces && overlays?.provinces?.topojson
						? dataUrl(overlays.provinces.topojson, manifest.version)
						: null}
					provinceColor={mapLayers.provinceColor}
					provinceWidth={mapLayers.provinceWidth}
				/>
			{:else if !manifest || displayed.loading}
				<p class="hint">Loading…</p>
			{:else}
				<p class="hint">No data.</p>
			{/if}
		</div>
		{#if breaks}
			<div class="legend-wrap">
				<Legend {breaks} {colors} />
			</div>
		{/if}
		<footer class="footnote">
			<span>Source: CBS microdata. Cells &lt; 10 suppressed for privacy.</span>
		</footer>
	</article>
</div>

<!-- Labels popover. Native HTML popover API — browser handles outside-click
     and ESC-to-close automatically. -->
<div id="labels-popover" popover="auto" class="labels-popover" aria-label="Place labels">
	<div class="lp-head">
		<strong>Place labels</strong>
		<span class="lp-count">{visibleLabelCount} of {totalLabelCount} visible</span>
		<button type="button" class="lp-close" onclick={closeLabelsPopover} title="Close">×</button>
	</div>
	{#if totalLabelCount === 0}
		<p class="lp-hint">
			No labels captured yet. Open the map, toggle <em>Frame</em>, and pan/zoom to a city to
			snapshot the visible place names.
		</p>
	{:else}
		<div class="lp-controls">
			<input
				type="search"
				class="lp-search"
				placeholder="Filter labels…"
				bind:value={labelFilter}
				autocomplete="off"
			/>
			{#if maxRankFound > 0}
				<label class="lp-slider">
					<span class="lp-slider-label">
						Min&nbsp;population&nbsp;rank
						<strong>{printView.minPopulationRank}</strong>
					</span>
					<input
						type="range"
						min="0"
						max={maxRankFound}
						step="1"
						value={printView.minPopulationRank}
						oninput={onSliderInput}
					/>
				</label>
			{/if}
		</div>
		<div class="lp-groups">
			{#each groupedLabels as [kind, items] (kind)}
				{@const visibleCount = items.filter((it) => !disabledSet.has(it.key)).length}
				<details class="lp-group" open={kind === 'country' || kind === 'region'}>
					<summary>
						<span class="lp-kind">{kind}</span>
						<span class="lp-group-count">{visibleCount}/{items.length}</span>
						<button
							type="button"
							class="lp-group-btn"
							onclick={(e) => {
								e.preventDefault();
								setKindEnabled(kind, visibleCount < items.length);
							}}
						>
							{visibleCount < items.length ? 'show all' : 'hide all'}
						</button>
					</summary>
					<ul class="lp-list">
						{#each items as it (it.key)}
							<li>
								<label class="lp-row">
									<input
										type="checkbox"
										checked={!disabledSet.has(it.key)}
										onchange={() => toggleLabel(it.key)}
									/>
									<span class="lp-name">{it.text}</span>
									{#if it.kindDetail}<span class="lp-detail">{it.kindDetail}</span>{/if}
									{#if it.populationRank > 0}
										<span class="lp-rank" title="Population rank">{it.populationRank}</span>
									{/if}
								</label>
							</li>
						{/each}
					</ul>
				</details>
			{/each}
			{#if groupedLabels.length === 0}
				<p class="lp-hint">No labels match &ldquo;{labelFilter}&rdquo;.</p>
			{/if}
		</div>
		<div class="lp-foot">
			<button type="button" class="lp-reset" onclick={resetLabels}>Reset</button>
		</div>
	{/if}
</div>

<style>
	.page {
		min-height: 100vh;
		background: #f5f5f5;
		padding: var(--spacing-4);
		display: flex;
		flex-direction: column;
		gap: var(--spacing-4);
		align-items: center;
	}
	.toolbar {
		width: 100%;
		max-width: 800px;
		display: flex;
		gap: var(--spacing-2);
		align-items: center;
	}
	.back {
		color: var(--color-muted);
		text-decoration: none;
		font-size: var(--text-sm);
	}
	.back:hover {
		color: var(--color-text);
	}
	.title-input {
		flex: 1;
		max-width: 360px;
		padding: 4px 8px;
		border: 1px solid var(--color-line);
		border-radius: var(--radius);
		font-size: var(--text-sm);
	}
	.grow {
		flex: 1;
	}
	button {
		padding: 4px 10px;
		background: var(--color-accent);
		color: var(--color-accent-fg);
		border: none;
		border-radius: var(--radius);
		font-size: var(--text-sm);
		cursor: pointer;
	}
	.sheet {
		width: 100%;
		max-width: 800px;
		background: #fff;
		padding: var(--spacing-4);
		border: 1px solid var(--color-line);
		border-radius: var(--radius);
		display: flex;
		flex-direction: column;
		gap: var(--spacing-3);
	}
	.sheet.landscape {
		max-width: 1100px;
	}
	/* Ghost-button variant for the labels-popover trigger — lower visual
	 * weight than the primary download/print buttons next to it. */
	button.ghost {
		background: transparent;
		color: var(--color-text);
		border: 1px solid var(--color-line);
		display: inline-flex;
		align-items: center;
		gap: 6px;
	}
	button.ghost:hover {
		background: rgba(31, 35, 40, 0.06);
	}
	button.ghost:disabled {
		opacity: 0.5;
		cursor: not-allowed;
	}
	.ghost-count {
		color: var(--color-muted);
		font-size: var(--text-xs);
		font-variant-numeric: tabular-nums;
	}

	/* --- Labels popover (native [popover] element) --- */
	.labels-popover {
		width: 320px;
		max-height: 70vh;
		padding: var(--spacing-3);
		border: 1px solid var(--color-line);
		border-radius: var(--radius);
		background: var(--color-bg-panel, #fff);
		color: var(--color-text);
		font-size: var(--text-sm);
		box-shadow: 0 12px 32px rgba(0, 0, 0, 0.18);
		/* The browser auto-positions popovers; align it under the trigger
		 * by setting position relative to the viewport via inset. We use
		 * `position-area` when available; otherwise default centring works. */
		margin: 0;
		overflow: hidden;
		display: flex;
		flex-direction: column;
	}
	.labels-popover::backdrop {
		background: rgba(0, 0, 0, 0.04);
	}
	.lp-head {
		display: flex;
		align-items: baseline;
		gap: var(--spacing-2);
		padding-bottom: var(--spacing-2);
		border-bottom: 1px solid var(--color-line);
	}
	.lp-count {
		color: var(--color-muted);
		font-size: var(--text-xs);
		font-variant-numeric: tabular-nums;
		margin-left: auto;
	}
	.lp-close {
		background: transparent;
		color: var(--color-muted);
		border: none;
		padding: 0 6px;
		font-size: 18px;
		line-height: 1;
		cursor: pointer;
		border-radius: var(--radius);
	}
	.lp-close:hover {
		background: rgba(31, 35, 40, 0.06);
		color: var(--color-text);
	}
	.lp-hint {
		color: var(--color-hint);
		font-size: var(--text-xs);
		line-height: 1.4;
		margin: var(--spacing-2) 0 0;
	}
	.lp-controls {
		display: flex;
		flex-direction: column;
		gap: var(--spacing-2);
		padding: var(--spacing-2) 0;
	}
	.lp-search {
		width: 100%;
		padding: 4px 8px;
		border: 1px solid var(--color-line);
		border-radius: var(--radius);
		font-size: var(--text-sm);
	}
	.lp-slider {
		display: flex;
		flex-direction: column;
		gap: 4px;
	}
	.lp-slider-label {
		font-size: var(--text-xs);
		color: var(--color-muted);
	}
	.lp-slider input[type='range'] {
		width: 100%;
	}
	.lp-groups {
		flex: 1;
		overflow-y: auto;
		display: flex;
		flex-direction: column;
		gap: 2px;
		margin: 0 calc(var(--spacing-3) * -1);
		padding: 0 var(--spacing-3);
	}
	.lp-group > summary {
		list-style: none;
		display: flex;
		align-items: center;
		gap: var(--spacing-2);
		padding: 4px 6px;
		cursor: pointer;
		border-radius: var(--radius);
		font-size: var(--text-xs);
		user-select: none;
	}
	.lp-group > summary::-webkit-details-marker {
		display: none;
	}
	.lp-group > summary::before {
		content: '▸';
		font-size: 10px;
		color: var(--color-hint);
		transition: transform 0.12s ease;
	}
	.lp-group[open] > summary::before {
		transform: rotate(90deg);
	}
	.lp-group > summary:hover {
		background: rgba(31, 35, 40, 0.05);
	}
	.lp-kind {
		text-transform: uppercase;
		letter-spacing: 0.06em;
		color: var(--color-muted);
		font-weight: 600;
	}
	.lp-group-count {
		color: var(--color-hint);
		font-variant-numeric: tabular-nums;
	}
	.lp-group-btn {
		margin-left: auto;
		padding: 0 6px;
		background: transparent;
		color: var(--color-muted);
		border: 1px solid var(--color-line);
		border-radius: var(--radius);
		font-size: var(--text-xs);
		cursor: pointer;
	}
	.lp-group-btn:hover {
		background: rgba(31, 35, 40, 0.06);
		color: var(--color-text);
	}
	.lp-list {
		list-style: none;
		margin: 0 0 var(--spacing-2) 16px;
		padding: 0;
		display: flex;
		flex-direction: column;
		gap: 1px;
	}
	.lp-row {
		display: flex;
		align-items: center;
		gap: var(--spacing-2);
		padding: 3px 6px;
		border-radius: var(--radius);
		cursor: pointer;
		font-size: var(--text-sm);
	}
	.lp-row:hover {
		background: rgba(31, 35, 40, 0.05);
	}
	.lp-name {
		flex: 1;
		white-space: nowrap;
		overflow: hidden;
		text-overflow: ellipsis;
	}
	.lp-detail {
		color: var(--color-muted);
		font-size: var(--text-xs);
		font-style: italic;
	}
	.lp-rank {
		font-size: var(--text-xs);
		color: var(--color-hint);
		font-variant-numeric: tabular-nums;
		min-width: 1.5em;
		text-align: right;
	}
	.lp-foot {
		padding-top: var(--spacing-2);
		border-top: 1px solid var(--color-line);
		display: flex;
		justify-content: flex-end;
	}
	.lp-reset {
		padding: 4px 10px;
		background: transparent;
		color: var(--color-muted);
		border: 1px solid var(--color-line);
		border-radius: var(--radius);
		font-size: var(--text-xs);
		cursor: pointer;
	}
	.lp-reset:hover {
		background: rgba(31, 35, 40, 0.06);
		color: var(--color-text);
	}
	.orient {
		display: inline-flex;
		border: 1px solid var(--color-line);
		border-radius: var(--radius);
		overflow: hidden;
	}
	.seg {
		padding: 4px var(--spacing-2);
		background: transparent;
		color: var(--color-text);
		border: none;
		border-radius: 0;
		font-size: var(--text-sm);
		cursor: pointer;
		display: inline-flex;
		align-items: center;
		gap: 6px;
	}
	.seg + .seg {
		border-left: 1px solid var(--color-line);
	}
	.seg:hover {
		background: rgba(31, 35, 40, 0.06);
	}
	.seg.active {
		background: var(--color-accent);
		color: var(--color-accent-fg);
	}
	.seg-glyph {
		font-family: ui-monospace, monospace;
	}
	.title {
		font-size: 18px;
		font-weight: 600;
		margin: 0;
		color: var(--color-text);
	}
	.map {
		width: 100%;
	}
	.legend-wrap {
		display: flex;
		justify-content: flex-start;
	}
	.footnote {
		display: flex;
		justify-content: space-between;
		gap: var(--spacing-3);
		font-size: var(--text-xs);
		color: var(--color-hint);
		border-top: 1px solid var(--color-line);
		padding-top: var(--spacing-2);
	}
	.hint {
		color: var(--color-hint);
		font-size: var(--text-sm);
		text-align: center;
		padding: var(--spacing-4);
	}

	@media print {
		.page {
			background: #fff;
			padding: 0;
		}
		.toolbar,
		.labels-popover {
			display: none;
		}
		.sheet {
			border: none;
			max-width: none;
			padding: 12mm;
		}
	}
</style>

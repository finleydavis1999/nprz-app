<script>
	import { onMount } from 'svelte';
	import { PUBLIC_PROTOMAPS_API_KEY } from '$env/static/public';
	import { dataUrl } from '$lib/data/url.js';
	import MapView from '$lib/map/Map.svelte';
	import { MAP_DEFAULTS } from '$lib/map/defaults.js';
	import ChoroplethLayer from '$lib/map/ChoroplethLayer.svelte';
	import BoundaryLayer from '$lib/map/BoundaryLayer.svelte';
	import BuiltUpLayer from '$lib/map/BuiltUpLayer.svelte';
	import ProvinceLayer from '$lib/map/ProvinceLayer.svelte';
	import BasemapLabelsLayer from '$lib/map/BasemapLabelsLayer.svelte';
	import LassoTool from '$lib/map/LassoTool.svelte';
	import FlowLayer from '$lib/map/FlowLayer.svelte';
	import Panel from '$lib/ui/Panel.svelte';
	import Field from '$lib/ui/Field.svelte';
	import Toggle from '$lib/ui/Toggle.svelte';
	import LogRangeFilter from '$lib/ui/LogRangeFilter.svelte';
	import ScaleToggle from '$lib/ui/ScaleToggle.svelte';
	import DatasetPicker from '$lib/ui/DatasetPicker.svelte';
	import YearPicker from '$lib/ui/YearPicker.svelte';
	import VariablePicker from '$lib/ui/VariablePicker.svelte';
	import CategoryFilters from '$lib/ui/CategoryFilters.svelte';
	import SaveLayerInput from '$lib/ui/SaveLayerInput.svelte';
	import SaveFlowLayerInput from '$lib/ui/SaveFlowLayerInput.svelte';
	import MapLayerControls from '$lib/ui/MapLayerControls.svelte';
	import StudyAreaControls from '$lib/ui/StudyAreaControls.svelte';
	import ClassificationControls from '$lib/ui/ClassificationControls.svelte';
	import LayerCalculator from '$lib/ui/LayerCalculator.svelte';
	import FloatingDock from '$lib/ui/FloatingDock.svelte';
	import DockToggleStrip from '$lib/ui/DockToggleStrip.svelte';
	import InspectPanel from '$lib/ui/InspectPanel.svelte';
	import InspectInteraction from '$lib/map/InspectInteraction.svelte';
	import NodeNamesLayer from '$lib/map/NodeNamesLayer.svelte';
	import FlowPies from '$lib/map/FlowPies.svelte';
	import Legend from '$lib/cartography/Legend.svelte';
	import { runFlows } from '$lib/data/flowQuery.js';
	import { schedulePrefetch } from '$lib/data/prefetch.js';
	import { classify } from '$lib/cartography/classify.js';
	import { paletteColors } from '$lib/cartography/palettes.js';
	import { stepExpression } from '$lib/cartography/expression.js';
	import { selection } from '$lib/state/selection.svelte.js';
	import { cartography } from '$lib/state/cartography.svelte.js';
	import { mapLayers } from '$lib/state/map-layers.svelte.js';
	import { manifestState } from '$lib/state/manifest.svelte.js';
	import { queryResult } from '$lib/state/query-result.svelte.js';
	import { studyArea } from '$lib/state/study-area.svelte.js';
	import { displayed, layers } from '$lib/state/layers.svelte.js';
	import { flow, flowCartography } from '$lib/state/flow.svelte.js';
	import { ui } from '$lib/state/ui.svelte.js';
	import { geoNames } from '$lib/state/geo-names.svelte.js';
	import { scaleLabel, scaleUnit } from '$lib/scales.js';

	let { data } = $props();
	let lassoActive = $state(false);

	const manifest = $derived(manifestState.data);

	// Preload area_code → name lookups for the current scale (and the flow
	// scale if different — used to label flow endpoints).
	$effect(() => {
		if (!manifest) return;
		geoNames.ensureLoaded(selection.scale);
		if (flow.enabled && flow.scale !== selection.scale) {
			geoNames.ensureLoaded(flow.scale);
		}
	});

	// Pin flow.scale to the active node scale. If the chosen flow dataset
	// doesn't have data at that scale (OViN is gem-only), the query is
	// short-circuited below and the flow layer simply doesn't render —
	// switching scales reliably resets the displayed flows.
	$effect(() => {
		if (flow.scale !== selection.scale) flow.scale = selection.scale;
	});

	// Whether the current flow dataset has data at the active scale.
	const flowScaleAvailable = $derived(!!manifest?.flows?.[flow.dataset]?.scales?.[selection.scale]);

	// Centroids cache keyed by scale so flow-scale switches don't re-fetch
	// repeatedly. Populated lazily on first use of each scale.
	let centroidsByScale = $state(
		/** @type {Record<string, Record<string, [number,number]>>} */ ({})
	);
	const centroids = $derived(centroidsByScale[flow.scale] ?? null);
	let flowResult = $state(
		/** @type {{flows:{o:string,d:string,value:number,count?:number}[], min:number, max:number, weighted?:boolean} | null} */ (
			null
		)
	);
	let flowQuerying = $state(false);
	let flowError = $state(/** @type {string | null} */ (null));
	// Auto-set minWeight to a high percentile on the first non-empty flow query,
	// and re-anchor whenever the user switches dataset/scale ("layer switch") —
	// otherwise a stale threshold can leave nearly every flow visible on the new
	// layer (or, when it falls above the new max, drop to zero and show all).
	// Within the same layer, the user's slider position is preserved; only
	// out-of-range values get re-anchored.
	let prevFlowLayerSignature = '';
	const FLOW_DEFAULT_TOP_FRACTION = 0.1;
	/**
	 * @param {{value:number}[]} flows
	 * @param {number} topFraction
	 */
	function flowPercentileThreshold(flows, topFraction) {
		if (flows.length === 0) return 0;
		const sorted = flows.map((f) => f.value).sort((a, b) => a - b);
		const idx = Math.min(Math.floor(sorted.length * (1 - topFraction)), sorted.length - 1);
		return sorted[idx] ?? 0;
	}

	onMount(() => {
		ui.load();
		studyArea.init();
		schedulePrefetch();
	});

	// Hard reset: snap every state singleton back to its declared defaults and
	// recenter the map. Saved layers are left intact — the calculator dock has
	// its own delete-all affordance — but the active selection is dropped.
	function resetAll() {
		selection.reset();
		flow.reset();
		cartography.reset();
		flowCartography.reset();
		mapLayers.reset();
		studyArea.clear();
		layers.setActive(null);
		ui.selected = null;
		ui.hovered = null;
		ui.selectedFlowNode = null;
		ui.flowMode = 'unified';
		ui.showLabels = false;
		const map = /** @type {any} */ (globalThis).__map;
		map?.jumpTo?.(MAP_DEFAULTS);
	}

	$effect(() => {
		studyArea.bindToScale(selection.scale);
	});

	// Load centroids for the current flow scale (used by FlowLayer for OD
	// curves and FlowPies for symbol placement).
	$effect(() => {
		const scale = flow.scale;
		const path = manifest?.geo?.[scale]?.centroids;
		const version = manifest?.version;
		if (!path || !version || centroidsByScale[scale]) return;
		fetch(dataUrl(path, version))
			.then((r) => {
				if (!r.ok) throw new Error(`HTTP ${r.status}`);
				return r.json();
			})
			.then((json) => {
				centroidsByScale = { ...centroidsByScale, [scale]: json };
			})
			.catch((e) => {
				flowError = `centroids: ${e.message}`;
			});
	});

	// Re-run flow query whenever flow selection changes (only while enabled).
	// Note: flow.minWeight is a client-side filter (see filteredFlows below).
	$effect(() => {
		if (!manifest || !flow.enabled || !flowScaleAvailable) {
			flowResult = null;
			return;
		}
		const args = {
			dataset: flow.dataset,
			scale: flow.scale,
			yearMin: flow.yearMin,
			yearMax: flow.yearMax,
			filters: flow.filters,
			includeSelfLoops: flow.includeSelfLoops
		};
		flowQuerying = true;
		flowError = null;
		const layerSignature = `${flow.dataset}|${flow.scale}`;
		const layerChanged = layerSignature !== prevFlowLayerSignature;
		prevFlowLayerSignature = layerSignature;
		runFlows(args)
			.then((res) => {
				flowResult = res;
				// A stale min-count cutoff is meaningless on a non-weighted layer —
				// clear it on layer switch so it can't silently hide flows.
				if (layerChanged && !res.weighted) flow.minCount = 0;
				if (res.flows.length === 0) {
					// Nothing to anchor against; leave threshold for next non-empty result.
				} else if (layerChanged || flow.minWeight > res.max) {
					// Layer switch, or user's saved threshold is out of range for the
					// new result — re-anchor at the percentile instead of dropping
					// to zero (which would render every flow).
					flow.minWeight = flowPercentileThreshold(res.flows, FLOW_DEFAULT_TOP_FRACTION);
				}
			})
			.catch((e) => {
				flowError = `flow query: ${e.message}`;
				flowResult = null;
			})
			.finally(() => {
				flowQuerying = false;
			});
	});

	const status = $derived.by(() => {
		if (displayed.error) return displayed.error;
		if (manifestState.error) return manifestState.error;
		if (displayed.loading || manifestState.loading) return 'querying…';
		const unit = scaleUnit(selection.scale);
		const active = displayed.activeLayer;
		const prefix = active ? `${active.name}: ` : '';
		return `${prefix}${displayed.data.size.toLocaleString()} ${unit}`;
	});

	const filteredFlows = $derived(
		flowResult
			? flowResult.flows.filter(
					(f) => f.value >= flow.minWeight && (f.count == null || f.count >= flow.minCount)
				)
			: []
	);

	const flowStatus = $derived.by(() => {
		if (flowError) return flowError;
		if (flowQuerying) return 'querying…';
		if (!flowResult) return null;
		const total = flowResult.flows.length;
		const shown = filteredFlows.length;
		return shown === total
			? `${total.toLocaleString()} flows`
			: `${shown.toLocaleString()} / ${total.toLocaleString()} flows`;
	});

	const sortedValues = $derived(
		[...displayed.data.values()].filter((v) => Number.isFinite(v) && v > 0)
	);

	const breaks = $derived.by(() => {
		if (sortedValues.length === 0) return null;
		return classify(sortedValues, { method: cartography.method, n: cartography.n });
	});

	const colors = $derived(breaks ? paletteColors(cartography.palette, cartography.n) : []);
	const fillColor = $derived(breaks ? stepExpression({ breaks, colors }) : '#eee');

	const flowValues = $derived(
		filteredFlows.map((f) => f.value).filter((v) => Number.isFinite(v) && v > 0)
	);

	const flowBreaks = $derived.by(() => {
		if (flowValues.length === 0) return null;
		return classify(flowValues, { method: flowCartography.method, n: flowCartography.n });
	});

	const flowColors = $derived(
		flowBreaks ? paletteColors(flowCartography.palette, flowCartography.n) : []
	);

	// Map keyed by `${o}|${d}` for fast lookup in the inspect panel.
	const flowsByPair = $derived.by(() => {
		/** @type {Map<string, {value:number, count:number|null}>} */
		// eslint-disable-next-line svelte/prefer-svelte-reactivity -- local lookup table, not directly mutated post-derivation
		const m = new Map();
		for (const f of filteredFlows)
			m.set(`${f.o}|${f.d}`, { value: f.value, count: f.count ?? null });
		return m;
	});

	// Flow-filter slider bounds — driven by the current full result so the
	// sliders don't jump as the user drags them.
	const flowMinValue = $derived(flowResult?.min ?? 0);
	const flowMaxValue = $derived(flowResult?.max ?? 0);
	// Weighted layers (OViN/ODiN) expose a raw observation count → Min count filter.
	const flowWeighted = $derived(flowResult?.weighted ?? false);
	const flowCountMax = $derived.by(() => {
		if (!flowResult) return 0;
		let m = 0;
		for (const f of flowResult.flows) if (f.count != null && f.count > m) m = f.count;
		return m;
	});

	// Geo selectors driven by current scale.
	const geoMain = $derived(manifest?.geo?.[selection.scale]);
	const geoOverlay = $derived(mapLayers.boundary ? manifest?.geo?.[mapLayers.boundaryScale] : null);
</script>

<div style="position: fixed; inset: 0;">
	<MapView
		center={MAP_DEFAULTS.center}
		zoom={MAP_DEFAULTS.zoom}
		apiKey={PUBLIC_PROTOMAPS_API_KEY}
		theme="white"
	>
		{#if manifest && geoMain}
			{#if selection.enabled}
				{#key selection.scale}
					<ChoroplethLayer
						sourceId="choropleth-{selection.scale}"
						geoUrl={dataUrl(geoMain.geojson, manifest.version)}
						promoteId={geoMain.idProp}
						valueByArea={displayed.data}
						selectedIds={studyArea.ids}
						{fillColor}
						fillOpacity={cartography.fillOpacity}
						lineColor={cartography.lineColor}
						lineWidth={cartography.lineWidth}
					/>
					<LassoTool active={lassoActive} fillLayerId="choropleth-{selection.scale}-fill" />
					{#if ui.showLabels}
						<NodeNamesLayer sourceId="choropleth-{selection.scale}" />
					{/if}
				{/key}
			{/if}
			{#if geoOverlay}
				{#key mapLayers.boundaryScale}
					<BoundaryLayer
						sourceId="overlay-{mapLayers.boundaryScale}"
						geoUrl={dataUrl(geoOverlay.geojson, manifest.version)}
						promoteId={geoOverlay.idProp}
						lineColor={mapLayers.boundaryColor}
						lineWidth={mapLayers.boundaryWidth}
						lineOpacity={mapLayers.boundaryOpacity}
					/>
				{/key}
			{/if}
			{#if flow.enabled && filteredFlows.length && flowBreaks && centroids}
				<FlowLayer
					sourceId="flow-{flow.dataset}-{flow.scale}"
					flows={filteredFlows}
					{centroids}
					breaks={flowBreaks}
					colors={flowColors}
					widthMin={flowCartography.widthMin}
					widthMax={flowCartography.widthMax}
					opacity={flowCartography.opacity}
					curvature={flowCartography.curvature}
					selectedNode={ui.selectedFlowNode}
					mode={ui.flowMode}
				/>
				{#if ui.selectedFlowNode}
					<FlowPies
						selectedNode={ui.selectedFlowNode}
						flows={filteredFlows}
						{centroids}
						scale={flow.scale}
					/>
				{/if}
			{/if}
			<InspectInteraction
				nodeFillLayerId="choropleth-{selection.scale}-fill"
				flowLineLayerId={flow.enabled ? `flow-${flow.dataset}-${flow.scale}-line` : null}
				nodeScale={selection.scale}
				flowScale={flow.scale}
				flowEnabled={flow.enabled}
			/>
			{#if mapLayers.builtup && manifest.overlays?.builtup}
				<BuiltUpLayer
					sourceId="builtup"
					geoUrl={dataUrl(manifest.overlays.builtup.geojson, manifest.version)}
					color={mapLayers.builtupColor}
					opacity={mapLayers.builtupOpacity}
				/>
			{/if}
			{#if mapLayers.provinces && manifest.overlays?.provinces}
				<ProvinceLayer
					sourceId="provinces"
					geoUrl={dataUrl(manifest.overlays.provinces.geojson, manifest.version)}
					lineColor={mapLayers.provinceColor}
					lineWidth={mapLayers.provinceWidth}
				/>
			{/if}
			<BasemapLabelsLayer visible={mapLayers.labels} />
		{/if}
	</MapView>
</div>

<div class="sidebar sidebar-left">
	<div class="header">
		<div class="brand-row">
			<div class="brand">NPRZ <span class="brand-sub">analytics</span></div>
			<div class="actions">
				<button
					type="button"
					class="action"
					onclick={resetAll}
					title="Reset view and all controls to defaults (saved layers are kept)"
				>
					⟲
				</button>
				{#if data.user}
					<form method="POST" action="?/logout" class="logout-form">
						<button type="submit" class="action" title="Sign out — {data.user.email}">↪</button>
					</form>
				{/if}
			</div>
		</div>
		<div
			class="status"
			class:busy={displayed.loading}
			class:err={displayed.error || manifestState.error}
		>
			{status}
		</div>
		{#if flowStatus}
			<div class="status" class:busy={flowQuerying} class:err={flowError}>flow: {flowStatus}</div>
		{/if}
	</div>

	<Panel title="Scale">
		<ScaleToggle />
	</Panel>

	<Panel title="Node data">
		{#if manifest}
			<div class="stack">
				<Toggle bind:checked={selection.enabled} label="Show nodes" />
				<DatasetPicker {manifest} />
				<YearPicker {manifest} />
				<VariablePicker {manifest} />
				<CategoryFilters {manifest} />
				<div class="save-divider"></div>
				<SaveLayerInput {manifest} />
			</div>
		{:else}
			<p class="hint">Loading manifest…</p>
		{/if}
	</Panel>

	<Panel title="Flow data" open={false}>
		{#if manifest}
			<div class="stack">
				<Toggle bind:checked={flow.enabled} label="Show flows" />
				<DatasetPicker {manifest} state={flow} section="flows" />
				{#if flow.enabled && !flowScaleAvailable}
					<p class="hint">
						This dataset is not available at {scaleLabel(selection.scale)} scale. Switch the node scale
						to view flows.
					</p>
				{/if}
				<YearPicker {manifest} state={flow} section="flows" />
				<CategoryFilters {manifest} state={flow} section="flows" />
				<LogRangeFilter
					label="Min weight"
					bind:value={flow.minWeight}
					floor={flowMinValue}
					max={flowMaxValue || 1}
					disabled={!flowResult || flowMaxValue === 0}
				/>
				{#if flowWeighted}
					<LogRangeFilter
						label="Min count"
						bind:value={flow.minCount}
						floor={1}
						max={flowCountMax || 1}
						disabled={!flowResult || flowCountMax === 0}
						integer
					/>
				{/if}
				<Field label="Self-loops">
					<input type="checkbox" bind:checked={flow.includeSelfLoops} />
				</Field>
				<div class="save-divider"></div>
				<SaveFlowLayerInput {manifest} />
			</div>
		{:else}
			<p class="hint">Loading manifest…</p>
		{/if}
	</Panel>

	<Panel title="Map layers" open={false}>
		<MapLayerControls />
	</Panel>
</div>

<div class="sidebar sidebar-right">
	<Panel title="Inspect" open>
		<InspectPanel
			nodeValueByArea={displayed.data}
			nodeValues={sortedValues}
			nodeBreaks={breaks}
			nodeColors={colors}
			nodeLabel={displayed.activeLayer?.name ?? 'live selection'}
			nodeScale={selection.scale}
			flowEnabled={flow.enabled}
			flowScale={flow.scale}
			{flowsByPair}
			{flowValues}
			{flowBreaks}
			{flowColors}
		/>
	</Panel>

	<Panel title="Node cartography">
		<div class="stack">
			<ClassificationControls />
			<Toggle bind:checked={ui.showLabels} label="Show names" />
			{#if breaks}
				<Legend {breaks} {colors} />
			{/if}
		</div>
	</Panel>

	<Panel title="Flow cartography" open={false}>
		<div class="stack">
			<ClassificationControls target={flowCartography} />
			{#if flowBreaks}
				<Legend breaks={flowBreaks} colors={flowColors} />
			{/if}
		</div>
	</Panel>
</div>

<FloatingDock
	title="Layer Calculator"
	open={ui.openDocks.calculator}
	x={ui.dockPositions.calculator.x}
	y={ui.dockPositions.calculator.y}
	width={340}
	onClose={() => ui.toggleDock('calculator')}
	onMove={(pos) => ui.setDockPosition('calculator', pos)}
>
	<LayerCalculator {manifest} />
</FloatingDock>

<FloatingDock
	title="Study area"
	open={ui.openDocks.studyArea}
	x={ui.dockPositions.studyArea.x}
	y={ui.dockPositions.studyArea.y}
	width={320}
	onClose={() => ui.toggleDock('studyArea')}
	onMove={(pos) => ui.setDockPosition('studyArea', pos)}
>
	<StudyAreaControls bind:lassoActive />
</FloatingDock>

<DockToggleStrip />

{#if queryResult.lastMs !== null}
	<div class="debug" title="Last query duration">{queryResult.lastMs} ms</div>
{/if}

<style>
	.sidebar {
		position: fixed;
		top: var(--spacing-4);
		z-index: 1;
		display: flex;
		flex-direction: column;
		gap: var(--spacing-2);
		width: 300px;
		max-height: calc(100vh - 2 * var(--spacing-4));
		overflow-y: auto;
	}
	.sidebar-left {
		left: var(--spacing-4);
		/* Scroll clearance so the last panel isn't trapped behind the fixed
		   DockToggleStrip pinned to the bottom-left. */
		padding-bottom: calc(var(--spacing-4) * 4);
	}
	.sidebar-right {
		right: var(--spacing-4);
		width: 320px;
	}
	.header {
		padding: var(--spacing-2) var(--spacing-3);
		background: var(--color-bg-panel);
		border: 1px solid var(--color-line);
		border-radius: var(--radius);
	}
	.brand {
		font-size: var(--text-base);
		font-weight: 600;
		color: var(--color-text);
		letter-spacing: 0.02em;
	}
	.brand-sub {
		color: var(--color-muted);
		font-weight: 400;
	}
	.brand-row {
		display: flex;
		align-items: baseline;
		justify-content: space-between;
		gap: var(--spacing-2);
	}
	.actions {
		display: flex;
		gap: 4px;
		align-items: center;
	}
	.logout-form {
		margin: 0;
	}
	.action {
		background: transparent;
		border: none;
		color: var(--color-hint);
		cursor: pointer;
		font-size: var(--text-sm);
		padding: 0 4px;
		text-decoration: none;
	}
	.action:hover {
		color: var(--color-text);
	}
	.status {
		font-size: var(--text-xs);
		color: var(--color-muted);
		font-variant-numeric: tabular-nums;
		margin-top: 2px;
	}
	.status.busy {
		color: var(--color-hint);
	}
	.status.err {
		color: #cf222e;
	}
	.stack {
		display: flex;
		flex-direction: column;
		gap: var(--spacing-3);
	}
	.hint {
		color: var(--color-hint);
		font-size: var(--text-sm);
		margin: 0;
	}
	.save-divider {
		border-top: 1px solid var(--color-line);
	}
	.debug {
		position: fixed;
		bottom: var(--spacing-2);
		left: var(--spacing-2);
		z-index: 1;
		padding: 2px var(--spacing-2);
		background: rgba(255, 255, 255, 0.85);
		color: var(--color-hint);
		font-size: var(--text-xs);
		font-variant-numeric: tabular-nums;
		border-radius: var(--radius);
		pointer-events: none;
	}
</style>

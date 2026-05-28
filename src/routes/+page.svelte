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
	import ModelDock from '$lib/ui/ModelDock.svelte';
	import ModelResults from '$lib/ui/ModelResults.svelte';
	import SavedLayers from '$lib/ui/SavedLayers.svelte';
	import FloatingDock from '$lib/ui/FloatingDock.svelte';
	import DockToggleStrip from '$lib/ui/DockToggleStrip.svelte';
	import InspectPanel from '$lib/ui/InspectPanel.svelte';
	import InspectInteraction from '$lib/map/InspectInteraction.svelte';
	import NodeNamesLayer from '$lib/map/NodeNamesLayer.svelte';
	import FlowPies from '$lib/map/FlowPies.svelte';
	import MapLegend from '$lib/ui/MapLegend.svelte';
	import PrintFrameOverlay from '$lib/map/PrintFrameOverlay.svelte';
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
	import { printView } from '$lib/state/print-view.svelte.js';
	import { geoNames } from '$lib/state/geo-names.svelte.js';
	import { scaleLabel, scaleUnit } from '$lib/scales.js';
	import DirectionalFlowLayer from '$lib/map/DirectionalFlowLayer.svelte';

	let { data } = $props();
	let lassoActive = $state(false);

	// Resolve which model the right-sidebar Model results panel should show.
	// If the active layer is a `model` parent → that id. If it's a `model-output`
	// child → walk to its parentId. Otherwise null (panel renders an empty hint).
	const activeModelParentId = $derived.by(() => {
		const a = displayed.activeLayer;
		if (!a) return null;
		if (a.kind === 'model') return a.id;
		if (a.kind === 'model-output' && a.parentId) return a.parentId;
		return null;
	});

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
		// Sort by magnitude — min-weight filtering compares `|value|`, so the
		// "top N% strongest" anchor must be picked over abs values too.
		// Otherwise signed datasets (SIM residuals) anchor on the top of the
		// positive tail and leave most of the negative tail invisible.
		const sorted = flows.map((f) => Math.abs(f.value)).sort((a, b) => a - b);
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
				// On layer switch, reset the min-count cutoff to the dataset-
				// appropriate default. For non-weighted layers it's meaningless,
				// so 0. For weighted layers (ovin etc.) we default to 10 — survey
				// flows below that are statistically noisy and dominate the
				// long tail of a typical query. The user can still drag it down.
				if (layerChanged) {
					flow.minCount = res.weighted ? 10 : 0;
				}
				if (res.flows.length === 0) {
					// Nothing to anchor against; leave threshold for next non-empty result.
				} else {
					// The slider compares against `|value|`, so the effective upper
					// bound is the larger of |min| and |max| — for signed data the
					// most-negative residual matters as much as the most-positive.
					const absMax = Math.max(Math.abs(res.min), Math.abs(res.max));
					if (layerChanged || flow.minWeight > absMax) {
						// Layer switch, or user's saved threshold is out of range for
						// the new result — re-anchor at the percentile instead of
						// dropping to zero (which would render every flow).
						flow.minWeight = flowPercentileThreshold(res.flows, FLOW_DEFAULT_TOP_FRACTION);
					}
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

	// When an active flow-domain layer is selected (a saved flow filter OR a
	// SIM model-output child), surface its values through the same FlowLayer
	// pipeline as the live `runFlows()` result. This lets the user *see* what
	// a SIM model fitted to OD flows — without it, the result is a coefficient
	// table and not much else.
	const effectiveFlowResult = $derived.by(() => {
		const map = displayed.flowsData;
		if (!map) return flowResult;
		let min = Infinity;
		let max = -Infinity;
		/** @type {{o: string, d: string, value: number}[]} */
		const arr = [];
		for (const [edge, value] of map) {
			if (!Number.isFinite(value)) continue;
			const sep = edge.indexOf('|');
			if (sep < 0) continue;
			const o = edge.slice(0, sep);
			const d = edge.slice(sep + 1);
			arr.push({ o, d, value });
			if (value < min) min = value;
			if (value > max) max = value;
		}
		if (arr.length === 0) return { flows: [], min: 0, max: 0, weighted: false };
		return { flows: arr, min, max, weighted: false };
	});

	// FlowLayer shows up iff the user has explicitly enabled the toggle.
	// Previously we ORed `displayed.flowsData != null` in here so a SIM
	// model auto-rendered its fitted flows, but that meant the toggle
	// couldn't hide flows while a SIM was active. The auto-show UX still
	// matters on the FIRST SIM activation (otherwise users wonder why
	// nothing rendered), so we hand it to an $effect below that flips the
	// toggle on once when displayed.flowsData transitions to non-null.
	const flowsShown = $derived(flow.enabled);

	// Auto-enable the flow toggle on the rising edge of "a SIM's fitted
	// child became active." Doesn't override a deliberate user toggle —
	// they can still turn it off after this fires. Uses displayed.activeLayer
	// (rather than reaching into the `layers` singleton) to avoid an extra
	// import here.
	let flowsAutoEnabledFor = $state(/** @type {string | null} */ (null));
	$effect(() => {
		const aid = displayed.activeLayer?.id ?? null;
		const hasFlowData = displayed.flowsData != null;
		if (hasFlowData && aid && aid !== flowsAutoEnabledFor) {
			flow.enabled = true;
			flowsAutoEnabledFor = aid;
		} else if (!hasFlowData) {
			flowsAutoEnabledFor = null;
		}
	});

	// Hard cap on rendered flows — MapLibre will draw more, but the bezier
	// geometry generation in FlowLayer + the per-feature paint expressions
	// freeze the browser well before that. 50k is arbitrary but well-tested.
	// Above the cap we keep the top-50k by value (so the heaviest flows
	// still render); the warning below the min-weight slider tells the user
	// to raise the threshold if they want a different subset.
	const FLOW_RENDER_CAP = 50_000;
	const filteredFlowsAll = $derived.by(() => {
		if (!effectiveFlowResult) return [];
		const studyIds = studyArea.ids;
		const scoped = studyIds.size > 0;
		const mode = flow.studyAreaMode;
		return effectiveFlowResult.flows.filter((f) => {
			if (Math.abs(f.value) < flow.minWeight) return false;
			if (f.count != null && f.count < flow.minCount) return false;
			if (scoped) {
				const oIn = studyIds.has(f.o);
				const dIn = studyIds.has(f.d);
				if (mode === 'within' ? !(oIn && dIn) : !(oIn || dIn)) return false;
			}
			return true;
		});
	});
	// Merge bidirectional pairs into single entries for directional-gradient
	// rendering. Each pair is keyed canonically (smaller area_code first) so
	// A→B and B→A collapse to one entry regardless of encounter order.
	// `fwdVal` is the flow in the canonical o→d direction, `revVal` the
	// reverse. `total` drives line width; the fwd/total ratio drives the
	// gradient split point. Only computed when directional mode is on.
	const directionalFlows = $derived.by(() => {
		if (!flowCartography.directional) return [];
		/** @type {Map<string, {o: string, d: string, fwdVal: number, revVal: number}>} */
		// eslint-disable-next-line svelte/prefer-svelte-reactivity -- local accumulator, not reactive state
		const pairs = new Map();
		for (const f of filteredFlows) {
			// Canonical orientation: smaller code is always `o`.
			const forward = f.o < f.d;
			const o = forward ? f.o : f.d;
			const d = forward ? f.d : f.o;
			const key = `${o}|${d}`;
			let pair = pairs.get(key);
			if (!pair) {
				pair = { o, d, fwdVal: 0, revVal: 0 };
				pairs.set(key, pair);
			}
			if (forward) pair.fwdVal += f.value;
			else pair.revVal += f.value;
		}
		const out = [];
		const minW = flow.minWeight;
		for (const p of pairs.values()) {
			const total = p.fwdVal + p.revVal;
			if (total <= 0) continue;
			// Honest sliver: if one direction is entirely absent AND a min-weight
			// filter is active, the absence almost always means "filtered below
			// threshold" rather than "truly zero" — so show a thin band of the
			// minority color rather than a pure single-color line. At minWeight=0
			// an absent direction is genuinely zero, so leave it pure.
			let fwdFrac = p.fwdVal / total;
			if (minW > 0) {
				if (p.revVal === 0)
					fwdFrac = 0.95; // reverse filtered → 5% blue sliver at the d end
				else if (p.fwdVal === 0) fwdFrac = 0.1; // forward filtered → 5% red sliver at the o end
			}
			out.push({ ...p, total, value: total, fwdFrac });
		}
		return out;
	});
	const flowsCapped = $derived(filteredFlowsAll.length > FLOW_RENDER_CAP);
	const filteredFlows = $derived.by(() => {
		if (!flowsCapped) return filteredFlowsAll;
		// Top-N by value — sort descending and slice.
		return [...filteredFlowsAll].sort((a, b) => b.value - a.value).slice(0, FLOW_RENDER_CAP);
	});

	const flowStatus = $derived.by(() => {
		if (flowError) return flowError;
		if (flowQuerying) return 'querying…';
		if (!effectiveFlowResult) return null;
		const total = effectiveFlowResult.flows.length;
		const shown = filteredFlows.length;
		if (flowsCapped) {
			return `${shown.toLocaleString()} of ${filteredFlowsAll.length.toLocaleString()} flows (capped — raise min weight)`;
		}
		return shown === total
			? `${total.toLocaleString()} flows`
			: `${shown.toLocaleString()} / ${total.toLocaleString()} flows`;
	});

	// Cartography is value-only-finite — negatives and zero are valid data for
	// model residuals, calc layers, and any Gaussian fitted output. classify()
	// (jenks/quantile/equal) handles them all correctly; the legacy `> 0` clause
	// was hiding genuine zero/negative areas as a side-effect of avoiding bias
	// on count datasets. Trust the classification + palette to render the data
	// the user asked for.
	const sortedValues = $derived([...displayed.data.values()].filter((v) => Number.isFinite(v)));

	// Diverging palette + classification kick in when the active layer's
	// values span both signs (e.g. NLM residuals, GWR β surfaces that flip
	// sign, calc layers like `a - b`). The user can opt out via
	// `cartography.forceSequential` if they want to read signed data
	// as sequential.
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
	const fillColor = $derived(breaks ? stepExpression({ breaks, colors }) : '#eee');

	// Flow values feed cartography classification. Keep all finite values so
	// SIM residuals (centered on 0, mix of signs) classify cleanly across
	// jenks/quantile/equal. FlowLayer renders width by abs(value).
	const flowValues = $derived(filteredFlows.map((f) => f.value).filter((v) => Number.isFinite(v)));

	// Same diverging detection as node side — SIM residuals and any other
	// signed flow data should route through the diverging palette +
	// pivot-anchored classification automatically.
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
	// sliders don't jump as the user drags them. The min-weight filter
	// compares against `|value|`, so the slider spans [0, max(|min|, |max|)].
	// For non-negative datasets this collapses to [0, max] (unchanged).
	const flowMinValue = 0;
	const flowMaxValue = $derived(
		effectiveFlowResult
			? Math.max(Math.abs(effectiveFlowResult.min), Math.abs(effectiveFlowResult.max))
			: 0
	);
	// Weighted layers (OViN/ODiN) expose a raw observation count → Min count filter.
	const flowWeighted = $derived(effectiveFlowResult?.weighted ?? false);
	const flowCountMax = $derived.by(() => {
		if (!effectiveFlowResult) return 0;
		let m = 0;
		for (const f of effectiveFlowResult.flows) if (f.count != null && f.count > m) m = f.count;
		return m;
	});

	// Geo selectors driven by current scale.
	const geoMain = $derived(manifest?.geo?.[selection.scale]);
	const geoOverlay = $derived(mapLayers.boundary ? manifest?.geo?.[mapLayers.boundaryScale] : null);
</script>

<div style="position: fixed; inset: 0;">
	<!-- Initial center/zoom are owned by the `mapView` singleton (which seeds
	     itself from MAP_DEFAULTS) so the view persists across navigations.
	     `MAP_DEFAULTS` is still imported above — `resetAll` uses it for the
	     map.jumpTo() snap-back. -->
	<MapView apiKey={PUBLIC_PROTOMAPS_API_KEY} theme="white">
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
			{#if flowsShown && centroids}
				{#if flowCartography.directional && directionalFlows.length}
					{#key `dir-${flow.dataset}-${flow.scale}`}
						<DirectionalFlowLayer
							sourceId="flow-directional-{flow.dataset}-{flow.scale}"
							pairs={directionalFlows}
							{centroids}
							widthMin={flowCartography.widthMin}
							widthMax={flowCartography.widthMax}
							opacity={flowCartography.opacity}
							curvature={flowCartography.curvature}
							method={flowCartography.method}
							n={flowCartography.n}
							selectedNode={ui.selectedFlowNode}
							mode={ui.flowMode}
						/>
						{#if ui.selectedFlowNode}
							<FlowPies
								selectedNode={ui.selectedFlowNode}
								flows={filteredFlows}
								{centroids}
								scale={flow.scale}
								minWeight={flow.minWeight}
							/>
						{/if}
					{/key}
				{:else if filteredFlows.length && flowBreaks}
					{#key `${flow.dataset}-${flow.scale}`}
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
								minWeight={flow.minWeight}
							/>
						{/if}
					{/key}
				{/if}
			{/if}
			<InspectInteraction
				nodeFillLayerId="choropleth-{selection.scale}-fill"
				flowLineLayerId={flow.enabled
					? flowCartography.directional
						? `flow-directional-${flow.dataset}-${flow.scale}-line`
						: `flow-${flow.dataset}-${flow.scale}-line`
					: null}
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
		{#if printView.showOverlay}
			<PrintFrameOverlay />
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
				<div class="saved-layers-section">
					<div class="saved-layers-head">Saved node layers</div>
					<SavedLayers {manifest} domain="node" />
				</div>
				<div class="save-divider"></div>
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
				<div class="saved-layers-section">
					<div class="saved-layers-head">Saved flow layers</div>
					<SavedLayers {manifest} domain="flow" />
				</div>
				<div class="save-divider"></div>
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
				{#if flowsCapped}
					<p
						class="flow-cap-warn"
						title="The map renders the heaviest {FLOW_RENDER_CAP.toLocaleString()} flows; raise the threshold to choose a different subset."
					>
						⚠ Showing the top {FLOW_RENDER_CAP.toLocaleString()} of {filteredFlowsAll.length.toLocaleString()}
						— raise the min-weight threshold to render fewer.
					</p>
				{/if}
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
				{#if studyArea.ids.size > 0}
					<Field
						label="Study area"
						info="Filter flows by the active lasso. Within: both origin and destination must be inside. Touches: either side inside."
					>
						<div class="seg" role="radiogroup" aria-label="Study area mode">
							<button
								type="button"
								class:active={flow.studyAreaMode === 'within'}
								aria-pressed={flow.studyAreaMode === 'within'}
								onclick={() => (flow.studyAreaMode = 'within')}
								title="Keep OD pairs where BOTH origin and destination are in the study area"
							>
								entirely within
							</button>
							<button
								type="button"
								class:active={flow.studyAreaMode === 'touches'}
								aria-pressed={flow.studyAreaMode === 'touches'}
								onclick={() => (flow.studyAreaMode = 'touches')}
								title="Keep OD pairs where EITHER side is in the study area"
							>
								origin OR destination within
							</button>
						</div>
					</Field>
				{/if}
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
			flowMinWeight={flow.minWeight}
		/>
	</Panel>

	<Panel title="Model results" open={activeModelParentId !== null}>
		<ModelResults parentId={activeModelParentId} showActiveChannel={true} />
	</Panel>

	<Panel title="Node cartography">
		<div class="stack">
			<ClassificationControls {useDiverging} />
			<Toggle bind:checked={ui.showLabels} label="Show names" />
		</div>
	</Panel>

	<Panel title="Flow cartography" open={false}>
		<div class="stack">
			<Toggle bind:checked={flowCartography.directional} label="Directional gradient" />
			<ClassificationControls target={flowCartography} useDiverging={flowUseDiverging} />
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
	<LayerCalculator />
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

<FloatingDock
	title="Model Calculator"
	open={ui.openDocks.model}
	x={ui.dockPositions.model.x}
	y={ui.dockPositions.model.y}
	width={380}
	onClose={() => ui.toggleDock('model')}
	onMove={(pos) => ui.setDockPosition('model', pos)}
>
	<ModelDock />
</FloatingDock>

<DockToggleStrip />

<MapLegend
	node={selection.enabled
		? {
				breaks,
				colors,
				title: displayed.activeLayer?.name ?? 'Areas'
			}
		: null}
	flow={flowsShown && !flowCartography.directional
		? {
				breaks: flowBreaks,
				colors: flowColors,
				title: `Flows · ${flow.dataset}`
			}
		: null}
/>

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
	/* Warning chip below the min-weight slider when the rendered flow count
	   is capped. Distinguishes from the muted .hint by warm color + slight
	   emphasis — actionable, not background info. */
	.flow-cap-warn {
		color: #b95000;
		font-size: var(--text-xs);
		margin: 0;
		padding: 2px var(--spacing-2);
		background: rgba(185, 80, 0, 0.08);
		border-radius: var(--radius);
		border-left: 2px solid #b95000;
	}
	.save-divider {
		border-top: 1px solid var(--color-line);
	}
	/* Small section header above the SavedLayers list (both node + flow) —
	   clarifies that the radio list is layers, not e.g. dataset choices. */
	.saved-layers-section {
		display: flex;
		flex-direction: column;
		gap: var(--spacing-1);
	}
	.saved-layers-head {
		font-size: var(--text-xs);
		color: var(--color-muted);
		text-transform: uppercase;
		letter-spacing: 0.5px;
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
	.seg {
		display: inline-flex;
		border: 1px solid var(--color-line);
		border-radius: var(--radius);
		overflow: hidden;
	}
	.seg button {
		background: transparent;
		border: none;
		padding: 2px var(--spacing-2);
		font-size: var(--text-xs);
		color: var(--color-muted);
		cursor: pointer;
	}
	.seg button + button {
		border-left: 1px solid var(--color-line);
	}
	.seg button.active {
		background: var(--color-accent);
		color: var(--color-accent-fg);
	}
</style>

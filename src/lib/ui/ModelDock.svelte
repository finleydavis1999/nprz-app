<script>
	// Model Calculator dock — NLM + SIM + GWR.
	//
	// Mirrors `LayerCalculator.svelte`: a list of saved models (with their
	// model-output children rendered through the sidebar SavedLayers list,
	// not nested here) plus an add-model form. The form's top-level NLM/SIM
	// switch picks the family; GWR is a checkbox inside the NLM body that
	// flips the global GLM into one-local-fit-per-area. Coefficients + fit
	// stats live in `layers.modelFits` and surface inline via ModelResults
	// in the expandable details pane on the parent row.

	import Field from './Field.svelte';
	import LayerPicker from './LayerPicker.svelte';
	import ModelResults from './ModelResults.svelte';
	import { selection } from '$lib/state/selection.svelte.js';
	import { layers } from '$lib/state/layers.svelte.js';
	import { studyArea } from '$lib/state/study-area.svelte.js';
	import { prefetchWebR } from '$lib/models/webr-client.js';
	import { slugify } from '$lib/data/layer-calc.js';
	import { nlmFormulaFor, simFormulaFor } from '$lib/models/formula.js';
	import { onMount } from 'svelte';

	/** @typedef {'none' | 'log' | 'log1p' | 'sqrt'} Transform */
	/** @typedef {'nlm' | 'sim'} ModelFamilyKind */

	let mdName = $state('');
	let mdNameTouched = $state(false);
	/** Top-level family switch — drives which form body renders. NLM uses
	 *  node-domain dependent + covariates; SIM uses a flow dependent + two
	 *  node-domain mass layers + RD-centroid distances. @type {ModelFamilyKind} */
	let mdFamilyKind = $state(/** @type {ModelFamilyKind} */ ('nlm'));

	// NLM-only state
	let mdDependentId = $state(/** @type {string | null} */ (null));
	/** @type {Transform} */
	let mdDependentTransform = $state('none');
	/** @type {string[]} */
	let mdCovariateIds = $state([]);
	/** Per-covariate transforms keyed by layer id. Persists across (de)select
	 *  toggles so the user doesn't lose their choice when ticking a covariate
	 *  off + back on. @type {Record<string, Transform>} */
	let mdCovariateTransforms = $state(/** @type {Record<string, Transform>} */ ({}));
	/** Per-covariate spatial-lag config keyed by layer id. null/absent means
	 *  "no lag, use raw values". When present the design-matrix step runs the
	 *  smooth-worker to produce a kernel-weighted mean of the covariate.
	 *  @type {Record<string, { kernel: 'exp'|'gauss'|'power', decay: number, maxDist: number }>} */
	let mdCovariateLags = $state(
		/** @type {Record<string, { kernel: string, decay: number, maxDist: number }>} */ ({})
	);
	/** @type {'gaussian' | 'poisson'} */
	let mdFamily = $state('gaussian');
	/** Optional NLM weights — a node-domain layer whose per-area values feed
	 *  speedglm's weights= arg (inverse-variance / WLS). null = unweighted. */
	let mdWeightsId = $state(/** @type {string | null} */ (null));
	/** Optional offset — a node-domain layer added to the linear predictor
	 *  without a coefficient. Typical use: offsetTransform='log' for Poisson
	 *  rate models (`deaths ~ ... + offset(log(population))`). */
	let mdOffsetId = $state(/** @type {string | null} */ (null));
	/** @type {Transform} */
	let mdOffsetTransform = $state('log');
	/** GWR mode toggle on the NLM form (Phase 3). Fits one local weighted
	 *  regression per area instead of one global GLM; produces per-coefficient
	 *  child layers + local R² + actual bandwidth surfaces. Weights/offsets
	 *  are ignored in v0 (the form disables them when GWR is on). */
	let mdGwrEnabled = $state(false);
	/** @type {'fixed' | 'adaptive'} */
	let mdGwrKernelType = $state('fixed');
	/** @type {'bi-square' | 'gaussian'} */
	let mdGwrKernelShape = $state('bi-square');
	/** True → golden-section search picks the bandwidth; false → use the
	 *  numeric `mdGwrBandwidth` directly. */
	let mdGwrBwAuto = $state(true);
	/** Numeric bandwidth: km for fixed kernels, integer k for adaptive.
	 *  Only consulted when mdGwrBwAuto is false. */
	let mdGwrBandwidth = $state(10);

	// SIM-only state
	let mdFlowId = $state(/** @type {string | null} */ (null));
	let mdMassOId = $state(/** @type {string | null} */ (null));
	let mdMassDId = $state(/** @type {string | null} */ (null));
	/** @type {Transform} */
	let mdMassOTransform = $state('log');
	/** @type {Transform} */
	let mdMassDTransform = $state('log');
	let mdIncludeSelfLoops = $state(false);
	let mdExpandToAllOD = $state(false);
	/** @type {'none' | 'production' | 'attraction'} */
	let mdConstraint = $state('none');
	/** SIM origin offset (broadcast per OD on origin code). */
	let mdSimOffsetId = $state(/** @type {string | null} */ (null));
	/** @type {Transform} */
	let mdSimOffsetTransform = $state('log');
	/** Competing-destinations extra covariate (per-destination, broadcast).
	 *  null = off; non-null carries the kernel + decay used to compute it. */
	let mdCompDest = $state(/** @type {{ kernel: string, decay: number } | null} */ (null));
	/** Radiation extra covariate (per-OD cumulative-closer mass). */
	let mdRadiation = $state(false);
	/** Zero-inflated Poisson fit via pscl::zeroinfl. Mutually exclusive with
	 *  non-'none' constraints in v0 (the form coerces). */
	let mdZeroInflated = $state(false);

	// Study-area scoping (shared NLM + SIM). When `mdStudyAreaScoped` is true
	// computeModelLayer subsets the model inputs to studyArea.ids before
	// fitting. `mdSimScopeMode` only matters for SIM (within | touches —
	// within keeps OD pairs where both o & d are in the area; touches keeps
	// OD pairs where either side is in the area).
	let mdStudyAreaScoped = $state(false);
	/** @type {'within' | 'touches'} */
	let mdSimScopeMode = $state('within');
	const studyAreaSize = $derived(studyArea.ids.size);
	const studyAreaAvailable = $derived(studyAreaSize > 0);

	let mdError = $state(/** @type {string | null} */ (null));
	let expandedId = $state(/** @type {string | null} */ (null));

	const TRANSFORM_OPTIONS = /** @type {const} */ ([
		{ value: 'none', label: 'identity' },
		{ value: 'log', label: 'log()' },
		{ value: 'log1p', label: 'log(1+x)' },
		{ value: 'sqrt', label: 'sqrt()' }
	]);

	// Layers available as model inputs: node-domain, same scale as the active
	// selection, and *not* already a model-output (which would create a
	// circular dependency if the user wired their own model into another).
	const sameScale = $derived(layers.items.filter((l) => l.scale === selection.scale));
	const nodeLayers = $derived(
		sameScale.filter((l) => (l.domain ?? 'node') === 'node' && l.kind !== 'model')
	);
	const modelParents = $derived(sameScale.filter((l) => l.kind === 'model'));
	const childrenByParent = $derived.by(() => {
		/** @type {Map<string, typeof sameScale>} */
		// eslint-disable-next-line svelte/prefer-svelte-reactivity
		const m = new Map();
		for (const l of sameScale) {
			if (l.kind !== 'model-output' || !l.parentId) continue;
			let arr = m.get(l.parentId);
			if (!arr) {
				arr = [];
				m.set(l.parentId, arr);
			}
			arr.push(l);
		}
		return m;
	});

	// Default dependent: first available node layer (only changes if the
	// previously-chosen one disappears, e.g. after a scale switch or deletion).
	const effectiveDependent = $derived(
		mdDependentId && nodeLayers.some((l) => l.id === mdDependentId)
			? mdDependentId
			: (nodeLayers[0]?.id ?? null)
	);

	// Covariate candidates exclude the chosen dependent (a column can't appear
	// on both sides of the formula).
	const covCandidates = $derived(nodeLayers.filter((l) => l.id !== effectiveDependent));

	// SIM inputs: a flow layer for the dependent + at least one node layer
	// (origin + destination mass can be the same layer — a single-mass
	// gravity model is legitimate).
	const flowLayers = $derived(sameScale.filter((l) => l.domain === 'flow' && l.kind !== 'model'));
	const effectiveFlow = $derived(
		mdFlowId && flowLayers.some((l) => l.id === mdFlowId) ? mdFlowId : (flowLayers[0]?.id ?? null)
	);
	const effectiveMassO = $derived(
		mdMassOId && nodeLayers.some((l) => l.id === mdMassOId)
			? mdMassOId
			: (nodeLayers[0]?.id ?? null)
	);
	const effectiveMassD = $derived(
		mdMassDId && nodeLayers.some((l) => l.id === mdMassDId)
			? mdMassDId
			: (nodeLayers[1]?.id ?? nodeLayers[0]?.id ?? null)
	);
	const simReady = $derived(
		mdFamilyKind === 'sim' && effectiveFlow && effectiveMassO && effectiveMassD
	);

	// Default name suggestion: "Model N" where N is one past the count of
	// existing models. Uniquified so the placeholder matches what gets saved.
	const modelCount = $derived(modelParents.length);
	const mdSuggestion = $derived(layers.uniqueName(`Model ${modelCount + 1}`));
	const mdEffective = $derived(mdNameTouched ? mdName : mdSuggestion);

	// Link is implied by family in phase 1 (identity for gaussian, log for
	// poisson). The plan keeps `link` user-settable for future flexibility;
	// the form just doesn't expose it yet.
	const mdLink = $derived(mdFamily === 'poisson' ? 'log' : 'identity');

	// R-style formula preview using the shared formula helpers — keeps the
	// pre-fit preview identical to the post-fit display in ModelResults.
	const formulaPreview = $derived.by(() => {
		if (mdFamilyKind === 'sim') {
			if (!simReady) return null;
			// SIM picker exposes both node + flow inputs; merge both into byId.
			const byId = new Map([...nodeLayers, ...flowLayers].map((l) => [l.id, l]));
			return simFormulaFor(
				{
					flowId: effectiveFlow,
					massOId: effectiveMassO,
					massDId: effectiveMassD,
					massOTransform: mdMassOTransform,
					massDTransform: mdMassDTransform,
					constraint: mdConstraint,
					compDest: mdCompDest,
					radiation: mdRadiation,
					zeroInflated: mdZeroInflated
				},
				byId
			);
		}
		if (!effectiveDependent) return null;
		const byId = new Map(nodeLayers.map((l) => [l.id, l]));
		return nlmFormulaFor(
			{
				dependentId: effectiveDependent,
				dependentTransform: mdDependentTransform,
				covariateIds: mdCovariateIds,
				covariateTransforms: mdCovariateTransforms,
				covariateLags: mdCovariateLags,
				glm: { family: mdFamily, link: mdLink },
				gwr: mdGwrEnabled
					? {
							enabled: true,
							kernelType: mdGwrKernelType,
							kernelShape: mdGwrKernelShape,
							bandwidth: mdGwrBwAuto ? 'auto' : mdGwrBandwidth
						}
					: null
			},
			byId
		);
	});

	function setCovTransform(id, value) {
		mdCovariateTransforms = { ...mdCovariateTransforms, [id]: value };
	}

	function toggleCovLag(id) {
		if (mdCovariateLags[id]) {
			const next = { ...mdCovariateLags };
			delete next[id];
			mdCovariateLags = next;
		} else {
			mdCovariateLags = {
				...mdCovariateLags,
				[id]: { kernel: 'exp', decay: 1, maxDist: 5 }
			};
		}
	}
	function patchCovLag(id, patch) {
		const current = mdCovariateLags[id];
		if (!current) return;
		mdCovariateLags = { ...mdCovariateLags, [id]: { ...current, ...patch } };
	}

	// Warm up webR the first time the dock opens so the user doesn't pay the
	// ~46 MB download on the first "Run". Fire-and-forget — no UI hook needed.
	onMount(() => {
		prefetchWebR();
	});

	function toggleExpanded(id) {
		expandedId = expandedId === id ? null : id;
	}

	function setActive(id) {
		layers.setActive(layers.activeId === id ? null : id);
	}

	function onSubmit(e) {
		e.preventDefault();
		mdError = null;
		try {
			if (mdFamilyKind === 'sim') {
				if (!effectiveFlow) {
					mdError = 'Pick a flow layer (dependent)';
					return;
				}
				if (!effectiveMassO || !effectiveMassD) {
					mdError = 'Pick origin and destination mass layers';
					return;
				}
				// v0 limitation: zero-inflated can't combine with non-'none'
				// constraints (pscl doesn't share design with the sparse-factor path).
				// The checkbox is disabled in the UI when constraint != 'none', so
				// reaching here with both should be impossible — belt-and-braces.
				const effectiveZI = mdZeroInflated && mdConstraint === 'none';
				// Study-area scoping is only meaningful if a study area is
				// actually drawn — otherwise saving with the flag on would
				// silently fit on the empty set. Coerce here.
				const effectiveStudyAreaScoped = mdStudyAreaScoped && studyAreaAvailable;
				layers.saveModel({
					name: mdEffective,
					family: 'sim',
					spec: {
						flowId: effectiveFlow,
						massOId: effectiveMassO,
						massDId: effectiveMassD,
						massOTransform: mdMassOTransform,
						massDTransform: mdMassDTransform,
						includeSelfLoops: mdIncludeSelfLoops,
						expandToAllOD: mdExpandToAllOD,
						constraint: mdConstraint,
						offsetId: mdSimOffsetId || null,
						offsetTransform: mdSimOffsetTransform,
						compDest: mdCompDest,
						radiation: mdRadiation,
						zeroInflated: effectiveZI,
						studyAreaScoped: effectiveStudyAreaScoped,
						simScopeMode: effectiveStudyAreaScoped ? mdSimScopeMode : 'within'
					}
				});
				mdName = '';
				mdNameTouched = false;
				mdSimOffsetId = null;
				mdCompDest = null;
				mdRadiation = false;
				mdZeroInflated = false;
				return;
			}

			const dependent = effectiveDependent;
			if (!dependent) {
				mdError = 'Pick a dependent layer';
				return;
			}
			if (mdCovariateIds.length === 0) {
				mdError = 'Pick at least one covariate';
				return;
			}
			// Only persist transforms / lags for currently-selected covariates
			// so we don't leak stale entries from a toggled-off pick into the
			// saved model record.
			const txForSelected = {};
			const lagsForSelected = {};
			for (const id of mdCovariateIds) {
				const t = mdCovariateTransforms[id];
				if (t && t !== 'none') txForSelected[id] = t;
				if (mdCovariateLags[id]) lagsForSelected[id] = mdCovariateLags[id];
			}
			// GWR mode disables weights+offset in v0 (no plumbing on the R
			// side yet); strip them from the spec instead of silently sending
			// them so the persisted record matches what actually ran.
			const gwrCfg = mdGwrEnabled
				? {
						enabled: true,
						kernelType: mdGwrKernelType,
						kernelShape: mdGwrKernelShape,
						bandwidth: mdGwrBwAuto ? 'auto' : mdGwrBandwidth
					}
				: null;
			const effectiveStudyAreaScopedNlm = mdStudyAreaScoped && studyAreaAvailable;
			layers.saveModel({
				name: mdEffective,
				family: 'nlm',
				spec: {
					dependentId: dependent,
					dependentTransform: mdDependentTransform,
					covariateIds: mdCovariateIds,
					covariateTransforms: txForSelected,
					covariateLags: lagsForSelected,
					weightsId: mdGwrEnabled ? null : mdWeightsId || null,
					offsetId: mdGwrEnabled ? null : mdOffsetId || null,
					offsetTransform: mdOffsetTransform,
					glm: { family: mdFamily, link: mdLink },
					gwr: gwrCfg,
					studyAreaScoped: effectiveStudyAreaScopedNlm
				}
			});
			mdName = '';
			mdNameTouched = false;
			mdCovariateIds = [];
			mdDependentTransform = 'none';
			mdCovariateTransforms = {};
			mdCovariateLags = {};
			mdWeightsId = null;
			mdOffsetId = null;
			// Don't reset mdGwrEnabled — keep the user's mode choice sticky
			// across consecutive model creations (typical workflow: build
			// several GWR variants from the same dependent set).
		} catch (err) {
			mdError = /** @type {Error} */ (err)?.message ?? String(err);
		}
	}

	// fmt + pStars helpers and the meta R² formatter live in ModelResults now;
	// the dock only needs a coarse "fit OK / not yet" indicator.
	function metaForRow(fit) {
		if (!fit) return null;
		const r = fit.fit?.adjRSquared;
		if (r == null || !Number.isFinite(r)) return 'fit';
		return `R²=${r.toFixed(3)}`;
	}
</script>

<div class="stack">
	{#if modelParents.length === 0}
		<p class="hint">No models yet — add one below using saved node-domain layers.</p>
	{:else}
		<ul class="layers">
			{#each modelParents as parent (parent.id)}
				{@const fit = layers.modelFits.get(parent.id)}
				{@const isActive = layers.activeId === parent.id}
				{@const loading = layers.loading.has(parent.id)}
				{@const err = layers.errors.get(parent.id)}
				{@const children = childrenByParent.get(parent.id) ?? []}
				{@const fitted = children.find((c) => c.channel === 'fitted')}
				{@const residual = children.find((c) => c.channel === 'residual')}
				{@const hasResults = !!(fitted && layers.results.get(fitted.id))}
				{@const extraChildCount = children.filter(
					(c) => c.channel !== 'fitted' && c.channel !== 'residual'
				).length}
				{@const familyBadge =
					parent.family === 'sim'
						? { txt: 'SIM', title: 'Spatial Interaction Model — Poisson gravity on OD flows' }
						: parent.spec?.gwr?.enabled
							? { txt: 'GWR', title: 'Geographically Weighted Regression — one local fit per area' }
							: { txt: 'NLM', title: 'Node-Level Model — GLM over per-area values' }}
				<li class="layer parent" class:active={isActive}>
					<span class="kind family-{familyBadge.txt.toLowerCase()}" title={familyBadge.title}>
						{familyBadge.txt}
					</span>
					<button
						type="button"
						class="name-btn"
						onclick={() => toggleExpanded(parent.id)}
						title="Show fit details"
					>
						<span class="name">{parent.name}</span>
						<span class="slug">({parent.slug})</span>
					</button>
					{#if loading}
						<span class="meta" title={layers.modelStatus.get(parent.id) ?? 'Fitting…'}>
							{layers.modelStatus.get(parent.id) ?? 'fitting…'}
						</span>
					{:else if err}
						<span class="meta err" title={err}>!</span>
					{:else if fit}
						<span class="meta" title="adjusted R²">{metaForRow(fit)}</span>
					{/if}
					<button
						type="button"
						class="del"
						onclick={() => layers.remove(parent.id)}
						title="Delete model + outputs"
					>
						×
					</button>

					{#if hasResults}
						<div class="channels" role="radiogroup" aria-label="Show on map">
							<span class="channels-label">Show:</span>
							{#each [fitted, residual].filter(Boolean) as child (child.id)}
								{@const isChildActive = layers.activeId === child.id}
								<button
									type="button"
									class="chan"
									class:active={isChildActive}
									aria-pressed={isChildActive}
									onclick={() => setActive(child.id)}
									title="Activate this output on the map"
								>
									{child.channel}
								</button>
							{/each}
							{#if extraChildCount > 0}
								<!--
									GWR generates one β surface per coefficient plus local_r2
									and bw_actual — too many for the dock's compact channel
									switcher. Point the user at the SavedLayers panel where
									every child is selectable + previewable.
								-->
								<span
									class="extra-hint"
									title="GWR generates per-coefficient β surfaces, local R², and bw_actual diagnostics. Switch to them from the SavedLayers list in Node data."
								>
									+ {extraChildCount} more (β, local R², bw) — see Layers
								</span>
							{/if}
						</div>
					{/if}

					{#if expandedId === parent.id && fit}
						<div class="details">
							<ModelResults parentId={parent.id} showFormula={false} />
						</div>
					{/if}
				</li>
			{/each}
		</ul>
	{/if}

	<form class="add" onsubmit={onSubmit}>
		<div class="add-head">Add {mdFamilyKind === 'sim' ? 'SIM' : 'NLM'} model</div>

		<Field
			label="Type"
			info="NLM = node-level GLM (gaussian or Poisson over per-area values). SIM = Poisson spatial-interaction model on OD flow counts. GWR is a per-area variant of NLM (toggle inside the NLM form)."
		>
			<div class="seg" role="radiogroup" aria-label="Model family">
				<button
					type="button"
					class:active={mdFamilyKind === 'nlm'}
					aria-pressed={mdFamilyKind === 'nlm'}
					onclick={() => (mdFamilyKind = 'nlm')}
					title="Node-Level Model: GLM over per-area values"
				>
					NLM
				</button>
				<button
					type="button"
					class:active={mdFamilyKind === 'sim'}
					aria-pressed={mdFamilyKind === 'sim'}
					onclick={() => (mdFamilyKind = 'sim')}
					title="Spatial Interaction Model: Poisson gravity over OD flows"
				>
					SIM
				</button>
			</div>
		</Field>

		<Field label="Name">
			<input
				type="text"
				placeholder={mdSuggestion}
				value={mdNameTouched ? mdName : ''}
				oninput={(e) => {
					const v = /** @type {HTMLInputElement} */ (e.currentTarget).value;
					mdName = v;
					mdNameTouched = v.length > 0;
					mdError = null;
				}}
				autocomplete="off"
			/>
		</Field>

		{#if mdFamilyKind === 'sim'}
			{#if flowLayers.length === 0 || nodeLayers.length < 1}
				<p class="hint">
					SIM needs a saved flow layer (dependent) and at least one node layer (mass — origin and
					destination can share it).
				</p>
			{:else}
				<Field
					label="Flow (y)"
					info="The OD flow layer to model. Counts are fitted as Poisson; weighted survey counts are rounded for the MLE (a notice surfaces in the fit results)."
				>
					<LayerPicker
						mode="single"
						layers={flowLayers}
						value={effectiveFlow}
						onChange={(id) => (mdFlowId = /** @type {string | null} */ (id))}
					/>
				</Field>
				<Field
					label="Constraint"
					info="None: unconstrained gravity (both masses free). Production: origin balancing factors absorb origin mass — coefficients reflect destination attractiveness. Attraction: same for destinations. Doubly-constrained is intentionally out of scope."
				>
					<div class="seg" role="radiogroup" aria-label="SIM constraint">
						<button
							type="button"
							class:active={mdConstraint === 'none'}
							aria-pressed={mdConstraint === 'none'}
							onclick={() => (mdConstraint = 'none')}
							title="Unconstrained gravity — both masses free"
						>
							none
						</button>
						<button
							type="button"
							class:active={mdConstraint === 'production'}
							aria-pressed={mdConstraint === 'production'}
							onclick={() => (mdConstraint = 'production')}
							title="Origin balancing factors absorb origin mass"
						>
							production
						</button>
						<button
							type="button"
							class:active={mdConstraint === 'attraction'}
							aria-pressed={mdConstraint === 'attraction'}
							onclick={() => (mdConstraint = 'attraction')}
							title="Destination balancing factors absorb destination mass"
						>
							attraction
						</button>
					</div>
				</Field>
				<Field
					label="Origin layer"
					info="Per-origin mass (e.g. residential population). Combine with the transform on the right (log is the gravity-model default). For production-constrained models the layer still defines the origin universe but its values are absorbed into per-origin fixed effects."
				>
					<div class="picker-row">
						<LayerPicker
							mode="single"
							layers={nodeLayers}
							value={effectiveMassO}
							onChange={(id) => (mdMassOId = /** @type {string | null} */ (id))}
						/>
						{#if mdConstraint === 'production'}
							<span
								class="readonly absorbed"
								title="Origin mass is absorbed by per-origin balancing factors; the layer still defines the origin universe"
							>
								factor(o)
							</span>
						{:else}
							<select
								class="tx"
								value={mdMassOTransform}
								onchange={(e) =>
									(mdMassOTransform = /** @type {Transform} */ (e.currentTarget.value))}
							>
								{#each TRANSFORM_OPTIONS as o (o.value)}
									<option value={o.value}>{o.label}</option>
								{/each}
							</select>
						{/if}
					</div>
				</Field>
				<Field
					label="Dest layer"
					info="Per-destination mass (e.g. jobs). Mirror of origin layer. Origin and destination CAN reuse the same node layer — a single-mass potential model is legitimate."
				>
					<div class="picker-row">
						<LayerPicker
							mode="single"
							layers={nodeLayers}
							value={effectiveMassD}
							onChange={(id) => (mdMassDId = /** @type {string | null} */ (id))}
						/>
						{#if mdConstraint === 'attraction'}
							<span
								class="readonly absorbed"
								title="Destination mass is absorbed by per-destination balancing factors; the layer still defines the destination universe"
							>
								factor(d)
							</span>
						{:else}
							<select
								class="tx"
								value={mdMassDTransform}
								onchange={(e) =>
									(mdMassDTransform = /** @type {Transform} */ (e.currentTarget.value))}
							>
								{#each TRANSFORM_OPTIONS as o (o.value)}
									<option value={o.value}>{o.label}</option>
								{/each}
							</select>
						{/if}
					</div>
				</Field>
				<Field
					label="Offset"
					info="Optional per-origin offset (e.g. log(population)) — added to the linear predictor without estimating a coefficient. Common for converting absolute counts to rates."
				>
					<div class="picker-row">
						<select
							value={mdSimOffsetId ?? ''}
							onchange={(e) => (mdSimOffsetId = e.currentTarget.value || null)}
							title="Optional per-origin offset (broadcast on origin code), added to the linear predictor without a coefficient."
						>
							<option value="">— none —</option>
							{#each nodeLayers as l (l.id)}
								<option value={l.id}>{l.name}</option>
							{/each}
						</select>
						{#if mdSimOffsetId}
							<select
								class="tx"
								value={mdSimOffsetTransform}
								onchange={(e) =>
									(mdSimOffsetTransform = /** @type {Transform} */ (e.currentTarget.value))}
							>
								{#each TRANSFORM_OPTIONS as o (o.value)}
									<option value={o.value}>{o.label}</option>
								{/each}
							</select>
						{/if}
					</div>
				</Field>
				<Field
					label="Competing dest"
					info="Adds a log1p(competing-destinations) covariate per OD: a kernel-weighted sum of OTHER destinations' masses, weighted by distance from the focal destination. Captures the 'why pick this destination when others are nearby?' effect."
				>
					<div class="picker-row">
						<label class="inline-check">
							<input
								type="checkbox"
								checked={!!mdCompDest}
								onchange={(e) =>
									(mdCompDest = e.currentTarget.checked ? { kernel: 'exp', decay: 5 } : null)}
							/>
							<span>add column</span>
						</label>
						{#if mdCompDest}
							<select
								class="tx"
								value={mdCompDest.kernel}
								onchange={(e) => (mdCompDest = { ...mdCompDest, kernel: e.currentTarget.value })}
								title="Kernel for the destination-to-destination decay"
							>
								<option value="exp">exp</option>
								<option value="gauss">gauss</option>
								<option value="power">power</option>
							</select>
							<label
								class="tx-lag-num"
								title={mdCompDest.kernel === 'power' ? 'Exponent β' : 'Decay d₀ (km)'}
							>
								<span>{mdCompDest.kernel === 'power' ? 'β' : 'd₀'}</span>
								<input
									type="number"
									step="0.5"
									min={mdCompDest.kernel === 'power' ? 0.5 : 0.1}
									max={mdCompDest.kernel === 'power' ? 5 : 100}
									value={mdCompDest.decay}
									onchange={(e) => (mdCompDest = { ...mdCompDest, decay: +e.currentTarget.value })}
								/>
							</label>
						{/if}
					</div>
				</Field>
				<Field
					label="Radiation"
					info="Adds the Simini–Maritan radiation term: for each OD pair, the cumulative mass of all destinations closer to the origin than the focal destination. Captures intervening-opportunities competition."
				>
					<label
						class="inline-check"
						title="Per-OD cumulative mass of destinations closer to the origin than the focal destination (Simini-Maritan term). No tunable kernel — uses raw distance ranking."
					>
						<input
							type="checkbox"
							checked={mdRadiation}
							onchange={(e) => (mdRadiation = e.currentTarget.checked)}
						/>
						<span>add log1p(radiation)</span>
					</label>
				</Field>
				<Field
					label="Zero-inflated"
					info="Two-process Poisson fit (pscl::zeroinfl): a logit part models the probability of structural zeros (impossible OD pairs); a Poisson part models counts on the active rest. Useful when many OD pairs are zero for structural rather than rate reasons."
				>
					<label
						class="inline-check"
						class:disabled={mdConstraint !== 'none'}
						title={mdConstraint !== 'none'
							? 'Zero-inflated needs an unconstrained design (pscl::zeroinfl). Set Constraint to "none" first.'
							: 'Two-process fit: logit for structural zeros, Poisson for active flows. pscl is installed on first use (one-time download, ~MB).'}
					>
						<input
							type="checkbox"
							checked={mdZeroInflated}
							disabled={mdConstraint !== 'none'}
							onchange={(e) => (mdZeroInflated = e.currentTarget.checked)}
						/>
						<span>pscl::zeroinfl (count + zero parts)</span>
					</label>
				</Field>
				<Field
					label="Self-loops"
					info="Whether OD pairs where origin == destination (intra-area movement) are included in the fit. Usually off — self-flows aren't well-defined geographically and distort the distance-decay estimate."
				>
					<input
						type="checkbox"
						checked={mdIncludeSelfLoops}
						onchange={(e) => (mdIncludeSelfLoops = e.currentTarget.checked)}
					/>
				</Field>
				<Field
					label="Study area"
					info="When on, restrict the SIM fit to OD pairs touching (or within) the currently drawn study area. 'within' = both o & d in the area (tighter — for self-contained subnetworks). 'touches' = either side in the area (broader — captures commuter flows in & out)."
				>
					<div class="picker-row">
						<label class="inline-check" class:disabled={!studyAreaAvailable}>
							<input
								type="checkbox"
								checked={mdStudyAreaScoped}
								disabled={!studyAreaAvailable}
								onchange={(e) => (mdStudyAreaScoped = e.currentTarget.checked)}
							/>
							<span>
								{studyAreaAvailable ? `Restrict to ${studyAreaSize} areas` : 'No study area drawn'}
							</span>
						</label>
						{#if mdStudyAreaScoped && studyAreaAvailable}
							<div class="seg" role="radiogroup" aria-label="SIM scope mode">
								<button
									type="button"
									class:active={mdSimScopeMode === 'within'}
									aria-pressed={mdSimScopeMode === 'within'}
									onclick={() => (mdSimScopeMode = 'within')}
									title="Keep OD pairs where BOTH origin and destination are in the study area."
								>
									within
								</button>
								<button
									type="button"
									class:active={mdSimScopeMode === 'touches'}
									aria-pressed={mdSimScopeMode === 'touches'}
									onclick={() => (mdSimScopeMode = 'touches')}
									title="Keep OD pairs where EITHER origin or destination is in the study area (broader)."
								>
									touches
								</button>
							</div>
						{/if}
					</div>
				</Field>
				<Field
					label="Zero flows"
					info="Include OD pairs with zero observed flow as actual zero-count rows. Statistically correct for Poisson MLE (otherwise β_distance is biased toward zero) but explodes the row count to ~|origins|·|destinations|. Heavy at PC4 / buurt scale."
				>
					<label
						class="inline-check"
						title="Include OD pairs with zero observed flow. Unbiased Poisson MLE for β_distance, but ≈|massO|·|massD| rows — heavy at PC4/buurt scale."
					>
						<input
							type="checkbox"
							checked={mdExpandToAllOD}
							onchange={(e) => (mdExpandToAllOD = e.currentTarget.checked)}
						/>
						<span>include for unbiased β</span>
					</label>
				</Field>
				{#if mdExpandToAllOD && (selection.scale === 'pc4' || selection.scale === 'buurt') && !(mdStudyAreaScoped && studyAreaAvailable)}
					<p class="flow-cap-warn-sim">
						⚠ Expanding at {selection.scale} scale builds millions of OD rows — fit may take a minute
						or run out of memory. Consider scoping to a study area above, or unchecking this option.
					</p>
				{/if}

				{#if formulaPreview}
					<Field label="Formula">
						<code class="formula">{formulaPreview}</code>
					</Field>
				{/if}

				{#if !mdError}
					<p class="hint">Saves as → {slugify(mdEffective)}</p>
				{/if}
				{#if mdError}
					<p class="err-msg">{mdError}</p>
				{/if}
				<button type="submit" class="primary" disabled={!simReady}>Fit SIM</button>
			{/if}
		{:else if nodeLayers.length < 2}
			<p class="hint">
				Save at least two node-domain layers (dependent + one covariate) to fit a model.
			</p>
		{:else}
			<Field
				label="Family"
				info="Gaussian (identity link): linear regression — appropriate when the dependent is real-valued and can take any sign. Poisson (log link): for non-negative integer counts. Choice drives both the loss function and the link function below."
			>
				<div class="seg" role="radiogroup" aria-label="GLM family">
					<button
						type="button"
						class:active={mdFamily === 'gaussian'}
						aria-pressed={mdFamily === 'gaussian'}
						onclick={() => (mdFamily = 'gaussian')}
					>
						gaussian
					</button>
					<button
						type="button"
						class:active={mdFamily === 'poisson'}
						aria-pressed={mdFamily === 'poisson'}
						onclick={() => (mdFamily = 'poisson')}
					>
						poisson
					</button>
				</div>
			</Field>

			<Field
				label="Link"
				info="The link function maps the linear predictor to the response scale. Identity for gaussian (predicts the value directly). Log for Poisson (predicts the log of the count, ensures fitted values stay positive)."
			>
				<span class="readonly">{mdLink} (from family)</span>
			</Field>

			<Field
				label="Dependent"
				info="The y variable. Transform is applied before fitting — e.g. log() to make a heavy-tailed count behave more linearly. The transform shows up in the formula and the fitted/residual output is on the transformed scale."
			>
				<div class="picker-row">
					<LayerPicker
						mode="single"
						layers={nodeLayers}
						value={effectiveDependent}
						onChange={(id) => (mdDependentId = /** @type {string | null} */ (id))}
					/>
					<select
						class="tx"
						title="Transform applied to the dependent before fitting"
						value={mdDependentTransform}
						onchange={(e) =>
							(mdDependentTransform = /** @type {Transform} */ (e.currentTarget.value))}
					>
						{#each TRANSFORM_OPTIONS as o (o.value)}
							<option value={o.value}>{o.label}</option>
						{/each}
					</select>
				</div>
			</Field>

			<Field
				label="Covariates"
				info="The x variables. Multi-select — checked layers become regression columns. The 'Transforms' section below configures each one's transform + optional spatial lag (replace raw values with a distance-weighted mean of neighbours)."
			>
				<LayerPicker
					mode="multi"
					layers={covCandidates}
					value={mdCovariateIds}
					onChange={(ids) => (mdCovariateIds = /** @type {string[]} */ (ids))}
					emptyHint="No other node layers available as covariates."
				/>
			</Field>

			{#if mdCovariateIds.length > 0}
				<Field
					label="Transforms"
					info="Per-covariate transform (log / log1p / sqrt) and optional spatial lag toggle. Lag replaces the raw covariate with a kernel-weighted mean of its neighbours — useful for testing whether a covariate's effect is purely local or spreads spatially."
				>
					<div class="tx-list">
						{#each mdCovariateIds as cid (cid)}
							{@const c = nodeLayers.find((l) => l.id === cid)}
							{@const lag = mdCovariateLags[cid]}
							{#if c}
								<div class="tx-row">
									<span class="tx-name" title={c.name}>{c.slug}</span>
									<select
										class="tx"
										value={mdCovariateTransforms[cid] ?? 'none'}
										onchange={(e) => setCovTransform(cid, e.currentTarget.value)}
									>
										{#each TRANSFORM_OPTIONS as o (o.value)}
											<option value={o.value}>{o.label}</option>
										{/each}
									</select>
									<button
										type="button"
										class="lag-toggle"
										class:on={!!lag}
										onclick={() => toggleCovLag(cid)}
										title="Spatial lag: replace this covariate with a kernel-weighted mean of its neighbours (mean mode, exclude self)"
									>
										lag
									</button>
								</div>
								{#if lag}
									<div class="tx-lag">
										<select
											class="tx"
											value={lag.kernel}
											onchange={(e) => patchCovLag(cid, { kernel: e.currentTarget.value })}
										>
											<option value="exp">exp</option>
											<option value="gauss">gauss</option>
											<option value="power">power</option>
										</select>
										<label
											class="tx-lag-num"
											title={lag.kernel === 'power' ? 'Exponent β' : 'Decay d₀ (km)'}
										>
											<span>{lag.kernel === 'power' ? 'β' : 'd₀'}</span>
											<input
												type="number"
												step="0.1"
												min={lag.kernel === 'power' ? 0.5 : 0.1}
												max={lag.kernel === 'power' ? 5 : 50}
												value={lag.decay}
												onchange={(e) => patchCovLag(cid, { decay: +e.currentTarget.value })}
											/>
										</label>
										<label class="tx-lag-num" title="Max neighbour distance (km)">
											<span>max</span>
											<input
												type="number"
												step="0.5"
												min="0.5"
												max="100"
												value={lag.maxDist}
												onchange={(e) => patchCovLag(cid, { maxDist: +e.currentTarget.value })}
											/>
										</label>
									</div>
								{/if}
							{/if}
						{/each}
					</div>
				</Field>
			{/if}

			<Field
				label="Weights"
				info="Optional per-area observation weights (typically population) for weighted least squares. Areas with larger weights contribute more to the fit. Set to 'unweighted' for ordinary GLM."
			>
				<select
					value={mdWeightsId ?? ''}
					onchange={(e) => (mdWeightsId = e.currentTarget.value || null)}
					disabled={mdGwrEnabled}
					title={mdGwrEnabled
						? 'Weights are ignored in v0 GWR — disable GWR to use weights.'
						: 'Optional per-area observation weights (e.g. population for inverse-variance scaling). Rows with zero or missing weight drop.'}
				>
					<option value="">— unweighted —</option>
					{#each nodeLayers.filter((l) => l.id !== effectiveDependent && !mdCovariateIds.includes(l.id)) as l (l.id)}
						<option value={l.id}>{l.name}</option>
					{/each}
				</select>
			</Field>

			<Field
				label="Offset"
				info="An offset is a covariate with its coefficient pinned at 1 — useful for converting counts to rates. Classic example: Poisson rate model with offset = log(population), so the fitted output is per-capita."
			>
				<div class="picker-row">
					<select
						value={mdOffsetId ?? ''}
						onchange={(e) => (mdOffsetId = e.currentTarget.value || null)}
						disabled={mdGwrEnabled}
						title={mdGwrEnabled
							? 'Offsets are ignored in v0 GWR. Disable GWR to use an offset.'
							: 'Optional offset — added to the linear predictor without estimating a coefficient. Classic use: log(population) for a Poisson rate model.'}
					>
						<option value="">— none —</option>
						{#each nodeLayers.filter((l) => l.id !== effectiveDependent && !mdCovariateIds.includes(l.id) && l.id !== mdWeightsId) as l (l.id)}
							<option value={l.id}>{l.name}</option>
						{/each}
					</select>
					{#if mdOffsetId && !mdGwrEnabled}
						<select
							class="tx"
							value={mdOffsetTransform}
							onchange={(e) =>
								(mdOffsetTransform = /** @type {Transform} */ (e.currentTarget.value))}
							title="Most rate models want log() of an exposure layer like population"
						>
							{#each TRANSFORM_OPTIONS as o (o.value)}
								<option value={o.value}>{o.label}</option>
							{/each}
						</select>
					{/if}
				</div>
			</Field>

			<Field
				label="Study area"
				info="When on, fit the NLM only on areas in the currently drawn study area (rather than every area at the chosen scale). Output children are also restricted to the subset — the residual/fitted choropleth renders only inside the area."
			>
				<label class="inline-check" class:disabled={!studyAreaAvailable}>
					<input
						type="checkbox"
						checked={mdStudyAreaScoped}
						disabled={!studyAreaAvailable}
						onchange={(e) => (mdStudyAreaScoped = e.currentTarget.checked)}
					/>
					<span>
						{studyAreaAvailable ? `Restrict to ${studyAreaSize} areas` : 'No study area drawn'}
					</span>
				</label>
			</Field>

			<Field
				label="GWR"
				info="Geographically weighted regression: fits one local regression per area, weighting neighbouring observations by a spatial kernel. Output is one β surface per coefficient + local R² + actual-bandwidth diagnostic, so you can see how relationships vary across space. Weights and offsets are ignored in v0."
			>
				<label
					class="inline-check"
					title="Geographically weighted regression: fits one local weighted regression per area, producing per-coefficient surfaces (β_intercept, β_<x>, …) + local R² + bandwidth diagnostics. Weights and offsets are ignored in v0."
				>
					<input
						type="checkbox"
						checked={mdGwrEnabled}
						onchange={(e) => (mdGwrEnabled = e.currentTarget.checked)}
					/>
					<span>local regressions per area</span>
				</label>
			</Field>

			{#if mdGwrEnabled}
				<Field
					label="Kernel type"
					info="Fixed: bandwidth is a distance in km, same for every focal point — sensible when the spatial process has a meaningful length scale (e.g. commuting catchment). Adaptive: bandwidth at each focal point is the distance to its k-th nearest neighbour — better when point density varies (e.g. urban vs rural)."
				>
					<div class="seg" role="radiogroup" aria-label="GWR kernel type">
						<button
							type="button"
							class:active={mdGwrKernelType === 'fixed'}
							aria-pressed={mdGwrKernelType === 'fixed'}
							onclick={() => (mdGwrKernelType = 'fixed')}
							title="Bandwidth is a fixed distance in km — same for every focal point"
						>
							fixed (km)
						</button>
						<button
							type="button"
							class:active={mdGwrKernelType === 'adaptive'}
							aria-pressed={mdGwrKernelType === 'adaptive'}
							onclick={() => (mdGwrKernelType = 'adaptive')}
							title="Bandwidth at each focal point is the distance to its k-th nearest neighbour"
						>
							adaptive (k-nn)
						</button>
					</div>
				</Field>
				<Field
					label="Kernel shape"
					info="Bi-square: hard cutoff at the bandwidth (weight = (1 − (d/h)²)² for d ≤ h, else 0). Cleaner per-area subset but loses some smoothness. Gaussian: every point gets non-zero weight, decays smoothly (exp(−½(d/h)²)) — better for smooth processes."
				>
					<div class="seg" role="radiogroup" aria-label="GWR kernel shape">
						<button
							type="button"
							class:active={mdGwrKernelShape === 'bi-square'}
							aria-pressed={mdGwrKernelShape === 'bi-square'}
							onclick={() => (mdGwrKernelShape = 'bi-square')}
							title="(1 - (d/h)²)² for d ≤ h, else 0 — compact support, drops to zero at the bandwidth"
						>
							bi-square
						</button>
						<button
							type="button"
							class:active={mdGwrKernelShape === 'gaussian'}
							aria-pressed={mdGwrKernelShape === 'gaussian'}
							onclick={() => (mdGwrKernelShape = 'gaussian')}
							title="exp(-½(d/h)²) — every point gets non-zero weight, decays smoothly"
						>
							gaussian
						</button>
					</div>
				</Field>
				<Field
					label="Bandwidth"
					info="Auto: golden-section search on residual SS picks a bandwidth that gives ≥70% of areas a valid fit. Manual: enter a km value (fixed kernel) or k-neighbour count (adaptive). Smaller bandwidth = more local detail but noisier; larger = smoother but converges to global GLM."
				>
					<div class="picker-row">
						<label
							class="inline-check"
							title="Golden-section search on residual SS picks the bandwidth automatically"
						>
							<input
								type="checkbox"
								checked={mdGwrBwAuto}
								onchange={(e) => (mdGwrBwAuto = e.currentTarget.checked)}
							/>
							<span>auto</span>
						</label>
						{#if !mdGwrBwAuto}
							<input
								type="number"
								class="bw-num"
								step={mdGwrKernelType === 'adaptive' ? 1 : 0.5}
								min={mdGwrKernelType === 'adaptive' ? 5 : 1}
								max={mdGwrKernelType === 'adaptive' ? 9999 : 500}
								value={mdGwrBandwidth}
								onchange={(e) => (mdGwrBandwidth = +e.currentTarget.value)}
								title={mdGwrKernelType === 'adaptive'
									? 'k = number of nearest neighbours (integer ≥ 5)'
									: 'h = bandwidth in km'}
							/>
							<span class="readonly bw-unit"
								>{mdGwrKernelType === 'adaptive' ? 'neighbours' : 'km'}</span
							>
						{/if}
					</div>
				</Field>
			{/if}

			{#if formulaPreview}
				<Field label="Formula">
					<code class="formula">{formulaPreview}</code>
				</Field>
			{/if}

			{#if !mdError && mdCovariateIds.length > 0}
				<p class="hint">Saves as → {slugify(mdEffective)}</p>
			{/if}
			{#if mdError}
				<p class="err-msg">{mdError}</p>
			{/if}
			<button type="submit" class="primary" disabled={mdCovariateIds.length === 0}>
				Fit model
			</button>
		{/if}
	</form>
</div>

<style>
	.stack {
		display: flex;
		flex-direction: column;
		gap: var(--spacing-3);
	}
	.primary {
		align-self: flex-end;
		padding: 2px var(--spacing-2);
		background: var(--color-accent);
		color: var(--color-accent-fg);
		border: none;
		border-radius: var(--radius);
		font-size: var(--text-sm);
		cursor: pointer;
	}
	.primary:disabled {
		background: var(--color-line);
		cursor: default;
	}
	.layers {
		list-style: none;
		margin: 0;
		padding: 0;
		display: flex;
		flex-direction: column;
		gap: var(--spacing-1);
	}
	.layer {
		display: grid;
		grid-template-columns: auto 1fr auto auto;
		align-items: center;
		gap: var(--spacing-1);
		font-size: var(--text-sm);
		padding: 2px var(--spacing-1);
		border-radius: var(--radius);
	}
	.layer.active {
		background: rgba(31, 35, 40, 0.06);
	}
	/* Channel-switcher: lets the user flip between this model's outputs (fitted /
	   residual) without leaving the dock. Sits on the parent row's full width. */
	.channels {
		grid-column: 1 / -1;
		display: flex;
		align-items: center;
		gap: var(--spacing-1);
		margin-top: 2px;
		padding-left: var(--spacing-3);
	}
	.channels-label {
		font-size: var(--text-xs);
		color: var(--color-muted);
	}
	.extra-hint {
		font-size: var(--text-xs);
		color: var(--color-hint);
		font-style: italic;
		margin-left: var(--spacing-1);
	}
	.chan {
		padding: 1px var(--spacing-2);
		font-size: var(--text-xs);
		font-family: ui-monospace, monospace;
		background: #fff;
		color: var(--color-muted);
		border: 1px solid var(--color-line);
		border-radius: var(--radius-pill);
		cursor: pointer;
	}
	.chan:hover {
		color: var(--color-text);
	}
	.chan.active {
		background: var(--color-accent);
		color: var(--color-accent-fg);
		border-color: var(--color-accent);
	}
	/* .radio styles removed — model rows no longer carry child-row radios since
	   activation moved to SavedLayers in the Node data panel and to the
	   inline .channels switcher above. */
	/* Family badge — 3-letter token (NLM / SIM / GWR) with a role-based
	   border so the family is visible from a glance. Width is intrinsic
	   instead of 1em so the longer 'SIM' / 'GWR' fits. */
	.kind {
		color: var(--color-muted);
		font-size: var(--text-xs);
		font-weight: 600;
		text-align: center;
		padding: 0 4px;
		border-radius: var(--radius);
		border: 1px solid var(--color-line);
		min-width: 2.6em;
	}
	.kind.family-sim {
		color: #6f3aa0;
		border-color: rgba(111, 58, 160, 0.4);
		background: rgba(111, 58, 160, 0.06);
	}
	.kind.family-gwr {
		color: #2a7f5f;
		border-color: rgba(42, 127, 95, 0.4);
		background: rgba(42, 127, 95, 0.06);
	}
	.kind.family-nlm {
		color: #205ea6;
		border-color: rgba(32, 94, 166, 0.4);
		background: rgba(32, 94, 166, 0.06);
	}
	.name-btn {
		background: transparent;
		border: none;
		cursor: pointer;
		text-align: left;
		padding: 0;
		font: inherit;
		color: var(--color-text);
		display: flex;
		gap: 4px;
		align-items: baseline;
		min-width: 0;
	}
	.name {
		overflow: hidden;
		text-overflow: ellipsis;
		white-space: nowrap;
	}
	.slug {
		color: var(--color-hint);
		font-size: var(--text-xs);
	}
	.meta {
		color: var(--color-hint);
		font-size: var(--text-xs);
		font-variant-numeric: tabular-nums;
	}
	.meta.err {
		color: #cf222e;
	}
	.del {
		background: transparent;
		border: none;
		color: var(--color-hint);
		cursor: pointer;
		font-size: var(--text-sm);
		padding: 0 2px;
	}
	.del:hover {
		color: var(--color-text);
	}
	.details {
		grid-column: 1 / -1;
		margin-top: var(--spacing-1);
		padding: var(--spacing-1) var(--spacing-2);
		background: rgba(0, 0, 0, 0.03);
		border-radius: var(--radius);
		display: flex;
		flex-direction: column;
		gap: var(--spacing-1);
		font-size: var(--text-xs);
	}
	/* .coefs and .fit-row live in ModelResults.svelte now (used in both the
	   dock's expand pane and the right-sidebar Model results panel). */
	.add {
		display: flex;
		flex-direction: column;
		gap: var(--spacing-1);
		padding-top: var(--spacing-2);
		border-top: 1px solid var(--color-line);
	}
	.add-head {
		font-weight: 600;
		color: var(--color-text);
		font-size: var(--text-sm);
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
	.readonly {
		color: var(--color-muted);
		font-size: var(--text-sm);
	}
	.formula {
		font-family: ui-monospace, monospace;
		font-size: var(--text-sm);
		color: var(--color-text);
		padding: 2px var(--spacing-2);
		background: rgba(0, 0, 0, 0.04);
		border-radius: var(--radius);
		display: inline-block;
		word-break: break-all;
	}
	/* Dependent picker + its inline transform select share one row. */
	.picker-row {
		display: flex;
		gap: var(--spacing-1);
		align-items: center;
		min-width: 0;
	}
	/* Layer-picker (the main control) gets most of the row; the inline
	   transform select (.tx) sits compact on the right. Previously both
	   were flex: 1 1 auto, so they ended up ~50/50 and the layer name got
	   clipped on long slugs. */
	.picker-row :global(select:not(.tx)) {
		flex: 1 1 70%;
		min-width: 0;
	}
	.picker-row :global(select.tx) {
		flex: 0 0 auto;
		width: auto;
		min-width: 5em;
	}
	.picker-row :global(.readonly) {
		flex: 0 0 auto;
	}
	/* Per-covariate transform list: one row per selected covariate. */
	.tx-list {
		display: flex;
		flex-direction: column;
		gap: 2px;
		max-height: 180px;
		overflow-y: auto;
	}
	.tx-row {
		display: grid;
		grid-template-columns: 1fr auto auto;
		gap: var(--spacing-1);
		align-items: center;
		font-size: var(--text-xs);
	}
	.lag-toggle {
		background: transparent;
		border: 1px solid var(--color-line);
		color: var(--color-muted);
		font-size: var(--text-xs);
		padding: 1px var(--spacing-1);
		border-radius: var(--radius);
		cursor: pointer;
	}
	.lag-toggle.on {
		background: var(--color-accent);
		color: var(--color-accent-fg);
		border-color: var(--color-accent);
	}
	.tx-lag {
		display: flex;
		gap: var(--spacing-1);
		align-items: center;
		padding-left: calc(var(--spacing-3) + var(--spacing-2));
		margin-top: -2px;
		margin-bottom: 2px;
		font-size: var(--text-xs);
	}
	.tx-lag-num {
		display: inline-flex;
		gap: 4px;
		align-items: baseline;
		color: var(--color-muted);
	}
	.tx-lag-num input {
		width: 4em;
		font-size: var(--text-xs);
	}
	.tx-name {
		font-family: ui-monospace, monospace;
		color: var(--color-text);
		overflow: hidden;
		text-overflow: ellipsis;
		white-space: nowrap;
	}
	select.tx {
		flex: 0 0 auto;
		font-size: var(--text-xs);
	}
	.inline-check {
		display: inline-flex;
		gap: var(--spacing-1);
		align-items: center;
		font-size: var(--text-xs);
		color: var(--color-muted);
	}
	.bw-num {
		width: 5em;
		font-size: var(--text-xs);
	}
	.bw-unit {
		font-size: var(--text-xs);
	}
	.inline-check.disabled {
		opacity: 0.5;
		cursor: not-allowed;
	}
	.readonly.absorbed {
		font-family: ui-monospace, monospace;
		font-size: var(--text-xs);
		color: var(--color-muted);
		padding: 2px var(--spacing-2);
		background: rgba(0, 0, 0, 0.04);
		border-radius: var(--radius);
	}
	.err-msg {
		font-size: var(--text-xs);
		color: #cf222e;
		margin: 0;
	}
	/* Yellow warning for the SIM expand-to-all-OD + large-scale combo. */
	.flow-cap-warn-sim {
		font-size: var(--text-xs);
		color: #b95000;
		margin: 0;
		padding: 2px var(--spacing-2);
		background: rgba(185, 80, 0, 0.08);
		border-left: 2px solid #b95000;
		border-radius: var(--radius);
	}
	.hint {
		font-size: var(--text-xs);
		color: var(--color-hint);
		margin: 0;
	}
</style>

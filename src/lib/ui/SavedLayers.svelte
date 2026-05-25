<script>
	// Saved-layer manager: switch which layer the map renders, peek at saved
	// parameters, and delete. Lives at the top of the Node data panel so it's
	// the primary "what am I looking at?" control.
	//
	// Extracted verbatim from the LayerCalculator's saved-layer list block; the
	// calculator dock now focuses purely on building calcs + smooths and uses
	// the chip palette under each form as its own (limited) layer reference.
	//
	// Model parents (kind: 'model') appear in the list for management but their
	// radio is disabled — they hold a spec, not per-area values, so activating
	// them would render an empty map. Activate one of their `model-output`
	// children (e.g. `mymodel — fitted`) instead.

	import Field from './Field.svelte';
	import KernelCurve from './KernelCurve.svelte';
	import { selection } from '$lib/state/selection.svelte.js';
	import { layers } from '$lib/state/layers.svelte.js';

	/** @typedef {'node' | 'flow' | 'all'} DomainFilter */
	/** @type {{ manifest: any, domain?: DomainFilter }} */
	let { manifest, domain = 'all' } = $props();

	// Filter items to the requested domain. 'all' (default) keeps the
	// pre-existing behaviour — shows everything, mounted at the top of the
	// Node-data panel. 'node' / 'flow' lets the same component render in
	// the opposite sidebar, focused on that domain's layers. Model parents
	// and their outputs are matched on their own domain field (NLM → node,
	// SIM → flow), so they land in the correct panel naturally.
	const visibleItems = $derived.by(() => {
		if (domain === 'all') return layers.items;
		return layers.items.filter((l) => (l.domain ?? 'node') === domain);
	});
	// Activation rule for flow-domain layers stays "non-activatable" for
	// filter layers (they're inputs to calcs) but model-output flow layers
	// (SIM fitted/residual) ARE the map source via displayed.flowsData.
	// The shared activatable() helper below handles this — no change needed
	// per-domain.

	let expandedId = $state(/** @type {string | null} */ (null));

	function toggleExpanded(id) {
		expandedId = expandedId === id ? null : id;
	}

	function setActive(id) {
		layers.setActive(layers.activeId === id ? null : id);
	}

	function onRemoveAll() {
		const n = layers.items.length;
		if (n === 0) return;
		const msg = `Delete all ${n} saved layer${n === 1 ? '' : 's'}? This cannot be undone.`;
		if (typeof window === 'undefined' || !window.confirm(msg)) return;
		layers.clearAll();
		expandedId = null;
	}

	function onSmoothKernel(layer, kernel) {
		const crossesPower = (layer.kernel === 'power') !== (kernel === 'power');
		const patch = crossesPower ? { kernel, decay: kernel === 'power' ? 2 : 1 } : { kernel };
		layers.updateSmoothParams(layer.id, patch);
	}

	function fieldLabel(fieldId) {
		return manifest?.datasets?.[selection.dataset]?.fields?.[fieldId]?.label ?? fieldId;
	}

	function valueLabel(layer, fieldId, valueId) {
		const ds = manifest?.datasets?.[layer.dataset];
		const values = ds?.fields?.[fieldId]?.values;
		return values?.find((v) => v.id === valueId)?.label ?? String(valueId);
	}

	function datasetLabel(layer) {
		return manifest?.datasets?.[layer.dataset]?.name ?? layer.dataset;
	}

	function kindGlyph(layer) {
		if (layer.kind === 'smooth') return '◈';
		if (layer.kind === 'calc') return 'ƒ';
		if (layer.kind === 'model') {
			// Distinguish NLM / SIM / NLM-GWR — the dock uses a colored badge
			// for the same purpose, but here we keep the single-character
			// glyph slot. 'S' for SIM, 'G' for GWR-enabled NLM, 'M' otherwise.
			if (layer.family === 'sim') return 'S';
			if (layer.spec?.gwr?.enabled) return 'G';
			return 'M';
		}
		if (layer.kind === 'model-output') return '↳';
		if (layer.domain === 'flow') return '~';
		return '◆';
	}

	// Activation rules:
	//  - model parents stay non-activatable: they have no per-area or per-OD
	//    results, just spec + coefficients. Use a child.
	//  - model-outputs are always activatable regardless of domain. Node-domain
	//    (NLM fitted/residual) renders on the choropleth via displayed.data;
	//    flow-domain (SIM) renders via displayed.flowsData.
	//  - flow-domain *filter* layers are activatable too — they become the
	//    map's flow source via displayed.flowsData (replacing the live
	//    flow query). Previously they were inputs-only, but with the new
	//    "Saved flow layers" panel that's where you switch flow sources.
	function activatable(layer) {
		if (layer.scale !== selection.scale) return false;
		if (layer.kind === 'model') return false;
		return true;
	}
</script>

<div class="layers-wrap">
	{#if visibleItems.length === 0}
		<p class="hint">No saved layers yet — configure below and click <strong>Save layer</strong>.</p>
	{:else}
		<ul class="layers">
			{#if domain !== 'flow'}
				<li class="layer live" class:active={layers.activeId === null}>
					<button
						type="button"
						class="radio"
						aria-pressed={layers.activeId === null}
						onclick={() => layers.setActive(null)}
						title="Show live selection"
					>
						{layers.activeId === null ? '●' : '○'}
					</button>
					<span class="kind" title="Live selection">·</span>
					<span class="name muted">live selection</span>
				</li>
			{/if}
			{#each visibleItems as layer (layer.id)}
				{@const isOff = !activatable(layer)}
				{@const offReason =
					layer.scale !== selection.scale
						? `Different scale (${layer.scale})`
						: layer.kind === 'model'
							? 'Activate a model output (fitted / residual) instead'
							: 'Set active'}
				{@const isActive = layers.activeId === layer.id}
				<li
					class="layer"
					class:active={isActive}
					class:off={isOff}
					class:child={layer.kind === 'model-output'}
				>
					<button
						type="button"
						class="radio"
						aria-pressed={isActive}
						disabled={isOff}
						onclick={() => setActive(layer.id)}
						title={offReason}
					>
						{isActive ? '●' : '○'}
					</button>
					<span class="kind" title="{layer.domain ?? 'node'} {layer.kind}">{kindGlyph(layer)}</span>
					<button
						type="button"
						class="name-btn"
						onclick={() => toggleExpanded(layer.id)}
						title="Show parameters"
					>
						<span class="name">{layer.name}</span>
						{#if layer.slug !== layer.name}
							<span class="slug">({layer.slug})</span>
						{/if}
					</button>
					{#if layers.loading.has(layer.id)}
						<span class="meta">…</span>
					{:else if layers.errors.get(layer.id)}
						<span class="meta err" title={layers.errors.get(layer.id)}>!</span>
					{:else if layers.results.get(layer.id)}
						<span class="meta">{layers.results.get(layer.id).size}</span>
					{/if}
					<button
						type="button"
						class="del"
						onclick={() => layers.remove(layer.id)}
						title="Delete layer"
					>
						×
					</button>
					{#if expandedId === layer.id}
						<div class="details">
							{#if layer.kind === 'filter'}
								<div class="line">
									<span class="k">Dataset</span><span>{datasetLabel(layer)}</span>
								</div>
								<div class="line"><span class="k">Scale</span><span>{layer.scale}</span></div>
								{#if layer.domain === 'flow'}
									<div class="line">
										<span class="k">Years</span>
										<span
											>{layer.yearMin === layer.yearMax
												? layer.yearMin
												: `${layer.yearMin}–${layer.yearMax}`}</span
										>
									</div>
								{:else}
									<div class="line"><span class="k">Year</span><span>{layer.year}</span></div>
								{/if}
								{#if layer.filters && Object.keys(layer.filters).length > 0}
									{#each Object.entries(layer.filters) as [fieldId, vals] (fieldId)}
										{#if vals && vals.length}
											<div class="line">
												<span class="k">{fieldLabel(fieldId)}</span>
												<span class="chips">
													{#each vals as v (v)}
														<span class="chip">{valueLabel(layer, fieldId, v)}</span>
													{/each}
												</span>
											</div>
										{/if}
									{/each}
								{:else}
									<div class="line">
										<span class="k">Filters</span><span class="muted">none</span>
									</div>
								{/if}
							{:else if layer.kind === 'calc'}
								<div class="line"><span class="k">Scale</span><span>{layer.scale}</span></div>
								<div class="line">
									<span class="k">Expression</span><code>{layer.expression}</code>
								</div>
							{:else if layer.kind === 'smooth'}
								{@const input = layers.items.find((i) => i.id === layer.inputId)}
								<div class="line">
									<span class="k">Input</span>
									<span class:muted={!input}>{input ? input.name : '— deleted —'}</span>
								</div>
								<div class="line"><span class="k">Scale</span><span>{layer.scale}</span></div>
								<Field label="Kernel">
									<select
										value={layer.kernel}
										onchange={(e) => onSmoothKernel(layer, e.currentTarget.value)}
									>
										<option value="exp">exponential</option>
										<option value="gauss">gaussian</option>
										<option value="power">power</option>
									</select>
								</Field>
								<Field
									label={layer.kernel === 'power' ? 'Exponent β' : 'Decay d₀ (km)'}
									value={layer.decay}
								>
									<input
										type="range"
										min={layer.kernel === 'power' ? 0.5 : 0.1}
										max={layer.kernel === 'power' ? 3 : 10}
										step="0.1"
										value={layer.decay}
										oninput={(e) =>
											layers.updateSmoothParams(
												layer.id,
												{ decay: +e.currentTarget.value },
												{ persist: false }
											)}
										onchange={(e) =>
											layers.updateSmoothParams(layer.id, { decay: +e.currentTarget.value })}
									/>
								</Field>
								<Field label="Max distance (km)" value={layer.maxDist}>
									<input
										type="range"
										min="0.5"
										max="100"
										step="0.5"
										value={layer.maxDist}
										oninput={(e) =>
											layers.updateSmoothParams(
												layer.id,
												{ maxDist: +e.currentTarget.value },
												{ persist: false }
											)}
										onchange={(e) =>
											layers.updateSmoothParams(layer.id, { maxDist: +e.currentTarget.value })}
									/>
								</Field>
								<KernelCurve kernel={layer.kernel} decay={layer.decay} maxDist={layer.maxDist} />
								<Field label="Output">
									<div class="seg" role="radiogroup" aria-label="Smoothing output">
										<button
											type="button"
											class:active={layer.mode === 'mean'}
											aria-pressed={layer.mode === 'mean'}
											onclick={() => layers.updateSmoothParams(layer.id, { mode: 'mean' })}
										>
											Average
										</button>
										<button
											type="button"
											class:active={layer.mode === 'sum'}
											aria-pressed={layer.mode === 'sum'}
											onclick={() => layers.updateSmoothParams(layer.id, { mode: 'sum' })}
										>
											Sum
										</button>
									</div>
								</Field>
								<Field label="Include self">
									<input
										type="checkbox"
										checked={layer.includeSelf}
										onchange={(e) =>
											layers.updateSmoothParams(layer.id, {
												includeSelf: e.currentTarget.checked
											})}
									/>
								</Field>
							{:else if layer.kind === 'model'}
								<div class="line"><span class="k">Scale</span><span>{layer.scale}</span></div>
								<div class="line">
									<span class="k">Family</span><span>{layer.spec?.glm?.family ?? '—'}</span>
								</div>
								<div class="line">
									<span class="k">Link</span><span>{layer.spec?.glm?.link ?? '—'}</span>
								</div>
								<div class="line">
									<span class="k">Outputs</span>
									<span class="muted"
										>{(layer.childIds ?? []).length} (activate one to see on map)</span
									>
								</div>
							{:else if layer.kind === 'model-output'}
								{@const par = layers.items.find((i) => i.id === layer.parentId)}
								<div class="line">
									<span class="k">From model</span>
									<span class:muted={!par}>{par ? par.name : '— deleted —'}</span>
								</div>
								<div class="line">
									<span class="k">Channel</span><span>{layer.channel}</span>
								</div>
							{/if}
						</div>
					{/if}
				</li>
			{/each}
		</ul>
		{#if visibleItems.length > 0}
			<!-- "Remove all" bulk-delete lives here now (it used to be in
			     LayerCalculator). Confirms before nuking — the action
			     wipes everything regardless of domain, so we call it out
			     in the confirm text. -->
			<div class="bulk-row">
				<button
					type="button"
					class="bulk-del"
					onclick={onRemoveAll}
					title={domain === 'flow'
						? 'Delete every saved layer (both node and flow). Confirms first.'
						: 'Delete every saved layer (confirms first).'}
				>
					Remove all saved layers
				</button>
			</div>
		{/if}
	{/if}
</div>

<style>
	.layers-wrap {
		display: flex;
		flex-direction: column;
		gap: var(--spacing-1);
	}
	.layers {
		list-style: none;
		margin: 0;
		padding: 0;
		display: flex;
		flex-direction: column;
		gap: 1px;
		/* Cap the list to ~12 rows of scrollable content. The panel can grow as
		   the user saves filters, calcs, smooths, and model outputs; without
		   this cap a power user with a dozen layers + several model children
		   pushes the rest of the Node data panel off-screen. The active row
		   stays in view via the layer.active background highlight. */
		max-height: 320px;
		overflow-y: auto;
		padding-right: var(--spacing-1); /* leave room for the scrollbar gutter */
	}
	.layer {
		display: grid;
		grid-template-columns: auto auto 1fr auto auto;
		align-items: center;
		gap: var(--spacing-1);
		font-size: var(--text-sm);
		padding: 2px var(--spacing-1);
		border-radius: var(--radius);
	}
	.layer.active {
		background: rgba(31, 35, 40, 0.06);
	}
	.layer.off {
		opacity: 0.5;
	}
	.layer.live {
		font-style: italic;
	}
	/* Model-output children are grouped visually under their parent: indented,
	   a vertical hairline on the left, and slightly muted so the parent reads
	   as the anchor. `items[]` already places children immediately after their
	   parent, so adjacency is free. */
	.layer.child {
		margin-left: var(--spacing-3);
		border-left: 2px solid var(--color-line);
		padding-left: var(--spacing-2);
		font-size: var(--text-xs);
	}
	.layer.child .name {
		color: var(--color-muted);
	}
	.radio {
		background: transparent;
		border: none;
		cursor: pointer;
		font-size: var(--text-sm);
		color: var(--color-muted);
		padding: 0 2px;
	}
	.radio:disabled {
		cursor: default;
	}
	.kind {
		color: var(--color-hint);
		font-size: var(--text-xs);
		width: 1em;
		text-align: center;
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
		gap: 2px;
		font-size: var(--text-xs);
	}
	.line {
		display: grid;
		grid-template-columns: 70px 1fr;
		gap: var(--spacing-2);
	}
	.k {
		color: var(--color-muted);
	}
	.chips {
		display: flex;
		flex-wrap: wrap;
		gap: 2px;
	}
	.chip {
		padding: 0 var(--spacing-1);
		border: 1px solid var(--color-line);
		border-radius: var(--radius-pill);
		background: #fff;
		color: var(--color-muted);
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
	.hint {
		font-size: var(--text-xs);
		color: var(--color-hint);
		margin: 0;
	}
	.muted {
		color: var(--color-muted);
	}
	.bulk-row {
		display: flex;
		justify-content: flex-end;
		margin-top: var(--spacing-1);
	}
	.bulk-del {
		background: transparent;
		border: 1px solid var(--color-line);
		border-radius: var(--radius);
		padding: 2px var(--spacing-2);
		font-size: var(--text-xs);
		color: var(--color-muted);
		cursor: pointer;
	}
	.bulk-del:hover {
		border-color: var(--color-muted);
		color: var(--color-text);
	}
	code {
		font-family: ui-monospace, monospace;
		font-size: var(--text-xs);
		color: var(--color-text);
	}
</style>

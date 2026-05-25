<script>
	import Field from './Field.svelte';
	import KernelCurve from './KernelCurve.svelte';
	import LayerPicker from './LayerPicker.svelte';
	import { selection } from '$lib/state/selection.svelte.js';
	import { layers } from '$lib/state/layers.svelte.js';
	import { slugify } from '$lib/data/layer-calc.js';

	let calcName = $state('');
	let calcNameTouched = $state(false);
	let calcExpr = $state('');
	let calcError = $state(/** @type {string | null} */ (null));
	let exprEditor = $state(/** @type {HTMLDivElement | null} */ (null));
	let dragSlug = $state(/** @type {string | null} */ (null));
	/** @type {'node' | 'flow'} */
	let calcDomain = $state('node');
	/** @type {'inflow' | 'outflow' | 'net'} */
	let flowAgg = $state('inflow');

	// Smoothed-layer form state.
	let smName = $state('');
	let smNameTouched = $state(false);
	let smInputId = $state(/** @type {string | null} */ (null));
	/** @type {'exp' | 'gauss' | 'power'} */
	let smKernel = $state('exp');
	let smDecay = $state(1);
	let smMaxDist = $state(5);
	/** @type {'mean' | 'sum'} */
	let smMode = $state('mean');
	let smIncludeSelf = $state(true);
	let smError = $state(/** @type {string | null} */ (null));

	const sameScale = $derived(layers.items.filter((l) => l.scale === selection.scale));
	const slugKind = $derived(new Map(sameScale.map((l) => [l.slug, l.kind])));
	const slugDomain = $derived(new Map(sameScale.map((l) => [l.slug, l.domain ?? 'node'])));
	const nodeLayers = $derived(sameScale.filter((l) => (l.domain ?? 'node') === 'node'));
	const flowLayers = $derived(sameScale.filter((l) => l.domain === 'flow'));

	// The chosen input falls back to the first eligible layer when it no longer
	// exists (after a scale switch or a deletion) — derived, so no effect needed.
	const smInput = $derived(
		smInputId && nodeLayers.some((l) => l.id === smInputId)
			? smInputId
			: (nodeLayers[0]?.id ?? null)
	);

	// Default smoothed-layer name — same approach as the calc form: a uniquified
	// suggestion ("<input> smoothed") shown as a placeholder, used verbatim until
	// the user types their own (smNameTouched).
	const smInputName = $derived(nodeLayers.find((l) => l.id === smInput)?.name ?? '');
	const smSuggestion = $derived(
		layers.uniqueName(smInputName ? `${smInputName} smoothed` : 'Smoothed 1')
	);
	const smEffective = $derived(smNameTouched ? smName : smSuggestion);

	// The decay control means a length scale (km) for exp/gauss and a positive
	// exponent for power — its label and range adapt to the kernel.
	const decayLabel = $derived(smKernel === 'power' ? 'Exponent β' : 'Decay d₀ (km)');
	const decayMin = $derived(smKernel === 'power' ? 0.5 : 0.1);
	const decayMax = $derived(smKernel === 'power' ? 3 : 10);
	const decayStep = $derived(0.1);

	// Default name for a calc layer: sequential "Calculation N" based on how
	// many calc layers already exist at this scale. Shown as a placeholder;
	// never pre-filled into the input's real value, so editing clears it.
	const calcCount = $derived(sameScale.filter((l) => l.kind === 'calc').length);
	// Uniquified so the gray placeholder matches the name the layer will get.
	const calcSuggestion = $derived(layers.uniqueName(`Calculation ${calcCount + 1}`));
	const calcEffective = $derived(calcNameTouched ? calcName : calcSuggestion);

	function onSaveCalc(e) {
		e.preventDefault();
		calcError = null;
		if (!calcExpr) return;
		// calcEffective is the typed name, or the already-uniquified default.
		const finalName = calcEffective;
		const slug = slugify(finalName);
		if (!slug) {
			calcError = 'Name required';
			return;
		}
		if (calcNameTouched && layers.slugTaken(slug)) {
			calcError = 'Name already in use';
			return;
		}
		try {
			layers.saveCalc(finalName, calcExpr, calcDomain);
			calcName = '';
			calcNameTouched = false;
			calcExpr = '';
			// eslint-disable-next-line svelte/no-dom-manipulating -- editor is a controlled contenteditable, not part of Svelte's tree
			if (exprEditor) exprEditor.replaceChildren();
		} catch (err) {
			calcError = /** @type {Error} */ (err)?.message ?? String(err);
		}
	}

	// Switching kernel changes what `decay` means (length scale ↔ exponent), so
	// reset it to a sensible default when crossing the power boundary.
	function onCreateKernel(kernel) {
		const crossesPower = (smKernel === 'power') !== (kernel === 'power');
		smKernel = kernel;
		if (crossesPower) smDecay = kernel === 'power' ? 2 : 1;
	}

	function onSaveSmooth(e) {
		e.preventDefault();
		smError = null;
		// smEffective is the typed name, or the already-uniquified default.
		const finalName = smEffective;
		const slug = slugify(finalName);
		if (!slug) {
			smError = 'Name required';
			return;
		}
		if (smNameTouched && layers.slugTaken(slug)) {
			smError = 'Name already in use';
			return;
		}
		try {
			layers.saveSmooth(finalName, {
				inputId: smInput,
				kernel: smKernel,
				decay: smDecay,
				maxDist: smMaxDist,
				mode: smMode,
				includeSelf: smIncludeSelf
			});
			smName = '';
			smNameTouched = false;
		} catch (err) {
			smError = /** @type {Error} */ (err)?.message ?? String(err);
		}
	}

	// Layer-list helpers (toggleExpanded/setActive/onSmoothKernel/fieldLabel/
	// valueLabel/datasetLabel) and the `manifest` prop moved to SavedLayers
	// when the saved-layer list left the calculator — the chip palette below
	// reads directly from `layers.items` and doesn't need manifest lookups.

	// The "Remove all saved layers" bulk-delete moved to SavedLayers along
	// with the layer list. The calculator dock is purely an editor now.

	// The expression editor is a contenteditable div so saved-layer slugs can
	// render as inline chip elements (atomic, contenteditable=false). Other
	// content (operators, numbers, parens, whitespace) is plain text. The
	// canonical string for math.js is built by serialising children — slugs
	// from data-slug, everything else from textContent.
	function serializeEditor() {
		if (!exprEditor) return '';
		const parts = [];
		for (const node of exprEditor.childNodes) {
			if (node.nodeType === Node.ELEMENT_NODE) {
				const slug = /** @type {HTMLElement} */ (node).dataset?.slug;
				if (slug) {
					parts.push(` ${slug} `);
					continue;
				}
				// Unexpected element (e.g. <br> from Enter): use its text.
				parts.push(/** @type {HTMLElement} */ (node).textContent ?? '');
			} else {
				parts.push(node.textContent ?? '');
			}
		}
		return parts.join('').replace(/\s+/g, ' ').trim();
	}

	function syncFromDom() {
		calcError = null;
		calcExpr = serializeEditor();
		// Browsers leave stray <br>s and empty text nodes after delete-all;
		// normalise so the placeholder pseudo-element renders cleanly.
		if (calcExpr === '' && exprEditor) {
			const onlyArtifacts = [...exprEditor.childNodes].every(
				(n) =>
					(n.nodeType === Node.TEXT_NODE && !n.textContent?.trim()) ||
					(n.nodeType === Node.ELEMENT_NODE && /** @type {HTMLElement} */ (n).tagName === 'BR')
			);
			// eslint-disable-next-line svelte/no-dom-manipulating -- controlled contenteditable
			if (onlyArtifacts && exprEditor.childNodes.length > 0) exprEditor.replaceChildren();
		}
	}

	function makeChipNode(slug) {
		const span = document.createElement('span');
		const kind = slugKind.get(slug);
		span.className = kind === 'calc' ? 'in-chip in-chip-calc' : 'in-chip';
		span.contentEditable = 'false';
		span.dataset.slug = slug;
		span.textContent = slug;
		return span;
	}

	// True if inserting this slug into the current calc requires wrapping with
	// a flow→node aggregator (flow input feeding a node-domain expression).
	function needsAggWrap(slug) {
		return calcDomain === 'node' && slugDomain.get(slug) === 'flow';
	}

	// Insert a chip (optionally wrapped in `<agg>( … )` for cross-domain refs).
	// Wraps with single spaces so the surrounding text doesn't fuse with neighbours.
	function insertChipAtCaret(slug) {
		if (!exprEditor) return;
		exprEditor.focus();
		const sel = window.getSelection();
		let range = sel && sel.rangeCount > 0 ? sel.getRangeAt(0) : null;
		if (!range || !exprEditor.contains(range.commonAncestorContainer)) {
			range = document.createRange();
			range.selectNodeContents(exprEditor);
			range.collapse(false);
		}
		range.deleteContents();
		const wrapped = needsAggWrap(slug);
		const lead = document.createTextNode(wrapped ? ` ${flowAgg}(` : ' ');
		const chip = makeChipNode(slug);
		const tail = document.createTextNode(wrapped ? ') ' : ' ');
		range.insertNode(tail);
		range.insertNode(chip);
		range.insertNode(lead);
		const after = document.createRange();
		after.setStartAfter(tail);
		after.collapse(true);
		sel?.removeAllRanges();
		sel?.addRange(after);
		syncFromDom();
	}

	function onChipDragStart(e, slug) {
		dragSlug = slug;
		e.dataTransfer?.setData('text/plain', slug);
		if (e.dataTransfer) e.dataTransfer.effectAllowed = 'copy';
	}

	function onChipDragEnd() {
		dragSlug = null;
	}

	function onInputDragOver(e) {
		if (!dragSlug && !e.dataTransfer?.types.includes('text/plain')) return;
		e.preventDefault();
		if (e.dataTransfer) e.dataTransfer.dropEffect = 'copy';
	}

	function onInputDrop(e) {
		const slug = e.dataTransfer?.getData('text/plain') ?? dragSlug;
		if (!slug) return;
		e.preventDefault();
		// Move the caret to the drop point before inserting.
		const sel = window.getSelection();
		// @ts-ignore — both APIs are platform-specific.
		const pos = document.caretPositionFromPoint?.(e.clientX, e.clientY);
		// @ts-ignore
		const fallbackRange = document.caretRangeFromPoint?.(e.clientX, e.clientY);
		let range = null;
		if (pos) {
			range = document.createRange();
			range.setStart(pos.offsetNode, pos.offset);
			range.collapse(true);
		} else if (fallbackRange) {
			range = fallbackRange;
		}
		if (range && exprEditor?.contains(range.startContainer)) {
			sel?.removeAllRanges();
			sel?.addRange(range);
		}
		insertChipAtCaret(slug);
		dragSlug = null;
	}

	// Strip rich content from paste — keep just the text.
	function onEditorPaste(e) {
		e.preventDefault();
		const text = e.clipboardData?.getData('text/plain') ?? '';
		if (!text) return;
		document.execCommand('insertText', false, text);
	}
</script>

<div class="stack">
	<form class="calc" onsubmit={onSaveCalc}>
		<div class="calc-head">Add calculation</div>
		<Field label="Output">
			<div class="seg" role="radiogroup" aria-label="Calc output domain">
				<button
					type="button"
					class:active={calcDomain === 'node'}
					aria-pressed={calcDomain === 'node'}
					onclick={() => (calcDomain = 'node')}
				>
					Node layer
				</button>
				<button
					type="button"
					class:active={calcDomain === 'flow'}
					aria-pressed={calcDomain === 'flow'}
					onclick={() => (calcDomain = 'flow')}
				>
					Flow layer
				</button>
			</div>
		</Field>
		<Field label="Name">
			<input
				type="text"
				placeholder={calcSuggestion}
				value={calcNameTouched ? calcName : ''}
				oninput={(e) => {
					const v = /** @type {HTMLInputElement} */ (e.currentTarget).value;
					calcName = v;
					calcNameTouched = v.length > 0;
					calcError = null;
				}}
				autocomplete="off"
			/>
		</Field>
		<Field label="Expression">
			<div
				class="expr-editor"
				class:empty={!calcExpr}
				role="textbox"
				tabindex="0"
				aria-label="Expression"
				data-placeholder="click a layer below, then type operators"
				contenteditable="true"
				bind:this={exprEditor}
				oninput={syncFromDom}
				ondragover={onInputDragOver}
				ondrop={onInputDrop}
				onpaste={onEditorPaste}
				spellcheck="false"
			></div>
		</Field>
		{#if sameScale.length > 0}
			{#if nodeLayers.length > 0}
				<div class="palette-group">
					<div class="palette-head">Node layers</div>
					<div class="palette" aria-label="Available node layers — click to insert">
						{#each nodeLayers as l (l.id)}
							{@const disabled = calcDomain === 'flow'}
							<button
								type="button"
								class="layer-chip"
								class:calc={l.kind === 'calc'}
								class:dim={disabled}
								{disabled}
								draggable={!disabled}
								ondragstart={(e) => onChipDragStart(e, l.slug)}
								ondragend={onChipDragEnd}
								onclick={() => insertChipAtCaret(l.slug)}
								title={disabled ? 'Switch output to Node to use this' : 'Click to insert'}
							>
								<span class="chip-kind"
									>{l.kind === 'calc' ? 'ƒ' : l.kind === 'smooth' ? '◈' : '◆'}</span
								>
								<span class="chip-slug">{l.slug}</span>
							</button>
						{/each}
					</div>
				</div>
			{/if}
			{#if flowLayers.length > 0}
				<div class="palette-group">
					<div class="palette-head">
						Flow layers{#if calcDomain === 'node'}
							<span class="muted">— wrapped with {flowAgg}( )</span>{/if}
					</div>
					<div class="palette" aria-label="Available flow layers — click to insert">
						{#each flowLayers as l (l.id)}
							<button
								type="button"
								class="layer-chip flow"
								class:calc={l.kind === 'calc'}
								draggable="true"
								ondragstart={(e) => onChipDragStart(e, l.slug)}
								ondragend={onChipDragEnd}
								onclick={() => insertChipAtCaret(l.slug)}
								title={calcDomain === 'node' ? `Inserts ${flowAgg}(${l.slug})` : 'Click to insert'}
							>
								<span class="chip-kind">{l.kind === 'calc' ? 'ƒ' : '~'}</span>
								<span class="chip-slug">{l.slug}</span>
							</button>
						{/each}
					</div>
				</div>
				{#if calcDomain === 'node'}
					<Field label="Flow as">
						<select
							bind:value={flowAgg}
							class="agg-select"
							title="Aggregator used when inserting a flow layer"
						>
							<option value="inflow">inflow( )</option>
							<option value="outflow">outflow( )</option>
							<option value="net">net( )</option>
						</select>
					</Field>
				{/if}
			{/if}
		{:else}
			<p class="hint">Save a layer first to use it in a calculation.</p>
		{/if}
		{#if calcExpr && !calcError}
			<p class="hint" title="Slug used in expressions">Saves as → {slugify(calcEffective)}</p>
		{/if}
		{#if calcError}
			<p class="err-msg">{calcError}</p>
		{/if}
		<button type="submit" class="primary" disabled={!calcExpr}>Add calculation</button>
	</form>

	<form class="calc" onsubmit={onSaveSmooth}>
		<div class="calc-head">Add smoothed layer</div>
		{#if nodeLayers.length === 0}
			<p class="hint">Save a node layer first — smoothing needs a node-domain input.</p>
		{:else}
			<Field label="Input">
				<LayerPicker
					mode="single"
					layers={nodeLayers}
					value={smInput}
					onChange={(id) => (smInputId = /** @type {string | null} */ (id))}
				/>
			</Field>
			<Field label="Kernel">
				<select value={smKernel} onchange={(e) => onCreateKernel(e.currentTarget.value)}>
					<option value="exp">exponential</option>
					<option value="gauss">gaussian</option>
					<option value="power">power</option>
				</select>
			</Field>
			<Field label={decayLabel} value={smDecay}>
				<input type="range" min={decayMin} max={decayMax} step={decayStep} bind:value={smDecay} />
			</Field>
			<Field label="Max distance (km)" value={smMaxDist}>
				<input type="range" min="0.5" max="100" step="0.5" bind:value={smMaxDist} />
			</Field>
			<KernelCurve kernel={smKernel} decay={smDecay} maxDist={smMaxDist} />
			<Field label="Output">
				<div class="seg" role="radiogroup" aria-label="Smoothing output">
					<button
						type="button"
						class:active={smMode === 'mean'}
						aria-pressed={smMode === 'mean'}
						onclick={() => (smMode = 'mean')}
					>
						Average
					</button>
					<button
						type="button"
						class:active={smMode === 'sum'}
						aria-pressed={smMode === 'sum'}
						onclick={() => (smMode = 'sum')}
					>
						Sum
					</button>
				</div>
			</Field>
			<Field label="Include self">
				<input type="checkbox" bind:checked={smIncludeSelf} />
			</Field>
			<Field label="Name">
				<input
					type="text"
					placeholder={smSuggestion}
					value={smNameTouched ? smName : ''}
					oninput={(e) => {
						const v = /** @type {HTMLInputElement} */ (e.currentTarget).value;
						smName = v;
						smNameTouched = v.length > 0;
						smError = null;
					}}
					autocomplete="off"
				/>
			</Field>
			{#if !smError}
				<p class="hint" title="Slug used in expressions">Saves as → {slugify(smEffective)}</p>
			{/if}
			{#if smError}
				<p class="err-msg">{smError}</p>
			{/if}
			<button type="submit" class="primary" disabled={!smInput}> Add smoothed layer </button>
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
	/* Layer-list styles (.layers/.layer/.radio/.kind/.name-btn/.name/.slug/
	   .meta/.del/.details/.line/.k/.chips/.chip) moved to SavedLayers.svelte
	   along with the layer list itself. */
	.calc {
		display: flex;
		flex-direction: column;
		gap: var(--spacing-1);
		padding-top: var(--spacing-2);
		border-top: 1px solid var(--color-line);
	}
	.calc-head {
		font-weight: 600;
		color: var(--color-text);
		font-size: var(--text-sm);
	}
	.palette {
		display: flex;
		flex-wrap: wrap;
		gap: var(--spacing-1);
		padding-left: calc(var(--label-col) + var(--spacing-2));
	}
	.layer-chip {
		display: inline-flex;
		align-items: center;
		gap: 3px;
		padding: 1px var(--spacing-2);
		border: 1px solid var(--color-accent);
		background: var(--color-accent);
		color: var(--color-accent-fg);
		border-radius: var(--radius-pill);
		font-size: var(--text-xs);
		font-family: ui-monospace, monospace;
		cursor: grab;
	}
	.layer-chip:hover {
		filter: brightness(1.4);
	}
	.layer-chip:active {
		cursor: grabbing;
	}
	.layer-chip.calc {
		background: #fff;
		color: var(--color-accent);
	}
	.layer-chip.flow {
		background: var(--color-bg-panel);
		color: var(--color-text);
		border-color: var(--color-line);
	}
	.layer-chip.flow.calc {
		background: #fff;
	}
	.layer-chip.dim {
		opacity: 0.4;
		cursor: not-allowed;
	}
	.palette-group {
		display: flex;
		flex-direction: column;
		gap: 2px;
	}
	.palette-head {
		font-size: var(--text-xs);
		color: var(--color-muted);
		padding-left: calc(var(--label-col) + var(--spacing-2));
	}
	.palette-head .muted {
		color: var(--color-hint);
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
	.agg-select {
		font-family: ui-monospace, monospace;
		font-size: var(--text-xs);
	}
	.chip-kind {
		font-size: 9px;
		opacity: 0.8;
	}
	.expr-editor {
		width: 100%;
		min-height: calc(var(--text-sm) + 8px);
		padding: 2px 6px;
		border: 1px solid var(--color-line);
		border-radius: var(--radius);
		background: #fff;
		font-family: ui-monospace, monospace;
		font-size: var(--text-sm);
		line-height: 1.6;
		outline: none;
		white-space: pre-wrap;
		word-break: break-word;
		cursor: text;
	}
	.expr-editor:focus-within {
		border-color: var(--color-accent);
	}
	.expr-editor.empty::before {
		content: attr(data-placeholder);
		color: var(--color-hint);
		pointer-events: none;
	}
	.expr-editor :global(.in-chip) {
		display: inline-block;
		padding: 0 var(--spacing-2);
		margin: 0 1px;
		background: var(--color-accent);
		color: var(--color-accent-fg);
		border: 1px solid var(--color-accent);
		border-radius: var(--radius-pill);
		font-size: var(--text-xs);
		line-height: 1.4;
		vertical-align: baseline;
		user-select: none;
	}
	.expr-editor :global(.in-chip-calc) {
		background: #fff;
		color: var(--color-accent);
	}
	.err-msg {
		font-size: var(--text-xs);
		color: #cf222e;
		margin: 0;
	}
	.hint {
		font-size: var(--text-xs);
		color: var(--color-hint);
		margin: 0;
	}
	.muted {
		color: var(--color-muted);
	}
	code {
		font-family: ui-monospace, monospace;
		font-size: var(--text-xs);
		color: var(--color-text);
	}
</style>

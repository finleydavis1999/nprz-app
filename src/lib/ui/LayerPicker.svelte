<script>
	// Reusable layer picker — pick one layer or many from a pre-filtered list.
	//
	// Two modes:
	//   - `single` (default): a `<select>` dropdown. `value` is a layer id or
	//     null; `onChange(id | null)` fires on user selection.
	//   - `multi`: a scrollable checkbox list (each row also shows the layer's
	//     slug for users building calc expressions). `value` is a string[] of
	//     ids; `onChange(newIds)` fires with the new array.
	//
	// The component is presentational: it doesn't know which layers to show —
	// the caller filters by scale / domain / kind before passing them in. That
	// keeps `LayerCalculator` and `ModelDock` free to apply their own rules
	// (exclude flow-domain inputs, exclude already-chosen dependent, exclude
	// model parents from being covariates, …).
	//
	// Empty state: when `layers` is empty (or shorter than `minOptions`), an
	// optional `emptyHint` paragraph replaces the control so the form doesn't
	// render a useless dropdown / checklist.

	/** @typedef {{ id: string, name: string, slug: string, kind?: string }} LayerItem */

	let {
		/** @type {LayerItem[]} */
		layers,
		/** @type {'single' | 'multi'} */
		mode = 'single',
		/** Selected layer id (single) or array of ids (multi).
		 *  @type {string | string[] | null} */
		value,
		/** @type {(next: string | string[] | null) => void} */
		onChange,
		/** Optional placeholder for the empty option in single mode (e.g.
		 *  "Pick a layer"). When null, the first layer is selected by default. */
		placeholder = null,
		/** Hint shown when there aren't enough layers to populate the picker. */
		emptyHint = null,
		/** Minimum number of `layers` before the control renders. Below this,
		 *  `emptyHint` (if set) takes over. */
		minOptions = 1,
		/** Disable the whole control. */
		disabled = false
	} = $props();

	function onSelectSingle(e) {
		const v = /** @type {HTMLSelectElement} */ (e.currentTarget).value;
		onChange?.(v || null);
	}

	function toggleMulti(id) {
		const current = Array.isArray(value) ? value : [];
		const next = current.includes(id) ? current.filter((x) => x !== id) : [...current, id];
		onChange?.(next);
	}

	function isChecked(id) {
		return Array.isArray(value) && value.includes(id);
	}
</script>

{#if layers.length < minOptions}
	{#if emptyHint}
		<p class="hint">{emptyHint}</p>
	{/if}
{:else if mode === 'single'}
	<select value={value ?? ''} onchange={onSelectSingle} {disabled}>
		{#if placeholder}
			<option value="">{placeholder}</option>
		{/if}
		{#each layers as l (l.id)}
			<option value={l.id}>{l.name}</option>
		{/each}
	</select>
{:else}
	<div class="multi" role="group">
		{#each layers as l (l.id)}
			<label class="row">
				<input
					type="checkbox"
					checked={isChecked(l.id)}
					{disabled}
					onchange={() => toggleMulti(l.id)}
				/>
				<span class="name">{l.name}</span>
				<span class="slug" title="Used in expressions">({l.slug})</span>
			</label>
		{/each}
	</div>
{/if}

<style>
	.multi {
		display: flex;
		flex-direction: column;
		gap: 2px;
		max-height: 180px;
		overflow-y: auto;
	}
	.row {
		display: grid;
		grid-template-columns: auto 1fr auto;
		gap: var(--spacing-1);
		align-items: center;
		font-size: var(--text-sm);
		cursor: pointer;
		padding: 1px;
	}
	.row:hover {
		background: rgba(31, 35, 40, 0.04);
		border-radius: var(--radius);
	}
	.name {
		overflow: hidden;
		text-overflow: ellipsis;
		white-space: nowrap;
	}
	.slug {
		color: var(--color-hint);
		font-size: var(--text-xs);
		font-family: ui-monospace, monospace;
	}
	.hint {
		font-size: var(--text-xs);
		color: var(--color-hint);
		margin: 0;
	}
</style>

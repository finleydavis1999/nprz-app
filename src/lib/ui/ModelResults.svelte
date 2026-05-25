<script>
	// Coefficients table + fit stats + formula for a single model.
	//
	// Used in two places:
	//   - ModelDock.svelte — inline in the expandable details pane on a model row.
	//   - +page.svelte right sidebar — wrapped in a <Panel title="Model results">
	//     so the active model's fit is always one glance away from the map.
	//
	// Props:
	//   parentId  string | null  — id of a `kind: 'model'` parent. If null, an
	//                              empty-state hint renders so the panel doesn't
	//                              disappear when nothing's active.
	//   showFormula  boolean    — toggle the formula row (the dock already shows
	//                              it on the form; the sidebar panel does too).
	//   showActiveChannel  boolean — show which output is active on the map
	//                              ("Showing: model — fitted"). Useful in the
	//                              sidebar; redundant in the dock.

	import { layers } from '$lib/state/layers.svelte.js';
	import { formulaFor } from '$lib/models/formula.js';

	let { parentId, showFormula = true, showActiveChannel = false } = $props();

	const parent = $derived(parentId ? layers.items.find((i) => i.id === parentId) : null);
	const fit = $derived(parentId ? layers.modelFits.get(parentId) : null);
	// Fit-state surfacing — without these, an in-flight fit and a failed fit
	// both render as "hasn't been fit yet", which is what shipped originally
	// and what made debugging painful.
	const isLoading = $derived(parentId ? layers.loading.has(parentId) : false);
	const errorMsg = $derived(parentId ? (layers.errors.get(parentId) ?? null) : null);
	// Informational notes (e.g. "weighted counts rounded"). Separate from
	// errorMsg — these don't mean the fit failed, just that the user should
	// know something about how it ran.
	const fitNotes = $derived(parentId ? (layers.modelNotes.get(parentId) ?? []) : []);
	// Coarse-grained status string while the fit is in flight ("Fitting
	// SIM…", "Distributing per-area results…", etc.). Falls back to a
	// generic message so the user always sees something more specific
	// than "fitting" while webR boots.
	const fitStatus = $derived(parentId ? (layers.modelStatus.get(parentId) ?? null) : null);

	// Build the same slug → layer lookup the form preview uses, so a saved
	// model's formula renders identically here.
	const byId = $derived.by(() => {
		// eslint-disable-next-line svelte/prefer-svelte-reactivity -- local lookup, not reactive
		const m = new Map();
		for (const l of layers.items) m.set(l.id, l);
		return m;
	});
	// Pass the parent (family + spec) so formulaFor can dispatch SIM vs NLM —
	// previously this passed only spec and SIM fits showed no formula at all.
	const formula = $derived(parent?.spec ? formulaFor(parent, byId) : null);

	// "Showing" channel — find the child that's the current active layer (if any).
	const activeChild = $derived.by(() => {
		const aid = layers.activeId;
		if (!aid || !parent) return null;
		const a = layers.items.find((i) => i.id === aid);
		if (!a || a.kind !== 'model-output' || a.parentId !== parent.id) return null;
		return a;
	});

	function fmt(x) {
		if (x == null || !Number.isFinite(x)) return '—';
		const abs = Math.abs(x);
		if (abs !== 0 && (abs < 0.001 || abs >= 1e6)) return x.toExponential(2);
		return x.toFixed(abs >= 100 ? 1 : 4);
	}

	function pStars(p) {
		if (p == null || !Number.isFinite(p)) return '';
		if (p < 0.001) return '***';
		if (p < 0.01) return '**';
		if (p < 0.05) return '*';
		if (p < 0.1) return '.';
		return '';
	}
</script>

{#if !parent}
	<p class="hint">Select a model output (or a model) to see coefficients.</p>
{:else if errorMsg}
	<div class="error">
		<strong>{parent.name} failed:</strong>
		<code class="err-text">{errorMsg}</code>
		<p class="hint">
			Check the browser console for the underlying R error. Fix the inputs and re-fit from the Model
			Calculator dock.
		</p>
	</div>
{:else if isLoading}
	<div class="fitting">
		<p class="hint">
			{fitStatus ?? `Fitting ${parent.name}…`}
			{#if !fit}
				<span class="hint-sub">First fit downloads webR + installs R packages (~30s).</span>
			{/if}
		</p>
		<button
			type="button"
			class="cancel"
			onclick={() => layers.cancelFit(parent.id)}
			title="Stop the in-flight fit. The model record stays, marked as cancelled — delete + recreate to retry."
		>
			Cancel
		</button>
	</div>
{:else if !fit}
	<p class="hint">{parent.name} hasn't been fit yet.</p>
{:else}
	<div class="results">
		{#if showFormula && formula}
			<div class="formula-row">
				<span class="label">Formula</span>
				<code class="formula">{formula}</code>
			</div>
		{/if}
		{#if showActiveChannel && activeChild}
			<div class="showing">
				Showing: <strong>{parent.name}</strong> — {activeChild.channel}
			</div>
		{/if}
		<table class="coefs">
			<thead>
				<tr><th>term</th><th>est</th><th>se</th><th>z</th><th>p</th><th></th></tr>
			</thead>
			<tbody>
				{#each fit.coefficients.name as cname, i (cname)}
					{@const part = cname.startsWith('count.')
						? 'count'
						: cname.startsWith('zero.')
							? 'zero'
							: null}
					{@const stripped = part ? cname.slice(part.length + 1) : cname}
					{@const prev = i > 0 ? fit.coefficients.name[i - 1] : null}
					{@const prevPart = prev?.startsWith('count.')
						? 'count'
						: prev?.startsWith('zero.')
							? 'zero'
							: null}
					{#if part && part !== prevPart}
						<tr class="part-head">
							<td colspan="6">{part === 'count' ? 'Poisson (count)' : 'Logit (structural zero)'}</td
							>
						</tr>
					{/if}
					<tr>
						<td class="term">{stripped}</td>
						<td>{fmt(fit.coefficients.est[i])}</td>
						<td>{fmt(fit.coefficients.se[i])}</td>
						<td>{fmt(fit.coefficients.z[i])}</td>
						<td>{fmt(fit.coefficients.p[i])}</td>
						<td class="stars">{pStars(fit.coefficients.p[i])}</td>
					</tr>
				{/each}
			</tbody>
		</table>
		<div class="fit-row">
			<span>R²={fmt(fit.fit.rSquared)}</span>
			<span>adj R²={fmt(fit.fit.adjRSquared)}</span>
			<span>RMSE={fmt(fit.fit.rmse)}</span>
			<span>AIC={fmt(fit.fit.aic)}</span>
			<span>BIC={fmt(fit.fit.bic)}</span>
			{#if fit.fit.sorensen != null}
				<span title="Sørensen–Dice agreement (SIM-specific)">Sørensen={fmt(fit.fit.sorensen)}</span>
			{/if}
		</div>
		{#if fitNotes.length > 0}
			<div class="notes">
				{#each fitNotes as note (note)}
					<p class="note">{note}</p>
				{/each}
			</div>
		{/if}
	</div>
{/if}

<style>
	.results {
		display: flex;
		flex-direction: column;
		gap: var(--spacing-1);
		font-size: var(--text-xs);
	}
	.formula-row {
		display: grid;
		grid-template-columns: var(--label-col) 1fr;
		gap: var(--spacing-2);
		align-items: baseline;
	}
	.label {
		color: var(--color-muted);
		font-size: var(--text-xs);
	}
	.formula {
		font-family: ui-monospace, monospace;
		font-size: var(--text-sm);
		color: var(--color-text);
		padding: 2px var(--spacing-2);
		background: rgba(0, 0, 0, 0.04);
		border-radius: var(--radius);
		word-break: break-all;
	}
	.showing {
		color: var(--color-muted);
		font-size: var(--text-xs);
	}
	.coefs {
		border-collapse: collapse;
		font-variant-numeric: tabular-nums;
	}
	.coefs th,
	.coefs td {
		text-align: right;
		padding: 1px var(--spacing-1);
	}
	.coefs th {
		color: var(--color-muted);
		font-weight: normal;
		border-bottom: 1px solid var(--color-line);
	}
	.coefs td.term {
		text-align: left;
		font-family: ui-monospace, monospace;
	}
	.coefs td.stars {
		color: var(--color-muted);
	}
	.coefs tr.part-head td {
		text-align: left;
		font-weight: 600;
		color: var(--color-text);
		padding-top: var(--spacing-1);
		border-top: 1px solid var(--color-line);
	}
	.fit-row {
		display: flex;
		flex-wrap: wrap;
		gap: var(--spacing-2);
		color: var(--color-muted);
		font-variant-numeric: tabular-nums;
	}
	/* Fit notes — non-error advisories about how the fit ran (e.g.
	   weighted survey counts were rounded). Same muted style as fit-row
	   but with a left rule to distinguish from coefficients above. */
	.notes {
		display: flex;
		flex-direction: column;
		gap: 2px;
		border-left: 2px solid var(--color-line);
		padding-left: var(--spacing-2);
	}
	.note {
		font-size: var(--text-xs);
		color: var(--color-muted);
		margin: 0;
	}
	.hint {
		font-size: var(--text-xs);
		color: var(--color-hint);
		margin: 0;
	}
	.hint-sub {
		display: block;
		font-size: var(--text-xs);
		color: var(--color-hint);
		margin-top: 2px;
	}
	/* Loading row — status text on the left, cancel button on the right. */
	.fitting {
		display: flex;
		align-items: flex-start;
		gap: var(--spacing-2);
		justify-content: space-between;
	}
	.cancel {
		flex: 0 0 auto;
		font-size: var(--text-xs);
		padding: 1px var(--spacing-2);
		background: #fff;
		color: #cf222e;
		border: 1px solid rgba(207, 34, 46, 0.4);
		border-radius: var(--radius);
		cursor: pointer;
	}
	.cancel:hover {
		background: rgba(207, 34, 46, 0.08);
	}
	.error {
		display: flex;
		flex-direction: column;
		gap: var(--spacing-1);
		padding: var(--spacing-1) var(--spacing-2);
		border: 1px solid #cf222e;
		border-radius: var(--radius);
		background: #ffebe9;
	}
	.error strong {
		color: #cf222e;
		font-size: var(--text-sm);
	}
	.err-text {
		font-family: ui-monospace, monospace;
		font-size: var(--text-xs);
		color: var(--color-text);
		word-break: break-word;
	}
</style>

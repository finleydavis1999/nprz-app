// Calc-layer expression parsing + evaluation. Uses math.js for safe AST-based
// evaluation (no Function/eval). Symbols in the expression must resolve to
// saved-layer slugs.
//
// Aggregator support: `inflow(flowSlug)`, `outflow(flowSlug)` and
// `net(flowSlug)` are recognised at parse time. The AST is rewritten so each
// `agg(slug)` call becomes a synthetic symbol (`__agg__slug`), and the caller
// pre-computes the aggregated per-node map under that synthetic name before
// `evaluateOverAreas`.
import { parse, SymbolNode } from 'mathjs';
import { AGGREGATOR_NAMES } from './flow-aggregations.js';

// Sensible default display name for a snapshot of `selection`. The user can
// always edit before saving. Joins active filter value labels; falls back to
// dataset + year when no filters are set. Expects the manifest object so it
// can resolve field/value labels.
export function defaultLayerName(manifest, sel) {
	const ds = manifest?.datasets?.[sel.dataset];
	const filters = sel.filters ?? {};
	/** @type {string[]} */
	const parts = [];
	for (const [fieldId, vals] of Object.entries(filters)) {
		if (!vals || !vals.length) continue;
		const field = ds?.fields?.[fieldId];
		const labels = vals.map((v) => field?.values?.find((x) => x.id === v)?.label ?? String(v));
		parts.push(labels.join('+'));
	}
	if (parts.length === 0) {
		const dsName = ds?.name?.split(/\s+/)[0] ?? sel.dataset;
		return `${dsName} ${sel.year}`;
	}
	return parts.join(' / ');
}

// Sanitise a free-text layer name into an identifier usable inside math.js
// expressions: leading letter/underscore, then alphanumerics/underscores.
// Display names keep their original spaces/punctuation; slugs are what the
// user types in expressions.
export function slugify(name) {
	const cleaned = String(name)
		.trim()
		.replace(/[^A-Za-z0-9_]+/g, '_')
		.replace(/^_+|_+$/g, '');
	if (!cleaned) return '';
	return /^[A-Za-z_]/.test(cleaned) ? cleaned : `_${cleaned}`;
}

// Given a base display name, return a name whose slug is free. If the base
// slug is taken, append a sequential integer (" 2", " 3", …) until free.
// `isTaken` is a predicate (slug) => boolean. Falls back to "Layer" for a
// blank base so the result is always a valid, saveable name.
export function uniqueLayerName(base, isTaken) {
	const root = String(base ?? '').trim() || 'Layer';
	if (!isTaken(slugify(root))) return root;
	for (let n = 2; ; n++) {
		const candidate = `${root} ${n}`;
		if (!isTaken(slugify(candidate))) return candidate;
	}
}

const AGG_SET = new Set(AGGREGATOR_NAMES);

function synthSlug(aggName, slug) {
	return `__${aggName}__${slug}`;
}

export { synthSlug };

// Parse an expression and return { compiled, symbols, aggs }.
//   - `symbols`: every SymbolNode name in the rewritten tree. Aggregator calls
//     are replaced with synthetic symbols so the caller knows to pre-fill them.
//   - `aggs`: list of { aggName, slug, synthSlug } the caller must resolve from
//     flow-domain layers and aggregate before evaluation.
export function parseExpression(expr) {
	const root = parse(expr);
	const aggs = [];
	const transformed = root.transform((n) => {
		if (!n.isFunctionNode) return n;
		const aggName = n.fn?.name ?? n.name;
		if (!AGG_SET.has(aggName)) return n;
		const arg = n.args?.[0];
		if (!arg || !arg.isSymbolNode || n.args.length !== 1) {
			throw new Error(`${aggName}() takes one layer slug, got ${n.args?.length ?? 0} args`);
		}
		const slug = arg.name;
		const synth = synthSlug(aggName, slug);
		aggs.push({ aggName, slug, synthSlug: synth });
		return new SymbolNode(synth);
	});
	const symbols = new Set();
	transformed.traverse((n) => {
		if (n.isSymbolNode) symbols.add(n.name);
	});
	return { compiled: transformed.compile(), symbols: [...symbols], aggs };
}

// Evaluate over a set of per-area inputs.
// inputs: Map<slug, Map<area_code, number>>
// Returns Map<area_code, number> — areas missing in any referenced input or
// producing a non-finite result are skipped.
export function evaluateOverAreas(compiled, symbols, inputs) {
	const out = new Map();
	if (symbols.length === 0) return out;
	const keys = new Set();
	for (const s of symbols) {
		const m = inputs.get(s);
		if (!m) return out; // missing dependency
		for (const k of m.keys()) keys.add(k);
	}
	for (const k of keys) {
		const scope = {};
		let ok = true;
		for (const s of symbols) {
			const v = inputs.get(s).get(k);
			if (v == null || !Number.isFinite(v)) {
				ok = false;
				break;
			}
			scope[s] = v;
		}
		if (!ok) continue;
		try {
			const v = compiled.evaluate(scope);
			if (Number.isFinite(v)) out.set(k, v);
		} catch {
			// e.g. division by zero produces Infinity (skipped above) or other math.js errors
		}
	}
	return out;
}

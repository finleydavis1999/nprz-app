// R-style formula builder shared between the ModelForm preview and the
// ModelResults panel. Single source of truth so the formula a user sees before
// fitting matches what gets shown alongside the fitted coefficients.
//
// Two builders + a dispatcher:
//   - `nlmFormulaFor(spec, byId)`  — node-level GLM (with optional GWR suffix).
//   - `simFormulaFor(spec, byId)`  — Poisson SIM (constraint / comp_dest /
//     radiation / zero-inflated split).
//   - `formulaFor({ family, spec }, byId)` — picks the right one. Both the
//     dock and the results panel call this so SIM and NLM formulas can never
//     drift apart.
//
// Notation conventions:
//   gaussian + identity → y ~ x1 + x2
//   poisson  + log      → log(y) ~ x1 + x2    (log-link surfaced explicitly)
//   with transforms     → log(y) ~ log(x1) + sqrt(x2)
//   GWR enabled         → "… ~ …   [GWR fixed bi-square bw=auto]"
//   SIM constrained     → log(y) ~ log(distance) + factor(o) + log(mass_d)
//   SIM zero-inflated   → "lhs ~ rhs | rhs     [Poisson | logit]"
//
// `byId` is any object/Map with `.get(id)` returning a layer-like value that
// has a `.slug` property — typically a Map built from `layers.items`. Returns
// null if the dependent can't be resolved; returns a `… ~ …` placeholder when
// covariates are missing (form-in-progress).
//
// Per-variable transforms come from `spec.dependentTransform` and
// `spec.covariateTransforms` (id-keyed). They reuse the same decoration as
// `design-matrix.decorateName`, so the formula and the R coefficient names
// match the design-matrix column keys.

import { decorateName } from './design-matrix.js';

/** Dispatch on family. Accepts the same `{ family, spec }` shape stored on a
 *  model parent in layers.items, so callers can pass `parent` directly.
 *  Callers that already have just a spec on hand (e.g. the dock's pre-save
 *  preview) can either wrap it as `{ family: 'nlm', spec: { ... } }` or call
 *  the family-specific helper directly. Returns null when the spec is
 *  unresolvable (typically a missing dependent layer).
 *
 *  @param {{ family?: 'nlm' | 'sim', spec?: any } & Record<string, any>} parentOrSpec
 *  @param {{ get(id: string): { slug: string } | undefined }} byId
 *  @returns {string | null}
 */
export function formulaFor(parentOrSpec, byId) {
	// Back-compat: callers that pass a bare spec (no `family`) get the NLM
	// builder — that's where every existing call site lived before SIM joined
	// this file.
	if (!parentOrSpec) return null;
	const family = parentOrSpec.family;
	const spec = parentOrSpec.spec ?? parentOrSpec;
	if (family === 'sim') return simFormulaFor(spec, byId);
	return nlmFormulaFor(spec, byId);
}

/**
 * @param {{
 *   dependentId: string,
 *   covariateIds: string[],
 *   dependentTransform?: 'none' | 'log' | 'log1p' | 'sqrt',
 *   covariateTransforms?: Record<string, 'none' | 'log' | 'log1p' | 'sqrt'>,
 *   covariateLags?: Record<string, { kernel: string, decay: number, maxDist: number }>,
 *   glm?: { family?: string, link?: string },
 *   gwr?: { enabled?: boolean, kernelType?: string, kernelShape?: string, bandwidth?: number | 'auto' } | null
 * }} spec
 * @param {{ get(id: string): { slug: string } | undefined }} byId
 * @returns {string | null}
 */
export function nlmFormulaFor(spec, byId) {
	const dep = byId.get(spec.dependentId);
	if (!dep) return null;
	const link = spec.glm?.link ?? 'identity';

	// Two stacked wrappers on the dependent: any user transform first, then
	// the link function for non-identity GLM links. E.g. user transform=log,
	// poisson family/log link → `log(log(y))`. Rare but not nonsensical, and
	// reflects what R is actually fitting.
	let lhs = decorateName(dep.slug, spec.dependentTransform);
	if (link === 'log') lhs = `log(${lhs})`;

	const tx = spec.covariateTransforms ?? {};
	const lags = spec.covariateLags ?? {};
	const covs = (spec.covariateIds ?? [])
		.map((id) => {
			const c = byId.get(id);
			if (!c) return null;
			// Order of wrapping: lag → transform. (User picks raw values; we
			// apply the spatial lag first, then the chosen monotonic transform.)
			let term = c.slug;
			const lag = lags[id];
			if (lag) term = `lag(${term},${lag.kernel},${lag.decay})`;
			return decorateName(term, tx[id]);
		})
		.filter(Boolean);
	const rhs = covs.length > 0 ? covs.join(' + ') : '…';
	const base = `${lhs} ~ ${rhs}`;
	// GWR annotation: suffix the formula with the kernel + bandwidth so the
	// preview reads `y ~ x + z   [GWR fixed bi-square bw=auto]`. The "(local)"
	// prefix on β_<x> in the child layer names then ties back to this label.
	if (spec.gwr?.enabled) {
		const kt = spec.gwr.kernelType ?? 'fixed';
		const ks = spec.gwr.kernelShape ?? 'bi-square';
		const bw = spec.gwr.bandwidth ?? 'auto';
		return `${base}   [GWR ${kt} ${ks} bw=${bw}]`;
	}
	return base;
}

/**
 * R-style formula for SIM. Mirrors the inline build the dock used to do —
 * extracted so ModelResults can render it post-fit too.
 *
 * @param {{
 *   flowId: string,
 *   massOId: string,
 *   massDId: string,
 *   massOTransform?: 'none' | 'log' | 'log1p' | 'sqrt',
 *   massDTransform?: 'none' | 'log' | 'log1p' | 'sqrt',
 *   constraint?: 'none' | 'production' | 'attraction',
 *   compDest?: { kernel: string, decay: number } | null,
 *   radiation?: boolean,
 *   zeroInflated?: boolean
 * }} spec
 * @param {{ get(id: string): { slug: string } | undefined }} byId
 * @returns {string | null}
 */
export function simFormulaFor(spec, byId) {
	const flow = byId.get(spec.flowId);
	const mO = byId.get(spec.massOId);
	const mD = byId.get(spec.massDId);
	if (!flow || !mO || !mD) return null;
	const decorate = (slug, t) => (t === 'none' || !t ? slug : `${t}(${slug})`);
	const lhs = `log(${flow.slug})`;
	const constraint = spec.constraint ?? 'none';
	// Constrained side becomes a per-area fixed effect, written `factor(o)` /
	// `factor(d)` in R formula notation. The absorbed mass drops from the RHS.
	const terms = ['log(distance_km)'];
	if (constraint !== 'production') {
		terms.push(decorate(`${mO.slug}_o`, spec.massOTransform ?? 'log'));
	} else {
		terms.push('factor(o)');
	}
	if (constraint !== 'attraction') {
		terms.push(decorate(`${mD.slug}_d`, spec.massDTransform ?? 'log'));
	} else {
		terms.push('factor(d)');
	}
	if (spec.compDest) terms.push('log1p(comp_dest)');
	if (spec.radiation) terms.push('log1p(radiation)');
	const rhs = terms.join(' + ');
	// Two-part formula notation when zero-inflated (Vuong-style display). v0
	// requires constraint='none' for ZI, but we render the split whenever the
	// flag is set so even an invalid spec is unambiguous.
	if (spec.zeroInflated && constraint === 'none') {
		return `${lhs} ~ ${rhs} | ${rhs}     [Poisson | logit]`;
	}
	return `${lhs} ~ ${rhs}`;
}

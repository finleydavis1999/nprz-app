import { describe, it, expect } from 'vitest';
import { formulaFor, simFormulaFor } from './formula.js';

function fakeById(map) {
	return { get: (id) => map[id] };
}

describe('formulaFor', () => {
	it('basic gaussian/identity formula', () => {
		const byId = fakeById({ d: { slug: 'y' }, x1: { slug: 'x1' }, x2: { slug: 'x2' } });
		const s = {
			dependentId: 'd',
			covariateIds: ['x1', 'x2'],
			glm: { family: 'gaussian', link: 'identity' }
		};
		expect(formulaFor(s, byId)).toBe('y ~ x1 + x2');
	});

	it('poisson/log surfaces log() around dependent', () => {
		const byId = fakeById({ d: { slug: 'jobs' }, x: { slug: 'pop' } });
		const s = {
			dependentId: 'd',
			covariateIds: ['x'],
			glm: { family: 'poisson', link: 'log' }
		};
		expect(formulaFor(s, byId)).toBe('log(jobs) ~ pop');
	});

	it('per-covariate transforms decorate the right-hand side', () => {
		const byId = fakeById({ d: { slug: 'y' }, x1: { slug: 'pop' }, x2: { slug: 'area' } });
		const s = {
			dependentId: 'd',
			covariateIds: ['x1', 'x2'],
			covariateTransforms: { x1: 'log', x2: 'sqrt' },
			glm: { family: 'gaussian', link: 'identity' }
		};
		expect(formulaFor(s, byId)).toBe('y ~ log(pop) + sqrt(area)');
	});

	it('dependent transform stacks with poisson log link (log of log)', () => {
		// User chose log on the dependent AND poisson family. Rare but valid:
		// fits the log of log(y). Whether the user wants this is on them; we
		// just render what's actually being fit.
		const byId = fakeById({ d: { slug: 'jobs' }, x: { slug: 'pop' } });
		const s = {
			dependentId: 'd',
			covariateIds: ['x'],
			dependentTransform: 'log',
			glm: { family: 'poisson', link: 'log' }
		};
		expect(formulaFor(s, byId)).toBe('log(log(jobs)) ~ pop');
	});

	it('renders … placeholder for the rhs while covariates are unselected', () => {
		const byId = fakeById({ d: { slug: 'y' } });
		expect(formulaFor({ dependentId: 'd', covariateIds: [] }, byId)).toBe('y ~ …');
	});

	it('spatial-lag covariate is rendered as lag(slug, kernel, decay), then transform', () => {
		const byId = fakeById({ d: { slug: 'y' }, x: { slug: 'pop' } });
		const s = {
			dependentId: 'd',
			covariateIds: ['x'],
			covariateTransforms: { x: 'log' },
			covariateLags: { x: { kernel: 'exp', decay: 1.5, maxDist: 10 } },
			glm: { family: 'gaussian', link: 'identity' }
		};
		// Lag wraps the slug first, then the transform wraps the lag.
		expect(formulaFor(s, byId)).toBe('y ~ log(lag(pop,exp,1.5))');
	});

	it('returns null when the dependent layer is missing', () => {
		const byId = fakeById({});
		expect(formulaFor({ dependentId: 'missing', covariateIds: [] }, byId)).toBeNull();
	});

	it('GWR mode annotates the formula with kernel + bandwidth', () => {
		const byId = fakeById({ d: { slug: 'y' }, x: { slug: 'pop' } });
		const s = {
			dependentId: 'd',
			covariateIds: ['x'],
			glm: { family: 'gaussian', link: 'identity' },
			gwr: {
				enabled: true,
				kernelType: 'adaptive',
				kernelShape: 'gaussian',
				bandwidth: 25
			}
		};
		expect(formulaFor(s, byId)).toBe('y ~ pop   [GWR adaptive gaussian bw=25]');
	});

	it('GWR mode disabled (enabled=false) renders the standard formula', () => {
		const byId = fakeById({ d: { slug: 'y' }, x: { slug: 'pop' } });
		const s = {
			dependentId: 'd',
			covariateIds: ['x'],
			glm: { family: 'gaussian', link: 'identity' },
			gwr: { enabled: false }
		};
		expect(formulaFor(s, byId)).toBe('y ~ pop');
	});

	it('dispatcher: family="sim" routes to simFormulaFor; bare spec routes to nlm', () => {
		const byId = fakeById({
			y: { slug: 'trips' },
			o: { slug: 'pop' },
			d: { slug: 'jobs' },
			dep: { slug: 'y' },
			x: { slug: 'x' }
		});
		// Wrapped { family, spec } → SIM builder
		const simParent = {
			family: 'sim',
			spec: { flowId: 'y', massOId: 'o', massDId: 'd' }
		};
		expect(formulaFor(simParent, byId)).toMatch(/^log\(trips\) ~ /);
		// Bare spec (back-compat for the pre-dispatcher call sites) → NLM
		expect(formulaFor({ dependentId: 'dep', covariateIds: ['x'] }, byId)).toBe('y ~ x');
	});
});

describe('simFormulaFor', () => {
	const byId = (m) => ({ get: (id) => m[id] });
	const baseLayers = byId({
		y: { slug: 'trips' },
		o: { slug: 'pop' },
		d: { slug: 'jobs' }
	});

	it('unconstrained: log(y) ~ log(distance) + log(pop_o) + log(jobs_d)', () => {
		const s = {
			flowId: 'y',
			massOId: 'o',
			massDId: 'd',
			massOTransform: 'log',
			massDTransform: 'log'
		};
		expect(simFormulaFor(s, baseLayers)).toBe(
			'log(trips) ~ log(distance_km) + log(pop_o) + log(jobs_d)'
		);
	});

	it('production constraint: origin mass becomes factor(o), destination mass stays', () => {
		const s = {
			flowId: 'y',
			massOId: 'o',
			massDId: 'd',
			constraint: 'production',
			massDTransform: 'log'
		};
		expect(simFormulaFor(s, baseLayers)).toBe(
			'log(trips) ~ log(distance_km) + factor(o) + log(jobs_d)'
		);
	});

	it('attraction constraint: destination mass becomes factor(d)', () => {
		const s = {
			flowId: 'y',
			massOId: 'o',
			massDId: 'd',
			constraint: 'attraction',
			massOTransform: 'log'
		};
		expect(simFormulaFor(s, baseLayers)).toBe(
			'log(trips) ~ log(distance_km) + log(pop_o) + factor(d)'
		);
	});

	it('compDest and radiation each append a log1p column', () => {
		const s = {
			flowId: 'y',
			massOId: 'o',
			massDId: 'd',
			compDest: { kernel: 'exp', decay: 5 },
			radiation: true
		};
		expect(simFormulaFor(s, baseLayers)).toBe(
			'log(trips) ~ log(distance_km) + log(pop_o) + log(jobs_d) + log1p(comp_dest) + log1p(radiation)'
		);
	});

	it("zero-inflated splits into '… | …    [Poisson | logit]' when constraint='none'", () => {
		const s = {
			flowId: 'y',
			massOId: 'o',
			massDId: 'd',
			constraint: 'none',
			zeroInflated: true
		};
		const out = simFormulaFor(s, baseLayers);
		expect(out).toContain(' | ');
		expect(out).toContain('[Poisson | logit]');
	});

	it('returns null when an input layer is missing', () => {
		const s = { flowId: 'missing', massOId: 'o', massDId: 'd' };
		expect(simFormulaFor(s, baseLayers)).toBeNull();
	});
});

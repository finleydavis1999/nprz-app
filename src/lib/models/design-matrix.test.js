import { describe, it, expect } from 'vitest';
import { buildNodeDesignMatrix, decorateName } from './design-matrix.js';

describe('buildNodeDesignMatrix', () => {
	it('intersects area codes across dependent + covariates and preserves alignment', () => {
		const dep = new Map([
			['A', 10],
			['B', 20],
			['C', 30]
		]);
		const cov = new Map([
			['B', 200],
			['C', 300],
			['D', 400] // D is not in dep, so it's dropped
		]);
		const dm = buildNodeDesignMatrix({
			dependentName: 'y',
			dependentValues: dep,
			covariates: [{ name: 'x', values: cov }]
		});
		// Only B and C survive the intersection; result is sorted by area code.
		expect(dm.areaCodes).toEqual(['B', 'C']);
		expect(Array.from(dm.columns.y)).toEqual([20, 30]);
		expect(Array.from(dm.columns.x)).toEqual([200, 300]);
	});

	it('drops rows where any input is non-finite (NaN, Infinity, undefined)', () => {
		const dep = new Map([
			['A', 1],
			['B', NaN], // dropped — dep is NaN
			['C', 3],
			['D', 4]
		]);
		const cov = new Map([
			['A', 10],
			['C', Infinity], // dropped — cov is Infinity
			['D', 40]
		]);
		const dm = buildNodeDesignMatrix({
			dependentName: 'y',
			dependentValues: dep,
			covariates: [{ name: 'x', values: cov }]
		});
		expect(dm.areaCodes).toEqual(['A', 'D']);
		expect(Array.from(dm.columns.y)).toEqual([1, 4]);
		expect(Array.from(dm.columns.x)).toEqual([10, 40]);
	});

	it('returns Float64Arrays (so the webR side gets numeric vectors without conversion)', () => {
		const dep = new Map([['A', 1]]);
		const dm = buildNodeDesignMatrix({
			dependentName: 'y',
			dependentValues: dep,
			covariates: []
		});
		expect(dm.columns.y).toBeInstanceOf(Float64Array);
	});

	it('throws on an empty dependent', () => {
		expect(() =>
			buildNodeDesignMatrix({
				dependentName: 'y',
				dependentValues: new Map(),
				covariates: []
			})
		).toThrow(/no values/i);
	});

	it('throws when no area codes survive the intersection', () => {
		// dep and cov share no codes
		const dep = new Map([['A', 1]]);
		const cov = new Map([['B', 2]]);
		expect(() =>
			buildNodeDesignMatrix({
				dependentName: 'y',
				dependentValues: dep,
				covariates: [{ name: 'x', values: cov }]
			})
		).toThrow(/no rows survived/i);
	});

	it('applies log transform to a covariate and decorates the column key', () => {
		const dep = new Map([
			['A', 10],
			['B', 20],
			['C', 30]
		]);
		const x = new Map([
			['A', 1],
			['B', Math.E],
			['C', Math.E * Math.E]
		]);
		const dm = buildNodeDesignMatrix({
			dependentName: 'y',
			dependentValues: dep,
			covariates: [{ name: 'x', values: x, transform: 'log' }]
		});
		expect(dm.areaCodes).toEqual(['A', 'B', 'C']);
		// Column is keyed by the decorated name — what flows into R col_names.
		expect(Array.from(dm.columns['log(x)'])).toEqual([0, 1, 2]);
		expect(dm.columns.x).toBeUndefined();
	});

	it('drops rows where log() of a non-positive value produces -Infinity / NaN', () => {
		const dep = new Map([
			['A', 1],
			['B', 2],
			['C', 3]
		]);
		const x = new Map([
			['A', 10],
			['B', 0], // log(0) = -Infinity → dropped
			['C', -5] // log(<0) = NaN     → dropped
		]);
		const dm = buildNodeDesignMatrix({
			dependentName: 'y',
			dependentValues: dep,
			covariates: [{ name: 'x', values: x, transform: 'log' }]
		});
		expect(dm.areaCodes).toEqual(['A']);
		expect(Array.from(dm.columns['log(x)'])).toEqual([Math.log(10)]);
	});

	it('log1p admits zero (log1p(0) = 0) where log would drop it', () => {
		const dep = new Map([
			['A', 1],
			['B', 2]
		]);
		const x = new Map([
			['A', 0],
			['B', 1]
		]);
		const dm = buildNodeDesignMatrix({
			dependentName: 'y',
			dependentValues: dep,
			covariates: [{ name: 'x', values: x, transform: 'log1p' }]
		});
		expect(dm.areaCodes).toEqual(['A', 'B']);
		expect(Array.from(dm.columns['log1p(x)'])).toEqual([0, Math.log(2)]);
	});

	it('applies dependentTransform and decorates the dependent column key', () => {
		const dm = buildNodeDesignMatrix({
			dependentName: 'y',
			dependentValues: new Map([
				['A', 1],
				['B', 4],
				['C', 9]
			]),
			dependentTransform: 'sqrt',
			covariates: []
		});
		expect(Array.from(dm.columns['sqrt(y)'])).toEqual([1, 2, 3]);
		expect(dm.columns.y).toBeUndefined();
	});

	it('weights add a (weights) column and drop rows with missing/non-positive weight', () => {
		const dep = new Map([
			['A', 10],
			['B', 20],
			['C', 30],
			['D', 40]
		]);
		const w = new Map([
			['A', 100],
			['B', 0], // non-positive → drop
			['C', -5], // negative → drop
			['D', 50]
		]);
		const dm = buildNodeDesignMatrix({
			dependentName: 'y',
			dependentValues: dep,
			covariates: [],
			weights: { name: 'pop', values: w }
		});
		expect(dm.areaCodes).toEqual(['A', 'D']);
		expect(Array.from(dm.columns.y)).toEqual([10, 40]);
		expect(Array.from(dm.columns['(weights)'])).toEqual([100, 50]);
	});

	it('offset adds (offset) column with transform applied + drops missing rows', () => {
		const dep = new Map([
			['A', 1],
			['B', 2],
			['C', 3]
		]);
		const off = new Map([
			['A', 100],
			['B', 200]
			// C missing → drops
		]);
		const dm = buildNodeDesignMatrix({
			dependentName: 'y',
			dependentValues: dep,
			covariates: [],
			offset: { name: 'pop', values: off, transform: 'log' }
		});
		expect(dm.areaCodes).toEqual(['A', 'B']);
		expect(Array.from(dm.columns['(offset)'])).toEqual([Math.log(100), Math.log(200)]);
	});

	it('decorateName matches the column-key convention', () => {
		expect(decorateName('pop', 'none')).toBe('pop');
		expect(decorateName('pop', 'log')).toBe('log(pop)');
		expect(decorateName('pop', undefined)).toBe('pop');
	});

	it('handles multiple covariates with row order independent of insertion order', () => {
		const dep = new Map([
			['C', 30],
			['A', 10],
			['B', 20]
		]);
		const cov1 = new Map([
			['A', 1],
			['B', 2],
			['C', 3]
		]);
		const cov2 = new Map([
			['A', 100],
			['B', 200],
			['C', 300]
		]);
		const dm = buildNodeDesignMatrix({
			dependentName: 'y',
			dependentValues: dep,
			covariates: [
				{ name: 'x1', values: cov1 },
				{ name: 'x2', values: cov2 }
			]
		});
		// Sorted by area code, so order is deterministic regardless of input.
		expect(dm.areaCodes).toEqual(['A', 'B', 'C']);
		expect(Array.from(dm.columns.y)).toEqual([10, 20, 30]);
		expect(Array.from(dm.columns.x1)).toEqual([1, 2, 3]);
		expect(Array.from(dm.columns.x2)).toEqual([100, 200, 300]);
	});
});

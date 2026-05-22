import { describe, it, expect } from 'vitest';
import { buildNeighborIndex, kernelWeight, smooth } from './spatial-lag.js';

// Three nodes on a line (RD metres): A at 0, B at 1000, C at 5000.
const centroids = {
	A: [0, 0],
	B: [1000, 0],
	C: [5000, 0]
};

describe('buildNeighborIndex', () => {
	it('keeps only pairs within maxDist and excludes self', () => {
		const idx = buildNeighborIndex(centroids, 2000);
		expect(idx.get('A').map((n) => n.code)).toEqual(['B']); // C is 5000 away
		expect(
			idx
				.get('B')
				.map((n) => n.code)
				.sort()
		).toEqual(['A']); // C is 4000 away
		expect(idx.get('C')).toEqual([]); // nothing within 2000
	});

	it('reports true Euclidean distance', () => {
		const idx = buildNeighborIndex(centroids, 2000);
		expect(idx.get('A')[0].dist).toBeCloseTo(1000, 6);
	});

	it('spans grid cells when maxDist is large', () => {
		const idx = buildNeighborIndex(centroids, 10000);
		expect(
			idx
				.get('A')
				.map((n) => n.code)
				.sort()
		).toEqual(['B', 'C']);
	});
});

describe('kernelWeight', () => {
	it('exp returns 1 at d=0 and e^-1 at d=p', () => {
		expect(kernelWeight('exp', 0, 1000)).toBe(1);
		expect(kernelWeight('exp', 1000, 1000)).toBeCloseTo(Math.exp(-1), 10);
	});

	it('gauss returns 1 at d=0 and e^-1 at d=p', () => {
		expect(kernelWeight('gauss', 0, 1000)).toBe(1);
		expect(kernelWeight('gauss', 1000, 1000)).toBeCloseTo(Math.exp(-1), 10);
	});

	it('power is d^-beta, and 0 at d=0 (self term added by smooth)', () => {
		expect(kernelWeight('power', 10, 2)).toBeCloseTo(0.01, 10);
		expect(kernelWeight('power', 0, 2)).toBe(0);
	});
});

describe('smooth', () => {
	const values = new Map([
		['A', 10],
		['B', 20],
		['C', 30]
	]);

	it('mean mode is a row-standardised weighted average including self', () => {
		const idx = buildNeighborIndex(centroids, 2000);
		const out = smooth(idx, values, {
			kernel: 'exp',
			decay: 1000,
			mode: 'mean',
			includeSelf: true
		});
		const wB = Math.exp(-1); // B is 1000 m from A, decay 1000
		expect(out.get('A')).toBeCloseTo((1 * 10 + wB * 20) / (1 + wB), 10);
		// result is pulled toward the neighbour but stays between the two values
		expect(out.get('A')).toBeGreaterThan(10);
		expect(out.get('A')).toBeLessThan(20);
	});

	it('sum mode is the un-normalised weighted sum (gravitational potential)', () => {
		const idx = buildNeighborIndex(centroids, 2000);
		const out = smooth(idx, values, { kernel: 'exp', decay: 1000, mode: 'sum', includeSelf: true });
		const wB = Math.exp(-1);
		expect(out.get('A')).toBeCloseTo(1 * 10 + wB * 20, 10);
	});

	it('skips a node with no contributors when includeSelf is false', () => {
		const idx = buildNeighborIndex(centroids, 2000);
		const out = smooth(idx, values, {
			kernel: 'exp',
			decay: 1000,
			mode: 'mean',
			includeSelf: false
		});
		expect(out.has('C')).toBe(false); // C has no neighbours within 2000 m
	});

	it('a lone node with includeSelf returns its own value unchanged', () => {
		const idx = buildNeighborIndex(centroids, 2000);
		const out = smooth(idx, values, {
			kernel: 'exp',
			decay: 1000,
			mode: 'mean',
			includeSelf: true
		});
		expect(out.get('C')).toBe(30);
	});

	it('power kernel + includeSelf weights self as 1 (not Infinity)', () => {
		const idx = buildNeighborIndex(centroids, 2000);
		const out = smooth(idx, values, { kernel: 'power', decay: 2, mode: 'mean', includeSelf: true });
		expect(Number.isFinite(out.get('A'))).toBe(true);
		expect(out.get('C')).toBe(30); // lone node → just self
	});
});

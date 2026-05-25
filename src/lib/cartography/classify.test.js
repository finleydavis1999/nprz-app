import { describe, it, expect } from 'vitest';
import { classify } from './classify.js';

// Classification suite — the diverging-mode is the newest addition;
// keep the existing sequential cases too as regression coverage.

describe('classify — sequential methods', () => {
	it('jenks falls back to equal-interval when too few unique values', () => {
		const breaks = classify([1, 1, 1, 1, 1, 1], { method: 'jenks', n: 5 });
		// 5 classes, 6 break-edges; degenerate input produces all-equal breaks.
		expect(breaks).toHaveLength(6);
		expect(breaks.every((v) => v === 1)).toBe(true);
	});

	it('quantile breaks are monotonic and span min..max', () => {
		const vals = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
		const breaks = classify(vals, { method: 'quantile', n: 4 });
		expect(breaks[0]).toBe(1);
		expect(breaks.at(-1)).toBe(10);
		for (let i = 1; i < breaks.length; i++) expect(breaks[i]).toBeGreaterThanOrEqual(breaks[i - 1]);
	});

	it('equal-interval breaks are evenly spaced', () => {
		const breaks = classify([0, 100], { method: 'equal', n: 5 });
		expect(breaks).toEqual([0, 20, 40, 60, 80, 100]);
	});

	it('returns null for empty input', () => {
		expect(classify([], { method: 'jenks', n: 5 })).toBeNull();
	});
});

describe('classify — diverging method', () => {
	it('anchors at the pivot with proportional split', () => {
		// Negative tail much shorter than positive; proportional split should
		// give negSide ~1 class, posSide most of n. Pivot=0 always appears.
		const vals = [-1, -0.5, 0, 1, 2, 3, 5, 7, 10];
		const breaks = classify(vals, {
			method: 'diverging',
			n: 5,
			pivot: 0,
			subMethod: 'equal',
			splitMode: 'proportional'
		});
		// Total breaks = n + 1 = 6.
		expect(breaks).toHaveLength(6);
		// Pivot is somewhere in the middle as a break edge.
		expect(breaks.includes(0)).toBe(true);
		// Negative side has at least one break-edge less than the pivot.
		expect(breaks.some((v) => v < 0)).toBe(true);
		// Positive side dominates the class count (proportional).
		const posBreaks = breaks.filter((v) => v > 0).length;
		const negBreaks = breaks.filter((v) => v < 0).length;
		expect(posBreaks).toBeGreaterThanOrEqual(negBreaks);
	});

	it('balanced split allocates ~n/2 classes per side', () => {
		const vals = [-10, -5, -1, 0, 0.1, 1, 5, 10];
		const breaks = classify(vals, {
			method: 'diverging',
			n: 4,
			pivot: 0,
			subMethod: 'equal',
			splitMode: 'balanced'
		});
		// n=4 → 5 break-edges. Both sides get 2 classes (3 break-edges each
		// before the pivot dedup), pivot is the join.
		expect(breaks).toHaveLength(5);
		expect(breaks.includes(0)).toBe(true);
	});

	it('falls back to sub-method when all values are on one side of pivot', () => {
		const allPos = classify([1, 2, 3, 4, 5], {
			method: 'diverging',
			n: 3,
			pivot: 0,
			subMethod: 'equal'
		});
		// Treated as sequential — 3 classes, 4 break-edges, no pivot inserted
		// (since the negative side was empty).
		expect(allPos).toHaveLength(4);
		expect(allPos[0]).toBe(1);
		expect(allPos.at(-1)).toBe(5);
	});

	it('respects a non-zero pivot', () => {
		// Pivot at 1 — split where 1 is the centre. Values above 1 go to
		// posSide, below to negSide.
		const vals = [-2, 0, 0.5, 1, 1.5, 3, 5];
		const breaks = classify(vals, {
			method: 'diverging',
			n: 4,
			pivot: 1,
			subMethod: 'equal',
			splitMode: 'balanced'
		});
		expect(breaks.includes(1)).toBe(true);
		expect(breaks.some((v) => v < 1)).toBe(true);
		expect(breaks.some((v) => v > 1)).toBe(true);
	});
});

import { describe, it, expect } from 'vitest';
import { buildSimDesignMatrix } from './sim-design.js';

// Centroids in RD metres (≈ EPSG:28992). Distances below are in km.
const centroids = {
	A: [0, 0],
	B: [3000, 4000], // distance to A = 5000 m = 5 km
	C: [6000, 8000] // distance to A = 10 km, to B = 5 km
};

describe('buildSimDesignMatrix', () => {
	it('joins observed flows with origin + dest masses + distance, dropping self-loops by default', () => {
		const flows = new Map([
			['A|B', 50],
			['B|A', 40],
			['A|C', 25],
			['A|A', 999] // self-loop, dropped by default
		]);
		const massO = new Map([
			['A', 100],
			['B', 200],
			['C', 300]
		]);
		const massD = new Map([
			['A', 1000],
			['B', 2000],
			['C', 3000]
		]);
		const dm = buildSimDesignMatrix({
			flows,
			flowName: 'trips',
			massO,
			massOName: 'pop',
			massD,
			massDName: 'jobs',
			centroids
		});

		// 3 OD pairs after self-loop drop.
		expect(dm.o).toEqual(['A', 'B', 'A']);
		expect(dm.d).toEqual(['B', 'A', 'C']);
		expect(dm.edgeKeys).toEqual(['A|B', 'B|A', 'A|C']);

		// y (raw flow counts) preserved as-is — Poisson handles the link.
		expect(Array.from(dm.columns.trips)).toEqual([50, 40, 25]);

		// Distance is pre-logged (km). A↔B is 5 km, A↔C is 10 km.
		const dKey = 'log(distance_km)';
		expect(dm.columns[dKey][0]).toBeCloseTo(Math.log(5), 10);
		expect(dm.columns[dKey][1]).toBeCloseTo(Math.log(5), 10);
		expect(dm.columns[dKey][2]).toBeCloseTo(Math.log(10), 10);

		// Masses default to log. Column keys decorated so coefs read naturally
		// as `log(pop_o)` / `log(jobs_d)`.
		const oKey = 'log(pop_o)';
		const dMassKey = 'log(jobs_d)';
		expect(dm.columns[oKey][0]).toBeCloseTo(Math.log(100), 10);
		expect(dm.columns[oKey][2]).toBeCloseTo(Math.log(100), 10); // both A
		expect(dm.columns[dMassKey][1]).toBeCloseTo(Math.log(1000), 10); // B→A
	});

	it('self-loops still drop on log(0) distance even when includeSelfLoops=true', () => {
		// distance(A→A) is 0 → log(0) = -Inf. The non-finite filter takes the
		// row out. Documented behaviour: callers wanting intra-zonal flows must
		// pre-compute a non-zero self-distance before handing flows in.
		const flows = new Map([['A|A', 999]]);
		expect(() =>
			buildSimDesignMatrix({
				flows,
				flowName: 'trips',
				massO: new Map([['A', 50]]),
				massOName: 'pop',
				massD: new Map([['A', 500]]),
				massDName: 'jobs',
				centroids,
				includeSelfLoops: true
			})
		).toThrow(/no od pairs survived/i);
	});

	it('drops OD pairs where a mass layer is missing the area', () => {
		const flows = new Map([
			['A|B', 50],
			['A|C', 25] // C not in massD → drop
		]);
		const massO = new Map([
			['A', 100],
			['B', 200]
		]);
		const massD = new Map([
			['A', 1000],
			['B', 2000]
			// C missing
		]);
		const dm = buildSimDesignMatrix({
			flows,
			flowName: 'trips',
			massO,
			massOName: 'pop',
			massD,
			massDName: 'jobs',
			centroids
		});
		expect(dm.edgeKeys).toEqual(['A|B']);
	});

	it('applies user-chosen mass transforms (e.g. sqrt) and decorates the column key', () => {
		const flows = new Map([['A|B', 10]]);
		const dm = buildSimDesignMatrix({
			flows,
			flowName: 'y',
			massO: new Map([
				['A', 16],
				['B', 25]
			]),
			massOName: 'pop',
			massOTransform: 'sqrt',
			massD: new Map([
				['A', 100],
				['B', 400]
			]),
			massDName: 'jobs',
			massDTransform: 'none',
			centroids
		});
		// sqrt-transformed origin mass for A is 4.
		expect(dm.columns['sqrt(pop_o)'][0]).toBe(4);
		// Untransformed dest mass for B is 400.
		expect(dm.columns['jobs_d'][0]).toBe(400);
	});

	it('expandToAllOD pads the cartesian product with y=0 for absent OD pairs', () => {
		// Observed: A→B only. With expand, we should see all 6 directed pairs
		// among {A,B,C}, minus the 3 self-loops (default drop).
		const flows = new Map([['A|B', 50]]);
		const massO = new Map([
			['A', 100],
			['B', 200],
			['C', 300]
		]);
		const massD = new Map([
			['A', 1000],
			['B', 2000],
			['C', 3000]
		]);
		const dm = buildSimDesignMatrix({
			flows,
			flowName: 'trips',
			massO,
			massOName: 'pop',
			massD,
			massDName: 'jobs',
			centroids,
			expandToAllOD: true
		});
		// 3 origins × 3 dests − 3 self-loops = 6 pairs.
		expect(dm.edgeKeys.length).toBe(6);
		expect(new Set(dm.edgeKeys)).toEqual(new Set(['A|B', 'A|C', 'B|A', 'B|C', 'C|A', 'C|B']));
		// The observed flow shows up at its original count; the other 5 are 0.
		const idxAB = dm.edgeKeys.indexOf('A|B');
		expect(dm.columns.trips[idxAB]).toBe(50);
		const totalY = Array.from(dm.columns.trips).reduce((s, v) => s + v, 0);
		expect(totalY).toBe(50); // exactly one non-zero row
	});

	it('expandToAllOD respects includeSelfLoops=false even when explicitly missing from flows', () => {
		const flows = new Map(); // no observed flows at all
		const masses = new Map([
			['A', 1],
			['B', 1]
		]);
		const dm = buildSimDesignMatrix({
			flows,
			flowName: 'y',
			massO: masses,
			massOName: 'm',
			massD: masses,
			massDName: 'm',
			centroids,
			expandToAllOD: true,
			includeSelfLoops: false
		});
		// Only the two off-diagonal pairs survive.
		expect(dm.edgeKeys.length).toBe(2);
		expect(new Set(dm.edgeKeys)).toEqual(new Set(['A|B', 'B|A']));
	});

	it('compDest adds log1p(comp_dest) column = log1p(kernel-weighted sum of competing masses)', () => {
		const flows = new Map([
			['A|B', 10],
			['A|C', 20]
		]);
		const massO = new Map([['A', 100]]);
		const massD = new Map([
			['A', 1000],
			['B', 2000],
			['C', 3000]
		]);
		const dm = buildSimDesignMatrix({
			flows,
			flowName: 'y',
			massO,
			massOName: 'mO',
			massD,
			massDName: 'mD',
			centroids,
			compDest: { kernel: 'exp', decay: 1000 }
		});
		expect(dm.columns['log1p(comp_dest)']).toBeInstanceOf(Float64Array);
		expect(dm.columns['log1p(comp_dest)'].length).toBe(2);
		// Values are finite; A→B and A→C have distinct comp_dest because the
		// destinations B and C have different competing-mass neighborhoods.
		expect(Number.isFinite(dm.columns['log1p(comp_dest)'][0])).toBe(true);
		expect(Number.isFinite(dm.columns['log1p(comp_dest)'][1])).toBe(true);
		expect(dm.columns['log1p(comp_dest)'][0]).not.toBe(dm.columns['log1p(comp_dest)'][1]);
	});

	it('radiation adds log1p(radiation) — closer-destinations cumulative mass', () => {
		const flows = new Map([
			['A|B', 10],
			['A|C', 20]
		]);
		const masses = new Map([
			['A', 100],
			['B', 50],
			['C', 200]
		]);
		const dm = buildSimDesignMatrix({
			flows,
			flowName: 'y',
			massO: masses,
			massOName: 'm',
			massD: masses,
			massDName: 'm',
			centroids,
			radiation: true
		});
		expect(dm.columns['log1p(radiation)']).toBeInstanceOf(Float64Array);
		// From A: B is 5km away (mass 50), C is 10km (mass 200).
		// rad(A,B) = 0 (no destinations closer than B, excluding A itself)
		// rad(A,C) = 50 (B's mass, since B is closer to A than C is)
		expect(dm.columns['log1p(radiation)'][0]).toBeCloseTo(Math.log1p(0), 10); // A→B
		expect(dm.columns['log1p(radiation)'][1]).toBeCloseTo(Math.log1p(50), 10); // A→C
	});

	it('throws when no OD pair survives the filters', () => {
		const flows = new Map([['A|Z', 10]]); // Z not in centroids
		expect(() =>
			buildSimDesignMatrix({
				flows,
				flowName: 'y',
				massO: new Map([['A', 1]]),
				massOName: 'm',
				massD: new Map([['Z', 1]]),
				massDName: 'm',
				centroids
			})
		).toThrow(/no od pairs survived/i);
	});
});

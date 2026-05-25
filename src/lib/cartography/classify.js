// Classification helpers — produce n+1 break boundaries from a numeric array.
import { ckmeans, quantile } from 'simple-statistics';

// `values` should be filtered to finite numbers before calling.
// Returns `[min, b1, b2, …, b_{n-1}, max]` (length n+1).
//
// Methods:
//   'jenks'     — natural breaks via ckmeans clustering on the value set.
//   'quantile'  — equal-count bins.
//   'equal'     — equal-extent bins from min to max.
//   'diverging' — for signed data anchored at a meaningful pivot (default
//                 0). Splits values at the pivot and runs `subMethod`
//                 (default 'jenks') independently on each side, then
//                 concatenates with the pivot as the centre break. Adapts
//                 to asymmetric tails — unlike "symmetric breaks around
//                 zero" which wastes half the palette when one side is
//                 short. `splitMode` decides how the `n` classes are
//                 allocated between sides: 'proportional' uses each side's
//                 count, 'balanced' splits roughly 50/50.
export function classify(
	values,
	{
		method = 'jenks',
		n = 5,
		manual = null,
		pivot = 0,
		subMethod = 'jenks',
		splitMode = 'proportional'
	} = {}
) {
	if (manual && Array.isArray(manual)) return [...manual];
	if (values.length === 0) return null;
	const sorted = [...values].sort((a, b) => a - b);
	switch (method) {
		case 'jenks':
			return jenks(sorted, n);
		case 'quantile':
			return quantileBreaks(sorted, n);
		case 'equal':
			return equalBreaks(sorted, n);
		case 'diverging':
			return divergingBreaks(sorted, n, pivot, subMethod, splitMode);
		default:
			throw new Error(`unknown classification method: ${method}`);
	}
}

function jenks(sorted, n) {
	// Degenerate: too few unique points → fall back to equal interval.
	const unique = new Set(sorted);
	if (unique.size < n) return equalBreaks(sorted, n);
	const clusters = ckmeans(sorted, n);
	const breaks = [sorted[0]];
	for (const c of clusters) breaks.push(c[c.length - 1]);
	return breaks;
}

function quantileBreaks(sorted, n) {
	const breaks = [sorted[0]];
	for (let i = 1; i < n; i++) breaks.push(quantile(sorted, i / n));
	breaks.push(sorted[sorted.length - 1]);
	return breaks;
}

function equalBreaks(sorted, n) {
	const min = sorted[0];
	const max = sorted[sorted.length - 1];
	const step = (max - min) / n;
	const breaks = [];
	for (let i = 0; i <= n; i++) breaks.push(min + step * i);
	return breaks;
}

// Dispatch helper — pick whichever sub-method the diverging caller named.
function applyMethod(sorted, n, subMethod) {
	switch (subMethod) {
		case 'jenks':
			return jenks(sorted, n);
		case 'quantile':
			return quantileBreaks(sorted, n);
		case 'equal':
			return equalBreaks(sorted, n);
		default:
			throw new Error(`unknown sub-method for diverging: ${subMethod}`);
	}
}

function divergingBreaks(sorted, n, pivot, subMethod, splitMode) {
	const negSide = sorted.filter((v) => v < pivot);
	const posSide = sorted.filter((v) => v >= pivot);
	// Degenerate cases — all on one side. Fall back to plain sub-method on
	// the populated side. No "centre" makes sense for sequential data.
	if (negSide.length === 0) return applyMethod(posSide, n, subMethod);
	if (posSide.length === 0) return applyMethod(negSide, n, subMethod);

	// Decide how many classes per side.
	let nNeg;
	let nPos;
	if (splitMode === 'balanced') {
		// Even split, longer side rounds up.
		nNeg = Math.floor(n / 2);
		nPos = n - nNeg;
	} else {
		// Proportional — each side gets at least 1 class.
		const totalCount = negSide.length + posSide.length;
		nNeg = Math.max(1, Math.round((n * negSide.length) / totalCount));
		nPos = Math.max(1, n - nNeg);
		// Re-derive nNeg so the total is exactly n after the posSide floor.
		nNeg = n - nPos;
		if (nNeg < 1) {
			nNeg = 1;
			nPos = n - nNeg;
		}
	}

	const negBreaks = applyMethod(negSide, nNeg, subMethod);
	const posBreaks = applyMethod(posSide, nPos, subMethod);
	// negBreaks: [negMin, …, negMaxClose]; we want [negMin, …, justBelowPivot, pivot, justAbovePivot, …, posMax].
	// Drop the trailing edge of negBreaks (closest negative to pivot)
	// because the pivot itself becomes the next edge — and drop the
	// leading edge of posBreaks (which is the smallest positive value)
	// for the same reason; pivot anchors the centre.
	const out = [...negBreaks.slice(0, -1), pivot, ...posBreaks.slice(1)];
	return out;
}

// A flow direction missing from the filtered set is not necessarily zero: when
// a min-weight filter is active it was dropped for being *below* the threshold,
// so we label it "< {min}" rather than implying an exact value. Once a pair is
// out of the filtered set we can't tell a genuinely-absent pair from a filtered
// one, so this reads as "no contribution at or above the threshold".
//
// The caller passes its own `fmt` so each surface keeps its own precision (the
// FlowPies tooltip stays compact; the inspect panel shows more decimals) while
// the threshold wording stays in one place.
export function fmtFlowMaybeBelow(v, belowThreshold, minWeight, fmt) {
	if (belowThreshold && minWeight > 0) return `< ${fmt(minWeight)}`;
	return fmt(v);
}

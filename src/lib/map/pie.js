// SVG path for a pie slice centered at (cx, cy) with radius r, swept clockwise
// from startAngle to endAngle (radians, 0 = east, -PI/2 = north). A near-full
// sweep degenerates the single-arc form, so it is drawn as two half-circle arcs
// instead. Shared by FlowPies, PrintMap and InspectPanel.
export function pieSlicePath(cx, cy, r, startAngle, endAngle) {
	// Full circle case — a single arc would be degenerate.
	if (endAngle - startAngle >= Math.PI * 2 - 1e-6) {
		return `M ${cx - r} ${cy} a ${r} ${r} 0 1 0 ${r * 2} 0 a ${r} ${r} 0 1 0 ${-r * 2} 0 Z`;
	}
	const x1 = cx + r * Math.cos(startAngle);
	const y1 = cy + r * Math.sin(startAngle);
	const x2 = cx + r * Math.cos(endAngle);
	const y2 = cy + r * Math.sin(endAngle);
	const large = endAngle - startAngle > Math.PI ? 1 : 0;
	return `M ${cx} ${cy} L ${x1} ${y1} A ${r} ${r} 0 ${large} 1 ${x2} ${y2} Z`;
}

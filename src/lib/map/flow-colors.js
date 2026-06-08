// Direction colors for flow visualisations: inflow = blue, outflow = red.
// Single source of truth shared by the live map (FlowPies), the print map
// (PrintMap) and the inspect panel (InspectPanel). These are data-viz colors
// baked into SVG / MapLibre paint output, not page chrome, so they live as JS
// constants rather than CSS --tokens.
export const INFLOW = '#1f77b4';
export const OUTFLOW = '#d62728';

// Neutral pair for the bidirectional gradient split. With no focal node the
// in/out (blue/red) meaning is undefined, so the split is colored by magnitude
// instead: the dominant direction is always orange, the minority always teal.
// ColorBrewer Dark2 hues — deliberately distinct from the blue/red in/out pair.
export const FLOW_MAJOR = '#d95f02'; // orange — dominant direction
export const FLOW_MINOR = '#7570b3'; // purple — minority direction (contrasts orange)

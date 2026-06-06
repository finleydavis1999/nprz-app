// Direction colors for flow visualisations: inflow = blue, outflow = red.
// Single source of truth shared by the live map (FlowPies), the print map
// (PrintMap) and the inspect panel (InspectPanel). These are data-viz colors
// baked into SVG / MapLibre paint output, not page chrome, so they live as JS
// constants rather than CSS --tokens.
export const INFLOW = '#1f77b4';
export const OUTFLOW = '#d62728';

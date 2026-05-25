// Projection factory for the print map.
//
// Our TopoJSON is already in EPSG:28992 (RD New) coordinates, so we use
// d3-geo's `geoIdentity()` (no re-projection) with reflectY(true) to flip
// the y-axis (RD is y-up, SVG is y-down). `fitSize` then scales+translates
// the geometry into the SVG viewport.
//
// Note: the original plan called for `geoConicConformal` parameterized as
// RD. That would be the right choice if our topojson were in WGS84 lon/lat.
// Since we baked the projection into the topojson at R-pipeline time,
// `geoIdentity` matches the data exactly with no re-projection cost.
import { geoIdentity } from 'd3-geo';

/**
 * Build an RD-aware d3 projection for the print SVG.
 *
 * Two modes:
 *  - "fit features" (default): scale/translate so the GeoJSON's full bbox fits
 *    inside `size`. Used for nationwide previews.
 *  - "fit extent": when `extent` is passed (`[[minX,minY],[maxX,maxY]]` in RD
 *    metres), scale/translate so that bbox fills `size`. Used when the user
 *    has framed a printable area on the live map.
 *
 * @param {[number, number]} size [width, height] in SVG units
 * @param {GeoJSON.GeoJsonObject} features
 * @param {[[number, number], [number, number]] | null} [extent]
 */
export function rdProjection(size, features, extent = null) {
	const p = geoIdentity().reflectY(true);
	if (extent) {
		// fitExtent takes [[x0,y0],[x1,y1]] target rect and a GeoJSON object;
		// hand it a synthetic polygon whose bbox matches the requested RD extent.
		const [[minX, minY], [maxX, maxY]] = extent;
		const bboxFeature = {
			type: 'Polygon',
			coordinates: [
				[
					[minX, minY],
					[maxX, minY],
					[maxX, maxY],
					[minX, maxY],
					[minX, minY]
				]
			]
		};
		return p.fitExtent(
			[
				[0, 0],
				[size[0], size[1]]
			],
			bboxFeature
		);
	}
	return p.fitSize(size, features);
}

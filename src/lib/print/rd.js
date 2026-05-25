// WGS84 ↔ RD New (EPSG:28992) helpers.
//
// The live map uses maplibre-gl which exposes WGS84 lng/lat. The print SVG is
// rendered with `geoIdentity()` over RD-projected topojson (EPSG:28992), so to
// frame the print extent from a live map viewport we need to forward-project
// WGS84 → RD. proj4js does this with a standard EPSG definition; it has no
// `window` dependency and is safe to import at module-eval time.
//
// Reference: epsg.io/28992 — official RD New (Rijksdriehoekstelsel) parameters
// including the Bessel ellipsoid + 7-parameter Bursa-Wolf transform.
import proj4 from 'proj4';

const RD =
	'+proj=sterea +lat_0=52.15616055555555 +lon_0=5.38763888888889 ' +
	'+k=0.9999079 +x_0=155000 +y_0=463000 +ellps=bessel ' +
	'+towgs84=565.4171,50.3319,465.5524,-0.398957,0.343988,-1.8774,4.0725 ' +
	'+units=m +no_defs';

proj4.defs('EPSG:28992', RD);
const fwd = proj4('EPSG:4326', 'EPSG:28992');

/**
 * Project a WGS84 [lng, lat] to RD New [x, y] in metres.
 * @param {[number, number]} lngLat
 * @returns {[number, number]}
 */
export function wgs84ToRd(lngLat) {
	const [x, y] = fwd.forward(lngLat);
	return [x, y];
}

/**
 * Project a maplibre LngLatBounds-like object to an axis-aligned RD bbox.
 * Accepts anything with `getSouthWest()`/`getNorthEast()` returning `{lng,lat}`
 * (i.e. maplibre's LngLatBounds), or a `[[w,s],[e,n]]` array.
 *
 * Projects all four corners (RD isn't aligned with WGS84 axes near the edges
 * of the Netherlands so projecting just SW/NE would clip) and returns the
 * tight axis-aligned bbox in RD metres: `[[minX,minY],[maxX,maxY]]`.
 *
 * @param {{getSouthWest():{lng:number,lat:number}, getNorthEast():{lng:number,lat:number}} | [[number,number],[number,number]]} bounds
 * @returns {[[number, number], [number, number]]}
 */
export function rdBoundsFromWgs84(bounds) {
	let w, s, e, n;
	if (Array.isArray(bounds)) {
		[[w, s], [e, n]] = bounds;
	} else {
		const sw = bounds.getSouthWest();
		const ne = bounds.getNorthEast();
		w = sw.lng;
		s = sw.lat;
		e = ne.lng;
		n = ne.lat;
	}
	const corners = [
		[w, s],
		[e, s],
		[e, n],
		[w, n]
	];
	let minX = Infinity,
		minY = Infinity,
		maxX = -Infinity,
		maxY = -Infinity;
	for (const c of corners) {
		const [x, y] = fwd.forward(c);
		if (x < minX) minX = x;
		if (y < minY) minY = y;
		if (x > maxX) maxX = x;
		if (y > maxY) maxY = y;
	}
	return [
		[minX, minY],
		[maxX, maxY]
	];
}

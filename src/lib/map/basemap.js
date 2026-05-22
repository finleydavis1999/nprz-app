// Basemap helpers: PMTiles protocol registration + protomaps style builders.
//
// Three sources, all using the same protomaps-themes-base layer set:
//   - protomapsApiStyle({apiKey, theme}): hosted MVT tiles via Protomaps API
//   - pmtilesStyle({url, theme}):         self-hosted PMTiles file (range-served)
//   - emptyStyle({background}):           plain background fallback
import maplibregl from 'maplibre-gl';
import * as pmtilesPkg from 'pmtiles';
import {
	layers as protomapsLayers,
	labels as protomapsLabels,
	namedTheme
} from 'protomaps-themes-base';

const ATTRIBUTION =
	'<a href="https://protomaps.com">Protomaps</a> | © <a href="https://openstreetmap.org">OpenStreetMap</a>';
const GLYPHS = 'https://fonts.protomaps.com/{fontstack}/{range}.pbf';
// Language for basemap place/road labels.
const LABEL_LANG = 'nl';

// Full protomaps layer set: base (fills/lines) + label (symbol) layers on top.
// `layers()` in protomaps-themes-base v4 excludes labels; they come from the
// separate `labels()` export. Keeping the labels last puts them above the base
// map; app layers insert below them via layer-order.js anchors.
function styleLayers(theme) {
	return [
		...protomapsLayers('protomaps', namedTheme(theme)),
		...protomapsLabels('protomaps', theme, LABEL_LANG)
	];
}

let protocolRegistered = false;
export function registerPmtilesProtocol() {
	if (protocolRegistered) return;
	const protocol = new pmtilesPkg.Protocol();
	maplibregl.addProtocol('pmtiles', protocol.tile);
	protocolRegistered = true;
}

export function protomapsApiStyle({ apiKey, theme = 'white' }) {
	return {
		version: 8,
		glyphs: GLYPHS,
		sources: {
			protomaps: {
				type: 'vector',
				tiles: [`https://api.protomaps.com/tiles/v4/{z}/{x}/{y}.mvt?key=${apiKey}`],
				minzoom: 0,
				maxzoom: 15,
				attribution: ATTRIBUTION
			}
		},
		layers: styleLayers(theme)
	};
}

export function pmtilesStyle({ url, theme = 'white' }) {
	return {
		version: 8,
		glyphs: GLYPHS,
		sources: {
			protomaps: {
				type: 'vector',
				url: `pmtiles://${url}`,
				attribution: ATTRIBUTION
			}
		},
		layers: styleLayers(theme)
	};
}

export function emptyStyle({ background = '#f3f0e8' } = {}) {
	return {
		version: 8,
		sources: {},
		layers: [{ id: 'background', type: 'background', paint: { 'background-color': background } }]
	};
}

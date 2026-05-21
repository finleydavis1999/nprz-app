// Basemap helpers: PMTiles protocol registration + protomaps style builders.
//
// Three sources, all using the same protomaps-themes-base layer set:
//   - protomapsApiStyle({apiKey, theme}): hosted MVT tiles via Protomaps API
//   - pmtilesStyle({url, theme}):         self-hosted PMTiles file (range-served)
//   - emptyStyle({background}):           plain background fallback
import maplibregl from 'maplibre-gl';
import * as pmtilesPkg from 'pmtiles';
import { layers as protomapsLayers, namedTheme } from 'protomaps-themes-base';

const ATTRIBUTION =
	'<a href="https://protomaps.com">Protomaps</a> | © <a href="https://openstreetmap.org">OpenStreetMap</a>';
const GLYPHS = 'https://protomaps.github.io/basemaps-assets/fonts/{fontstack}/{range}.pbf';
const SPRITES = 'https://protomaps.github.io/basemaps-assets/sprites/v4/light';

let protocolRegistered = false;
export function registerPmtilesProtocol() {
	if (protocolRegistered) return;
	const protocol = new pmtilesPkg.Protocol();
	maplibregl.addProtocol('pmtiles', protocol.tile);
	protocolRegistered = true;
}

export function protomapsApiStyle({ apiKey, theme = 'white' }) {
	const baseLayers = protomapsLayers('protomaps', namedTheme(theme), { lang: 'nl' });

	// Post-process: darken only genuine built-up area layers
	const layers = baseLayers.map((layer) => {
		if (layer.id === 'buildings') {
			return { ...layer, paint: { ...layer.paint, 'fill-color': '#d0ccc6' } };
		}
		if (layer.id === 'landuse_industrial') {
			return { ...layer, paint: { ...layer.paint, 'fill-color': '#cdc9c3' } };
		}
		return layer;
	});

	return {
		version: 8,
		glyphs: GLYPHS,
		sprite: SPRITES,
		sources: {
			protomaps: {
				type: 'vector',
				tiles: [`https://api.protomaps.com/tiles/v4/{z}/{x}/{y}.mvt?key=${apiKey}`],
				minzoom: 0,
				maxzoom: 15,
				attribution: ATTRIBUTION
			}
		},
		layers
	};
}

export function pmtilesStyle({ url, theme = 'white' }) {
	return {
		version: 8,
		glyphs: GLYPHS,
		sprite: SPRITES,
		sources: {
			protomaps: {
				type: 'vector',
				url: `pmtiles://${url}`,
				attribution: ATTRIBUTION
			}
		},
		layers: protomapsLayers('protomaps', namedTheme(theme), { lang: 'nl' })
	};
}

export function emptyStyle({ background = '#f3f0e8' } = {}) {
	return {
		version: 8,
		sources: {},
		layers: [{ id: 'background', type: 'background', paint: { 'background-color': background } }]
	};
}

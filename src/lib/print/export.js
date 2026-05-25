const NS = 'http://www.w3.org/2000/svg';

/**
 * Compose a single, self-contained SVG document that mirrors the on-screen
 * print sheet: title at top, the framed map in the middle, the legend, then
 * the source-attribution footer. Used by both the SVG and PNG export paths
 * so what the user sees in the print preview is what they download.
 *
 * The composer embeds a `<style>` block mirroring PrintMap.svelte's scoped
 * styles for label / scalebar text. Without it, the serialized SVG loses
 * those styles when opened outside the live document (Illustrator, or the
 * `<img>` decode step of the PNG rasterizer).
 *
 * @param {Object} opts
 * @param {string} opts.titleText
 * @param {SVGSVGElement | null | undefined} opts.mapSvgEl  the map SVG element rendered by PrintMap.svelte
 * @param {{breaks:number[], colors:string[], format?:(n:number)=>string} | null} opts.legend
 * @param {string} opts.footnoteText
 * @param {number} opts.mapWidth   target map width in CSS px
 * @param {number} opts.mapHeight  target map height in CSS px
 * @returns {SVGSVGElement}
 */
export function composePrintSheet({
	titleText = '',
	mapSvgEl,
	legend,
	footnoteText = '',
	mapWidth,
	mapHeight
}) {
	const padX = 24;
	const padY = 24;
	const titleSize = 22;
	const titleH = titleSize + 10;
	const gap = 14;
	const legendRowH = 18;
	const legendH = legend ? Math.max(legendRowH * legend.colors.length + 4, 24) : 0;
	const footerH = footnoteText ? 22 : 0;

	const totalW = mapWidth + padX * 2;
	const totalH = padY + titleH + gap + mapHeight + gap + legendH + gap + footerH + padY;

	const svg = document.createElementNS(NS, 'svg');
	svg.setAttribute('xmlns', NS);
	svg.setAttribute('viewBox', `0 0 ${totalW} ${totalH}`);
	svg.setAttribute('width', String(totalW));
	svg.setAttribute('height', String(totalH));

	// Inline stylesheet — keeps text labels styled when the SVG is opened
	// outside the Svelte runtime (Illustrator/Inkscape) or rasterized via
	// <img> for the PNG path. Mirrors the scoped <style> in PrintMap.svelte.
	const style = document.createElementNS(NS, 'style');
	style.textContent = `
		.map-label, .pie-label {
			font-family: system-ui, sans-serif; fill: #1f2328;
			paint-order: stroke; stroke: rgba(255,255,255,0.95);
			stroke-width: 2; stroke-linejoin: round;
		}
		.pie-label { font-size: 9px; }
		.map-label { font-size: 6px; }
		.scalebar-label {
			font-family: system-ui, sans-serif; fill: #1f2328; font-size: 10px;
			paint-order: stroke; stroke: rgba(255,255,255,0.95);
			stroke-width: 2.5; stroke-linejoin: round;
		}
		.place-label {
			font-family: system-ui, sans-serif; fill: #1f2328;
			paint-order: stroke; stroke: rgba(255,255,255,0.95);
			stroke-width: 2.5; stroke-linejoin: round; font-weight: 500;
		}
		.place-label--country { font-size: 14px; font-weight: 700; letter-spacing: 0.08em; text-transform: uppercase; }
		.place-label--region  { font-size: 11px; font-weight: 600; letter-spacing: 0.04em; text-transform: uppercase; }
		.place-label--locality { font-size: 10px; font-weight: 600; }
		.place-label--subplace { font-size: 8px;  font-weight: 500; fill: #4a5159; }
	`;
	svg.appendChild(style);

	// White paper background.
	const bg = document.createElementNS(NS, 'rect');
	bg.setAttribute('x', '0');
	bg.setAttribute('y', '0');
	bg.setAttribute('width', String(totalW));
	bg.setAttribute('height', String(totalH));
	bg.setAttribute('fill', '#ffffff');
	svg.appendChild(bg);

	let cursorY = padY;

	if (titleText) {
		const t = document.createElementNS(NS, 'text');
		t.setAttribute('x', String(padX));
		t.setAttribute('y', String(cursorY + titleSize - 4));
		t.setAttribute('font-family', 'system-ui, sans-serif');
		t.setAttribute('font-size', String(titleSize));
		t.setAttribute('font-weight', '600');
		t.setAttribute('fill', '#1f2328');
		t.textContent = titleText;
		svg.appendChild(t);
	}
	cursorY += titleH + gap;

	// Embed the map as a nested <svg> so its own viewBox is preserved
	// independently of the parent's. We clone to avoid mutating the live DOM.
	if (mapSvgEl) {
		const inner = /** @type {SVGSVGElement} */ (mapSvgEl.cloneNode(true));
		inner.setAttribute('x', String(padX));
		inner.setAttribute('y', String(cursorY));
		inner.setAttribute('width', String(mapWidth));
		inner.setAttribute('height', String(mapHeight));
		inner.setAttribute('preserveAspectRatio', 'xMidYMid meet');
		// Strip any inline width/height that would override our attributes.
		inner.removeAttribute('style');
		svg.appendChild(inner);
	}
	cursorY += mapHeight + gap;

	if (legend) {
		const fmt = legend.format ?? ((n) => Math.round(n).toLocaleString());
		const swatchW = 22;
		const swatchH = 14;
		const g = document.createElementNS(NS, 'g');
		g.setAttribute('transform', `translate(${padX} ${cursorY})`);
		legend.colors.forEach((color, i) => {
			const y = i * legendRowH;
			const rect = document.createElementNS(NS, 'rect');
			rect.setAttribute('x', '0');
			rect.setAttribute('y', String(y + (legendRowH - swatchH) / 2));
			rect.setAttribute('width', String(swatchW));
			rect.setAttribute('height', String(swatchH));
			rect.setAttribute('fill', color);
			rect.setAttribute('stroke', '#d0d7de');
			rect.setAttribute('stroke-width', '0.5');
			g.appendChild(rect);
			const t = document.createElementNS(NS, 'text');
			t.setAttribute('x', String(swatchW + 8));
			t.setAttribute('y', String(y + legendRowH - 5));
			t.setAttribute('font-family', 'system-ui, sans-serif');
			t.setAttribute('font-size', '11');
			t.setAttribute('fill', '#57606a');
			t.textContent = `${fmt(legend.breaks[i])} – ${fmt(legend.breaks[i + 1])}`;
			g.appendChild(t);
		});
		svg.appendChild(g);
		// (cursorY would advance here, but the footer uses absolute placement
		// at totalH − padY so the next-stage advance isn't read again.)
	}

	if (footnoteText) {
		const t = document.createElementNS(NS, 'text');
		t.setAttribute('x', String(padX));
		t.setAttribute('y', String(totalH - padY - 4));
		t.setAttribute('font-family', 'system-ui, sans-serif');
		t.setAttribute('font-size', '10');
		t.setAttribute('fill', '#8c959f');
		t.textContent = footnoteText;
		svg.appendChild(t);
	}

	return svg;
}

// Serialize an inline <svg> element to a standalone .svg file and trigger a
// browser download. The output opens in Illustrator / Inkscape with all paths
// editable; vector text remains text.
export function downloadSvg(svgEl, filename = 'map.svg') {
	if (!svgEl) return;
	const url = URL.createObjectURL(svgToBlob(svgEl));
	triggerDownload(url, filename);
	URL.revokeObjectURL(url);
}

/**
 * Rasterize the inline `<svg>` to a PNG at the requested DPI and trigger a
 * browser download. Uses the SVG's `viewBox` as the natural drawing size and
 * scales by `dpi / 96` (CSS px → device px). Caps the long edge so a missed
 * unit conversion can't silently produce a 60-MB image.
 *
 * Pure-vector inputs (which is all the print map produces today — choropleth
 * paths, flow lines, scale bar, text labels) cross-domain cleanly into the
 * canvas. If you ever introduce external raster references inside the SVG,
 * the canvas will taint and `toBlob` will throw — handle there.
 *
 * @param {SVGSVGElement | null | undefined} svgEl
 * @param {string} filename
 * @param {{ dpi?: number, maxEdgePx?: number }} [opts]
 * @returns {Promise<void>}
 */
export async function downloadPng(svgEl, filename = 'map.png', opts = {}) {
	if (!svgEl) return;
	const { dpi = 300, maxEdgePx = 8000 } = opts;

	const { width, height } = svgNaturalSize(svgEl);
	if (!width || !height) return;
	const scale = Math.min(dpi / 96, maxEdgePx / Math.max(width, height));
	const pxW = Math.round(width * scale);
	const pxH = Math.round(height * scale);

	const svgUrl = URL.createObjectURL(svgToBlob(svgEl));
	try {
		const img = new Image();
		img.decoding = 'async';
		img.src = svgUrl;
		await img.decode();

		const canvas = document.createElement('canvas');
		canvas.width = pxW;
		canvas.height = pxH;
		const cx = canvas.getContext('2d');
		if (!cx) return;
		// White background — PNG has alpha; printers and most viewers handle
		// transparency fine but a non-transparent paper-coloured backdrop
		// matches what the SVG looks like in the preview.
		cx.fillStyle = '#ffffff';
		cx.fillRect(0, 0, pxW, pxH);
		cx.drawImage(img, 0, 0, pxW, pxH);

		await new Promise((resolve) => {
			canvas.toBlob((blob) => {
				if (!blob) return resolve();
				const pngUrl = URL.createObjectURL(blob);
				triggerDownload(pngUrl, filename);
				URL.revokeObjectURL(pngUrl);
				resolve();
			}, 'image/png');
		});
	} finally {
		URL.revokeObjectURL(svgUrl);
	}
}

/** Serialize an SVG element to a standalone-document Blob (with XML prolog). */
function svgToBlob(svgEl) {
	const clone = /** @type {SVGSVGElement} */ (svgEl.cloneNode(true));
	clone.setAttribute('xmlns', 'http://www.w3.org/2000/svg');
	clone.setAttribute('xmlns:xlink', 'http://www.w3.org/1999/xlink');
	const xml = new XMLSerializer().serializeToString(clone);
	return new Blob(['<?xml version="1.0" standalone="no"?>\n', xml], {
		type: 'image/svg+xml;charset=utf-8'
	});
}

/** Natural pixel size of an SVG: prefer viewBox, fall back to bounding box. */
function svgNaturalSize(svgEl) {
	const vb = svgEl.viewBox?.baseVal;
	if (vb && vb.width > 0 && vb.height > 0) {
		return { width: vb.width, height: vb.height };
	}
	const r = svgEl.getBoundingClientRect();
	return { width: r.width, height: r.height };
}

function triggerDownload(url, filename) {
	const a = document.createElement('a');
	a.href = url;
	a.download = filename;
	document.body.appendChild(a);
	a.click();
	a.remove();
}

import { test as base, expect } from '@playwright/test';

// Tests run with AUTH_DISABLED=true (set in playwright.config.js webServer.env),
// so no login dance is needed.
//
// Worker-scoped browser context so OPFS (parquet cache), localStorage, and
// the basemap HTTP cache persist across tests within the worker. Each test
// still gets a fresh page from that shared context, so URL state and Svelte
// in-memory state reset between tests. The suite runs `fullyParallel: true`
// (2 workers on CI, 4 locally — see playwright.config.js), and the shared
// context below is reused across every test a given worker runs.
//
// Playwright's built-in `context` fixture is already test-scoped and can't be
// overridden to worker-scope, so we expose a separate `sharedContext` fixture
// and rebuild `page` from it. All three describe blocks below share it.
const test = base.extend({
	sharedContext: [
		async ({ browser }, use) => {
			const ctx = await browser.newContext();
			// Disable the app's background parquet prefetch: it pulls all ~289 MB
			// of data into OPFS and starves the query each test is waiting on.
			await ctx.addInitScript(() => {
				window.__E2E_NO_PREFETCH__ = true;
				// Render only the heaviest handful of flows. A flow query returns up
				// to ~72k OD pairs; generating bezier geometry for thousands of them
				// freezes the page and, across parallel workers, drags the whole
				// suite to a crawl. Value-based assertions read the top flows, which
				// the cap keeps.
				window.__E2E_FLOW_RENDER_CAP__ = 200;
			});
			// Stub the Protomaps basemap (vector tiles + glyph fonts). Every test
			// loads the map, and these are internet round-trips no assertion
			// depends on. An empty body is a valid empty vector-tile / glyph PBF,
			// so maplibre draws a blank basemap without emitting load errors.
			await ctx.route(
				(url) => url.hostname === 'api.protomaps.com' || url.hostname === 'fonts.protomaps.com',
				(route) => route.fulfill({ status: 200, contentType: 'application/x-protobuf', body: '' })
			);
			await use(ctx);
			await ctx.close();
		},
		{ scope: 'worker' }
	],
	page: async ({ sharedContext }, use) => {
		const page = await sharedContext.newPage();
		await use(page);
		await page.close();
	}
});

test.describe('app', () => {
	test.beforeEach(async ({ page }) => {
		await page.goto('/');
		// Choropleth ready when the node legend renders. Use `.first()` because
		// once flows are enabled (state persists in the worker context), a
		// second `.legend` appears in the flow cartography panel.
		await expect(page.locator('.legend').first()).toBeVisible({ timeout: 30_000 });
		await expect(page.locator('.status').first()).not.toContainText('querying');
	});

	test('renders header, status, sidebar panels, no console errors', async ({ page }) => {
		const errors = [];
		page.on('pageerror', (e) => errors.push(e.message));
		page.on('console', (m) => {
			if (m.type() === 'error') errors.push(m.text());
		});

		await expect(page.locator('.brand')).toContainText('NPRZ');
		await expect(page.locator('.status').first()).toContainText(/PC4s|gemeenten|buurten/);

		// Left sidebar: data inputs.
		const leftTitles = await page.locator('.sidebar-left details.panel summary').allTextContents();
		expect(leftTitles).toEqual(['Scale', 'Node data', 'Flow data', 'Map layers']);

		// Right sidebar: inspect + model results + cartography.
		const rightTitles = await page
			.locator('.sidebar-right details.panel summary')
			.allTextContents();
		expect(rightTitles).toEqual([
			'Inspect',
			'Model results',
			'Node cartography',
			'Flow cartography'
		]);

		// Dock toggle strip exposes Layer Calculator, Study area, and Print.
		await expect(page.locator('.strip')).toBeVisible();
		await expect(page.locator('.strip .tool', { hasText: 'Layer Calculator' })).toBeVisible();
		await expect(page.locator('.strip .tool', { hasText: 'Study area' })).toBeVisible();
		await expect(page.locator('.strip a.tool.print', { hasText: 'Print' })).toBeVisible();

		await page.waitForTimeout(500);
		expect(errors.filter((e) => !/sourcemap/i.test(e))).toEqual([]);
	});

	test('saving a layer is one click and auto-uniquifies the default name', async ({ page }) => {
		// Node data panel is open by default — save the current selection twice
		// with no typing. The button must never lock up on the default collision.
		const saveBtn = page.locator('.sidebar-left .save-row button', { hasText: 'Save layer' });
		await expect(saveBtn).toBeEnabled();
		await saveBtn.click();
		await expect(saveBtn).toBeEnabled();
		await saveBtn.click();

		// Both saves land in the Node data panel's "Saved node layers" section;
		// the second gets a sequential suffix.
		const savedList = page
			.locator('.sidebar-left .saved-layers-section')
			.filter({ hasText: 'Saved node layers' });
		const names = savedList.locator('ul.layers .layer:not(.live) .name');
		await expect(names).toHaveCount(2);
		const first = (await names.nth(0).textContent())?.trim();
		expect((await names.nth(1).textContent())?.trim()).toBe(`${first} 2`);

		// Clean up persisted layers for sibling tests.
		const del = savedList.locator('ul.layers .layer:not(.live) .del');
		while (await del.count()) await del.first().click();
	});

	test('clicking a node populates the inspect panel', async ({ page }) => {
		const inspect = page.locator('.sidebar-right details.panel', { hasText: 'Inspect' });
		await expect(inspect.locator('.hint')).toContainText('Hover or click');

		// Click the centre of the map — that should hit a node.
		const mapBox = await page.locator('canvas.maplibregl-canvas').boundingBox();
		if (!mapBox) throw new Error('map canvas not visible');
		await page.mouse.click(mapBox.x + mapBox.width / 2, mapBox.y + mapBox.height / 2);

		await expect(inspect.locator('.badge.node')).toBeVisible({ timeout: 5_000 });
		await expect(inspect.locator('code.id')).not.toBeEmpty();
	});

	test('flow toggle adds curved flow lines to the map', async ({ page }) => {
		// Open Flow data panel and flip the enable switch.
		const flowPanel = page.locator('details.panel', { hasText: 'Flow data' });
		await flowPanel.locator('summary').click();
		const enable = flowPanel
			.locator('label.toggle', { hasText: 'Show flows' })
			.locator('input[type="checkbox"]');
		await enable.check();

		// Status row for flows appears with a positive feature count.
		await expect(page.locator('.status').nth(1)).toContainText(/flow:.*flows/i, {
			timeout: 15_000
		});
		await expect(page.locator('.status').nth(1)).not.toContainText('querying', {
			timeout: 15_000
		});

		// Wait for source data to settle, then assert layer + features exist.
		await page.waitForFunction(
			() => {
				const m = window.__map;
				return (
					!!m?.getSource?.('flow-ovin-gem') &&
					!!m?.getLayer?.('flow-ovin-gem-line') &&
					m.querySourceFeatures('flow-ovin-gem').length > 0
				);
			},
			{ timeout: 15_000 }
		);

		// Disable flows again — the toggle persists in the worker context's
		// localStorage, and leaving it on makes every later test re-query OD
		// data (and a heavy aggregation) it doesn't need.
		await enable.uncheck();
		await expect(page.locator('.status')).toHaveCount(1);
	});

	test('ODiN flow exposes an age range filter that narrows the query', async ({ page }) => {
		test.slow();
		const flowPanel = page.locator('details.panel', { hasText: 'Flow data' });
		await flowPanel.locator('summary').click();
		const enable = flowPanel
			.locator('label.toggle', { hasText: 'Show flows' })
			.locator('input[type="checkbox"]');
		await enable.check();

		// The age range slider renders for ODiN (the default flow dataset) and
		// defaults to the full 0–99 span. It must NOT appear as a category chip.
		const ageField = flowPanel.locator('label.field', { hasText: 'Leeftijd' });
		await expect(ageField).toHaveCount(1);
		await expect(ageField).toContainText(/0\s*[–-]\s*99/);

		const status = page.locator('.status').nth(1);
		await expect(status).toContainText(/flow:.*flows/i, { timeout: 15_000 });
		await expect(status).not.toContainText('querying', { timeout: 15_000 });

		// `flow: … N flows` — N is the total OD pairs returned by the query
		// (the number right before "flows", whether or not a "shown / total"
		// split is present). That's what the age WHERE filter changes.
		const totalFlows = async () => {
			const txt = await status.innerText();
			const m = txt.match(/([\d,]+)\s*flows/i);
			return m ? Number(m[1].replace(/,/g, '')) : NaN;
		};
		const fullCount = await totalFlows();
		expect(fullCount).toBeGreaterThan(0);

		// Drag the lower (ageMin) handle up to 90 → restrict to ages 90–99.
		// The first `.thumb` input is the lower handle.
		const minThumb = ageField.locator('input.thumb').first();
		await minThumb.evaluate((el) => {
			el.value = '90';
			el.dispatchEvent(new Event('input', { bubbles: true }));
		});
		await expect(ageField).toContainText(/90\s*[–-]\s*99/);
		await expect(status).not.toContainText('querying', { timeout: 15_000 });

		// Restricting to the oldest band returns strictly fewer flows than the
		// full 0–99 range — proves the age bound reaches the DuckDB query.
		await expect.poll(totalFlows, { timeout: 15_000 }).toBeLessThan(fullCount);

		// Reset the range so persisted flow state doesn't leak into later tests.
		await minThumb.evaluate((el) => {
			el.value = '0';
			el.dispatchEvent(new Event('input', { bubbles: true }));
		});
		await expect(ageField).toContainText(/0\s*[–-]\s*99/);
		await enable.uncheck();
		await expect(page.locator('.status')).toHaveCount(1);
	});

	test('divideYears toggle normalises categorical-period flow values to per-year', async ({
		page
	}) => {
		test.slow();
		const flowPanel = page.locator('details.panel', { hasText: 'Flow data' });
		await flowPanel.locator('summary').click();

		// Use werkwerk ("Baanverhuizingen"): it carries the same 6/6/11-year
		// divideYears periods as woon-werk but its parquet is ~8 MB vs woon-werk's
		// ~60 MB, so the two full-table scans this test runs stay fast on a
		// CPU-contended CI runner instead of blowing the query timeout.
		await flowPanel
			.locator('label.field', { hasText: 'Dataset' })
			.locator('select')
			.selectOption({ label: 'Baanverhuizingen 1999-2017' });
		const enable = flowPanel
			.locator('label.toggle', { hasText: 'Show flows' })
			.locator('input[type="checkbox"]');
		await enable.check();

		// Wait for the werkwerk flow source to populate.
		await page.waitForFunction(
			() => (window.__map?.querySourceFeatures?.('flow-werkwerk-gem')?.length ?? 0) > 0,
			{ timeout: 20_000 }
		);
		const maxValue = () =>
			page.evaluate(() =>
				(window.__map.querySourceFeatures('flow-werkwerk-gem') ?? []).reduce(
					(mx, f) => Math.max(mx, f.properties.value ?? 0),
					0
				)
			);
		const before = await maxValue();
		expect(before).toBeGreaterThan(0);

		// Flip "Data per jaar" — every period total is divided by its year span
		// (6–11), so the largest flow value must shrink.
		const divToggle = flowPanel
			.locator('label.toggle', { hasText: 'Data per jaar' })
			.locator('input[type="checkbox"]');
		await expect(divToggle).toHaveCount(1);
		await divToggle.check();
		await expect(page.locator('.status').nth(1)).not.toContainText('querying', { timeout: 15_000 });
		await expect.poll(maxValue, { timeout: 15_000 }).toBeLessThan(before);

		// Reset state for sibling tests: disable flows FIRST (the maxValue poll
		// above already let the divideYears query settle, so no in-flight query
		// can re-add the status row), then switch the dataset back to ODiN while
		// disabled — the switch also clears the divideYears toggle.
		await enable.uncheck();
		await expect(page.locator('.status')).toHaveCount(1);
		await flowPanel
			.locator('label.field', { hasText: 'Dataset' })
			.locator('select')
			.selectOption({ label: 'Verplaatsingen 2004-2024 (OViN/ODiN)' });
	});

	test('directional gradient mode swaps the classic flow layer for the paired layer', async ({
		page
	}) => {
		const flowPanel = page.locator('details.panel', { hasText: 'Flow data' });
		await flowPanel.locator('summary').click();
		const enable = flowPanel
			.locator('label.toggle', { hasText: 'Show flows' })
			.locator('input[type="checkbox"]');
		await enable.check();

		// Classic layer renders first.
		await page.waitForFunction(() => !!window.__map?.getLayer?.('flow-ovin-gem-line'), {
			timeout: 15_000
		});

		// Flip on directional gradient in the Flow cartography panel.
		const cartoPanel = page.locator('details.panel', { hasText: 'Flow cartography' });
		await cartoPanel.locator('summary').click();
		const directional = cartoPanel
			.locator('label.toggle', { hasText: 'Directional gradient' })
			.locator('input[type="checkbox"]');
		await directional.check();

		// The paired directional layer + source replace the classic one.
		await page.waitForFunction(
			() => {
				const m = window.__map;
				return (
					!!m?.getSource?.('flow-directional-ovin-gem') &&
					!!m?.getLayer?.('flow-directional-ovin-gem-line') &&
					!m?.getLayer?.('flow-ovin-gem-line') &&
					m.querySourceFeatures('flow-directional-ovin-gem').length > 0
				);
			},
			{ timeout: 15_000 }
		);

		// The split is colored by magnitude (major=orange / minor=teal), not by
		// the arbitrary canonical direction, and a balance-dot layer marks each
		// gradient split.
		const directionalShape = await page.evaluate(() => {
			const m = window.__map;
			const colorExpr = JSON.stringify(
				m.getPaintProperty('flow-directional-ovin-gem-line', 'line-color')
			);
			const cks = [
				...new Set(m.querySourceFeatures('flow-directional-ovin-gem').map((f) => f.properties.ck))
			];
			const balLayer = !!m.getLayer('flow-directional-ovin-gem-balance-dot');
			const balFeats = m.querySourceFeatures('flow-directional-ovin-gem-balance');
			return {
				colorExpr,
				cks,
				balLayer,
				balanceCount: balFeats.length,
				balanceIsPoint: balFeats[0]?.geometry?.type ?? null
			};
		});
		expect(directionalShape.colorExpr).toContain('#d95f02'); // major / orange
		expect(directionalShape.colorExpr).toContain('#7570b3'); // minor / purple
		expect(directionalShape.cks).toEqual(expect.arrayContaining(['major', 'minor']));
		expect(directionalShape.balLayer).toBe(true);
		expect(directionalShape.balanceCount).toBeGreaterThan(0);
		expect(directionalShape.balanceIsPoint).toBe('Point');

		// Toggling off restores the classic per-direction layer.
		await directional.uncheck();
		await page.waitForFunction(
			() => {
				const m = window.__map;
				return (
					!!m?.getLayer?.('flow-ovin-gem-line') && !m?.getLayer?.('flow-directional-ovin-gem-line')
				);
			},
			{ timeout: 15_000 }
		);

		await enable.uncheck();
		await expect(page.locator('.status')).toHaveCount(1);
	});

	test('spider view: in/out modes render plain circles, unified renders pies', async ({ page }) => {
		const flowPanel = page.locator('details.panel', { hasText: 'Flow data' });
		await flowPanel.locator('summary').click();
		const enable = flowPanel
			.locator('label.toggle', { hasText: 'Show flows' })
			.locator('input[type="checkbox"]');
		await enable.check();

		await page.waitForFunction(
			() =>
				!!window.__map?.getLayer?.('flow-ovin-gem-line') &&
				window.__map.querySourceFeatures('flow-ovin-gem').length > 0,
			{ timeout: 15_000 }
		);

		// Select a node that actually has flows, to enter a non-empty spider view.
		// Pan the busiest flow node to the map centre (the centre clears the
		// overlaying sidebars and reliably registers a click), and hide the flow
		// lines so the click lands on the node polygon, not a line.
		const target = await page.evaluate(() => {
			const m = window.__map;
			const flowFeats = m.querySourceFeatures('flow-ovin-gem');
			if (!flowFeats.length) return null;
			const count = new Map();
			const coordOf = new Map();
			for (const f of flowFeats) {
				const { o, d } = f.properties;
				const cs = f.geometry?.coordinates;
				if (!cs || cs.length < 2) continue;
				count.set(o, (count.get(o) ?? 0) + 1);
				count.set(d, (count.get(d) ?? 0) + 1);
				if (!coordOf.has(o)) coordOf.set(o, cs[0]);
				if (!coordOf.has(d)) coordOf.set(d, cs[cs.length - 1]);
			}
			let best = null;
			let bestN = 0;
			for (const [code, n] of count) {
				if (n > bestN) {
					bestN = n;
					best = code;
				}
			}
			if (!best) return null;
			for (const id of ['flow-ovin-gem-line', 'flow-ovin-gem-casing']) {
				if (m.getLayer(id)) m.setLayoutProperty(id, 'visibility', 'none');
			}
			m.jumpTo({ center: coordOf.get(best) });
			return best;
		});
		expect(target).toBeTruthy();

		// Click the centre of the map — now over the panned-in busy node.
		const mapBox = await page.locator('canvas.maplibregl-canvas').boundingBox();
		if (!mapBox) throw new Error('map canvas not visible');
		await page.mouse.click(mapBox.x + mapBox.width / 2, mapBox.y + mapBox.height / 2);

		// The click selects the node (inspect panel shows it).
		const inspect = page.locator('.sidebar-right details.panel', { hasText: 'Inspect' });
		await expect(inspect.locator('.badge.node')).toBeVisible({ timeout: 5_000 });

		// FlowPies overlay appears for the selected node.
		const pies = page.locator('svg.pies');
		await expect(pies).toBeVisible({ timeout: 5_000 });
		await expect(pies.locator('g.pie').first()).toBeVisible();

		const countShapes = () =>
			page.evaluate(() => {
				const groups = [...document.querySelectorAll('svg.pies g.pie')];
				return {
					groups: groups.length,
					paths: groups.reduce((n, g) => n + g.querySelectorAll('path').length, 0),
					circles: groups.reduce((n, g) => n + g.querySelectorAll('circle').length, 0)
				};
			});

		const seg = (label) => page.locator('button', { hasText: new RegExp(`^${label}$`) }).first();

		// Out mode: no pie slices (<path>), only plain circles.
		await seg('Out').click();
		await expect.poll(async () => (await countShapes()).paths).toBe(0);
		expect((await countShapes()).circles).toBeGreaterThan(0);

		// In mode: same — plain circles, no pies.
		await seg('In').click();
		await expect.poll(async () => (await countShapes()).paths).toBe(0);

		// Unified mode: at least one node carries both directions → a two-slice pie.
		await seg('Unified').click();
		await expect.poll(async () => (await countShapes()).paths).toBeGreaterThan(0);

		// Restore line visibility and clean up the flow toggle for later tests.
		await page.evaluate(() => {
			const m = window.__map;
			for (const id of ['flow-ovin-gem-line', 'flow-ovin-gem-casing']) {
				if (m.getLayer(id)) m.setLayoutProperty(id, 'visibility', 'visible');
			}
		});
		await enable.uncheck();
		await expect(page.locator('.status')).toHaveCount(1);
	});

	test('map layers panel toggles boundary, built-up, province, and basemap labels', async ({
		page
	}) => {
		// Sequentially toggles three map overlays, polls basemap-label visibility
		// twice, then navigates to /print which fetches ~2 MB of topojson before
		// rendering. The 30s default is tight on slower runs; give it 90s.
		test.slow();
		const panel = page.locator('details.panel', { hasText: 'Map layers' });
		// The fixed dock toggle strip overlaps the bottom-left; scroll the panel
		// to the top of the sidebar so it and its controls clear the strip.
		const summary = panel.locator('summary');
		await summary.evaluate((el) => el.scrollIntoView({ block: 'start' }));
		await summary.click();

		const toggle = (name) =>
			panel.locator('label.toggle', { hasText: name }).locator('input[type="checkbox"]');

		// Boundary overlay appears as a map layer.
		await toggle('Boundary overlay').check();
		await page.waitForFunction(() => !!window.__map?.getLayer?.('overlay-gem-boundary-line'), {
			timeout: 15_000
		});

		// Built-up area fill appears as a map layer.
		await toggle('Built-up areas').check();
		await page.waitForFunction(() => !!window.__map?.getLayer?.('builtup-fill'), {
			timeout: 15_000
		});

		// Province boundary line appears as a map layer.
		await toggle('Province boundaries').check();
		await page.waitForFunction(() => !!window.__map?.getLayer?.('provinces-line'), {
			timeout: 15_000
		});

		// Cartographic z-order, bottom -> top:
		// choropleth fill < boundary overlay < built-up < province < basemap labels.
		const order = await page.evaluate(() => {
			const m = window.__map;
			const ids = m.getLayersOrder();
			const firstLabel = ids.find((id) => {
				const l = m.getLayer(id);
				return l && l.source === 'protomaps' && l.type === 'symbol';
			});
			return {
				choropleth: ids.indexOf('choropleth-gem-fill'),
				boundary: ids.indexOf('overlay-gem-boundary-line'),
				builtup: ids.indexOf('builtup-fill'),
				province: ids.indexOf('provinces-line'),
				firstLabel: ids.indexOf(firstLabel)
			};
		});
		expect(order.choropleth).toBeGreaterThan(-1);
		expect(order.choropleth).toBeLessThan(order.boundary);
		expect(order.boundary).toBeLessThan(order.builtup);
		expect(order.builtup).toBeLessThan(order.province);
		expect(order.province).toBeLessThan(order.firstLabel);

		// Basemap labels are on by default; toggling off hides every Protomaps
		// symbol layer, toggling back on restores them.
		const symVisibilities = () =>
			page.evaluate(() => {
				const m = window.__map;
				return m
					.getLayersOrder()
					.map((id) => m.getLayer(id))
					.filter((l) => l && l.source === 'protomaps' && l.type === 'symbol')
					.map((l) => m.getLayoutProperty(l.id, 'visibility') ?? 'visible');
			});
		const initial = await symVisibilities();
		expect(initial.length).toBeGreaterThan(0);
		expect(initial.every((v) => v === 'visible')).toBe(true);

		const labels = toggle('Basemap labels');
		await labels.uncheck();
		await expect.poll(symVisibilities).toEqual(initial.map(() => 'none'));
		await labels.check();
		await expect.poll(symVisibilities).toEqual(initial);

		// Boundary, built-up + province stay enabled — they carry through to the
		// d3-geo print/export view as their own SVG layer groups.
		await page.click('a.tool.print');
		await page.waitForURL('/print');
		await page
			.locator('.sheet svg g.provinces path')
			.first()
			.waitFor({ state: 'attached', timeout: 15_000 });
		expect(await page.locator('.sheet svg g.boundary path').count()).toBeGreaterThan(0);
		expect(await page.locator('.sheet svg g.builtup path').count()).toBeGreaterThan(0);
		expect(await page.locator('.sheet svg g.provinces path').count()).toBeGreaterThan(0);
	});

	test('scale toggle switches gemeente → PC4', async ({ page }) => {
		// Default scale is gemeente; switch to PC4 and verify the status flips.
		await expect(page.locator('.status').first()).toContainText('gemeenten');
		await page.locator('.seg label', { hasText: 'PC4' }).click();
		await expect(page.locator('.status').first()).toContainText('PC4s', { timeout: 15_000 });
	});

	test('Buurt scale falls back to the CBS dataset and shows the variable picker', async ({
		page
	}) => {
		await expect(page.locator('.status').first()).toContainText('gemeenten');
		await page.locator('.seg label', { hasText: 'Buurt' }).click();
		await expect(page.locator('.status').first()).toContainText('buurten', { timeout: 20_000 });
		// Existing datasets are pc4+gem only — DatasetPicker falls back to the
		// CBS dataset, which surfaces the variable picker.
		const nodePanel = page.locator('details.panel', { hasText: 'Node data' });
		await expect(nodePanel.locator('label.field', { hasText: 'Variabele' })).toBeVisible();
		await expect(page.locator('.status').first()).not.toContainText('querying', {
			timeout: 20_000
		});
	});

	test('filter chip toggles affect status reset state', async ({ page }) => {
		const nodeStatus = page.locator('.status').first();
		const baselineStatus = await nodeStatus.textContent();
		await page.locator('.chip', { hasText: '12-18' }).first().click();
		await expect(page.locator('button.link', { hasText: /Reset \(/ }).first()).toBeVisible();
		await expect(nodeStatus).not.toContainText('querying', { timeout: 10_000 });
		await page.locator('button.link', { hasText: /Reset/ }).first().click();
		await expect(nodeStatus).toContainText(/PC4s|gemeenten|buurten/);
		expect(await nodeStatus.textContent()).toBe(baselineStatus);
	});

	test('classification method swap changes legend break values', async ({ page }) => {
		const cartoPanel = page.locator('details.panel').filter({
			has: page.locator('summary', { hasText: /^Node cartography$/ })
		});
		// Legend moved out of the cartography panel into the MapLegend overlay;
		// read break labels from there instead.
		const mapLegend = page.locator('.map-legend .legend').first();
		const before = await mapLegend.locator('.label').allTextContents();
		const methodSelect = cartoPanel.locator('label.field', { hasText: 'Method' }).locator('select');
		await methodSelect.selectOption({ label: 'Quantile' });
		await page.waitForTimeout(400);
		const after = await mapLegend.locator('.label').allTextContents();
		expect(after).not.toEqual(before);
		expect(after.length).toBe(before.length);
		// Reset to default so sibling tests assert against unchanged breaks.
		await methodSelect.selectOption({ label: 'Jenks (natural breaks)' });
	});

	test('print route renders SVG with one path per feature, shares classification', async ({
		page
	}) => {
		// Capture screen-side node-legend breaks first. Reads from the
		// MapLegend overlay (where the legend now lives, not the cartography
		// panel) — first slot is the node legend.
		const screenBreaks = await page
			.locator('.map-legend .legend')
			.first()
			.locator('.label')
			.allTextContents();
		await page.click('a.tool.print');
		await page.waitForURL('/print');
		await expect(page.locator('.sheet svg')).toBeVisible({ timeout: 15_000 });
		// Default scale is gemeente — expect ~342 features.
		await page.locator('.sheet svg path').first().waitFor({ state: 'attached', timeout: 15_000 });
		const pathCount = await page.locator('.sheet svg path').count();
		expect(pathCount).toBeGreaterThan(300);
		// Title + footer present.
		await expect(page.locator('.title')).toContainText('Persoonsgegevens');
		await expect(page.locator('.footnote')).toContainText('CBS microdata');
		// Stylish single-segment scale bar rendered at bottom-left of the SVG.
		await expect(page.locator('.sheet svg g.scalebar')).toBeVisible();
		// Same node-classification breaks as the screen view.
		const printBreaks = await page.locator('.legend .label').allTextContents();
		expect(printBreaks).toEqual(screenBreaks);
	});

	test('smoothed layer: spatial-lag of a node layer computes via the worker', async ({ page }) => {
		// Save a node layer to act as the smoothing input.
		const saveRow = page
			.locator('details.panel', { hasText: 'Node data' })
			.locator('form.save-row');
		await saveRow.locator('input[type="text"]').fill('e2eBase');
		await saveRow.getByRole('button', { name: 'Save layer', exact: true }).click();

		// The Saved-layers list now lives in the Node data sidebar panel (it
		// moved out of the Layer Calculator dock when the dock became a pure
		// editor). The base filter layer (◆) resolves to a numeric count there.
		const savedList = page
			.locator('.sidebar-left .saved-layers-section')
			.filter({ hasText: 'Saved node layers' });
		const baseRow = savedList.locator('.layer').filter({
			has: page.locator('.kind', { hasText: '◆' })
		});
		await expect(baseRow.locator('.meta')).toHaveText(/^\d+$/, { timeout: 15_000 });

		// Open the layer calculator dock to use the smoothed-layer form.
		await page.locator('.strip .tool', { hasText: 'Layer Calculator' }).click();
		const dock = page.locator('.dock', { hasText: 'Layer Calculator' });
		await expect(dock).toBeVisible();

		// The "Add smoothed layer" form auto-fills a default name and auto-picks
		// the node layer as input, so it submits immediately without typing.
		const smoothForm = dock.locator('form.calc', { hasText: 'Add smoothed layer' });
		await smoothForm.getByRole('button', { name: 'Add smoothed layer' }).click();

		// The new smooth-kind row (◈) appears in the sidebar with a numeric
		// count — the spatial-lag Worker fetched RD centroids and produced
		// a value per node.
		const smoothRow = savedList.locator('.layer').filter({
			has: page.locator('.kind', { hasText: '◈' })
		});
		await expect(smoothRow.locator('.meta')).toHaveText(/^\d+$/, { timeout: 15_000 });

		// Clean up so sibling tests see an empty layer list.
		await smoothRow.locator('.del').click();
		await baseRow.locator('.del').click();
		await expect(savedList.locator('.layer:not(.live)')).toHaveCount(0);
		await dock.locator('.dock-close').click();
	});

	test('model calculator dock opens and shows an add-model form when 2+ node layers exist', async ({
		page
	}) => {
		// Reach a state with two saved node-domain filter layers — the minimum
		// the add-model form requires (one dependent + one covariate).
		const saveBtn = page.locator('.sidebar-left .save-row button', { hasText: 'Save layer' });
		await saveBtn.click();
		await saveBtn.click();
		const savedList = page
			.locator('.sidebar-left .saved-layers-section')
			.filter({ hasText: 'Saved node layers' });
		await expect(savedList.locator('.layer:not(.live)')).toHaveCount(2);

		// Open the Model dock and verify the add-model form is present.
		const dock = page.locator('.dock', { hasText: 'Model Calculator' });
		await expect(dock).toHaveCount(0);
		await page.locator('.strip .tool', { hasText: 'Model Calculator' }).click();
		await expect(dock).toBeVisible();
		await expect(dock.locator('.add-head', { hasText: 'Add NLM model' })).toBeVisible();
		// Dependent picker is auto-populated from the saved node layers.
		const dependentOptions = dock.locator('select').first().locator('option');
		await expect(dependentOptions).toHaveCount(2);
		// At least one covariate checkbox is rendered (the other layer).
		// LayerPicker (multi mode) renders each option as a .row with a checkbox.
		await expect(dock.locator('.multi .row input[type="checkbox"]')).toHaveCount(1);
		// COOP/COEP must be set for SharedArrayBuffer (webR's fast channel).
		// This is the only way to check the headers without a separate request.
		const resp = await page.request.get('/');
		expect(resp.headers()['cross-origin-opener-policy']).toBe('same-origin');
		expect(resp.headers()['cross-origin-embedder-policy']).toBe('require-corp');

		// Clean up.
		await dock.locator('.dock-close').click();
		const layerRows = savedList.locator('.layer:not(.live)');
		// Delete each via the × button — count goes down on each removal.
		while ((await layerRows.count()) > 0) {
			await layerRows.first().locator('.del').click();
		}
	});

	// Gated end-to-end webR fit test. Skipped by default because it pays the
	// full webR boot + speedglm install on first run (~30-60s). Enable with
	// `E2E_WITH_WEBR=1 npm run test:e2e` for the rare full-stack run before
	// shipping changes to the model pipeline.
	test('webR end-to-end: fit an NLM and verify children populate (gated)', async ({ page }) => {
		test.skip(
			process.env.E2E_WITH_WEBR !== '1',
			'Set E2E_WITH_WEBR=1 to run the webR fit test (boots webR + installs speedglm)'
		);
		// Allow ~120s — first run downloads webR, installs speedglm + MASS +
		// Matrix, then runs an actual fit.
		test.setTimeout(120_000);

		// Reach the same two-saved-layer baseline the previous test used.
		const saveBtn = page.locator('.sidebar-left .save-row button', { hasText: 'Save layer' });
		await saveBtn.click();
		await saveBtn.click();
		const savedList = page
			.locator('.sidebar-left .saved-layers-section')
			.filter({ hasText: 'Saved node layers' });
		await expect(savedList.locator('.layer:not(.live)')).toHaveCount(2);

		// Open the Model dock and submit the auto-populated form.
		await page.locator('.strip .tool', { hasText: 'Model Calculator' }).click();
		const dock = page.locator('.dock', { hasText: 'Model Calculator' });
		await expect(dock).toBeVisible();
		// One covariate is rendered (the non-dependent layer); tick it.
		await dock.locator('.multi .row input[type="checkbox"]').first().check();
		await dock.locator('button', { hasText: 'Fit model' }).click();

		// Wait for the fit to land — the parent row gets an R² meta when the
		// fit succeeds. Generous timeout for the first-run webR cold start.
		await expect(dock.locator('.layer.parent .meta', { hasText: /R²=/ })).toBeVisible({
			timeout: 100_000
		});

		// SavedLayers should now list the two model-output children (fitted +
		// residual) alongside the parent. Their slugs follow `<parent>_fitted` /
		// `<parent>_residual`.
		await dock.locator('.dock-close').click();
		const savedLayers = page.locator('.sidebar-left .saved-layers');
		await expect(savedLayers.locator('text=/_fitted/').first()).toBeVisible();
		await expect(savedLayers.locator('text=/_residual/').first()).toBeVisible();

		// Cleanup — remove the model parent (cascades children).
		await page.locator('.strip .tool', { hasText: 'Model Calculator' }).click();
		await dock.locator('.layer.parent .del').first().click();
		await dock.locator('.dock-close').click();
		const layerRows = savedList.locator('.layer:not(.live)');
		while ((await layerRows.count()) > 0) {
			await layerRows.first().locator('.del').click();
		}
	});
});

// Pure HTTP/JSON contract assertions on the served manifest — no app boot, no
// map, no node query. Lives outside `describe('app')` so it skips the heavy
// `beforeEach` (goto + choropleth render) it would otherwise pay for nothing.
test.describe('manifest contract', () => {
	test('manifest exposes the restored flow variables with full value parity', async ({ page }) => {
		// Validates the original-metadata → manifest parity port end-to-end through
		// the served manifest: every dropped dimension is back with all its source
		// value levels, and the two special query-modifier fields are toggles.
		const m = await (await page.request.get('/data/manifest.json')).json();
		const keys = (d) => Object.keys(m.flows[d].fields);

		expect(keys('migration')).toEqual(expect.arrayContaining(['sec', 'inkchanges', 'divideYears']));
		expect(keys('werkwerk')).toEqual(expect.arrayContaining(['sectorsector', 'divideYears']));
		expect(keys('woonwerk')).toEqual(expect.arrayContaining(['sectorcat', 'divideYears']));
		expect(keys('ovin')).toEqual(expect.arrayContaining(['hhfilter']));

		// Full value-level parity against the original .js metadata.
		expect(m.flows.migration.fields.sec.values).toHaveLength(10);
		expect(m.flows.migration.fields.inkchanges.values).toHaveLength(5);
		expect(m.flows.werkwerk.fields.sectorsector.values).toHaveLength(51);
		expect(m.flows.woonwerk.fields.sectorcat.values).toHaveLength(9);
		// soortbaan now exposes the previously-dropped 3rd source level.
		for (const d of ['werkwerk', 'woonwerk']) {
			expect(m.flows[d].fields.soortbaan.values.map((v) => v.id)).toContain(3);
		}

		// Special fields are toggles; ODiN declares its household dedup column.
		expect(m.flows.migration.fields.divideYears.type).toBe('toggle');
		expect(m.flows.ovin.fields.hhfilter.type).toBe('toggle');
		expect(m.flows.ovin.hhDedupCol).toBe('hhid');
		// divideYears needs each period's calendar-year span.
		expect(m.flows.werkwerk.fields.year.values.every((v) => Number.isFinite(v.years))).toBe(true);
	});
});

// UI-shell-only tests: they exercise the dock toggle strip and never touch node
// data, so they wait for the always-present `.strip` (rendered top-level by
// DockToggleStrip, outside the manifest-gated sidebars) instead of the
// choropleth `.legend`. The node query still runs in the background; the test
// just doesn't block on it.
test.describe('app (shell only)', () => {
	test.beforeEach(async ({ page }) => {
		await page.goto('/');
		await expect(page.locator('.strip')).toBeVisible({ timeout: 30_000 });
	});

	test('layer calculator dock opens from the toggle strip', async ({ page }) => {
		await expect(page.locator('.dock', { hasText: 'Layer Calculator' })).toHaveCount(0);
		await page.locator('.strip .tool', { hasText: 'Layer Calculator' }).click();
		const dock = page.locator('.dock', { hasText: 'Layer Calculator' });
		await expect(dock).toBeVisible();
		// Close button hides it. Cleans up persisted dock state for sibling tests.
		await dock.locator('.dock-close').click();
		await expect(page.locator('.dock', { hasText: 'Layer Calculator' })).toHaveCount(0);
	});
});

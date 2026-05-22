import { test as base, expect } from '@playwright/test';

// Tests run with AUTH_DISABLED=true (set in playwright.config.js webServer.env),
// so no login dance is needed.
//
// Worker-scoped browser context so OPFS (parquet cache), localStorage, and
// the basemap HTTP cache persist across tests within the worker. Each test
// still gets a fresh page from that shared context, so URL state and Svelte
// in-memory state reset between tests. With `fullyParallel: false` in the
// playwright config this means: 1 worker + 8 sequential tests + 1 cold-start.
//
// Playwright's built-in `context` fixture is already test-scoped and can't be
// overridden to worker-scope, so we expose a separate `sharedContext` fixture
// and rebuild `page` from it.
const test = base.extend({
	sharedContext: [
		async ({ browser }, use) => {
			const ctx = await browser.newContext();
			// Disable the app's background parquet prefetch: it pulls all ~289 MB
			// of data into OPFS and starves the query each test is waiting on.
			await ctx.addInitScript(() => {
				window.__E2E_NO_PREFETCH__ = true;
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
		await expect(page.locator('.status').first()).toContainText(/PC4s|gemeenten/);

		// Left sidebar: data inputs.
		const leftTitles = await page.locator('.sidebar-left details.panel summary').allTextContents();
		expect(leftTitles).toEqual(['Scale', 'Node data', 'Flow data', 'Map layers']);

		// Right sidebar: inspect + cartography.
		const rightTitles = await page
			.locator('.sidebar-right details.panel summary')
			.allTextContents();
		expect(rightTitles).toEqual(['Inspect', 'Node cartography', 'Flow cartography']);

		// Dock toggle strip exposes Layer Calculator, Study area, and Print.
		await expect(page.locator('.strip')).toBeVisible();
		await expect(page.locator('.strip .tool', { hasText: 'Layer Calculator' })).toBeVisible();
		await expect(page.locator('.strip .tool', { hasText: 'Study area' })).toBeVisible();
		await expect(page.locator('.strip a.tool.print', { hasText: 'Print' })).toBeVisible();

		await page.waitForTimeout(500);
		expect(errors.filter((e) => !/sourcemap/i.test(e))).toEqual([]);
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

	test('saving a layer is one click and auto-uniquifies the default name', async ({ page }) => {
		// Node data panel is open by default — save the current selection twice
		// with no typing. The button must never lock up on the default collision.
		const saveBtn = page.locator('.sidebar-left .save-row button', { hasText: 'Save layer' });
		await expect(saveBtn).toBeEnabled();
		await saveBtn.click();
		await expect(saveBtn).toBeEnabled();
		await saveBtn.click();

		// Both saves land as separate layers; the second gets a sequential suffix.
		await page.locator('.strip .tool', { hasText: 'Layer Calculator' }).click();
		const dock = page.locator('.dock', { hasText: 'Layer Calculator' });
		const names = dock.locator('ul.layers .layer:not(.live) .name');
		await expect(names).toHaveCount(2);
		const first = (await names.nth(0).textContent())?.trim();
		expect((await names.nth(1).textContent())?.trim()).toBe(`${first} 2`);

		// Clean up persisted layers + dock state for sibling tests.
		const del = dock.locator('ul.layers .layer:not(.live) .del');
		while (await del.count()) await del.first().click();
		await dock.locator('.dock-close').click();
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

	test('map layers panel toggles boundary, built-up, province, and basemap labels', async ({
		page
	}) => {
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

	test('filter chip toggles affect status reset state', async ({ page }) => {
		const nodeStatus = page.locator('.status').first();
		const baselineStatus = await nodeStatus.textContent();
		await page.locator('.chip', { hasText: '12-18' }).first().click();
		await expect(page.locator('button.link', { hasText: /Reset \(/ }).first()).toBeVisible();
		await expect(nodeStatus).not.toContainText('querying', { timeout: 10_000 });
		await page.locator('button.link', { hasText: /Reset/ }).first().click();
		await expect(nodeStatus).toContainText(/PC4s|gemeenten/);
		expect(await nodeStatus.textContent()).toBe(baselineStatus);
	});

	test('classification method swap changes legend break values', async ({ page }) => {
		const cartoPanel = page.locator('details.panel').filter({
			has: page.locator('summary', { hasText: /^Node cartography$/ })
		});
		const before = await cartoPanel.locator('.legend .label').allTextContents();
		const methodSelect = cartoPanel.locator('label.field', { hasText: 'Method' }).locator('select');
		await methodSelect.selectOption({ label: 'Quantile' });
		await page.waitForTimeout(400);
		const after = await cartoPanel.locator('.legend .label').allTextContents();
		expect(after).not.toEqual(before);
		expect(after.length).toBe(before.length);
		// Reset to default so sibling tests assert against unchanged breaks.
		await methodSelect.selectOption({ label: 'Jenks (natural breaks)' });
	});

	test('print route renders SVG with one path per feature, shares classification', async ({
		page
	}) => {
		// Capture screen-side node-legend breaks first. Scope to the node
		// cartography panel so the flow legend (when present) doesn't pollute.
		const nodeCartoLegend = page
			.locator('details.panel', { has: page.locator('summary', { hasText: /^Node cartography$/ }) })
			.locator('.legend');
		const screenBreaks = await nodeCartoLegend.locator('.label').allTextContents();
		await page.click('a.tool.print');
		await page.waitForURL('/print');
		await expect(page.locator('.sheet svg')).toBeVisible({ timeout: 15_000 });
		// Default scale is gemeente — expect ~342 features.
		await page.locator('.sheet svg path').first().waitFor({ state: 'attached', timeout: 15_000 });
		const pathCount = await page.locator('.sheet svg path').count();
		expect(pathCount).toBeGreaterThan(300);
		// Title + footer present.
		await expect(page.locator('.title')).toContainText('Persoonsgegevens');
		await expect(page.locator('.footnote')).toContainText('EPSG:28992');
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

		// Open the layer calculator dock.
		await page.locator('.strip .tool', { hasText: 'Layer Calculator' }).click();
		const dock = page.locator('.dock', { hasText: 'Layer Calculator' });
		await expect(dock).toBeVisible();

		// The saved node filter layer (◆) resolves to a numeric feature count.
		const baseRow = dock.locator('.layer').filter({
			has: page.locator('.kind', { hasText: '◆' })
		});
		await expect(baseRow.locator('.meta')).toHaveText(/^\d+$/, { timeout: 15_000 });

		// The "Add smoothed layer" form auto-fills a default name and auto-picks
		// the node layer as input, so it submits immediately without typing.
		const smoothForm = dock.locator('form.calc', { hasText: 'Add smoothed layer' });
		await smoothForm.getByRole('button', { name: 'Add smoothed layer' }).click();

		// A smooth-kind row (◈) appears with a numeric count — the spatial-lag
		// Worker fetched RD centroids and produced a value per node.
		const smoothRow = dock.locator('.layer').filter({
			has: page.locator('.kind', { hasText: '◈' })
		});
		await expect(smoothRow.locator('.meta')).toHaveText(/^\d+$/, { timeout: 15_000 });

		// Clean up so sibling tests see an empty layer list.
		await smoothRow.locator('.del').click();
		await baseRow.locator('.del').click();
		await expect(dock.locator('.layer', { hasText: 'e2e' })).toHaveCount(0);
		await dock.locator('.dock-close').click();
	});
});

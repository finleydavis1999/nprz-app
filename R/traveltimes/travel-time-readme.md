# Travel-time matrix build

National multimodal travel-time matrices (car / public transport / bike / walk)
at gemeente, PC4 and buurt scale, built with r5r over OpenStreetMap and the
OVapi national GTFS feed.

## Prerequisites

- **Java 21** (Temurin), `JAVA_HOME` set. Newer JDKs may error.
- `install.packages(c("r5r","arrow","dplyr","jsonlite","sf","ggplot2"))`
  (plus `mapview` for interactive centroid maps).
- **CBS 100m grid** (not in the repo) at
  `raw-data/cbs/grid_100m_2024/cbs_vk100_2024_v1.gpkg` — only needed if you rebuild centroids; the committed JSONs mean you can skip it.
- ~10 GB disk. RAM is the binding constraint: the national network build OOMs
  below roughly 20 GB.

Run from the project root, in a **fresh R session** — the Java heap is fixed
when r5r first loads.

Centroids are already committed, so you can go straight to the matrices.

## Option A — staged, with a checkpoint before buurt

```r
setwd("<project root>")
source("R/traveltimes/ttbuildnational.R")

build_all("gem")     # gemeente only: a cheap national smoke test, minutes
build_all()          # gemeente + PC4, then stops with a buurt projection

# CAP["buurt"] <- 90L    # optional: tighten the cap if the projection is large
build_all("buurt")
```

`build_all("gem")` is worth running first. It is only 342 origins, but it
exercises the download, the network build and the orchestration, and surfaces
any memory problem during the network build rather than hours into PC4.

`build_all()` stops after PC4 and prints a projected row count, file size and run
time for buurt, so you can set `CAP["buurt"]` before committing to it.

## Option B — everything in one go, unattended

```r
setwd("<project root>")
sink("traveltime-build-log.txt", split = TRUE)

source("R/traveltimes/ttbuildnational.R")
build_all(c("gem", "pc4"))   # no stop: buurt is not in scales
build_all("buurt")

sink()
```

The second call reuses the downloaded inputs and the built network, so it does
not rebuild anything. All verification output goes to the log for inspection
afterwards. (`sink` captures the check tables; batch progress and the GTFS banner
go to the console only.)

To include centroids in the same run, prepend:

```r
source("R/traveltimes/build-centroids.R")
cents <- build_all_centroids()
qa    <- qa_centroids(cents)
```

## Configuration

The heap is sized from available RAM and the departure date is picked from the
GTFS calendar, so neither needs setting. **Check the paths, though** — `NET_DIR`
and `OUT_DIR` default to `D:/`, which may not exist on your machine. Override
anything by assigning it **before** sourcing:

```r
NET_DIR <- "E:/nprz-tt"; OUT_DIR <- "E:/nprz-tt-out"; HEAP <- "-Xmx32G"
source("R/traveltimes/ttbuildnational.R")
```

| setting         | default               | purpose                   |
| --------------- | --------------------- | ------------------------- |
| `NET_DIR`       | `D:/NPRZ_net`         | OSM, GTFS, `network.dat`  |
| `OUT_DIR`       | `D:/NPRZ_tt_out`      | per-batch intermediates   |
| `PARQUET_DIR`   | `static/data/parquet` | final output              |
| `HEAP`          | auto (RAM - 6 GB)     | JVM heap                  |
| `DEPART`        | auto                  | departure datetime        |
| `CAP`           | 150 min               | travel-time cap per scale |
| `SPLIT_BY_MODE` | buurt only            | one file per mode         |

Runs are batched and resumable: an interruption costs one batch, not the scale.

## Output

`o_code, d_code, mode, minutes, distance_m`

- `mode`: 1 car, 2 public transport, 3 bike, 4 walk (frozen ids)
- `minutes`: PT is the median over a 30-minute departure window
- `distance_m`: **straight-line** between centroids, so mode-independent. r5r's
  `travel_time_matrix` returns no network distance.
- **Pairs beyond the cap are absent, not zero.** Downstream, treat a missing pair
  as "beyond the horizon" — dropping them silently biases any joined analysis.
- **Intrazonal** (`o == d`) times are set from geometry, not taken from r5r,
  which returns access-leg artefacts. The representative internal distance is
  (2/3)·√(mean area / π) for that scale, divided by the mode's median observed
  speed. One value per scale per mode.

Buurt writes four per-mode files; gemeente and PC4 one each.

## Checks

`verify_scale` runs automatically after each scale: schema, duplicate pairs,
origin and destination coverage, geographic spread, cap, mode ordering, implied
speeds, self-pair times, car symmetry, isolated origins, and ten random journeys
drawn fresh each run for external spot-checking.

`snap_report(core, scale)` gives the distance from each centroid to the road
network. Anything over ~300 m inflates every journey from that area by a walking
access leg.

## Centroids

Every polygon gets exactly one centroid; nothing is dropped. All scales use the
app's own shapefiles, so centroids match the polygons the app renders. Method is
recorded in a `source` flag:

1. **population** — population-weighted mean of CBS 100m cell centres
2. **cells** — unweighted cell centres, where cells exist but no population is
   recorded (excludes water; catches CBS's <5-inhabitant suppression)
3. **builtup** — area-weighted centroid of built-up parts (ports, industry)
4. **surface** — `st_point_on_surface`, for genuinely empty areas

Two gates then apply: **-medoid** where the weighted mean landed in a void
between settlements, and **-snapped** where it fell outside its own polygon
(possible for concave, crescent or split shapes). Both relocate to the populated
cell with the greatest gravity-weighted access to the area's population.

Jobs cannot weight a centroid: job data is a count per area, not a distribution
within it. Fallbacks 2 and 3 are the best proxy until LISA provides job
locations.

### Two passes, and one outstanding correction

The road-snap correction needs a built network, which does not exist at
centroid-build time, so centroids are finished in two passes:

1. `build_all_centroids()` — deterministic, and reproduces the committed JSONs.
2. `fix_centroids_by_snap(core, scale)` — after the network is built, moves
   centroids sitting far from the road network onto it, where the snapped point
   stays inside the polygon.

**The committed JSONs are pass-1 output; pass 2 has not been run.** Re-running
`build_all_centroids()` after a correction would overwrite it, so run pass 1
before pass 2, not after. To apply the correction:

```r
source("R/traveltimes/ttbuildnational.R")
core <- build_national_network()
snap_report(core, "pc4")             # lists centroids >300 m from a road
fix_centroids_by_snap(core, "pc4")   # moves them onto the network
unlink(file.path(OUT_DIR, "pc4"), recursive = TRUE)
build_all("pc4")                     # rebuild that scale
```

Centroid maps are written to `figs/centroids/` by `build_all_centroids()`, along
with `qa_centroids()` for a numeric quality summary and, with `mapview`
installed, `inspect_all()` / `inspect_flagged()` / `find_centroid()` for
interactive checking on an OSM basemap.

## Optional: single-province test

Not needed to build the national matrix — a development aid that runs the same
code path scoped to one province in under an hour.

```r
HEAP <- "-Xmx11G"
source("R/traveltimes/testprovince.R")
build_test(c("gem","pc4","buurt"))
```

Set `TEST_PROVINCE` at the top. Paths redirect to `*_test` directories, one per
province. This proves correctness, not capacity.

## Not included

- **Fares.** The Dutch distance-based tariff doesn't fit the GTFS fare standard;
  it could be derived from the tariff formula over computed distances later.
- **Congestion.** Car uses free-flow OSM speeds, so peak times are optimistic —
  the field standard for accessibility work.
- **Realised times.** Scheduled timetable only.
- **App integration.** Not registered in `manifest.json`; nothing reads these yet.

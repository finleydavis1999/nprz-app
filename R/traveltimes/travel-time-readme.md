# Travel-time matrix build

National multimodal travel-time matrices (car / public transport / bike / walk)
at gemeente, PC4 and buurt scale, built with r5r over OpenStreetMap and the
OVapi national GTFS feed.

## Prerequisites

- **Java 21** (Temurin), `JAVA_HOME` set. Newer JDKs may error.
- `install.packages(c("r5r","arrow","dplyr","jsonlite","sf","ggplot2"))`
  (plus `mapview` for interactive centroid maps).
- **CBS 100m grid** at `raw-data/cbs/grid_100m_2024/cbs_vk100_2024_v1.gpkg`
  (not in the repo).
- ~10 GB disk. RAM is the binding constraint: the national network build OOMs
  below roughly 20 GB.

## Run

Two steps, both from the project root, each in a **fresh R session** (the Java
heap is fixed when r5r first loads).

```r
# 1. centroids (a few minutes)
source("R/traveltimes/build-centroids.R")
cents <- build_all_centroids()
qa    <- qa_centroids(cents)

Centroids are built in two passes, because the road-snap correction needs a
network that doesnt exist yet at centroid-build time:

1. `build_all_centroids()` — deterministic; reproduces the committed JSONs
   except for step 2.
2. `fix_centroids_by_snap(core, scale)` — after the network is built, moves
   centroids sitting far from the road network onto it (only where the snapped
   point stays inside the polygon), then rewrite the affected matrices.

The committed JSONs are pass-1 output (pre snap-correction). Running `build_all_centroids()` reproduces them exactly; running
`fix_centroids_by_snap()` afterwards modifies them, and the diff shows which centroids moved.

# 2. matrices
source("R/traveltimes/ttbuildnational.R")
build_all()            # gem + pc4, then stops with a buurt projection
build_all("buurt")     # after deciding CAP["buurt"]
```

Nothing needs editing. The heap is sized from available RAM and the departure
date is picked from the GTFS calendar; both are printed. Override anything by
assigning it **before** sourcing:

```r
NET_DIR <- "E:/tt"; HEAP <- "-Xmx32G"
source("R/traveltimes/ttbuildnational.R")
```

| setting         | default               | purpose                   |
| --------------- | --------------------- | ------------------------- |
| `NET_DIR`       | `D:/NPRZ_net`         | OSM, GTFS, `network.dat`  |
| `OUT_DIR`       | `D:/NPRZ_tt_out`      | per-batch intermediates   |
| `PARQUET_DIR`   | `static/data/parquet` | final output              |
| `HEAP`          | auto (RAM − 6 GB)     | JVM heap                  |
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
- **Intrazonal** (`o == d`) times are set from area geometry and mode speed, not
  taken from r5r, which returns arbitrary access-leg artefacts.

Buurt writes four per-mode files; gemeente and PC4 one each.

## Checks

`verify_scale` runs automatically after each scale: schema, duplicate pairs,
origin and destination coverage, geographic spread, cap, mode ordering, implied
speeds, self-pair times, car symmetry, isolated origins, and ten random journeys
drawn fresh each run for external spot-checking.

`snap_report(core, scale)` gives the distance from each centroid to the road
network. Anything over ~300 m inflates every journey from that area by a walking
access leg. To correct:

```r
fix_centroids_by_snap(core, "pc4")   # moves flagged centroids onto the network
build_all("pc4")                      # delete OUT_DIR/pc4 first to force a re-run
```

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

## Optional: single-province test

Not needed to build the national matrix — a development aid that runs the same
code path scoped to one province in under an hour.

```r
HEAP <- "-Xmx11G"
source("R/traveltimes/testprovince.R")
build_test(c("gem","pc4","buurt"))
```

Set `TEST_PROVINCE` at the top. Paths redirect to `*_test` directories. Changing
the province forces a network rebuild, or the previous one is silently reused.
This proves correctness, not capacity.

## Not included

- **Fares.** The Dutch distance-based tariff doesn't fit the GTFS fare standard;
  it could be derived from the tariff formula over computed distances later.
- **Congestion.** Car uses free-flow OSM speeds, so peak times are optimistic —
  the field standard for accessibility work.
- **Realised times.** Scheduled timetable only.
- **App integration.** Not registered in `manifest.json`; nothing reads these yet.

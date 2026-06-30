# Travel-time build pipeline

How the travel-time matrices and weighted centroids were produced, for review
and re-running. The heavy build runs OUTSIDE the app (local R + r5r over a large
OSM/GTFS network); only the final per-scale parquets + manifest entry land in
the repo.

## What's in the repo (committed)

- `R/nodes/build-all-centroids.R` -- population-weighted centroids (PC4 / buurt /
  gemeente) from the CBS 100m grid. Reuses the cbs-vk100.R cell method.
- `static/data/geo/{pc4,buurt,gem}-centroids-weighted.json` -- the outputs.
- `R/edges/traveltime.R` -- combines the two-cluster matrix outputs into one
  app-shaped edge dataset per scale + manifest entry. Includes a validation
  block (cap held, all modes, Rotterdam Zuid spot checks).
- `static/data/parquet/traveltime-edges-{gem,pc4,buurt}.parquet` -- outputs.

## What's NOT in the repo (local build, ~hours of compute)

The network build + matrix runs. All kept together as a reference script under
`R/edges/tt-build-pipeline.R/` (not wired into `R/build.R`); inputs and intermediate
outputs live on a local drive (`D:/NPRZ_tt_out`, gitignored scope).It integrated and includes the following;

- `tt-build/01-build-network.R`  -- r5r network from OSM (Geofabrik) + OVapi GTFS
- `tt-build/02-run-matrix.R`     -- gemeente / PC4 / buurt matrices, per cluster
- `tt-build/03-run-matrix-buurt-checkpointed.R` -- batched/resumable heavy run

## Method summary

- Engine: r5r (R5/Conveyal). PT: OVapi national GTFS. Network: OpenStreetMap.
- Modes: car, public transport (walk+transit), bike, walk.
- Departure: weekday 09:00; PT smoothed over a 30-min window (median).
- Cap: 90 min one-way (Marchetti-based; captures ~97% of commuters). Pairs over
  the cap are ABSENT from the matrix, not zero -- treat absence as "beyond
  commuting horizon" when joining to commute data.
- Origins/destinations: population-weighted centroids.
- Coverage: whole country, built as TWO overlapping province clusters (a single
  national network OOMs on 16 GB RAM). Cluster A = 8 southern provinces (all
  major cities + Rotterdam commute shed); cluster B = northern/eastern provinces
  with Flevoland+Gelderland as overlap buffer. Overlap >= 90-min car reach so
  cross-cluster commute pairs route correctly. Clusters combined + deduped in
  traveltime.R.

## Output schema (app-shaped edge)

`o_code, d_code, year(=1, the 2026 snapshot), mode(1=car,2=OV,3=bike,4=walk),
minutes, count, weight` -- weight = minutes. yearAggregation = "mean" so the
single-year SUM/mean returns the value itself (travel time is not summable).

## Known limitations

- Scheduled times, not realised (no delays/crowding) -- field standard.
- Single departure (09:00 weekday) -- other times can be added later.
- Cost not included (Dutch distance fares don't sit in GTFS) -- bonus for later.
- Buurt is finest but descriptive-only: home microdata is PC4, so buurt-level
  access can't feed outcome modelling.
- Car uses free-flow OSM speeds (no congestion). No elevation (flat NL).
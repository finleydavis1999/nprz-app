# =============================================================================
# Travel-time matrix build pipeline  (REFERENCE -- not run by `npm run data`)
# =============================================================================
# How the travel-time matrices in static/data/parquet/traveltime-edges-*.parquet
# were produced. This is an OFFLINE build: it needs a large local OSM/GTFS
# network, hours of compute, and a local scratch drive (paths below point at
# D:/). It is kept here so the pipeline is legible and reproducible (e.g. to
# rebuild with a new GTFS year, a different cap, or on a bigger machine for a
# full single national network). traveltime.R does the final combine step and
# IS the part wired toward the app.
#
# Engine: r5r (R5/Conveyal). PT: OVapi national GTFS. Network: OpenStreetMap.
# Java 21 required; heap set BEFORE library(r5r). 16 GB RAM cannot build one
# national network (transit tables OOM), hence the two-cluster approach.
# ASCII only (webR parser safety).
#
# Sections:
#   0. Inputs / one-time downloads
#   1. Build a cluster network (run once per cluster, separate folders)
#   2. Run gemeente + PC4 matrices for a cluster
#   3. Run buurt matrix for a cluster (batched + resumable)
# Outputs land in D:/NPRZ_tt_out and are combined by R/edges/traveltime.R.
# =============================================================================


# --- 0. INPUTS ----------------------------------------------------------------
# GTFS (OVapi national feed), refreshed daily, ~6-month forward calendar window
# stored in calendar_dates.txt (no calendar.txt):
#   download.file("https://gtfs.ovapi.nl/nl/gtfs-nl.zip", "<dir>/gtfs-nl.zip", mode="wb")
# OSM province extracts (Geofabrik), e.g.:
#   https://download.geofabrik.de/europe/netherlands/<province>-latest.osm.pbf
#
# Clusters (overlapping; overlap >= 90-min car reach so cross-cluster commute
# pairs route correctly):
#   Cluster A (D:/NPRZ_tracer)   : zuid-holland, noord-brabant, utrecht,
#                                  noord-holland, zeeland, gelderland, limburg,
#                                  flevoland   (8 provinces; all major cities)
#   Cluster B (D:/NPRZ_clusterB) : groningen, friesland, drenthe, overijssel
#                                  + flevoland, gelderland (overlap buffer)


# --- 1. BUILD A CLUSTER NETWORK -----------------------------------------------
# Put the cluster's province .pbf files + gtfs-nl.zip in DATA_DIR, then:
build_cluster_network <- function(DATA_DIR, heap = "-Xmx12G") {
  options(java.parameters = heap)   # MUST precede library(r5r); restart R if it
  library(r5r)                      # was already loaded at a different heap
  # clear any stale network before a rebuild:
  f <- c(file.path(DATA_DIR, "network.dat"),
         file.path(DATA_DIR, "network_settings.json"),
         list.files(DATA_DIR, pattern = "\\.mapdb", full.names = TRUE))
  file.remove(f[file.exists(f)])
  r5r_core <- build_network(DATA_DIR)   # 20-40 min; transit tables are the cost
  stopifnot(file.exists(file.path(DATA_DIR, "network.dat")))
  r5r_core
}


# --- 2. GEMEENTE + PC4 MATRICES (per cluster) ---------------------------------
# Modes from one network (network is mode-agnostic; mode chosen per call).
# PT uses a 30-min departure window (median) for robustness; 90-min cap.
.load_pts <- function(path) {
  c0 <- jsonlite::fromJSON(path)
  data.frame(id = names(c0), lon = vapply(c0, `[`, numeric(1), 1),
             lat = vapply(c0, `[`, numeric(1), 2), row.names = NULL)
}
.norm_t <- function(df) { col <- grep("travel_time", names(df), value = TRUE)[1]
  names(df)[names(df) == col] <- "minutes"; df }

run_modes <- function(r5r_core, orig, dest,
                      depart = as.POSIXct("2026-06-26 09:00:00", tz = "Europe/Amsterdam"),
                      cap = 90) {
  library(r5r); library(dplyr)
  tr <- .norm_t(travel_time_matrix(r5r_core, origins = orig, destinations = dest,
        mode = c("WALK","TRANSIT"), departure_datetime = depart,
        time_window = 30, percentiles = 50, max_trip_duration = cap)); tr$mode <- "transit"
  one <- function(m) { d <- travel_time_matrix(r5r_core, origins = orig, destinations = dest,
        mode = m, departure_datetime = depart, max_trip_duration = cap)
        names(d)[names(d) == "travel_time_p50"] <- "minutes"; d }
  ca <- one("CAR");     ca$mode <- "car"
  bi <- one("BICYCLE"); bi$mode <- "bike"
  wa <- one("WALK");    wa$mode <- "walk"
  dplyr::bind_rows(tr, ca, bi, wa)
}

run_gem_pc4 <- function(r5r_core, cluster_tag, out_dir = "D:/NPRZ_tt_out") {
  library(arrow)
  dir.create(out_dir, showWarnings = FALSE)
  gem <- .load_pts("C:/NPRZ_project/static/data/geo/gem-centroids-weighted.json")
  pc4 <- .load_pts("C:/NPRZ_project/static/data/geo/pc4-centroids-weighted.json")
  write_parquet(run_modes(r5r_core, gem, gem),
                sprintf("%s/tt-gemeente-cluster%s.parquet", out_dir, cluster_tag))
  write_parquet(run_modes(r5r_core, pc4, pc4),
                sprintf("%s/tt-pc4-cluster%s.parquet", out_dir, cluster_tag))
}


# --- 3. BUURT MATRIX (per cluster; batched + resumable) -----------------------
# ~14k origins is too big to hold or to lose on a crash. Origins processed in
# batches of 500; each batch writes its own parquet and is skipped on re-run, so
# a crash resumes from the last completed batch. Combine batches later with
# arrow::open_dataset().
run_buurt <- function(r5r_core, cluster_tag, out_dir = "D:/NPRZ_tt_out", batch = 500,
                      depart = as.POSIXct("2026-06-26 09:00:00", tz = "Europe/Amsterdam"),
                      cap = 90) {
  library(arrow); library(dplyr)
  OUT <- sprintf("%s/buurt_%s", out_dir, cluster_tag)
  dir.create(OUT, recursive = TRUE, showWarnings = FALSE)
  buurt <- .load_pts("C:/NPRZ_project/static/data/geo/buurt-centroids-weighted.json")
  starts <- seq(1, nrow(buurt), by = batch)
  for (s in starts) {
    e <- min(s + batch - 1, nrow(buurt))
    f <- sprintf("%s/batch_%05d_%05d.parquet", OUT, s, e)
    if (file.exists(f)) next                       # resume: skip done batches
    write_parquet(run_modes(r5r_core, buurt[s:e, ], buurt, depart, cap), f)
    message("batch ", s, "-", e)
  }
}


# --- ORCHESTRATION (what was actually run) ------------------------------------
# Restart R between clusters (clean JVM). Roughly:
#   coreA <- build_cluster_network("D:/NPRZ_tracer")
#   run_gem_pc4(coreA, "A"); run_buurt(coreA, "A")
#   # restart R
#   coreB <- build_cluster_network("D:/NPRZ_clusterB")
#   run_gem_pc4(coreB, "B"); run_buurt(coreB, "B")
# Then: source("R/edges/traveltime.R"); build_traveltime()
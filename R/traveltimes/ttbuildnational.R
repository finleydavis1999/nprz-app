# =============================================================================
# ttbuildnational.R -- national travel-time matrix build + verification
# =============================================================================
# Builds one national r5r network and travel-time matrices at gemeente, PC4 and
# buurt scale, for car / public transport / bike / walk, then verifies the
# result. Single pass: no clustering, no combine step.
#
#   setwd("<project root>")
#   source("R/traveltimes/ttbuildnational.R")
#   build_all()                 # gem + pc4, then stops with a buurt projection
#   build_all("buurt")          # after deciding CAP["buurt"]
#
# Run in a FRESH R session: the Java heap is fixed when r5r first loads.
# Requires Java 21 and: r5r arrow dplyr jsonlite sf
# Centroids must exist first -- see R/traveltimes/build-centroids.R
#
# Everything downloads itself and the departure date is chosen automatically
# from the GTFS calendar. The only settings you may need to touch are in CONFIG.
# ASCII only.
# =============================================================================


# ============================ CONFIG =========================================
# Set these before sourcing to override, e.g.  NET_DIR <- "E:/tt"

if (!exists("NET_DIR")) NET_DIR <- "D:/NPRZ_net"      # OSM + GTFS + network.dat
if (!exists("OUT_DIR")) OUT_DIR <- "D:/NPRZ_tt_out"   # per-batch intermediates
if (!exists("PARQUET_DIR")) PARQUET_DIR <- "static/data/parquet"

# Heap is auto-sized to (RAM - 6 GB) and printed. Override if the guess is wrong;
# the national network build is the memory peak and OOMs below roughly 20 GB.
.detect_heap <- function(reserve_gb = 6, min_gb = 8, max_gb = 48) {
  ram <- tryCatch({
    if (.Platform$OS.type == "windows") {
      # wmic is deprecated on newer Windows; fall back to PowerShell
      x <- suppressWarnings(system("wmic ComputerSystem get TotalPhysicalMemory",
                                   intern = TRUE, ignore.stderr = TRUE))
      v <- suppressWarnings(as.numeric(gsub("\\D", "", x[grepl("^[0-9 ]+$", x)][1])) / 1e9)
      if (is.na(v) || !is.finite(v)) {
        y <- suppressWarnings(system(paste(
          "powershell -NoProfile -Command",
          "\"(Get-CimInstance Win32_ComputerSystem).TotalPhysicalMemory\""),
          intern = TRUE, ignore.stderr = TRUE))
        v <- suppressWarnings(as.numeric(gsub("\\D", "", y[grepl("[0-9]", y)][1])) / 1e9)
      }
      v
    } else if (file.exists("/proc/meminfo")) {
      as.numeric(sub("\\D+", "", grep("MemTotal", readLines("/proc/meminfo"),
                                      value = TRUE)[1])) / 1e6
    } else NA
  }, error = function(e) NA)

  if (is.na(ram) || !is.finite(ram)) {
    message("NOTE: could not detect RAM. Defaulting to ", min_gb,
            "G, which is NOT enough for a national build -- set HEAP manually ",
            "before sourcing, e.g. HEAP <- \"-Xmx24G\"")
    return(paste0("-Xmx", min_gb, "G"))
  }
  message("detected ", round(ram), " GB RAM")
  paste0("-Xmx", max(min_gb, min(max_gb, floor(ram - reserve_gb))), "G")
}
if (!exists("HEAP")) HEAP <- .detect_heap()

# Departure: NULL = pick a Wednesday 09:00 automatically from the GTFS calendar
# window (the OVapi feed only covers a rolling ~6 months, so a hardcoded date
# goes stale and silently returns no transit).
if (!exists("DEPART")) DEPART <- NULL
if (!exists("TZ"))     TZ     <- "Europe/Amsterdam"

# Travel-time cap in minutes, per scale. Generous caps keep the matrix reusable;
# trim per analysis later. Buurt at 150 is large -- build_all() prints a
# projection after PC4 so you can decide before committing to it.
if (!exists("CAP"))   CAP   <- c(gem = 150L, pc4 = 150L, buurt = 150L)
if (!exists("BATCH")) BATCH <- c(gem = 500L, pc4 = 500L, buurt = 250L)
if (!exists("SPLIT_BY_MODE"))
  SPLIT_BY_MODE <- c(gem = FALSE, pc4 = FALSE, buurt = TRUE)   # buurt -> 4 files

if (!exists("SCALES")) SCALES <- c("gem", "pc4", "buurt")
PT_WINDOW <- 30L; PT_PERCENTILE <- 50L
# =============================================================================


options(java.parameters = HEAP)   # MUST precede library(r5r)
suppressPackageStartupMessages({
  library(r5r); library(arrow); library(dplyr); library(jsonlite); library(sf)
})
message("Java heap: ", HEAP)

MODE_ID   <- c(car = 1L, transit = 2L, bike = 3L, walk = 4L)   # frozen ids
MODE_NAME <- setNames(names(MODE_ID), MODE_ID)
CENTROIDS <- c(gem   = "static/data/geo/gem-centroids-weighted.json",
               pc4   = "static/data/geo/pc4-centroids-weighted.json",
               buurt = "static/data/geo/buurt-centroids-weighted.json")


# --------------------------- requirements ------------------------------------
check_requirements <- function(scales = SCALES) {
  if (!dir.exists("static/data/geo"))
    stop("run from the project root -- 'static/data/geo' not found in ", getwd())
  for (sc in scales) if (!file.exists(CENTROIDS[[sc]]))
    stop("missing centroids: ", CENTROIDS[[sc]],
         "\n  Run: source('R/traveltimes/build-centroids.R'); build_all_centroids()")
  if (Sys.getenv("JAVA_HOME") == "")
    message("note: JAVA_HOME is unset -- r5r needs Java 21")
  dir.create(NET_DIR, recursive = TRUE, showWarnings = FALSE)
  dir.create(OUT_DIR, recursive = TRUE, showWarnings = FALSE)
  dir.create(PARQUET_DIR, recursive = TRUE, showWarnings = FALSE)
  invisible(TRUE)
}


# --------------------------- inputs ------------------------------------------
.get_file <- function(url, dest, min_bytes, tries = 4) {
  if (file.exists(dest) && file.info(dest)$size > min_bytes) {
    message("have ", basename(dest)); return(invisible(TRUE))
  }
  for (a in seq_len(tries)) {
    ok <- tryCatch({ download.file(url, dest, mode = "wb"); TRUE },
                   error = function(e) { message("  retry ", a, ": ",
                                                 conditionMessage(e)); FALSE })
    if (ok && file.exists(dest) && file.info(dest)$size > min_bytes)
      return(invisible(TRUE))
    Sys.sleep(15)
  }
  stop("failed to download ", url)
}

fetch_inputs <- function(dir = NET_DIR) {
  options(timeout = 7200)
  osm <- file.path(dir, "netherlands.osm.pbf")
  .get_file("https://download.geofabrik.de/europe/netherlands-latest.osm.pbf",
            osm, 1e9)
  .get_file("https://gtfs.ovapi.nl/nl/gtfs-nl.zip",
            file.path(dir, "gtfs-nl.zip"), 5e7)
  # r5r merges EVERY .pbf in the folder; a leftover province extract would
  # double-count geometry alongside the national file.
  others <- setdiff(list.files(dir, pattern = "\\.osm\\.pbf$", full.names = TRUE), osm)
  if (length(others)) {
    message("removing overlapping extracts: ", paste(basename(others), collapse = ", "))
    file.remove(others)
  }
  invisible(TRUE)
}

# The OVapi feed has no calendar.txt -- all service dates live in
# calendar_dates.txt, covering a rolling window from the download date. Pick a
# Wednesday comfortably inside it rather than hardcoding a date that goes stale.
resolve_depart <- function(dir = NET_DIR, depart = DEPART) {
  cd <- utils::read.csv(unz(file.path(dir, "gtfs-nl.zip"), "calendar_dates.txt"))
  dates <- sort(unique(as.Date(as.character(cd$date), format = "%Y%m%d")))
  lo <- min(dates); hi <- max(dates)

  if (is.null(depart)) {
    wed <- dates[format(dates, "%u") == "3" & dates >= lo + 14]
    if (!length(wed)) wed <- dates[dates >= lo + 14]
    if (!length(wed)) wed <- dates
    depart <- paste(format(wed[1], "%Y-%m-%d"), "09:00:00")
    auto <- TRUE
  } else auto <- FALSE

  d <- as.Date(substr(depart, 1, 10))
  cat("\n=====================================================\n",
      " GTFS service window : ", format(lo), " -> ", format(hi), "\n",
      " departure           : ", depart, if (auto) "  (auto)" else "", "\n",
      " status              : ", if (d >= lo && d <= hi) "OK" else "OUTSIDE WINDOW", "\n",
      "=====================================================\n\n", sep = "")
  if (d < lo || d > hi)
    stop("DEPART is outside the GTFS window. Set DEPART <- NULL to auto-pick.")
  as.POSIXct(depart, tz = TZ)
}


# --------------------------- network -----------------------------------------
build_national_network <- function(dir = NET_DIR, rebuild = FALSE) {
  if (rebuild) {
    f <- c(file.path(dir, c("network.dat", "network_settings.json")),
           list.files(dir, pattern = "\\.mapdb", full.names = TRUE))
    file.remove(f[file.exists(f)])
  }
  message("building network (long step: 30-90 min nationally)...")
  core <- r5r::build_network(dir)
  stopifnot(file.exists(file.path(dir, "network.dat")))
  message("network.dat: ", round(file.info(file.path(dir, "network.dat"))$size/1e6), " MB")
  core
}


# --------------------------- matrix runs -------------------------------------
load_points <- function(path) {
  c0 <- fromJSON(path)
  data.frame(id = names(c0), lon = vapply(c0, `[`, numeric(1), 1),
             lat = vapply(c0, `[`, numeric(1), 2), row.names = NULL)
}
.norm_time <- function(df) {
  col <- grep("^travel_time", names(df), value = TRUE)[1]
  names(df)[names(df) == col] <- "minutes"; df
}

run_modes <- function(core, orig, dest, depart, cap) {
  tr <- .norm_time(travel_time_matrix(core, origins = orig, destinations = dest,
        mode = c("WALK","TRANSIT"), departure_datetime = depart,
        time_window = PT_WINDOW, percentiles = PT_PERCENTILE,
        max_trip_duration = cap)); tr$mode <- "transit"
  one <- function(m, tag) {
    d <- .norm_time(travel_time_matrix(core, origins = orig, destinations = dest,
         mode = m, departure_datetime = depart, max_trip_duration = cap))
    d$mode <- tag; d
  }
  bind_rows(tr, one("CAR","car"), one("BICYCLE","bike"), one("WALK","walk"))
}

# Batched and resumable: each batch writes its own parquet and is skipped on
# re-run, so an interruption costs one batch rather than the whole scale.
run_scale <- function(core, scale, depart, points = NULL) {
  pts  <- if (is.null(points)) load_points(CENTROIDS[[scale]]) else points
  cap  <- CAP[[scale]]; bsz <- BATCH[[scale]]
  odir <- file.path(OUT_DIR, scale)
  dir.create(odir, recursive = TRUE, showWarnings = FALSE)
  message(sprintf("[%s] %d origins, cap %d min, batch %d", scale, nrow(pts), cap, bsz))
  for (s in seq(1, nrow(pts), by = bsz)) {
    e <- min(s + bsz - 1, nrow(pts))
    f <- file.path(odir, sprintf("batch_%05d_%05d.parquet", s, e))
    if (file.exists(f)) { message("  skip ", s, "-", e); next }
    t0 <- Sys.time()
    write_parquet(run_modes(core, pts[s:e, ], pts, depart, cap), f)
    message(sprintf("  batch %d-%d: %.1f min", s, e,
            as.numeric(difftime(Sys.time(), t0, units = "mins"))))
  }
  invisible(pts$id)
}


# --------------------------- assemble ----------------------------------------
# distance_m is STRAIGHT-LINE between centroids. r5r's travel_time_matrix does
# not return network distance (that needs detailed_itineraries, far too slow at
# national scale), so it is mode-independent: one column, not one per mode.
assemble_scale <- function(scale, points = NULL) {
  pts <- if (is.null(points)) load_points(CENTROIDS[[scale]]) else points
  rd  <- st_transform(st_as_sf(pts, coords = c("lon","lat"), crs = 4326), 28992)
  co  <- st_coordinates(rd); ix <- setNames(seq_len(nrow(pts)), pts$id)

  df <- open_dataset(file.path(OUT_DIR, scale)) |>
    filter(!is.na(minutes), minutes >= 0) |>
    select(from_id, to_id, minutes, mode) |> collect() |>
    transmute(o_code = as.character(from_id), d_code = as.character(to_id),
              mode = unname(MODE_ID[mode]), minutes = as.double(minutes))

  df <- df |> filter(o_code %in% pts$id, d_code %in% pts$id)
  
  oi <- ix[df$o_code]; di <- ix[df$d_code]
  df$distance_m <- round(sqrt((co[oi,1]-co[di,1])^2 + (co[oi,2]-co[di,2])^2))
  df <- set_intrazonal(df, scale)

  files <- character(0)
  if (isTRUE(SPLIT_BY_MODE[[scale]])) {
    for (m in names(MODE_ID)) {
      sub <- df |> filter(mode == MODE_ID[[m]]) |>
        select(o_code, d_code, minutes, distance_m) |> arrange(o_code, d_code)
      f <- sprintf("%s/traveltime-%s-%s.parquet", PARQUET_DIR, scale, m)
      write_parquet(sub, f); files <- c(files, f)
      message(sprintf("  wrote %s : %s rows, %.1f MB", basename(f),
              format(nrow(sub), big.mark=","), file.info(f)$size/1e6))
    }
  } else {
    f <- sprintf("%s/traveltime-edges-%s.parquet", PARQUET_DIR, scale)
    write_parquet(arrange(df, mode, o_code, d_code), f); files <- f
    message(sprintf("  wrote %s : %s rows, %.1f MB", basename(f),
            format(nrow(df), big.mark=","), file.info(f)$size/1e6))
  }
  attr(df, "files") <- files
  df
}


# --------------------------- verification ------------------------------------
# Each check corresponds to a failure this pipeline has actually hit.
verify_scale <- function(df, scale, expected_ids = NULL) {
  cat("\n############ VERIFY", scale, "############\n")
  problems <- character(0); P <- function(m) problems <<- c(problems, m)

  want <- c("o_code","d_code","mode","minutes","distance_m")
  cat("\n[1] schema: ", paste(names(df), collapse=", "),
      " | rows: ", format(nrow(df), big.mark=","), "\n", sep="")
  if (!all(want %in% names(df))) P("wrong schema")

  ndup <- nrow(df) - nrow(distinct(df, o_code, d_code, mode))
  cat("[2] duplicate (o,d,mode): ", ndup, "\n", sep="")
  if (ndup > 0) P(paste(ndup, "duplicate rows"))

  exp_ids <- expected_ids %||% names(fromJSON(CENTROIDS[[scale]]))
  got <- unique(df$o_code); miss <- setdiff(exp_ids, got)
  cat("[3] origins: ", length(got), " of ", length(exp_ids), " expected\n", sep="")
  if (length(miss)) {
    cat("    missing ", length(miss), " -- first 15: ",
        paste(utils::head(miss,15), collapse=", "), "\n", sep="")
    if (length(miss) > length(exp_ids)*0.02) P(paste(length(miss), "origins missing"))
  }

  gd <- unique(df$d_code); md <- setdiff(exp_ids, gd)
  cat("[3b] destinations: ", length(gd), " of ", length(exp_ids),
      if (length(md)) paste0(" | ", length(md), " never reachable") else "", "\n", sep="")

  cat("[4] origins by leading character (a missing region shows here):\n")
  print(as.data.frame(df |> distinct(o_code) |>
        mutate(pre = substr(o_code,1,1)) |> count(pre, name="origins")))

  nna <- sum(is.na(df$minutes))
  mx  <- max(df$minutes, na.rm = TRUE)
  cat("[5] max minutes: ", mx, " | cap ", CAP[[scale]],
      " | NA minutes: ", nna, "\n", sep = "")
  if (nna > 0) P(paste(nna, "rows with NA minutes"))
  if (is.finite(mx) && mx > CAP[[scale]]) P("cap exceeded")
  if (length(unique(df$mode)) != 4) P("not all four modes present")

  bm <- df |> group_by(mode) |>
    summarise(n=n(), med=median(minutes, na.rm=TRUE),
              max=max(minutes, na.rm=TRUE), .groups="drop") |>
    mutate(mode_name = MODE_NAME[as.character(mode)])
  cat("[6] by mode:\n"); print(as.data.frame(bm[,c("mode_name","n","med","max")]))
  if (bm$med[bm$mode==1] > bm$med[bm$mode==2]) P("car slower than transit")

  car <- df |> filter(mode==1L, minutes>0, distance_m>500) |>
    mutate(kmh = (distance_m/1000)/(minutes/60))
  cat("[7] implied car speed km/h (straight-line, understates):\n    ")
  print(round(quantile(car$kmh, c(.01,.5,.99), na.rm=TRUE), 1))

  # Self-pair time is roughly twice the walk from the centroid to the road
  # Self-pairs carry the intrazonal value set from geometry, so this reports
  # rather than diagnoses -- centroid-to-road distance is the [snap] report.
  self <- df |> filter(o_code == d_code, mode == 1L)
  cat("[8] self-pairs: ", nrow(self), " | intrazonal car time ",
      unique(self$minutes)[1], " min (set from geometry; see [snap] for ",
      "centroid-to-road distances)\n", sep = "")
  if (length(unique(self$minutes)) > 1) P("intrazonal times not constant")

  a <- car |> select(o_code,d_code,minutes)
  sym <- a |> inner_join(rename(a, o2=d_code, d2=o_code, rev=minutes),
                         by=c("o_code"="o2","d_code"="d2")) |>
    mutate(diff = abs(minutes-rev))
  if (nrow(sym)) {
    cat("[9] car |A->B - B->A|: median ", median(sym$diff),
        " p99 ", quantile(sym$diff,.99), "\n", sep="")
    if (median(sym$diff) > 5) P("car matrix asymmetric")
  }

  reach <- df |> filter(mode==1L) |> count(o_code, name="n_dest") |> arrange(n_dest)
  cat("[10] origins reaching fewest destinations by car:\n")
  print(as.data.frame(utils::head(reach, 5)))

  cat("[11] random journeys (different every run -- check a few externally):\n")
  print(as.data.frame(df |> filter(mode %in% c(1L,2L), minutes>5) |>
        slice_sample(n=10) |>
        mutate(mode_name = MODE_NAME[as.character(mode)],
               km = round(distance_m/1000,1),
               kmh = round((distance_m/1000)/(minutes/60),1)) |>
        select(o_code,d_code,mode_name,minutes,km,kmh)), row.names = FALSE)

  cat("\n", if (!length(problems)) "  ALL CHECKS PASSED\n" else
      paste0("  PROBLEMS:\n", paste0("   - ", problems, collapse="\n"), "\n"), sep="")
  invisible(problems)
}
`%||%` <- function(a,b) if (is.null(a)) b else a
# --------------------------- centroid snap check ------------------------------
# A centroid far from the road network inflates every journey from that area by
# a walking access leg (and shows up as a large self-pair time). Nobody in NL
# lives 500 m from a road, so a large snap distance means the centroid is in the
# wrong place -- typically a weighted mean landing in water or an open field.
.geo_for <- function(scale)
  sprintf("static/data/geo/%s.geojson", c(gem="gemeenten", pc4="pc4", buurt="buurt")[[scale]])

snap_report <- function(core, scale, points = NULL, threshold = 300) {
  pts <- if (is.null(points)) load_points(CENTROIDS[[scale]]) else points
  sn <- tryCatch(r5r::find_snap(r5r_network = core, points = pts, mode = "CAR"),
    error = function(e)
      tryCatch(r5r::find_snap(r5r_core = core, points = pts, mode = "CAR"),
        error = function(e2) { message("  snap check unavailable: ",
                                       conditionMessage(e2)); NULL }))
  if (is.null(sn)) return(invisible(NULL))
  sn <- as.data.frame(sn)
  cat("\n[snap] distance from centroid to road network (m), ", scale, ":\n    ", sep="")
  print(round(quantile(sn$distance, c(.5,.9,.99,1), na.rm = TRUE)))
  bad <- sn[!is.na(sn$distance) & sn$distance > threshold, ]
  bad <- bad[order(-bad$distance), ]
  cat("    over ", threshold, " m: ", nrow(bad), "\n", sep="")
  if (nrow(bad)) print(utils::head(bad[, c("point_id","distance")], 15), row.names = FALSE)
  invisible(sn)
}

# Relocate flagged centroids onto the road network, keeping them inside their own
# polygon. Rewrites the centroid JSON. Run once, then rebuild the matrices.
# Relocate centroids that sit far from the road network onto it, keeping them
# inside their own polygon. Rewrites the centroid JSON; rebuild the affected
# scale afterwards. Needs a built network, so this is a second pass after
# build_all_centroids().
fix_centroids_by_snap <- function(core, scale, threshold = 300) {
  pts <- load_points(CENTROIDS[[scale]])

  # r5r renamed the network argument in 2.3; try both.
  sn <- tryCatch(r5r::find_snap(r5r_network = core, points = pts, mode = "CAR"),
    error = function(e)
      tryCatch(r5r::find_snap(r5r_core = core, points = pts, mode = "CAR"),
        error = function(e2) { message("  snap check unavailable: ",
                                       conditionMessage(e2)); NULL }))
  if (is.null(sn)) return(invisible(NULL))
  sn <- as.data.frame(sn)

  bad <- sn[!is.na(sn$distance) & sn$distance > threshold, ]
  if (!nrow(bad)) {
    message("[", scale, "] nothing over ", threshold, " m"); return(invisible(0))
  }

  polys <- sf::st_read(.geo_for(scale), quiet = TRUE) |>
    dplyr::transmute(area_code = as.character(area_code)) |> sf::st_make_valid()

  moved <- 0
  for (i in seq_len(nrow(bad))) {
    id <- as.character(bad$point_id[i])
    poly <- polys[polys$area_code == id, ]
    if (!nrow(poly)) next
    p <- sf::st_sfc(sf::st_point(c(bad$snap_lon[i], bad$snap_lat[i])), crs = 4326)
    # only move it if the snapped point is still inside its own polygon
    if (lengths(sf::st_within(p, sf::st_transform(poly, 4326))) > 0) {
      pts$lon[pts$id == id] <- bad$snap_lon[i]
      pts$lat[pts$id == id] <- bad$snap_lat[i]
      moved <- moved + 1
    }
  }

  res <- setNames(lapply(seq_len(nrow(pts)),
                         function(i) c(pts$lon[i], pts$lat[i])), pts$id)
  jsonlite::write_json(res, CENTROIDS[[scale]], auto_unbox = TRUE, digits = 6)
  message("[", scale, "] moved ", moved, " of ", nrow(bad),
          " onto the road network (rest left: snap point outside the polygon)")
  invisible(moved)
}

# --------------------------- intrazonal times ---------------------------------
# Travel time from an area to itself. Left raw it is whatever r5r's access legs
# happen to produce (often 0, sometimes tens of minutes). Instead set it from
# geometry: the mean distance from the centre of a disc of equal area to a random
# point in it is (2/3)*sqrt(A/pi), divided by that mode's observed median speed.
set_intrazonal <- function(df, scale) {
  r_eff <- tryCatch({
    polys <- sf::st_read(.geo_for(scale), quiet = TRUE) |> sf::st_transform(28992)
    v <- (2/3) * sqrt(mean(as.numeric(sf::st_area(polys)), na.rm = TRUE) / pi)
    if (!is.finite(v) || v <= 0) stop("non-finite area") else v
  }, error = function(e) unname(c(gem = 5000, pc4 = 900, buurt = 400)[scale]))

  kmh <- c("1" = 40, "2" = 18, "3" = 14, "4" = 4.5)      # km/h defaults
  spd <- df |> dplyr::filter(minutes > 0, distance_m > 1000, distance_m < 5000) |>
    dplyr::group_by(mode) |>
    dplyr::summarise(k = median((distance_m/1000)/(minutes/60), na.rm = TRUE),
                     .groups = "drop")
  ok <- is.finite(spd$k) & spd$k > 1
  if (any(ok)) kmh[as.character(spd$mode[ok])] <- spd$k[ok]

  tt <- round((r_eff/1000) / kmh * 60)
  tt[!is.finite(tt) | tt < 1] <- 1
  names(tt) <- names(kmh)                    # pmax drops names; be explicit

  cat("\n[intrazonal] internal distance ", round(r_eff), " m\n", sep = "")
  print(data.frame(mode = MODE_NAME[names(kmh)], kmh = round(kmh, 1),
                   minutes = unname(tt)), row.names = FALSE)

  idx <- df$o_code == df$d_code
  v <- unname(tt[as.character(df$mode[idx])]); v[is.na(v)] <- 2
  df$minutes[idx] <- v
  df
}

# --------------------------- orchestration -----------------------------------
estimate_buurt <- function(pc4_df, pc4_minutes) {
  n4 <- length(names(fromJSON(CENTROIDS[["pc4"]])))
  nb <- length(names(fromJSON(CENTROIDS[["buurt"]])))
  f  <- (nb/n4)^2
  sz <- sum(file.info(attr(pc4_df,"files"))$size, na.rm=TRUE)/1e6
  cat(sprintf(paste0("\n--- BUURT PROJECTION (from PC4 actuals) ---\n",
    "  %d buurt vs %d pc4 -> %.1fx pairs\n  rows  ~%s\n  size  ~%.0f MB",
    " (split across 4 mode files)\n  time  ~%.1f hours\n",
    "  Set CAP['buurt'] if that is too large, then: build_all('buurt')\n",
    "-------------------------------------------\n"),
    nb, n4, f, format(round(nrow(pc4_df)*f), big.mark=","), sz*f, (pc4_minutes*f)/60))
}

build_all <- function(scales = SCALES, rebuild_network = FALSE) {
  check_requirements(scales)
  fetch_inputs()
  depart <- resolve_depart()
  core <- build_national_network(rebuild = rebuild_network)

  for (sc in scales) {
    t0 <- Sys.time()
    snap_report(core, sc)
    run_scale(core, sc, depart)
    df <- assemble_scale(sc)
    verify_scale(df, sc)
    mins <- as.numeric(difftime(Sys.time(), t0, units = "mins"))
    if (sc == "pc4" && "buurt" %in% scales) {
      estimate_buurt(df, mins)
      message("stopping before buurt -- review the projection, then build_all('buurt')")
      return(invisible(NULL))
    }
    rm(df); gc()
  }
  message("\nDONE. Matrices in ", PARQUET_DIR)
}

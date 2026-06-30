# Travel times: r5r multimodal travel-time matrix (car / PT / bike / walk),
# 9:00 weekday departure, 90-minute cap. Built outside the app (see the
# travel-times status note / R scripts in R/edges/tt-build/) and combined here
# from two overlapping province clusters into one app-shaped edge dataset per
# scale.
#
# Source: D:/NPRZ_tt_out/  (per-cluster, per-scale outputs)
#   tt-gemeente-cluster{A,B}.parquet         (single-file)
#   pc4_B/ + tt-pc4-clusterA.parquet         (A single-file, B batched)
#   buurt_{A,B}/                             (both batched)
# Output: static/data/parquet/traveltime-edges-{gem,pc4,buurt}.parquet
#
# Schema out (matches the edge pattern: o_code, d_code, year, <filters>, weight):
#   o_code, d_code, year(=1 snapshot), mode(int 1-4), minutes, count, weight
#   weight = minutes (the value the flow query aggregates); for a single
#   snapshot year SUM over one year == the value itself, so no false summing.
#
# NOTE: this dataset is registered so it lives in the repo and is queryable.
# How the app *consumes* travel time (model inputs, cross-refs, derived nodal
# "avg time to work") is an open integration question -- see the email/qs list.
# ASCII only (webR parser).

source("R/lib/parquet.R")
library(arrow)
library(dplyr)

# mode string -> frozen integer id (do not renumber; mirrors node-spec rule)
.MODE_ID <- c(car = 1L, transit = 2L, bike = 3L, walk = 4L)
.TT_SRC  <- "D:/NPRZ_tt_out"   # local build output; not in repo

# Read one scale's two clusters (single-file or batched folder), combine, dedup.
# A pair can appear in both clusters' buffer overlap -- an identical (o,d,mode)
# routed in either cluster should give the same minutes, so we keep the minimum
# (defensive: identical in practice, min avoids any buffer-edge discrepancy).
.read_cluster <- function(path) {
  if (dir.exists(path)) {
    arrow::open_dataset(path) |> dplyr::collect()
  } else {
    arrow::read_parquet(path)
  }
}

.combine_scale <- function(a_path, b_path) {
  a <- .read_cluster(a_path)
  b <- .read_cluster(b_path)
  dplyr::bind_rows(a, b) |>
    dplyr::filter(!is.na(minutes), minutes >= 0) |>
    dplyr::mutate(mode_id = .MODE_ID[mode]) |>
    dplyr::group_by(from_id, to_id, mode_id) |>
    dplyr::summarise(minutes = min(minutes), .groups = "drop")
}

# Write one scale to app-shaped parquet via the house helper (DuckDB COPY).
.write_scale <- function(df, scale) {
  con <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  out <- dplyr::transmute(
    df,
    o_code = as.character(from_id),
    d_code = as.character(to_id),
    year   = 1L,                       # single 2026 snapshot (see manifest)
    mode   = as.integer(mode_id),
    minutes = as.double(minutes),
    count   = as.double(minutes),
    weight  = as.double(minutes)
  )
  duckdb::duckdb_register(con, "tt_tmp", out)
  on.exit(duckdb::duckdb_unregister(con, "tt_tmp"), add = TRUE)
  write_parquet_from_query(
    con,
    "SELECT * FROM tt_tmp ORDER BY mode, o_code, d_code",
    sprintf("static/data/parquet/traveltime-edges-%s.parquet", scale)
  )
  nrow(out)
}

build_traveltime <- function() {
  scales <- list(
    gem = list(a = file.path(.TT_SRC, "tt-gemeente-clusterA.parquet"),
               b = file.path(.TT_SRC, "tt-gemeente-clusterB.parquet")),
    pc4 = list(a = file.path(.TT_SRC, "tt-pc4-clusterA.parquet"),
               b = file.path(.TT_SRC, "pc4_B")),
    buurt = list(a = file.path(.TT_SRC, "buurt_A"),
                 b = file.path(.TT_SRC, "buurt_B"))
  )

  combined <- list()
  for (s in names(scales)) {
    cat("combining", s, "...\n")
    df <- .combine_scale(scales[[s]]$a, scales[[s]]$b)
    n  <- .write_scale(df, s)
    combined[[s]] <- df
    cat("  wrote", n, "rows\n")
  }

  .validate(combined)

  list(
    name = "Travel times 2026 (r5r)",
    description = paste(
      "Multimodal travel times (car, public transport, bike, walk),",
      "weekday 09:00 departure, 90-minute cap. Built with r5r over OVapi GTFS",
      "+ OpenStreetMap; population-weighted centroids. Pairs over 90 min are",
      "absent (beyond commuting horizon), not zero. Scheduled times, no delays."
    ),
    weighted = FALSE,
    weightCol = "weight",
    yearAggregation = "mean",   # single snapshot: mean over 1 year = the value
    scales = list(
      gem   = "parquet/traveltime-edges-gem.parquet",
      pc4   = "parquet/traveltime-edges-pc4.parquet",
      buurt = "parquet/traveltime-edges-buurt.parquet"
    ),
    fields = list(
      year = list(
        type = "multi", label = "Jaar",
        values = list(list(id = 1L, label = "2026"))
      ),
      mode = list(
        type = "multi", label = "Vervoerwijze",
        values = list(
          list(id = 1L, label = "Auto"),
          list(id = 2L, label = "OV"),
          list(id = 3L, label = "Fiets"),
          list(id = 4L, label = "Lopen")
        )
      )
    )
  )
}

# ---- validation: completeness + sanity (no assumed time bands) ---------------
.validate <- function(combined) {
  cat("\n=== travel-time validation ===\n")
  inv_mode <- setNames(names(.MODE_ID), .MODE_ID)
 
  for (s in names(combined)) {
    df <- combined[[s]]
    cat("\n[", s, "] rows:", nrow(df),
        "| minutes range:", min(df$minutes), "-", max(df$minutes), "\n")
    by_mode <- df |> dplyr::group_by(mode_id) |>
      dplyr::summarise(n = dplyr::n(), med = median(minutes),
                       max = max(minutes), .groups = "drop")
    by_mode$mode <- inv_mode[as.character(by_mode$mode_id)]
    print(by_mode[, c("mode", "n", "med", "max")])
    # hard structural checks (these are facts, not assumptions)
    stopifnot(max(df$minutes) <= 90)            # cap held
    stopifnot(all(df$minutes >= 0))             # no negatives / NAs
    stopifnot(length(unique(df$mode_id)) == 4)  # all four modes present
    # ordering sanity: across the scale, car should be faster than transit
    meds <- tapply(df$minutes, df$mode_id, median)
    stopifnot(meds["1"] <= meds["2"])           # car <= transit (median)
  }
 
  # Print travel times for a set of named pairs across all modes, for manual
  # eyeballing against Google Maps. No pass/fail band -- you judge. Edit this
  # list with pairs you know.
  pc4 <- combined$pc4
  cat("\n-- named PC4 pairs (all modes, minutes) -- eyeball vs Google Maps --\n")
  pairs <- list(
    c("3081","3011"),   # Zuid -> centre
    c("3078","3013"),   # outer Zuid -> centre
    c("3089","3014"),   # outer Zuid -> centre
    c("3071","3012")    # Zuid -> near-centre
  )
  for (p in pairs) {
    r <- pc4[pc4$from_id==p[1] & pc4$to_id==p[2], ]
    r$mode <- inv_mode[as.character(r$mode_id)]
    cat(sprintf("  %s -> %s : ", p[1], p[2]))
    if (nrow(r)) cat(paste(sprintf("%s=%g", r$mode, r$minutes), collapse="  "), "\n")
    else cat("(no pairs within cap)\n")
  }
 
  # random sample for a broader eyeball
  cat("\n-- random sample (pc4) --\n")
  set.seed(1)
  samp <- pc4[sample(nrow(pc4), 8), ]
  samp$mode <- inv_mode[as.character(samp$mode_id)]
  print(samp[, c("from_id", "to_id", "mode", "minutes")])
  cat("=== end validation ===\n")
}
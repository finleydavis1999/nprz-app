# accessibility-decay.R  (Stage 1 of the accessibility pipeline)
# -----------------------------------------------------------------------------
# Estimate a distance-decay curve for a vulnerable group: how sharply that
# group's observed commuting falls off with travel time. This is the
# methodological core -- everything downstream applies these curves.
#
# TWO DECAY SOURCES (a  group is estimated from whichever can isolate it):
#   - woonwerk : native dims  opl (education), inks (income), age. Thick.
#   - ODiN     : hhtype (single-parent), mode/car-ownership. Thin -> pool years.
# This file starts with the woonwerk path (low-educated first, the cleanest and
# aligned with Rotterdam publications). The ODiN path + non-car mode restriction
# come next, behind the same `estimate_decay()` interface.
#
# Travel time is attached from the r5r matrix (traveltime-edges-pc4.parquet).
# Output: a decay curve per group (binned mean trip share vs minutes) plus the
# raw (minutes, weight) pairs -- both inspectable.
# ASCII only.

library(arrow)
library(dplyr)

TT_PATH <- "static/data/parquet/traveltime-edges-pc4.parquet"
WW_PATH <- "static/data/parquet/woonwerk-edges-pc4.parquet"

# mode ids in the travel-time matrix (from traveltime.R): 1=car 2=OV 3=bike 4=walk
.MODE <- c(car = 1L, transit = 2L, bike = 3L, walk = 4L)

# ---- load + join travel time onto commute flows ------------------------------
# Returns commute rows with `minutes` attached for the chosen mode. woonwerk is
# already aggregated; `count` is the (weighted) number of commuters on that
# (o,d, group-cell). We keep only rows matching the group filter.
#
# group_filter: named list of woonwerk field -> allowed integer id(s), e.g.
#   list(opl = 1)                 low-educated
#   list(inks = c(1,2))           low-income (bottom 40%)
# mode: which travel-time mode defines "distance" for the decay (default car;
#   for non-car groups we restrict to c("transit","bike","walk") later).
.load_commute_with_tt <- function(group_filter, mode = "car", area = ZUID_PC4,
                                  period = 2L, tt_path = TT_PATH, ww_path = WW_PATH) {

  mode_id <- .MODE[[mode]]                       # resolve BEFORE filter (avoid
  tt <- read_parquet(tt_path) |>                 # collision with the `mode` col)
    filter(mode == mode_id) |>
    select(o_code, d_code, minutes)

ww <- read_parquet(ww_path)

  ww <- ww |> filter(o_code %in% area)
  if (!is.null(period)) ww <- ww |> filter(year == period)   # ADD THIS LINE
  # apply the group filter (only on fields present + non-NA)
  for (f in names(group_filter)) {
    vals <- group_filter[[f]]
    ww <- ww |> filter(.data[[f]] %in% vals)
  }
  # collapse to (o,d) commuter weight for this group (sum over any remaining dims)
  ww <- ww |>
    filter(!is.na(o_code), !is.na(d_code)) |>
    group_by(o_code, d_code) |>
    summarise(commuters = sum(count, na.rm = TRUE), .groups = "drop")

  # join travel time; drop pairs with no routed time (beyond 90-min cap = not a
  # commuting-horizon pair, correctly excluded)
  inner_join(ww, tt, by = c("o_code", "d_code")) |>
    filter(is.finite(minutes), commuters > 0)
}

# ---- estimate the decay curve ------------------------------------------------
# Binned observed decay: share of the group's commuters at each travel-time bin.
# This is the empirical deterrence function f(t). We report it binned (for the
# inspectable histogram) and also return the raw pairs for curve-fitting later.
estimate_decay <- function(group_filter, mode = "car", area = ZUID_PC4,
                           period = 2L, bin_width = 5, label = "group") {
    d <- .load_commute_with_tt(group_filter, mode = mode, area = area, period = period)
  if (nrow(d) == 0)
    stop("no commute rows for this group/mode -- check the filter and join keys")

  total <- sum(d$commuters)
  # bin by travel time; share of all group commuters whose commute falls in bin
  curve <- d |>
    mutate(bin = floor(minutes / bin_width) * bin_width) |>
    group_by(bin) |>
    summarise(commuters = sum(commuters), .groups = "drop") |>
    mutate(share = commuters / total,
           cum_share = cumsum(share)) |>
    arrange(bin)

  # a couple of headline summaries for sanity + inspect
  med <- d |> arrange(minutes) |>
    mutate(cw = cumsum(commuters) / total) |>
    filter(cw >= 0.5) |> slice(1) |> pull(minutes)

  structure(list(
    label     = label,
    mode      = mode,
    n_pairs   = nrow(d),
    n_commuters = total,
    median_minutes = med,
    curve     = curve,           # binned: bin, commuters, share, cum_share
    raw       = d                # o_code,d_code,commuters,minutes (for fitting)
  ), class = "decay_curve")
}

print.decay_curve <- function(x, ...) {
  cat(sprintf("decay[%s] mode=%s  pairs=%d  commuters=%.0f  median=%d min\n",
              x$label, x$mode, x$n_pairs, x$n_commuters, x$median_minutes))
  cat("  bin(min)  share  cum\n")
  apply(head(x$curve, 20), 1, function(r)
    cat(sprintf("  %6s   %.3f  %.3f\n", r["bin"], as.numeric(r["share"]),
                as.numeric(r["cum_share"]))))
  invisible(x)
}

# ---- run: low-educated first (opl = 1), by car -------------------------------
# (Test the cleanest path before wiring anything downstream.)
if (sys.nframe() == 0) {
  setwd("C:/NPRZ_project")
  low_edu <- estimate_decay(list(opl = 1), mode = "car", label = "low-educated")
  print(low_edu)
}
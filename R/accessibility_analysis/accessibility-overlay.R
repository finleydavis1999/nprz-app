# accessibility-overlay.R  (Stage 3: overlay + policy-oriented result set)
# -----------------------------------------------------------------------------
# Turns accessibility scores into a diagnosis. For education/income groups the
# vulnerable-population DENSITY comes from the SAME woonwerk set used for the
# decay (opl/inks native), not census -- census (vk100 var 18 etc.) is only for
# groups woonwerk can't isolate (single-parent, migrant).
#
# Emits BOTH:
#   - total count of the group in each PC4  -> structural-policy signal
#       (large populations with poor access -> structural intervention)
#   - rate/share of the group               -> concentration signal
#       (few vulnerable people with poor access -> personal-policy signal)
# rather than one blended "need" number, so the two policy modes stay separable.
#
# Also pre-bakes (functions ready, invoke on Friday -- no rebuild needed):
#   - PT-vs-car accessibility gap  -> where PT connectivity is the bottleneck
#   - plain reachable-jobs count within an isochrone -> Gemeente-legible measure
#   - reference-group hook (NOT wired: needs a 2nd decay curve; left for later)
#
# Depends on accessibility-score.R sourced. ASCII only.

library(arrow); library(dplyr)

VK100_PATH <- "static/data/parquet/cbs-vk100-pc4.parquet"
WW_PATH    <- "static/data/parquet/woonwerk-edges-pc4.parquet"

# ---- group population per PC4 (density) --------------------------------------
# Education/income: count residents from woonwerk (origin side) matching the
# group filter, for the chosen period. This is the group's total headcount per
# origin PC4 (commuters; a proxy for working-age group population).
density_from_woonwerk <- function(group_filter, period = 2L,
                                  area = ZUID_PC4, ww_path = WW_PATH) {
  w <- read_parquet(ww_path) |> filter(o_code %in% area, year == period)
  for (f in names(group_filter)) w <- w |> filter(.data[[f]] %in% group_filter[[f]])
  w |> group_by(o_code) |>
    summarise(group_pop = sum(count, na.rm = TRUE), .groups = "drop") |>
    rename(area_code = o_code)
}

# Total working population per PC4 (all groups) -> denominator for the rate.
totalpop_from_woonwerk <- function(period = 2L, area = ZUID_PC4, ww_path = WW_PATH) {
  read_parquet(ww_path) |> filter(o_code %in% area, year == period) |>
    group_by(o_code) |>
    summarise(total_pop = sum(count, na.rm = TRUE), .groups = "drop") |>
    rename(area_code = o_code)
}

# Census density (for groups woonwerk can't isolate: single-parent=18 etc.)
density_from_vk100 <- function(variable_ids, year = NULL, vk_path = VK100_PATH) {
  v <- read_parquet(vk_path) |> filter(variable %in% variable_ids)
  if (!is.null(year)) v <- v |> filter(year == !!year)
  v |> group_by(area_code) |>
    summarise(group_pop = sum(count, na.rm = TRUE), .groups = "drop")
}

# ---- overlay: accessibility x (total + rate), study area ---------------------
# density: a data.frame(area_code, group_pop) from either source above.
# Emits total, rate, and normalised access -- keeps structural vs personal
# signals separate rather than collapsing them.
build_overlay <- function(result, density, total_pop = NULL, area = ZUID_PC4) {
  ov <- result$scores |> rename(area_code = o_code) |>
    filter(area_code %in% area) |>
    left_join(density, by = "area_code") |>
    mutate(group_pop = coalesce(group_pop, 0))

  if (!is.null(total_pop))
    ov <- ov |> left_join(total_pop, by = "area_code") |>
      mutate(group_rate = ifelse(total_pop > 0, group_pop / total_pop, NA))

  rng01 <- function(x) if (diff(range(x, na.rm=TRUE)) == 0) rep(0.5, length(x)) else
                        (x - min(x, na.rm=TRUE)) / diff(range(x, na.rm=TRUE))
  ov |>
    mutate(acc_norm = rng01(accessibility),
           access_gap = 1 - acc_norm,                 # 1 = worst access in area
           # structural signal: many people * poor access
           structural = group_pop * access_gap,
           # concentration signal: high share * poor access
           concentration = if ("group_rate" %in% names(ov))
                             group_rate * access_gap else NA) |>
    arrange(desc(structural))
}

# ---- EXTRA 1: PT-vs-car gap (where PT is the bottleneck) ----------------------
# Runs the SAME group on two modes and differences the scores. Big positive gap
# = car reaches lots this group can't reach by PT -> PT connectivity problem.
# (Invoke Friday; needs no rebuild -- just calls compute_accessibility twice.)
pt_car_gap <- function(group_filter, area = ZUID_PC4, period = 2L, job_year = 2017L) {
  car <- compute_accessibility(group_filter, mode = "car", area = area,
                               period = period, job_year = job_year)$scores |>
         rename(acc_car = accessibility)
  pt  <- compute_accessibility(group_filter, mode = "transit", area = area,
                               period = period, job_year = job_year)$scores |>
         rename(acc_pt = accessibility)
  car |> left_join(pt, by = "o_code") |>
    mutate(acc_pt = coalesce(acc_pt, 0),
           pt_car_gap = acc_car - acc_pt,
           pt_ratio = ifelse(acc_car > 0, acc_pt / acc_car, NA)) |>
    arrange(desc(pt_car_gap))
}

# ---- EXTRA 2: plain reachable-jobs count within an isochrone ------------------
# Gemeente-legible: "how many relevant jobs reachable within N minutes by mode"
# -- a hard count, no decay weighting. Also splits relevant vs ALL jobs so you
# can see mismatch (jobs exist but not relevant, vs no jobs reachable at all).
reachable_jobs <- function(group_filter, mode = "transit", minutes_cap = 45,
                           area = ZUID_PC4, job_year = 2017L, tt_path = TT_PATH) {
  mode_id <- .MODE[[mode]]
  tt <- read_parquet(tt_path) |>
    filter(mode == mode_id, o_code %in% area, minutes <= minutes_cap) |>
    select(o_code, d_code, minutes)
  rel <- jobs_by_dest(group_filter, year = job_year) |> rename(jobs_rel = jobs)
  alljobs <- jobs_by_dest(list(), year = job_year) |> rename(jobs_all = jobs)
  tt |> left_join(rel, by = "d_code") |> left_join(alljobs, by = "d_code") |>
    group_by(o_code) |>
    summarise(reach_relevant = sum(jobs_rel, na.rm = TRUE),
              reach_all      = sum(jobs_all, na.rm = TRUE), .groups = "drop") |>
    mutate(relevant_share = ifelse(reach_all > 0, reach_relevant / reach_all, NA)) |>
    arrange(reach_relevant)   # worst-off first
}

# ---- inspectable bundle (what a future inspect panel surfaces) ---------------
inspect_bundle <- function(result, overlay) {
  list(label = result$label, mode = result$mode,
       areas = overlay,                          # choropleth + hover
       decay = result$decay$curve,               # histogram
       decay_summary = list(median_minutes = result$decay$median_minutes,
                            n_commuters = result$decay$n_commuters),
       top_structural = overlay |> select(area_code, accessibility, group_pop,
                                          structural) |> head(5))
}

# ---- run: low-educated Zuid, full overlay ------------------------------------
if (sys.nframe() == 0) {
  setwd("C:/NPRZ_project")
  source("R/accessibility_analysis/rotterdam-zuid-pc4.R")
  source("R/accessibility_analysis/accessibilitydecay.R")
  source("R/accessibility_analysis/accessibility-score.R")

  res <- compute_accessibility(list(opl = 1), mode = "car", period = 2L,
                               job_year = 2017L, label = "low-educated Zuid")
  dens  <- density_from_woonwerk(list(opl = 1), period = 2L)   # from woonwerk!
  total <- totalpop_from_woonwerk(period = 2L)
  ov <- build_overlay(res, density = dens, total_pop = total)
  print(ov |> select(area_code, accessibility, group_pop, group_rate,
                      structural, concentration))
}
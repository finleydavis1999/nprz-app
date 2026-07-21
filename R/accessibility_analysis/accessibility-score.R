# accessibility-score.R  (Stage 2 of the accessibility pipeline)
# -----------------------------------------------------------------------------
# Group-specific accessibility: for each origin, sum the group-relevant jobs
# reachable, each weighted by the group's decay-implied willingness to travel
# that far. Uses the Stage-1 decay curve + the travel-time matrix + group-
# relevant destination jobs (banen-werk).
#
#   accessibility(o) = sum over d of  jobs_group(d) * f(minutes[o,d])
#
# where f() is the group's decay weight at that travel time, derived from the
# Stage-1 curve. Output is node-shaped (area_code, year, variable, count,
# weight) so it drops into the app's data model / inspect directly.
#
# Area-agnostic: pass `area` (defaults to ZUID_PC4). Depends on
# accessibilitydecay.R (estimate_decay, .MODE, TT_PATH) being sourced first.
# ASCII only.

library(arrow); library(dplyr)

BANEN_PATH <- "static/data/parquet/banen-werk-pc4.parquet"

# ---- turn a Stage-1 decay curve into a continuous weight f(minutes) ----------
# The curve gives share-of-commuters per time bin. We use the *cumulative*
# survival form as the deterrence weight: f(t) = P(a commuter travels >= t),
# i.e. the fraction of this group still "willing" at time t. This is a standard
# accessibility deterrence function and reads directly off the Stage-1 curve.
# (Refinement noted in plan: replace with a fitted / hazard-based f later.)
decay_weight_fn <- function(decay) {
  cur <- decay$curve |> arrange(bin)
  # survival at the START of each bin = 1 - cumulative share up to previous bin
  cur <- cur |> mutate(surv = 1 - (cum_share - share))
  # step function: for a given minute, take surv of the bin it falls in
  bw <- if (nrow(cur) > 1) cur$bin[2] - cur$bin[1] else 5
  function(minutes) {
    idx <- findInterval(minutes, cur$bin)
    idx[idx < 1] <- 1
    w <- cur$surv[idx]
    w[is.na(w)] <- 0
    w
  }
}

# ---- group-relevant destination jobs per PC4 ---------------------------------
# banen-werk carries the same opl/inks/sector breakdown, so "low-educated jobs"
# is jobs filtered to the matching group. `job_filter` mirrors the group filter
# (e.g. list(opl = 1) = jobs held by low-educated workers, a proxy for jobs
# suitable for that group).
jobs_by_dest <- function(job_filter, year = NULL, banen_path = BANEN_PATH) {
  j <- read_parquet(banen_path)
  if (!is.null(year)) j <- j |> filter(year == !!year)
  for (f in names(job_filter)) j <- j |> filter(.data[[f]] %in% job_filter[[f]])
  j |> filter(!is.na(area_code)) |>
    group_by(area_code) |>
    summarise(jobs = sum(count, na.rm = TRUE), .groups = "drop") |>
    rename(d_code = area_code)
}

# ---- Stage 2: accessibility score per origin ---------------------------------
# group_filter : defines the resident group (for the decay curve)
# job_filter   : defines relevant destination jobs (defaults to same as group)
# mode         : travel-time mode (for non-car groups, restrict later)
# area         : study-area origins (defaults ZUID_PC4)
compute_accessibility <- function(group_filter, job_filter = group_filter,
                                  mode = "car", area = ZUID_PC4,
                                  period = 2L, job_year = 2017L,
                                  label = "group", tt_path = TT_PATH) {
  mode_id <- .MODE[[mode]]

  # 1. group decay -> weight function
   decay <- estimate_decay(group_filter, mode = mode, area = area,
                          period = period, label = label)
  wfn <- decay_weight_fn(decay)

  # 2. relevant jobs per destination
  jobs <- jobs_by_dest(job_filter, year = job_year)

  # 3. travel times FROM each study-area origin (to everywhere reachable)
  tt <- read_parquet(tt_path) |>
    filter(mode == mode_id, o_code %in% area) |>
    select(o_code, d_code, minutes)

  # 4. weight each reachable job by decay(minutes), sum per origin
  acc <- tt |>
    inner_join(jobs, by = "d_code") |>
    mutate(w = wfn(minutes), weighted_jobs = jobs * w) |>
    group_by(o_code) |>
    summarise(accessibility = sum(weighted_jobs, na.rm = TRUE), .groups = "drop")

  list(decay = decay, jobs = jobs, scores = acc, mode = mode, label = label)
}

# ---- node-shaped output for the app ------------------------------------------
# variable id encodes the (group x mode) combination; extend the map as groups
# are added. year is a snapshot placeholder for now.
to_node_parquet <- function(result, variable_id, year = 1L, out_path) {
  df <- result$scores |>
    transmute(area_code = o_code, year = year, variable = as.integer(variable_id),
              count = accessibility, weight = accessibility)
  write_parquet(df, out_path)
  message("wrote ", out_path, " (", nrow(df), " areas)")
  invisible(df)
}

# ---- run: low-educated Zuid, car ---------------------------------------------
if (sys.nframe() == 0) {
  setwd("C:/NPRZ_project")
library(arrow); library(dplyr)
source("R/accessibility_analysis/rotterdam-zuid-pc4.R")
source("R/accessibility_analysis/accessibilitydecay.R")
source("R/accessibility_analysis/accessibility-score.R") 

res <- compute_accessibility(list(opl = 1), mode = "car", label = "low-educated Zuid")
print(res$scores |> arrange(desc(accessibility)))}
# =============================================================================
# 03_process_example_data.R
# Process example nodal and edge data from SQLite files
#
# Sources (D:/data-finley/data-finley/):
#   nodes-2018.sqlite      — demographics, banen-werk, banen-woon
#   edges-woonwerk-2018.sqlite — home-work commuting flows
#
# Years exported: 2007, 2012, 2015, 2017 (nodes)
#                 20072012, 20122017 (edges — 5-year periods)
#
# Output → app-development/static/data/:
#   nodes_demographics_gem.parquet   — population by age/income/edu/sec
#   nodes_banen_werk_gem.parquet     — jobs at work location by age/income/edu
#   nodes_banen_woon_gem.parquet     — jobs at home location by age/income/edu
#   nodes_summary_gem.parquet        — wide-format totals for map display
#   edges_woonwerk_gem.parquet       — OD flows between gemeenten
#   edges_woonwerk_summary_gem.parquet — per-origin totals for nodal display
#
# GM codes: numeric in source (e.g. 363) → padded to GM0363 format
# =============================================================================

setwd("C:/NPRZ_project")
source("R_scripts/preparing_data/utils.R")

library(DBI)
library(RSQLite)
library(dplyr)
library(arrow)
library(tidyr)

# ── Config ────────────────────────────────────────────────────────────────────
NODES_DB   <- "D:/data-finley/data-finley/nodes-2018.sqlite"
EDGES_DB   <- "D:/data-finley/data-finley/edges-woonwerk-2018.sqlite"
EXPORT_DIR <- "C:/NPRZ_project/app-development/static/data"
WERKWERK_DB  <- "D:/data-finley/data-finley/edges-werkwerk-2018.sqlite"
MIGRATION_DB <- "D:/data-finley/data-finley/edges-migration-2018.sqlite"

TARGET_YEARS_NODES <- c(2007L, 2012L, 2015L, 2017L)
TARGET_YEARS_EDGES <- c("20072012", "20122017")

# Format numeric gemeente code to GM0363
format_gm <- function(x) sprintf("GM%04d", as.integer(x))

# Age category labels (from JS metadata)
AGE_LABELS <- c(
  "1" = "Jonger dan 18",
  "2" = "18-23",
  "3" = "24-29",
  "4" = "30-40",
  "5" = "40-59",
  "6" = "60+"
)

# Income category labels
INK_LABELS <- c(
  "1" = "< 20%",
  "2" = "20-40%",
  "3" = "40-60%",
  "4" = "60-80%",
  "5" = "80-100%"
)

# Education category labels
OPL_LABELS <- c(
  "1" = "Laag",
  "2" = "Midden",
  "3" = "Hoog"
)

# Socioeconomic position labels (demographics only)
SEC_LABELS <- c(
  "1" = "Actief (werkend)",
  "2" = "Uitkering",
  "3" = "Pensioen",
  "4" = "Scholier/student"
)

# ── Connect ───────────────────────────────────────────────────────────────────
con_nodes <- dbConnect(SQLite(), NODES_DB)
con_edges <- dbConnect(SQLite(), EDGES_DB)
message("Connected to SQLite databases")

# ── Helper: query with year filter ───────────────────────────────────────────
query_years <- function(con, table, years, dim_col, dim_null_cols, id_col = "gem") {
  # dim_col: the dimension we are breaking down by (NOT NULL)
  # dim_null_cols: all other dimensions that must be NULL
  
  year_list <- paste(years, collapse = ",")
  null_conditions <- paste(sprintf("AND %s IS NULL", dim_null_cols), collapse = " ")
  
  sql <- sprintf("
    SELECT %s, year, %s, SUM(value) AS value
    FROM %s
    WHERE year IN (%s)
    AND %s IS NOT NULL
    %s
    GROUP BY %s, year, %s
    ORDER BY %s, year, %s
  ", id_col, dim_col, table, year_list,
     dim_col, null_conditions,
     id_col, dim_col, id_col, dim_col)
  
  dbGetQuery(con, sql)
}

query_total <- function(con, table, years, marginal_col, null_cols, id_col = "gem") {
  # Total = marginal on age (NOT NULL), all others NULL
  year_list <- paste(years, collapse = ",")
  null_conditions <- paste(sprintf("AND %s IS NULL", null_cols), collapse = " ")
  
  sql <- sprintf("
    SELECT %s, year, SUM(value) AS total
    FROM %s
    WHERE year IN (%s)
    AND %s IS NOT NULL
    %s
    GROUP BY %s, year
    ORDER BY %s, year
  ", id_col, table, year_list,
     marginal_col, null_conditions,
     id_col, id_col)
  
  dbGetQuery(con, sql)
}

# ── SECTION 1: Banen-werk (jobs at work location) ─────────────────────────────
message("\n=== Section 1: Banen-werk (jobs at work location) ===")

werk_null_cols <- c("opl", "inks", "sectorcat", "soortbaan")

# Total workers per gemeente per year
werk_total <- query_total(con_nodes, "woonwerk_werk_19992018_gem",
                           TARGET_YEARS_NODES, "age", werk_null_cols) |>
  mutate(gemeentecode = format_gm(gem), .keep = "unused") |>
  rename(jaar = year, total_banen_werk = total)

message("  Total rows: ", nrow(werk_total))

# Age breakdown
werk_age <- query_years(con_nodes, "woonwerk_werk_19992018_gem",
                         TARGET_YEARS_NODES, "age", werk_null_cols) |>
  mutate(
    gemeentecode = format_gm(gem),
    age_label    = AGE_LABELS[as.character(age)],
    .keep = "unused"
  ) |>
  rename(jaar = year, n = value)

# Income breakdown  
werk_ink <- query_years(con_nodes, "woonwerk_werk_19992018_gem",
                         TARGET_YEARS_NODES, "inks",
                         c("age", "opl", "sectorcat", "soortbaan")) |>
  mutate(
    gemeentecode = format_gm(gem),
    ink_label    = INK_LABELS[as.character(inks)],
    .keep = "unused"
  ) |>
  rename(jaar = year, n = value)

# Education breakdown
werk_opl <- query_years(con_nodes, "woonwerk_werk_19992018_gem",
                         TARGET_YEARS_NODES, "opl",
                         c("age", "inks", "sectorcat", "soortbaan")) |>
  mutate(
    gemeentecode = format_gm(gem),
    opl_label    = OPL_LABELS[as.character(opl)],
    .keep = "unused"
  ) |>
  rename(jaar = year, n = value)

# Export detailed parquet
banen_werk_out <- list(
  total     = werk_total,
  by_age    = werk_age,
  by_income = werk_ink,
  by_edu    = werk_opl
)

write_parquet(werk_total, file.path(EXPORT_DIR, "nodes_banen_werk_totaal_gem.parquet"))
write_parquet(werk_age,   file.path(EXPORT_DIR, "nodes_banen_werk_leeftijd_gem.parquet"))
write_parquet(werk_ink,   file.path(EXPORT_DIR, "nodes_banen_werk_inkomen_gem.parquet"))
write_parquet(werk_opl,   file.path(EXPORT_DIR, "nodes_banen_werk_opleiding_gem.parquet"))
message("  Exported banen-werk parquets")

# ── SECTION 2: Banen-woon (jobs at home location = employed residents) ────────
message("\n=== Section 2: Banen-woon (employed residents at home location) ===")

woon_null_cols <- c("opl", "inks", "sectorcat", "soortbaan")

woon_total <- query_total(con_nodes, "woonwerk_woon_19992018_gem",
                           TARGET_YEARS_NODES, "age", woon_null_cols) |>
  mutate(gemeentecode = format_gm(gem), .keep = "unused") |>
  rename(jaar = year, total_banen_woon = total)

woon_age <- query_years(con_nodes, "woonwerk_woon_19992018_gem",
                         TARGET_YEARS_NODES, "age", woon_null_cols) |>
  mutate(
    gemeentecode = format_gm(gem),
    age_label    = AGE_LABELS[as.character(age)],
    .keep = "unused"
  ) |>
  rename(jaar = year, n = value)

woon_ink <- query_years(con_nodes, "woonwerk_woon_19992018_gem",
                         TARGET_YEARS_NODES, "inks",
                         c("age", "opl", "sectorcat", "soortbaan")) |>
  mutate(
    gemeentecode = format_gm(gem),
    ink_label    = INK_LABELS[as.character(inks)],
    .keep = "unused"
  ) |>
  rename(jaar = year, n = value)

woon_opl <- query_years(con_nodes, "woonwerk_woon_19992018_gem",
                         TARGET_YEARS_NODES, "opl",
                         c("age", "inks", "sectorcat", "soortbaan")) |>
  mutate(
    gemeentecode = format_gm(gem),
    opl_label    = OPL_LABELS[as.character(opl)],
    .keep = "unused"
  ) |>
  rename(jaar = year, n = value)

write_parquet(woon_total, file.path(EXPORT_DIR, "nodes_banen_woon_totaal_gem.parquet"))
write_parquet(woon_age,   file.path(EXPORT_DIR, "nodes_banen_woon_leeftijd_gem.parquet"))
write_parquet(woon_ink,   file.path(EXPORT_DIR, "nodes_banen_woon_inkomen_gem.parquet"))
write_parquet(woon_opl,   file.path(EXPORT_DIR, "nodes_banen_woon_opleiding_gem.parquet"))
message("  Exported banen-woon parquets")

# ── SECTION 3: Demographics (persoonsgegevens) ────────────────────────────────
message("\n=== Section 3: Demographics (persoonsgegevens) ===")

demo_null_cols <- c("sec", "hh", "opl", "inks")

demo_total <- query_total(con_nodes, "demographics19992018_gem",
                           TARGET_YEARS_NODES, "age", demo_null_cols) |>
  mutate(gemeentecode = format_gm(gem), .keep = "unused") |>
  rename(jaar = year, total_inwoners = total)

demo_age <- query_years(con_nodes, "demographics19992018_gem",
                         TARGET_YEARS_NODES, "age", demo_null_cols) |>
  mutate(
    gemeentecode = format_gm(gem),
    age_label    = AGE_LABELS[as.character(age)],
    .keep = "unused"
  ) |>
  rename(jaar = year, n = value)

demo_ink <- query_years(con_nodes, "demographics19992018_gem",
                         TARGET_YEARS_NODES, "inks",
                         c("age", "sec", "hh", "opl")) |>
  mutate(
    gemeentecode = format_gm(gem),
    ink_label    = INK_LABELS[as.character(inks)],
    .keep = "unused"
  ) |>
  rename(jaar = year, n = value)

demo_opl <- query_years(con_nodes, "demographics19992018_gem",
                         TARGET_YEARS_NODES, "opl",
                         c("age", "sec", "hh", "inks")) |>
  mutate(
    gemeentecode = format_gm(gem),
    opl_label    = OPL_LABELS[as.character(opl)],
    .keep = "unused"
  ) |>
  rename(jaar = year, n = value)

demo_sec <- query_years(con_nodes, "demographics19992018_gem",
                         TARGET_YEARS_NODES, "sec",
                         c("age", "hh", "opl", "inks")) |>
  mutate(
    gemeentecode = format_gm(gem),
    sec_label    = SEC_LABELS[as.character(sec)],
    .keep = "unused"
  ) |>
  rename(jaar = year, n = value)

write_parquet(demo_total, file.path(EXPORT_DIR, "nodes_demo_totaal_gem.parquet"))
write_parquet(demo_age,   file.path(EXPORT_DIR, "nodes_demo_leeftijd_gem.parquet"))
write_parquet(demo_ink,   file.path(EXPORT_DIR, "nodes_demo_inkomen_gem.parquet"))
write_parquet(demo_opl,   file.path(EXPORT_DIR, "nodes_demo_opleiding_gem.parquet"))
write_parquet(demo_sec,   file.path(EXPORT_DIR, "nodes_demo_sec_gem.parquet"))
message("  Exported demographics parquets")

# ── SECTION 4: Summary wide-format for map display ────────────────────────────
# One row per gemeente per year — totals only, plus derived ratios
# This is what the Svelte app displays as choropleth variables
message("\n=== Section 4: Summary wide-format for map display ===")

nodes_summary <- werk_total |>
  left_join(woon_total, by = c("gemeentecode", "jaar")) |>
  left_join(demo_total, by = c("gemeentecode", "jaar")) |>
  mutate(
    # Jobs-to-residents ratio (key for spatial interaction modelling)
    ratio_banen_inwoners = round(total_banen_werk / pmax(total_inwoners, 1), 4),
    # Employment rate proxy (employed residents / total pop)
    ratio_werkenden_inwoners = round(total_banen_woon / pmax(total_inwoners, 1), 4)
  )

write_parquet(nodes_summary, file.path(EXPORT_DIR, "nodes_summary_gem.parquet"))
message("  nodes_summary_gem.parquet: ", nrow(nodes_summary), " rows")
message("  Columns: ", paste(names(nodes_summary), collapse = ", "))

# ── SECTION 5: Woonwerk edges ─────────────────────────────────────────────────
message("\n=== Section 5: Woonwerk edges (home-work flows) ===")

con_edges <- dbConnect(SQLite(), EDGES_DB)

edges_raw <- dbGetQuery(con_edges, "
  SELECT woongem, werkgem, year, SUM(value) AS flow_value
  FROM woonwerk_19992018_gem
  WHERE year IN ('20072012', '20122017')
  AND age IS NOT NULL
  AND opl IS NULL AND inks IS NULL
  AND sectorcat IS NULL AND soortbaan IS NULL
  GROUP BY woongem, werkgem, year
  ORDER BY year, flow_value DESC
")

dbDisconnect(con_edges)

message("  Raw OD pairs: ", nrow(edges_raw))

# Divide by 6 (both periods are 6 collection years: 2007-2012 and 2012-2017)
# Source: JS metadata rowSumCalculation: range[1] - range[0] + 1
edges_out <- edges_raw |>
  mutate(
    origin_id      = format_gm(woongem),
    destination_id = format_gm(werkgem),
    flow_value     = round(flow_value / 6),
    variable_name  = "woonwerk",
    periode        = year
  ) |>
  filter(flow_value > 0) |>
  select(origin_id, destination_id, periode, flow_value, variable_name)

message("  After averaging and filtering zeros: ", nrow(edges_out))
message("  By period:")
print(edges_out |> count(periode))
message("  Top 5 flows (20122017):")
print(edges_out |> filter(periode == "20122017") |> head(5))

# Per-origin summary
edges_summary <- edges_out |>
  group_by(origin_id, periode) |>
  summarise(
    total_outflow  = sum(flow_value, na.rm = TRUE),
    n_destinations = n_distinct(destination_id),
    .groups = "drop"
  )

write_parquet(edges_out,     file.path(EXPORT_DIR, "edges_woonwerk_gem.parquet"))
write_parquet(edges_summary, file.path(EXPORT_DIR, "edges_woonwerk_summary_gem.parquet"))

message("  Exported edges_woonwerk_gem.parquet")
message("  Exported edges_woonwerk_summary_gem.parquet")

# ── SECTION 6: Werkwerk edges (job-to-job moves) ─────────────────────────────
message("\n=== Section 6: Werkwerk edges (job moves) ===")

con_ww <- dbConnect(SQLite(), WERKWERK_DB)

werkwerk_raw <- dbGetQuery(con_ww, "
  SELECT GEMy1, GEMy2, year, SUM(value) AS flow_value
  FROM werkwerk_19992018_gem
  WHERE year IN ('07-12', '12-17')
  AND age IS NOT NULL
  AND opl IS NULL AND inks IS NULL
  AND sectorcat2 IS NULL AND sectorsector IS NULL AND soortbaan IS NULL
  GROUP BY GEMy1, GEMy2, year
  ORDER BY year, flow_value DESC
")

dbDisconnect(con_ww)

message("  Raw OD pairs: ", nrow(werkwerk_raw))

# Both periods are 6 years (2007-2012, 2012-2017)
werkwerk_out <- werkwerk_raw |>
  mutate(
    origin_id      = format_gm(GEMy1),
    destination_id = format_gm(GEMy2),
    flow_value     = flow_value,
    variable_name  = "werkwerk",
    periode        = year
  ) |>
  filter(flow_value > 0) |>
  select(origin_id, destination_id, periode, flow_value, variable_name)

message("  After averaging: ", nrow(werkwerk_out))
print(werkwerk_out |> count(periode))

werkwerk_summary <- werkwerk_out |>
  group_by(origin_id, periode) |>
  summarise(
    total_outflow  = sum(flow_value, na.rm = TRUE),
    n_destinations = n_distinct(destination_id),
    .groups = "drop"
  )

write_parquet(werkwerk_out,     file.path(EXPORT_DIR, "edges_werkwerk_gem.parquet"))
write_parquet(werkwerk_summary, file.path(EXPORT_DIR, "edges_werkwerk_summary_gem.parquet"))
message("  Exported edges_werkwerk_gem.parquet")

# ── SECTION 7: Migration edges (residential moves) ───────────────────────────
message("\n=== Section 7: Migration edges (residential moves) ===")



con_mig <- dbConnect(SQLite(), MIGRATION_DB)

migration_raw <- dbGetQuery(con_mig, "
  SELECT gemPre, gemPost, year, SUM(value) AS flow_value
  FROM verhuizingen_19992018_gem
  WHERE year IN ('p07-10', 'p11-14', 'p15-18')
  AND age IS NOT NULL
  AND opl IS NULL AND inks IS NULL
  AND sec IS NULL AND hh IS NULL AND inkchanges IS NULL
  GROUP BY gemPre, gemPost, year
  ORDER BY year, flow_value DESC
")

dbDisconnect(con_mig)

message("  Raw OD pairs: ", nrow(migration_raw))

migration_out <- migration_raw |>
  mutate(
    origin_id      = format_gm(gemPre),
    destination_id = format_gm(gemPost),
    flow_value     = flow_value,
    variable_name  = "migration",
    periode        = year
  ) |>
  filter(flow_value > 0) |>
  select(origin_id, destination_id, periode, flow_value, variable_name)

message("  After averaging: ", nrow(migration_out))
print(migration_out |> count(periode))

migration_summary <- migration_out |>
  group_by(origin_id, periode) |>
  summarise(
    total_outflow  = sum(flow_value, na.rm = TRUE),
    n_destinations = n_distinct(destination_id),
    .groups = "drop"
  )

write_parquet(migration_out,     file.path(EXPORT_DIR, "edges_migration_gem.parquet"))
write_parquet(migration_summary, file.path(EXPORT_DIR, "edges_migration_summary_gem.parquet"))
message("  Exported edges_migration_gem.parquet")

# ── SECTION 8: Disconnect & summary ──────────────────────────────────────────
dbDisconnect(con_nodes)
dbDisconnect(con_edges)

message("\n========================================")
message("=== PIPELINE COMPLETE ===")
message("========================================")
message("\nFiles exported to: ", EXPORT_DIR)
message("\nNode files (map display):")
message("  nodes_summary_gem.parquet         — totals + ratios, all years")
message("Node files (breakdown/analysis):")
message("  nodes_banen_werk_totaal_gem        — jobs at work location")
message("  nodes_banen_werk_leeftijd_gem      — by age")
message("  nodes_banen_werk_inkomen_gem       — by income")
message("  nodes_banen_werk_opleiding_gem     — by education")
message("  nodes_banen_woon_totaal_gem        — employed residents")
message("  nodes_banen_woon_leeftijd_gem      — by age")
message("  nodes_banen_woon_inkomen_gem       — by income")
message("  nodes_banen_woon_opleiding_gem     — by education")
message("  nodes_demo_totaal_gem              — total population")
message("  nodes_demo_leeftijd_gem            — by age")
message("  nodes_demo_inkomen_gem             — by income")
message("  nodes_demo_opleiding_gem           — by education")
message("  nodes_demo_sec_gem                 — by socioeconomic position")
message("\nEdge files:")
message("  edges_woonwerk_gem.parquet         — OD flows, two periods")
message("  edges_woonwerk_summary_gem.parquet — per-origin totals")
message("\nNext: add nodes_summary_gem variables to config.ts VARIABLES array")
message("      update EDGE_DATASETS in config.ts to include edges_woonwerk_gem")
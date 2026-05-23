# cbs-vk100: CBS 100m statistical grid (2023) aggregated to Buurt/PC4/Gemeente.
#
# Source: raw-data/cbs_vk100_2023.gpkg, layer vierkant_100m (390k cells).
# Output: static/data/parquet/cbs-vk100-{buurt,pc4,gem}.parquet
#
# Long format — one row per (area_code, variable): columns area_code, year,
# variable, count, weight. `variable` is the integer id from vk100_spec(); the
# same spec drives both the aggregation and the manifest variable picker, so the
# ids must stay stable across builds (saved layers persist them to localStorage).
#
# Each grid cell is assigned to the area whose polygon contains the cell
# centroid. Counts are summed; percentages / averages / distances are
# aggregated as weighted means (population- or housing-weighted).
source("R/lib/parquet.R")
suppressPackageStartupMessages({
  library(sf)
  library(dplyr)
})
sf::sf_use_s2(FALSE)

# Variable spec — single source of truth. Columns:
#   id     stable integer id (frozen — saved layers persist it)
#   column vk100 source column
#   label  Dutch label shown in the picker
#   group  optgroup in the picker
#   rule   "sum" (counts) or "wmean" (weighted mean)
#   weight weight column for wmean; NA for sum
vk100_spec <- function() {
  tibble::tribble(
    ~id,  ~column,                                                 ~label,                                          ~group,                     ~rule,   ~weight,
    1L,   "aantal_inwoners",                                       "Inwoners",                                      "Bevolking",                "sum",   NA_character_,
    2L,   "aantal_mannen",                                         "Mannen",                                        "Bevolking",                "sum",   NA_character_,
    3L,   "aantal_vrouwen",                                        "Vrouwen",                                       "Bevolking",                "sum",   NA_character_,
    4L,   "aantal_inwoners_0_tot_15_jaar",                         "Inwoners 0 tot 15 jaar",                        "Bevolking",                "sum",   NA_character_,
    5L,   "aantal_inwoners_15_tot_25_jaar",                        "Inwoners 15 tot 25 jaar",                       "Bevolking",                "sum",   NA_character_,
    6L,   "aantal_inwoners_25_tot_45_jaar",                        "Inwoners 25 tot 45 jaar",                       "Bevolking",                "sum",   NA_character_,
    7L,   "aantal_inwoners_45_tot_65_jaar",                        "Inwoners 45 tot 65 jaar",                       "Bevolking",                "sum",   NA_character_,
    8L,   "aantal_inwoners_65_jaar_en_ouder",                      "Inwoners 65 jaar en ouder",                     "Bevolking",                "sum",   NA_character_,
    9L,   "aantal_geboorten",                                      "Geboorten",                                     "Bevolking",                "sum",   NA_character_,
    10L,  "percentage_geb_nederland_herkomst_nederland",           "% geboren in NL, herkomst Nederland",           "Bevolking",                "wmean", "aantal_inwoners",
    11L,  "percentage_geb_nederland_herkomst_overig_europa",       "% geboren in NL, herkomst overig Europa",       "Bevolking",                "wmean", "aantal_inwoners",
    12L,  "percentage_geb_nederland_herkomst_buiten_europa",       "% geboren in NL, herkomst buiten Europa",       "Bevolking",                "wmean", "aantal_inwoners",
    13L,  "percentage_geb_buiten_nederland_herkomst_europa",       "% geboren buiten NL, herkomst Europa",          "Bevolking",                "wmean", "aantal_inwoners",
    14L,  "percentage_geb_buiten_nederland_herkmst_buiten_europa", "% geboren buiten NL, herkomst buiten Europa",   "Bevolking",                "wmean", "aantal_inwoners",
    15L,  "aantal_part_huishoudens",                               "Particuliere huishoudens",                      "Huishoudens",              "sum",   NA_character_,
    16L,  "aantal_eenpersoonshuishoudens",                         "Eenpersoonshuishoudens",                        "Huishoudens",              "sum",   NA_character_,
    17L,  "aantal_meerpersoonshuishoudens_zonder_kind",            "Meerpersoonshuishoudens zonder kinderen",       "Huishoudens",              "sum",   NA_character_,
    18L,  "aantal_eenouderhuishoudens",                            "Eenouderhuishoudens",                           "Huishoudens",              "sum",   NA_character_,
    19L,  "aantal_tweeouderhuishoudens",                           "Tweeouderhuishoudens",                          "Huishoudens",              "sum",   NA_character_,
    20L,  "gemiddelde_huishoudensgrootte",                         "Gemiddelde huishoudensgrootte",                 "Huishoudens",              "wmean", "aantal_part_huishoudens",
    21L,  "aantal_woningen",                                       "Woningen",                                      "Wonen",                    "sum",   NA_character_,
    22L,  "aantal_woningen_bouwjaar_voor_1945",                    "Woningen bouwjaar voor 1945",                   "Wonen",                    "sum",   NA_character_,
    23L,  "aantal_woningen_bouwjaar_45_tot_65",                    "Woningen bouwjaar 1945 tot 1965",               "Wonen",                    "sum",   NA_character_,
    24L,  "aantal_woningen_bouwjaar_65_tot_75",                    "Woningen bouwjaar 1965 tot 1975",               "Wonen",                    "sum",   NA_character_,
    25L,  "aantal_woningen_bouwjaar_75_tot_85",                    "Woningen bouwjaar 1975 tot 1985",               "Wonen",                    "sum",   NA_character_,
    26L,  "aantal_woningen_bouwjaar_85_tot_95",                    "Woningen bouwjaar 1985 tot 1995",               "Wonen",                    "sum",   NA_character_,
    27L,  "aantal_woningen_bouwjaar_95_tot_05",                    "Woningen bouwjaar 1995 tot 2005",               "Wonen",                    "sum",   NA_character_,
    28L,  "aantal_woningen_bouwjaar_05_tot_15",                    "Woningen bouwjaar 2005 tot 2015",               "Wonen",                    "sum",   NA_character_,
    29L,  "aantal_woningen_bouwjaar_15_en_later",                  "Woningen bouwjaar 2015 en later",               "Wonen",                    "sum",   NA_character_,
    30L,  "aantal_meergezins_woningen",                            "Meergezinswoningen",                            "Wonen",                    "sum",   NA_character_,
    31L,  "percentage_koopwoningen",                               "% koopwoningen",                                "Wonen",                    "wmean", "aantal_woningen",
    32L,  "percentage_huurwoningen",                               "% huurwoningen",                                "Wonen",                    "wmean", "aantal_woningen",
    33L,  "aantal_huurwoningen_in_bezit_woningcorporaties",        "Huurwoningen van woningcorporaties",            "Wonen",                    "sum",   NA_character_,
    34L,  "aantal_niet_bewoonde_woningen",                         "Niet-bewoonde woningen",                        "Wonen",                    "sum",   NA_character_,
    35L,  "gemiddelde_woz_waarde_woning",                          "Gemiddelde WOZ-waarde woning",                  "Wonen",                    "wmean", "aantal_woningen",
    36L,  "gemiddeld_gasverbruik_woning",                          "Gemiddeld gasverbruik per woning",              "Energie",                  "wmean", "aantal_woningen",
    37L,  "gemiddeld_elektriciteitsverbruik_woning",               "Gemiddeld elektriciteitsverbruik per woning",   "Energie",                  "wmean", "aantal_woningen",
    38L,  "aantal_personen_met_uitkering_onder_aowlft",            "Personen met uitkering (onder AOW-leeftijd)",   "Inkomen",                  "sum",   NA_character_,
    39L,  "dichtstbijzijnde_grote_supermarkt_afstand_in_km",       "Afstand tot grote supermarkt (km)",             "Afstand tot voorzieningen", "wmean", "aantal_inwoners",
    40L,  "dichtstbijzijnde_winkels_ov_dagel_levensm_afst_in_km",  "Afstand tot overige dagelijkse levensmiddelen (km)", "Afstand tot voorzieningen", "wmean", "aantal_inwoners",
    41L,  "dichtstbijzijnde_huisartsenpraktijk_afstand_in_km",     "Afstand tot huisartsenpraktijk (km)",           "Afstand tot voorzieningen", "wmean", "aantal_inwoners",
    42L,  "dichtstbijzijnde_kinderdagverblijf_afstand_in_km",      "Afstand tot kinderdagverblijf (km)",            "Afstand tot voorzieningen", "wmean", "aantal_inwoners",
    43L,  "dichtstbijzijnde_basisonderwijs_afstand_in_km",         "Afstand tot basisonderwijs (km)",               "Afstand tot voorzieningen", "wmean", "aantal_inwoners",
    44L,  "dichtstbijzijnde_voortgezet_onderwijs_afstand_in_km",   "Afstand tot voortgezet onderwijs (km)",         "Afstand tot voorzieningen", "wmean", "aantal_inwoners",
    45L,  "dichtstbijzijnde_treinstation_afstand_in_km",           "Afstand tot treinstation (km)",                 "Afstand tot voorzieningen", "wmean", "aantal_inwoners",
    46L,  "dichtstbijzijnde_ziekenh_excl_buitenpoli_afst_in_km",   "Afstand tot ziekenhuis (km)",                   "Afstand tot voorzieningen", "wmean", "aantal_inwoners",
    47L,  "dichtstbijzijnde_restaurant_afstand_in_km",             "Afstand tot restaurant (km)",                   "Afstand tot voorzieningen", "wmean", "aantal_inwoners",
    48L,  "dichtstbijzijnde_oprit_hoofdverkeersweg_afstand_in_km", "Afstand tot oprit hoofdverkeersweg (km)",       "Afstand tot voorzieningen", "wmean", "aantal_inwoners",
    49L,  "dichtstbijzijnde_brandweerkazerne_afstand_in_km",       "Afstand tot brandweerkazerne (km)",             "Afstand tot voorzieningen", "wmean", "aantal_inwoners",
    50L,  "grote_supermarkt_aantal_binnen_1_km",                   "Grote supermarkten binnen 1 km",                "Nabijheid voorzieningen",  "wmean", "aantal_inwoners",
    51L,  "winkels_ov_dagel_levensm_aantal_binnen_1_km",           "Dagelijkse-levensmiddelenwinkels binnen 1 km",  "Nabijheid voorzieningen",  "wmean", "aantal_inwoners",
    52L,  "huisartsenpraktijk_aantal_binnen_1_km",                 "Huisartsenpraktijken binnen 1 km",              "Nabijheid voorzieningen",  "wmean", "aantal_inwoners",
    53L,  "kinderdagverblijf_aantal_binnen_1_km",                  "Kinderdagverblijven binnen 1 km",               "Nabijheid voorzieningen",  "wmean", "aantal_inwoners",
    54L,  "basisonderwijs_aantal_binnen_1_km",                     "Basisscholen binnen 1 km",                      "Nabijheid voorzieningen",  "wmean", "aantal_inwoners",
    55L,  "restaurant_aantal_binnen_1_km",                         "Restaurants binnen 1 km",                       "Nabijheid voorzieningen",  "wmean", "aantal_inwoners"
  )
}

# Replace CBS sentinel codes (< -99990, e.g. -99997 suppressed) with NA.
.clean_sentinels <- function(df) {
  num <- vapply(df, is.numeric, logical(1))
  df[num] <- lapply(df[num], function(x) ifelse(x < -99990, NA_real_, as.double(x)))
  df
}

# Weighted mean; NA when no valid (value, weight) pair exists.
.wmean <- function(value, weight) {
  ok <- !is.na(value) & !is.na(weight) & weight > 0
  if (!any(ok)) return(NA_real_)
  sum(value[ok] * weight[ok]) / sum(weight[ok])
}

# Read the vk100 attribute table via DuckDB's SQLite scanner (the gpkg feature
# table is a plain SQLite table — selecting only attribute columns skips the
# geometry BLOB entirely). Returns an sf POINT layer of cell centroids in RD.
.vk100_cells <- function(gpkg, spec) {
  con <- duckdb_with_sqlite(gpkg, alias = "vk")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  cols <- paste(c("crs28992res100m", spec$column), collapse = ", ")
  df <- DBI::dbGetQuery(con, sprintf("SELECT %s FROM vk.vierkant_100m", cols))

  missing <- setdiff(spec$column, names(df))
  if (length(missing)) {
    stop("cbs-vk100: columns missing from gpkg: ", paste(missing, collapse = ", "))
  }
  df <- .clean_sentinels(df)

  # crs28992res100m = "E<easting/100>N<northing/100>" of the cell's SW corner;
  # the centroid is +50 m on each axis.
  e <- as.numeric(sub("^E(\\d+)N(\\d+)$", "\\1", df$crs28992res100m))
  n <- as.numeric(sub("^E(\\d+)N(\\d+)$", "\\2", df$crs28992res100m))
  df$.x <- e * 100 + 50
  df$.y <- n * 100 + 50
  sf::st_as_sf(df, coords = c(".x", ".y"), crs = 28992)
}

# Aggregate cell centroids to one scale. `polys` is an sf with an `area_code`
# column. Returns a long data frame: area_code, variable (id), value.
.aggregate_scale <- function(cells, polys, spec) {
  joined <- sf::st_join(cells, polys["area_code"], join = sf::st_within)
  tab <- sf::st_drop_geometry(joined)
  outside <- sum(is.na(tab$area_code))
  tab <- tab[!is.na(tab$area_code), , drop = FALSE]
  # A centroid on a shared border could match two polygons — keep one.
  tab <- dplyr::distinct(tab, crs28992res100m, .keep_all = TRUE)
  cat("  ", nrow(tab), "cells assigned,", outside, "outside\n")

  g <- dplyr::group_by(tab, area_code)
  parts <- lapply(seq_len(nrow(spec)), function(i) {
    s <- spec[i, ]
    agg <- if (s$rule == "sum") {
      dplyr::summarise(g, value = sum(.data[[s$column]], na.rm = TRUE), .groups = "drop")
    } else {
      dplyr::summarise(g, value = .wmean(.data[[s$column]], .data[[s$weight]]), .groups = "drop")
    }
    agg$variable <- s$id
    agg
  })
  dplyr::bind_rows(parts) |> dplyr::filter(!is.na(value))
}

# Write one scale's long data frame to parquet via DuckDB COPY.
.write_vk100_parquet <- function(con, long_df, scale) {
  out_df <- dplyr::transmute(
    long_df,
    area_code = as.character(area_code),
    year      = 2023L,
    variable  = as.integer(variable),
    count     = as.double(value),
    weight    = as.double(value)
  )
  duckdb::duckdb_register(con, "vk100_tmp", out_df)
  on.exit(duckdb::duckdb_unregister(con, "vk100_tmp"), add = TRUE)
  write_parquet_from_query(
    con,
    "SELECT * FROM vk100_tmp ORDER BY variable, area_code",
    sprintf("static/data/parquet/cbs-vk100-%s.parquet", scale)
  )
}

build_cbs_vk100 <- function() {
  spec <- vk100_spec()

  cat("reading CBS 100m grid...\n")
  cells <- .vk100_cells("raw-data/cbs_vk100_2023.gpkg", spec)
  cat("  ", nrow(cells), "grid cells\n")

  # Join-target polygons — all already RD/EPSG:28992, no transform needed.
  polys <- list(
    buurt = sf::read_sf("raw-data/geo-data/cbsgebiedsindelingen2025.gpkg",
                        layer = "buurt_gegeneraliseerd") |>
      dplyr::transmute(area_code = statcode),
    pc4   = sf::read_sf("raw-data/geo-data/cbs_pc4_2024_v1.gpkg",
                        layer = "cbs_pc4_2024") |>
      dplyr::transmute(area_code = sprintf("%04d", as.integer(postcode))),
    gem   = sf::read_sf("raw-data/geo-data/cbsgebiedsindelingen2025.gpkg",
                        layer = "gemeente_gegeneraliseerd") |>
      dplyr::transmute(area_code = statcode)
  )

  con <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  present <- integer(0)
  for (scale in names(polys)) {
    cat("aggregating to", scale, "...\n")
    long_df <- .aggregate_scale(cells, polys[[scale]], spec)
    present <- union(present, unique(long_df$variable))
    .write_vk100_parquet(con, long_df, scale)
  }

  # Advertise only variables that produced data. The 2023 grid populates all of
  # them; this guard stays as a safety net for file versions where a column is
  # all-sentinel (as the WOZ/energy/proximity columns were in the 2024 grid).
  avail <- spec[spec$id %in% present, ]
  dropped <- spec$column[!(spec$id %in% present)]
  if (length(dropped)) {
    cat("note:", length(dropped), "variable(s) empty in source, omitted:\n      ",
      paste(dropped, collapse = ", "), "\n")
  }

  list(
    name = "CBS Vierkant 100m 2023",
    description = paste(
      "CBS 100m-grid 2023, geaggregeerd per gebied via cel-centroïde-in-polygoon.",
      "Aantallen worden gesommeerd; percentages, gemiddelden en afstanden zijn",
      "inwoner- of woninggewogen gemiddelden. Cellen <5 inwoners zijn door CBS onderdrukt."
    ),
    warning = paste(
      "Data in this set has been aggregated up from the '100m vierkant' scale.",
      "Absolute counts are summed, other variables are using a population-weighted",
      "average. Keep that in mind when interpreting the data."
    ),
    scales = list(
      buurt = "parquet/cbs-vk100-buurt.parquet",
      pc4   = "parquet/cbs-vk100-pc4.parquet",
      gem   = "parquet/cbs-vk100-gem.parquet"
    ),
    fields = list(
      year = list(
        type = "single", label = "Jaar",
        values = list(list(id = 2023L, label = "2023")),
        default = 2023L, min = 2023L, max = 2023L
      ),
      variable = list(
        type = "single", label = "Variabele",
        values = lapply(seq_len(nrow(avail)), function(i) {
          list(id = avail$id[i], label = avail$label[i], group = avail$group[i])
        }),
        default = avail$id[1]
      )
    ),
    countCol  = "count",
    weightCol = "weight",
    yearAggregation = "sum",
    defaultClassification = list(method = "jenks", n = 5L, palette = "YlOrRd")
  )
}

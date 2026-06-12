# R/nodes/cbs-kwb.R
#
# CBS Kerncijfers Wijken en Buurten (KWB) aggregated for app use.
#
# Produces (long-format, matching cbs-vk100's schema):
#   static/data/parquet/cbs-kwb-buurt.parquet    — native buurt resolution
#   static/data/parquet/cbs-kwb-gem.parquet      — aggregated buurt -> gemeente
#
# Each row: area_code, year, variable (integer id), count, weight.
# `variable` ids are FROZEN from creation — saved layers persist them to
# localStorage, so changing an id later silently breaks user-saved layers.
# Adding new ids at the end is safe.
#
# Years: 2015, 2017, 2022, 2023, 2024, 2025. Builds in ascending order so a
# partial run still produces parquets containing whatever years completed.
#
# Variables added vs old cbs-census-buurt.R:
#   - Bedrijfsvestigingen (business establishments, total) — 2022+
#   - Arbeid: working population, net labour participation, % employees vs
#     self-employed, permanent vs flex contracts — 2023+ in full
#   - Area per scale (oppervlakte land, in ha) — for normalisation
#
# Variables NOT carried forward from old script:
#   - Origin-percentage variables — the CBS schema changed in 2024 (geboorteland /
#     herkomst vs Westers / NietWesters). Carrying both is messy, so origin
#     percentages are omitted here; revisit as a follow-up if needed.
#
# To run:
#   source("R/nodes/cbs-kwb.R")
#   build_cbs_kwb()
# =============================================================================

source("R/lib/parquet.R")  # write_parquet_from_query() — shared DuckDB writer
suppressPackageStartupMessages({
  library(dplyr)
  library(tibble)
  library(cbsodataR)
})

# ── Years and table ids ───────────────────────────────────────────────────────
# Ascending order: a partial run produces a parquet with whatever completed.
.KWB_YEARS <- list(
  `2015` = "83220NED",
  `2017` = "83765NED",
  `2022` = "85318NED",
  `2023` = "85618NED",
  `2024` = "85984NED",
  `2025` = "86165NED"
)

# ── Helpers ───────────────────────────────────────────────────────────────────

# Replace CBS sentinel codes (< -99990) with NA.
.clean_sentinel <- function(x) {
  if (!is.numeric(x)) return(x)
  ifelse(x < -99990, NA_real_, x)
}

# Weighted mean; NA if no valid (value, weight) pair.
.wmean <- function(values, weights) {
  w  <- ifelse(is.na(weights) | weights <= 0, 0, weights)
  ok <- !is.na(values) & w > 0
  if (!any(ok)) return(NA_real_)
  sum(values[ok] * w[ok]) / sum(w[ok])
}

# Null-coalesce: return first non-all-NA vector, else next, ..., else NA vec.
# Use to pick the first column that exists for a year.
.coalesce_col <- function(df, candidates) {
  for (col in candidates) {
    if (col %in% names(df)) {
      v <- df[[col]]
      if (!all(is.na(v))) return(v)
    }
  }
  rep(NA_real_, nrow(df))
}

.cbs_raw_dir <- function() {
  d <- file.path("raw-data", "cbs")
  dir.create(d, recursive = TRUE, showWarnings = FALSE)
  d
}

.get_kwb <- function(year_str) {
  table_id <- .KWB_YEARS[[year_str]]
  path     <- file.path(.cbs_raw_dir(), paste0("kwb_", year_str, ".rds"))
  if (!file.exists(path)) {
    cat(sprintf("Downloading KWB %s (table %s)...\n", year_str, table_id))
    kwb <- cbsodataR::cbs_get_data(table_id)
    saveRDS(kwb, path)
    cat("  Saved:", nrow(kwb), "rows\n")
  }
  readRDS(path)
}

# ── Variable spec ─────────────────────────────────────────────────────────────
#
# Single source of truth. Fields:
#   id            — frozen integer, do not change
#   internal      — short internal name; the wide-table column name and the key
#                   `.kwb_value_for()` switches on. `weight_col` references it.
#   label         — Dutch display label
#   group         — picker optgroup
#   rule          — "sum" (counts) or "wmean" (weighted mean)
#   weight_col    — `internal` name of the column to weight by, for wmean.
#
# The per-year KWB source column for each `internal` name lives in
# `.kwb_value_for()`: CBS shifts the column suffixes across years, so each
# variable carries a fallback chain of candidate column names.
.kwb_spec <- function() {
  spec <- tribble(
    ~id,  ~internal,           ~label,                                    ~group,            ~rule,    ~weight_col,
    1L,   "aantal_inwoners",   "Inwoners totaal",                         "Bevolking",       "sum",    NA_character_,
    2L,   "mannen",            "Mannen",                                  "Bevolking",       "sum",    NA_character_,
    3L,   "vrouwen",           "Vrouwen",                                 "Bevolking",       "sum",    NA_character_,
    4L,   "leeftijd_0_15",     "Inwoners 0-15 jaar",                      "Bevolking",       "sum",    NA_character_,
    5L,   "leeftijd_15_25",    "Inwoners 15-25 jaar",                     "Bevolking",       "sum",    NA_character_,
    6L,   "leeftijd_25_45",    "Inwoners 25-45 jaar",                     "Bevolking",       "sum",    NA_character_,
    7L,   "leeftijd_45_65",    "Inwoners 45-65 jaar",                     "Bevolking",       "sum",    NA_character_,
    8L,   "leeftijd_65_plus",  "Inwoners 65 jaar en ouder",               "Bevolking",       "sum",    NA_character_,
    9L,   "huishoudens",       "Particuliere huishoudens",                "Huishoudens",     "sum",    NA_character_,
    10L,  "hh_eenpers",        "Eenpersoonshuishoudens",                  "Huishoudens",     "sum",    NA_character_,
    11L,  "hh_eenouder",       "Eenouderhuishoudens",                     "Huishoudens",     "sum",    NA_character_,
    12L,  "hh_tweeouder",      "Tweeouderhuishoudens",                    "Huishoudens",     "sum",    NA_character_,
    13L,  "hh_grootte",        "Gemiddelde huishoudensgrootte",           "Huishoudens",     "wmean",  "huishoudens",
    14L,  "woningen",          "Woningen totaal",                         "Wonen",           "sum",    NA_character_,
    15L,  "woz",               "Gemiddelde WOZ-waarde",                   "Wonen",           "wmean",  "woningen",
    16L,  "bijstand",          "Bijstandsontvangers",                     "Inkomen",         "sum",    NA_character_,
    17L,  "ao_uitkering",      "AO-uitkeringsontvangers",                 "Inkomen",         "sum",    NA_character_,
    18L,  "ww_uitkering",      "WW-uitkeringsontvangers",                 "Inkomen",         "sum",    NA_character_,
    19L,  "aow_uitkering",     "AOW-ontvangers",                          "Inkomen",         "sum",    NA_character_,
    20L,  "personenautos",     "Personenauto's",                          "Mobiliteit",      "sum",    NA_character_,
    21L,  "leerl_po",          "Leerlingen primair onderwijs",            "Onderwijs",       "sum",    NA_character_,
    22L,  "leerl_vo",          "Leerlingen voortgezet onderwijs",         "Onderwijs",       "sum",    NA_character_,
    23L,  "stud_mbo",          "Studenten MBO",                           "Onderwijs",       "sum",    NA_character_,
    24L,  "stud_hbo",          "Studenten HBO",                           "Onderwijs",       "sum",    NA_character_,
    25L,  "stud_wo",           "Studenten WO",                            "Onderwijs",       "sum",    NA_character_,
    26L,  "stedelijkheid",     "Stedelijkheid (1=zeer sterk, 5=niet)",    "Stedelijkheid",   "wmean",  "aantal_inwoners",
    27L,  "adres_dichtheid",   "Omgevingsadressendichtheid",              "Stedelijkheid",   "wmean",  "aantal_inwoners",
    28L,  "afst_huisarts",     "Afstand tot huisartsenpraktijk (km)",     "Voorzieningen",   "wmean",  "aantal_inwoners",
    29L,  "afst_supermarkt",   "Afstand tot grote supermarkt (km)",       "Voorzieningen",   "wmean",  "aantal_inwoners",
    30L,  "afst_basisond",     "Afstand tot basisonderwijs (km)",         "Voorzieningen",   "wmean",  "aantal_inwoners",
    31L,  "opp_land_ha",       "Oppervlakte land (ha)",                   "Gebied",          "sum",    NA_character_,
    # New variables (2022+ for businesses, 2023+ in full for arbeid):
    32L,  "bedrijfsvest",      "Bedrijfsvestigingen totaal",              "Economie",        "sum",    NA_character_,
    33L,  "werkzame_bevolk",   "Werkzame beroepsbevolking",               "Arbeid",          "sum",    NA_character_,
    34L,  "arbeidsparticip",   "Nettoarbeidsparticipatie (%)",            "Arbeid",          "wmean",  "aantal_inwoners",
    35L,  "pct_werknemers",    "% Werknemers",                            "Arbeid",          "wmean",  "werkzame_bevolk",
    36L,  "werkn_vast",        "Werknemers vaste arbeidsrelatie",         "Arbeid",          "sum",    NA_character_,
    37L,  "werkn_flex",        "Werknemers flexibele arbeidsrelatie",     "Arbeid",          "sum",    NA_character_,
    38L,  "pct_zelfstand",     "% Zelfstandigen",                         "Arbeid",          "wmean",  "werkzame_bevolk",
    39L,  "jeugdzorg",         "Jongeren met jeugdzorg",                  "Zorg",            "sum",    NA_character_,
    40L,  "pct_jeugdzorg",     "% Jongeren met jeugdzorg",                "Zorg",            "wmean",  "aantal_inwoners",
    41L,  "wmo_clienten",      "Wmo-cliënten",                            "Zorg",            "sum",    NA_character_,
    42L,  "pct_wmo_clienten",  "Wmo-cliënten relatief (%)",               "Zorg",            "wmean",  "aantal_inwoners"
  )
  # Declare label bytes as UTF-8 so jsonlite emits accented characters (ë)
  # correctly regardless of how the file was source()'d (default source() marks
  # strings "unknown", which jsonlite would escape as raw <c3><ab> bytes).
  Encoding(spec$label) <- "UTF-8"
  spec
}

# Map a spec variable to its source column in kwb_raw, coalescing over the
# candidate names CBS uses across years. Returns a numeric vector (NA where the
# year lacks the variable). The candidate chains are year-agnostic — the first
# column that exists and isn't all-NA wins — so no year argument is needed.
.kwb_value_for <- function(internal_name, kwb_raw) {
  cands <- switch(
    internal_name,
    aantal_inwoners  = c("AantalInwoners_5"),
    mannen           = c("Mannen_6"),
    vrouwen          = c("Vrouwen_7"),
    leeftijd_0_15    = c("k_0Tot15Jaar_8"),
    leeftijd_15_25   = c("k_15Tot25Jaar_9"),
    leeftijd_25_45   = c("k_25Tot45Jaar_10"),
    leeftijd_45_65   = c("k_45Tot65Jaar_11"),
    leeftijd_65_plus = c("k_65JaarOfOuder_12"),
    huishoudens      = c("HuishoudensTotaal_29", "HuishoudensTotaal_28"),
    hh_eenpers       = c("Eenpersoonshuishoudens_30", "Eenpersoonshuishoudens_29"),
    hh_eenouder      = c("HuishoudensZonderKinderen_31", "HuishoudensZonderKinderen_30"),
    hh_tweeouder    = c("HuishoudensMetKinderen_32", "HuishoudensMetKinderen_31"),
    hh_grootte       = c("GemiddeldeHuishoudensgrootte_33", "GemiddeldeHuishoudensgrootte_32"),
    woningen         = c("Woningvoorraad_38", "Woningvoorraad_35", "Woningvoorraad_34"),
    woz              = c("GemiddeldeWOZWaardeVanWoningen_39", "GemiddeldeWOZWaardeVanWoningen_36", "GemiddeldeWOZWaardeVanWoningen_35"),
    bijstand         = c("PersonenPerSoortUitkeringBijstand_87", "PersonenPerSoortUitkeringBijstand_92", "PersonenPerSoortUitkeringBijstand_83", "PersonenPerSoortUitkeringBijstand_74"),
    ao_uitkering     = c("PersonenPerSoortUitkeringAO_88",  "PersonenPerSoortUitkeringAO_93",  "PersonenPerSoortUitkeringAO_84",  "PersonenPerSoortUitkeringAO_75"),
    ww_uitkering     = c("PersonenPerSoortUitkeringWW_89",  "PersonenPerSoortUitkeringWW_94",  "PersonenPerSoortUitkeringWW_85",  "PersonenPerSoortUitkeringWW_76"),
    aow_uitkering    = c("PersonenPerSoortUitkeringAOW_90", "PersonenPerSoortUitkeringAOW_95", "PersonenPerSoortUitkeringAOW_86", "PersonenPerSoortUitkeringAOW_77"),
    personenautos    = c("PersonenautoSTotaal_104", "PersonenautoSTotaal_109", "PersonenautoSTotaal_100", "PersonenautoSTotaal_86"),
    leerl_po         = c("LeerlingenPo_62", "LeerlingenPo_65", "LeerlingenPo_61"),
    leerl_vo         = c("LeerlingenVoInclVavo_63", "LeerlingenVoInclVavo_66", "LeerlingenVo_62"),
    stud_mbo         = c("StudentenMboExclExtranei_64", "StudentenMboExclExtranei_67", "StudentenMbo_63"),
    stud_hbo         = c("StudentenHbo_65", "StudentenHbo_68", "StudentenHbo_64"),
    stud_wo          = c("StudentenWo_66", "StudentenWo_69", "StudentenWo_65"),
    stedelijkheid    = c("MateVanStedelijkheid_120", "MateVanStedelijkheid_125", "MateVanStedelijkheid_116", "MateVanStedelijkheid_104", "MateVanStedelijkheid_105"),
    adres_dichtheid  = c("Omgevingsadressendichtheid_121", "Omgevingsadressendichtheid_126", "Omgevingsadressendichtheid_117", "Omgevingsadressendichtheid_105", "Omgevingsadressendichtheid_106"),
    afst_huisarts    = c("AfstandTotHuisartsenpraktijk_110", "AfstandTotHuisartsenpraktijk_115", "AfstandTotHuisartsenpraktijk_106", "AfstandTotHuisartsenpraktijk_94"),
    afst_supermarkt  = c("AfstandTotGroteSupermarkt_111",    "AfstandTotGroteSupermarkt_116",    "AfstandTotGroteSupermarkt_107",    "AfstandTotGroteSupermarkt_95"),
    afst_basisond    = c("AfstandTotSchool_113", "AfstandTotSchool_118", "AfstandTotSchool_109", "AfstandTotSchoolBasisonderwijs_42", "AfstandTotSchool_97"),
    opp_land_ha      = c("OppervlakteLand_116", "OppervlakteLand_121", "OppervlakteLand_112", "OppervlakteLand_100", "OppervlakteLand_101"),
    bedrijfsvest     = c("BedrijfsvestigingenTotaal_95", "BedrijfsvestigingenTotaal_100"),
    werkzame_bevolk  = c("WerkzameBeroepsbevolking_70", "WerkzameBeroepsbevolking_73"),
    arbeidsparticip  = c("Nettoarbeidsparticipatie_71", "Nettoarbeidsparticipatie_74"),
    pct_werknemers   = c("PercentageWerknemers_72", "PercentageWerknemers_75"),
    werkn_vast       = c("WerknemersMetVasteArbeidsrelatie_73", "WerknemersMetVasteArbeidsrelatie_76", "WerknemersMetVasteArbeidsr_73"),
    werkn_flex       = c("WerknemersMetFlexibeleArbeidsrelatie_74", "WerknemersMetFlexibeleArbeidsrelatie_77", "WerknemersMetFlexibeleArbe_74"),
    pct_zelfstand    = c("PercentageZelfstandigen_75", "PercentageZelfstandigen_78"),
    jeugdzorg        = c("JongerenMetJeugdzorgInNatura_91", "JongerenMetJeugdzorgInNatura_96"),
    pct_jeugdzorg    = c("PercentageJongerenMetJeugdzorg_92", "PercentageJongerenMetJeugdzorg_97"),
    wmo_clienten     = c("WmoClienten_93", "WmoClienten_98"),
    pct_wmo_clienten = c("WmoClientenRelatief_94", "WmoClientenRelatief_99"),
    NULL
  )
  if (is.null(cands)) {
    stop(sprintf("kwb_value_for: unknown internal name '%s'", internal_name))
  }
  .clean_sentinel(.coalesce_col(kwb_raw, cands))
}

# Build a wide intermediate table for one year, with all spec variables as
# columns (named by their `internal` name). Filtered to Buurt rows only.
.kwb_wide_for_year <- function(kwb_raw, year_int) {
  spec <- .kwb_spec()
  df <- tibble(
    area_code   = trimws(kwb_raw$WijkenEnBuurten),
    soort_regio = trimws(kwb_raw$SoortRegio_2)
  )
  for (i in seq_len(nrow(spec))) {
    s <- spec[i, ]
    df[[s$internal]] <- .kwb_value_for(s$internal, kwb_raw)
  }
  df <- df[df$soort_regio == "Buurt", , drop = FALSE]
  df$soort_regio <- NULL
  df$year <- year_int
  df
}

# Convert a wide year-table to long format matching cbs-vk100's schema.
# Returns: area_code, year, variable (id), count, weight.
.wide_to_long <- function(wide_df) {
  spec <- .kwb_spec()
  out <- list()
  for (i in seq_len(nrow(spec))) {
    s <- spec[i, ]
    col <- s$internal
    if (!(col %in% names(wide_df))) next
    vals <- wide_df[[col]]
    keep <- !is.na(vals)
    if (!any(keep)) next
    out[[length(out) + 1L]] <- tibble(
      area_code = wide_df$area_code[keep],
      year      = wide_df$year[keep],
      variable  = s$id,
      count     = as.double(vals[keep]),
      weight    = as.double(vals[keep])
    )
  }
  if (length(out) == 0L) {
    return(tibble(area_code = character(), year = integer(),
                  variable = integer(), count = double(), weight = double()))
  }
  bind_rows(out)
}

# Aggregate the wide buurt table up to gemeente. Same rules as the spec:
# sum for counts, weighted mean for wmean variables. Returns a wide gem-level
# table that .wide_to_long can convert.
.aggregate_to_gem <- function(wide_buurt) {
  spec <- .kwb_spec()
  # Gemeente code derived from buurt code: buurt is "BU<gem4><wijk2><buurt2>",
  # gemeente is "GM<gem4>".
  wide_buurt$gem <- paste0("GM", substr(wide_buurt$area_code, 3, 6))

  # Build aggregation per spec entry.
  result <- wide_buurt |>
    select(area_code = gem, year) |>
    distinct()

  for (i in seq_len(nrow(spec))) {
    s   <- spec[i, ]
    col <- s$internal
    if (!(col %in% names(wide_buurt))) next
    agg <- wide_buurt |>
      group_by(area_code = gem, year)
    if (s$rule == "sum") {
      agg <- agg |> summarise(v = sum(.data[[col]], na.rm = TRUE), .groups = "drop")
    } else {
      wcol <- s$weight_col
      if (is.na(wcol) || !(wcol %in% names(wide_buurt))) {
        # Fall back to equal weighting if weight column missing.
        agg <- agg |> summarise(v = mean(.data[[col]], na.rm = TRUE), .groups = "drop")
      } else {
        agg <- agg |> summarise(
          v = .wmean(.data[[col]], .data[[wcol]]),
          .groups = "drop"
        )
      }
    }
    names(agg)[3] <- col
    result <- result |> left_join(agg, by = c("area_code", "year"))
  }
  result
}

# ── Build, write, manifest ────────────────────────────────────────────────────

# Write one scale's accumulated long table to parquet via the shared DuckDB
# COPY helper (zstd), sorted for stable diffs. Mirrors .write_vk100_parquet().
.write_kwb_parquet <- function(con, long_df, out_path) {
  duckdb::duckdb_register(con, "kwb_tmp", long_df)
  on.exit(duckdb::duckdb_unregister(con, "kwb_tmp"), add = TRUE)
  write_parquet_from_query(
    con,
    "SELECT * FROM kwb_tmp ORDER BY variable, year, area_code",
    out_path
  )
}

build_cbs_kwb <- function() {
  cat("\n=== CBS KWB long-format build (buurt + gemeente) ===\n")

  buurt_path <- "static/data/parquet/cbs-kwb-buurt.parquet"
  gem_path   <- "static/data/parquet/cbs-kwb-gem.parquet"

  con <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))

  # Process years in ascending order; rewrite parquets after each year so a
  # crash leaves complete-up-to-year-N output rather than nothing.
  buurt_accum <- list()
  gem_accum   <- list()

  for (yr in names(.KWB_YEARS)) {  # ascending: list iterates in insertion order
    year_int <- as.integer(yr)
    cat(sprintf("\nYear %s...\n", yr))
    kwb_raw <- .get_kwb(yr)

    wide_buurt <- .kwb_wide_for_year(kwb_raw, year_int)
    cat(sprintf("  %d buurten\n", nrow(wide_buurt)))

    long_buurt <- .wide_to_long(wide_buurt)
    buurt_accum[[yr]] <- long_buurt

    wide_gem  <- .aggregate_to_gem(wide_buurt)
    long_gem  <- .wide_to_long(wide_gem)
    gem_accum[[yr]] <- long_gem
    cat(sprintf("  %d gemeenten\n", nrow(wide_gem)))

    # Rewrite parquets incrementally.
    .write_kwb_parquet(con, bind_rows(buurt_accum), buurt_path)
    .write_kwb_parquet(con, bind_rows(gem_accum),   gem_path)
  }

  cat("\nDone.\n")
  cat(sprintf("  buurt: %d rows, %d KB\n",
              nrow(bind_rows(buurt_accum)), file.size(buurt_path) %/% 1024L))
  cat(sprintf("  gem:   %d rows, %d KB\n",
              nrow(bind_rows(gem_accum)),   file.size(gem_path)   %/% 1024L))

  .kwb_manifest_entry()
}

# Manifest entry for the app — one dataset, both scales. Shape matches the other
# nodes (cbs-vk100); build.R collects this into static/data/manifest.json.
.kwb_manifest_entry <- function() {
  spec <- .kwb_spec()
  list(
    name = "CBS Kerncijfers Wijken en Buurten",
    description = paste(
      "CBS Kerncijfers Wijken en Buurten, meerjarig (2015-2025), op buurt- en",
      "gemeenteniveau. Gemeentecijfers zijn opgeteld uit de buurten (aantallen)",
      "of inwoner-/woninggewogen gemiddeld (percentages, afstanden).",
      "Bedrijfsvestigingen vanaf 2022, arbeids- en zorgvariabelen vanaf 2023."
    ),
    scales = list(
      buurt = "parquet/cbs-kwb-buurt.parquet",
      gem   = "parquet/cbs-kwb-gem.parquet"
    ),
    fields = list(
      year = list(
        type = "single", label = "Jaar",
        values = lapply(as.integer(names(.KWB_YEARS)),
                        function(y) list(id = y, label = as.character(y))),
        default = 2024L
      ),
      variable = list(
        type = "single", label = "Variabele",
        values = lapply(seq_len(nrow(spec)), function(i) {
          list(id = spec$id[i], label = spec$label[i], group = spec$group[i])
        }),
        default = 1L
      )
    ),
    countCol = "count",
    weightCol = "weight",
    yearAggregation = "sum",
    defaultClassification = list(method = "jenks", n = 5L, palette = "YlOrRd")
  )
}

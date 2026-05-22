# OViN/ODiN: CBS Onderzoek Verplaatsingen in Nederland (2004-2017, OViN) +
# Onderweg in Nederland (2018-2024, ODiN), OD edges.
# Source: raw-data/edges-ovin-2024.sqlite, table ovin20042024 (~3.0M trip rows).
# Output:
#   static/data/parquet/ovin-edges-gem.parquet  (uses c_vgemf / c_agemf)
#   static/data/parquet/ovin-edges-pc4.parquet  (uses c_vpcf  / c_apcf)
source("R/lib/parquet.R")

build_ovin <- function() {
  con <- duckdb_with_sqlite("raw-data/edges-ovin-2024.sqlite")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))

  write_parquet_from_query(con, "
    SELECT
      'GM' || printf('%04d', CAST(c_vgemf AS INTEGER)) AS o_code,
      'GM' || printf('%04d', CAST(c_agemf AS INTEGER)) AS d_code,
      CAST(year AS INTEGER) AS year,
      CAST(c_motief  AS INTEGER) AS motief,
      CAST(c_modus   AS INTEGER) AS modus,
      CAST(c_opl     AS INTEGER) AS opl,
      CAST(c_hhtype  AS INTEGER) AS hhtype,
      CAST(c_maatsch AS INTEGER) AS maatsch,
      COUNT(*)::BIGINT     AS count,
      SUM(factorv)::DOUBLE AS weight
    FROM src.ovin20042024
    WHERE c_vgemf IS NOT NULL AND c_agemf IS NOT NULL
    GROUP BY o_code, d_code, year, motief, modus, opl, hhtype, maatsch
    ORDER BY year, o_code, d_code
  ", "static/data/parquet/ovin-edges-gem.parquet")

  write_parquet_from_query(con, "
    SELECT
      printf('%04d', CAST(c_vpcf AS INTEGER)) AS o_code,
      printf('%04d', CAST(c_apcf AS INTEGER)) AS d_code,
      CAST(year AS INTEGER) AS year,
      CAST(c_motief  AS INTEGER) AS motief,
      CAST(c_modus   AS INTEGER) AS modus,
      CAST(c_opl     AS INTEGER) AS opl,
      CAST(c_hhtype  AS INTEGER) AS hhtype,
      CAST(c_maatsch AS INTEGER) AS maatsch,
      COUNT(*)::BIGINT     AS count,
      SUM(factorv)::DOUBLE AS weight
    FROM src.ovin20042024
    WHERE c_vpcf IS NOT NULL AND c_apcf IS NOT NULL
    GROUP BY o_code, d_code, year, motief, modus, opl, hhtype, maatsch
    ORDER BY year, o_code, d_code
  ", "static/data/parquet/ovin-edges-pc4.parquet")

  list(
    name        = "Verplaatsingen 2004-2024 (OViN/ODiN)",
    description = "Onderzoek Verplaatsingen in Nederland (OViN/ODiN) 2004-2024. Per-trip records aggregated to herkomst-bestemming gemeenten. Weergegeven waarde = som van factorv (gewogen ritten over de gekozen periode); count = aantal waarnemingen (ritten in de steekproef) waarop dat berust.",
    scales = list(
      gem = "parquet/ovin-edges-gem.parquet",
      pc4 = "parquet/ovin-edges-pc4.parquet"
    ),
    fields = list(
      year = list(
        # Range: user picks an inclusive [min, max] interval; per-trip counts
        # are aggregated across years and normalised per yearAggregation.
        type = "range", label = "Periode",
        min = 2004L, max = 2024L,
        defaultMin = 2018L, defaultMax = 2018L
      ),
      motief = list(
        type = "multi", label = "Motief",
        values = list(
          list(id = 1L, label = "Van en naar het werk"),
          list(id = 2L, label = "Zakelijk bezoek"),
          list(id = 3L, label = "Diensten/persoonlijke verzorging"),
          list(id = 4L, label = "Winkelen/boodschappen"),
          list(id = 5L, label = "Onderwijs volgen"),
          list(id = 6L, label = "Visite/logeren"),
          list(id = 7L, label = "Sociaal recreatief overig"),
          list(id = 8L, label = "Toeren/wandelen"),
          list(id = 9L, label = "Overig, incl. diensten/zorg")
        )
      ),
      modus = list(
        type = "multi", label = "Modus",
        values = list(
          list(id = 1L, label = "Auto (bestuurder of passagier)"),
          list(id = 2L, label = "Trein, bus/tram/metro"),
          list(id = 3L, label = "Lopen, fiets, bromfiets"),
          list(id = 4L, label = "Overig")
        )
      ),
      opl = list(
        type = "multi", label = "Opleiding",
        values = list(
          list(id = 0L, label = "Onbekend"),
          list(id = 1L, label = "BO/LO, LBO/VGLO/LAVO/MAVO/MULO"),
          list(id = 2L, label = "MBO/HAVO/Atheneum/Gymnasium/MMS/HBS"),
          list(id = 3L, label = "HBO/Universiteit"),
          list(id = 4L, label = "Overig")
        )
      ),
      hhtype = list(
        type = "multi", label = "Huishoudtype",
        values = list(
          list(id = 1L, label = "Eenpersoonshuishouden"),
          list(id = 2L, label = "Paar zonder kinderen"),
          list(id = 3L, label = "Paar + kind(eren)"),
          list(id = 4L, label = "1-oudergezin"),
          list(id = 5L, label = "Overig")
        )
      ),
      maatsch = list(
        type = "multi", label = "Maatschappelijke participatie",
        values = list(
          list(id = 1L, label = "Werkzaam 12-30 uur/week"),
          list(id = 2L, label = "Werkzaam >= 30 uur/week"),
          list(id = 3L, label = "Eigen huishouding"),
          list(id = 4L, label = "Scholier/student"),
          list(id = 5L, label = "Werkloos/WAO"),
          list(id = 6L, label = "Gepensioneerd/VUT"),
          list(id = 7L, label = "Overig"),
          list(id = 8L, label = "Nvt")
        )
      )
    ),
    countCol  = "count",
    weightCol = "weight",
    # OViN/ODiN is a weighted survey: `weight` (SUM(factorv)) is the weighted
    # trip estimate, `count` (COUNT(*)) is the raw number of survey trips it
    # rests on. `weighted` flags the frontend to expose that count.
    weighted  = TRUE,
    # The displayed value divides weight by years*365 for trips per avg. day.
    yearAggregation = "daily",
    defaultClassification = list(method = "quantile", n = 5L, palette = "YlOrRd")
  )
}

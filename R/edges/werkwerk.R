# Werk-Werk: CBS microdata 1999-2017, job → job moves OD edges.
# Source: raw-data/edges-werkwerk-2018.sqlite.
# Output: static/data/parquet/werkwerk-edges-{gem,pc4}.parquet
source("R/lib/parquet.R")

build_werkwerk <- function() {
  con <- duckdb_with_sqlite("raw-data/edges-werkwerk-2018.sqlite")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))

  period_case <- "CASE year
      WHEN '07-12' THEN 1
      WHEN '12-17' THEN 2
      WHEN '07-17' THEN 3
    END"

  write_parquet_from_query(con, sprintf("
    SELECT
      'GM' || printf('%%04d', CAST(GEMy1 AS INTEGER)) AS o_code,
      'GM' || printf('%%04d', CAST(GEMy2 AS INTEGER)) AS d_code,
      %s AS year,
      CAST(age      AS INTEGER) AS age,
      CAST(inks     AS INTEGER) AS inks,
      CAST(opl      AS INTEGER) AS opl,
      CAST(sectorsector AS INTEGER) AS sectorsector,
      CAST(soortbaan AS INTEGER) AS soortbaan,
      SUM(value)::DOUBLE AS count,
      SUM(value)::DOUBLE AS weight
    FROM src.werkwerk_19992018_gem
    WHERE GEMy1 IS NOT NULL AND GEMy2 IS NOT NULL AND year IN ('07-12','12-17','07-17')
    GROUP BY o_code, d_code, year, age, inks, opl, sectorsector, soortbaan
    ORDER BY year, o_code, d_code
  ", period_case), "static/data/parquet/werkwerk-edges-gem.parquet")

  write_parquet_from_query(con, sprintf("
    SELECT
      printf('%%04d', CAST(POSTCODEy1 AS INTEGER)) AS o_code,
      printf('%%04d', CAST(POSTCODEy2 AS INTEGER)) AS d_code,
      %s AS year,
      CAST(age      AS INTEGER) AS age,
      CAST(inks     AS INTEGER) AS inks,
      CAST(opl      AS INTEGER) AS opl,
      CAST(sectorsector AS INTEGER) AS sectorsector,
      CAST(soortbaan AS INTEGER) AS soortbaan,
      SUM(value)::DOUBLE AS count,
      SUM(value)::DOUBLE AS weight
    FROM src.werkwerk_19992018_pc
    WHERE POSTCODEy1 IS NOT NULL AND POSTCODEy2 IS NOT NULL AND year IN ('07-12','12-17','07-17')
    GROUP BY o_code, d_code, year, age, inks, opl, sectorsector, soortbaan
    ORDER BY year, o_code, d_code
  ", period_case), "static/data/parquet/werkwerk-edges-pc4.parquet")

  list(
    name        = "Baanverhuizingen 1999-2017",
    description = "CBS microdata, job → job moves between successive observed employments. Cells <10 suppressed for privacy.",
    scales = list(
      gem = "parquet/werkwerk-edges-gem.parquet",
      pc4 = "parquet/werkwerk-edges-pc4.parquet"
    ),
    fields = list(
      year = list(
        # `years` = calendar-year span of each period, used by the divideYears
        # toggle to normalise period totals to a per-year figure.
        type = "multi", label = "Periode",
        values = list(
          list(id = 1L, label = "2007-2012", years = 6L),
          list(id = 2L, label = "2012-2017", years = 6L),
          list(id = 3L, label = "2007-2017", years = 11L)
        )
      ),
      age = list(
        type = "multi", label = "Leeftijd",
        values = list(
          list(id = 1L, label = "< 18"),
          list(id = 2L, label = "18-23"),
          list(id = 3L, label = "24-29"),
          list(id = 4L, label = "30-40"),
          list(id = 5L, label = "40-59"),
          list(id = 6L, label = "60+")
        )
      ),
      opl = list(
        type = "multi", label = "Opleiding",
        values = list(
          list(id = 1L, label = "Laag"),
          list(id = 2L, label = "Midden"),
          list(id = 3L, label = "Hoog")
        )
      ),
      inks = list(
        type = "multi", label = "Brutoinkomen",
        values = list(
          list(id = 1L, label = "< 20%"),
          list(id = 2L, label = "20-40%"),
          list(id = 3L, label = "40-60%"),
          list(id = 4L, label = "60-80%"),
          list(id = 5L, label = "80-100%")
        )
      ),
      sectorsector = list(
        # Sectorverandering: sector-of-origin → sector-of-destination pair,
        # encoded "<from>-<to>" over the 1..9 sector classes (see woonwerk
        # `sectorcat` for the class labels). Ported 1:1 from the original
        # metadata (edges-werkwerk-2018.js).
        type = "multi", label = "Sectorverandering",
        values = list(
          list(id = 1L, label = "1-1"), list(id = 2L, label = "1-2"), list(id = 3L, label = "1-3"),
          list(id = 4L, label = "1-4"), list(id = 5L, label = "1-5"), list(id = 6L, label = "1-6"),
          list(id = 7L, label = "1-7"), list(id = 8L, label = "2-1"), list(id = 9L, label = "2-2"),
          list(id = 10L, label = "2-3"), list(id = 11L, label = "2-4"), list(id = 12L, label = "2-5"),
          list(id = 13L, label = "2-6"), list(id = 14L, label = "2-7"), list(id = 15L, label = "3-1"),
          list(id = 16L, label = "3-2"), list(id = 17L, label = "3-3"), list(id = 18L, label = "3-4"),
          list(id = 19L, label = "3-5"), list(id = 20L, label = "3-6"), list(id = 21L, label = "3-7"),
          list(id = 22L, label = "4-1"), list(id = 23L, label = "4-2"), list(id = 24L, label = "4-3"),
          list(id = 25L, label = "4-4"), list(id = 26L, label = "4-5"), list(id = 27L, label = "4-6"),
          list(id = 28L, label = "4-7"), list(id = 29L, label = "5-1"), list(id = 30L, label = "5-2"),
          list(id = 31L, label = "5-3"), list(id = 32L, label = "5-4"), list(id = 33L, label = "5-5"),
          list(id = 34L, label = "5-6"), list(id = 35L, label = "5-7"), list(id = 36L, label = "6-1"),
          list(id = 37L, label = "6-2"), list(id = 38L, label = "6-3"), list(id = 39L, label = "6-4"),
          list(id = 40L, label = "6-5"), list(id = 41L, label = "6-6"), list(id = 42L, label = "6-7"),
          list(id = 43L, label = "7-1"), list(id = 44L, label = "7-2"), list(id = 45L, label = "7-3"),
          list(id = 46L, label = "7-4"), list(id = 47L, label = "7-5"), list(id = 48L, label = "7-6"),
          list(id = 49L, label = "7-7"), list(id = 50L, label = "8-8"), list(id = 51L, label = "9-9")
        )
      ),
      soortbaan = list(
        type = "multi", label = "Soort baan",
        values = list(
          list(id = 1L, label = "DGA"),
          list(id = 2L, label = "Overig"),
          list(id = 3L, label = "Stagaire, WSW, Oproep, Uitzend")
        )
      ),
      divideYears = list(
        type = "toggle", label = "Data per jaar", default = FALSE
      )
    )
  )
}

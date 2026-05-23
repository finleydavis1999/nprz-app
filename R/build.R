# Orchestrator: `npm run data` → `Rscript R/build.R`
#
# Builds:
#   static/data/manifest.json
#   static/data/parquet/<dataset>-<scale>.parquet
#   static/data/geo/{pc4,gemeenten,buurt}.{geojson,topo.json}
#   static/data/geo/{provincies,builtup}.geojson
#
# Run from the project root.
if (!file.exists("R/build.R")) {
  stop("Run from the project root: Rscript R/build.R")
}

source("R/_manifest.R")
source("R/geo/pc4.R")
source("R/geo/gemeenten.R")
source("R/geo/buurt.R")
source("R/geo/overlays.R")
source("R/nodes/demographics.R")
source("R/nodes/banen-werk.R")
source("R/nodes/banen-woon.R")
source("R/edges/ovin.R")
source("R/edges/woonwerk.R")
source("R/edges/werkwerk.R")
source("R/edges/migration.R")
source("R/nodes/cbs-vk100.R")

cat("=== building geo ===\n")
geo <- list(
  pc4   = build_pc4(),
  gem   = build_gemeenten(),
  buurt = build_buurt()
)

cat("\n=== building cartographic overlays ===\n")
overlays <- list(
  provinces = build_provincies(),
  builtup   = build_builtup()
)

cat("\n=== building node datasets ===\n")
datasets <- list(
  demographics  = build_demographics(),
  `banen-werk`  = build_banen_werk(),
  `banen-woon`  = build_banen_woon(),
  `cbs-vk100`   = build_cbs_vk100()
)

cat("\n=== building flow datasets ===\n")
flows <- list(
  ovin = build_ovin(),
  woonwerk = build_woonwerk(),
  werkwerk = build_werkwerk(),
  migration = build_migration()
)

cat("\n=== writing manifest ===\n")
write_manifest(datasets, geo, flows, overlays = overlays)
cat("\nDone.\n")

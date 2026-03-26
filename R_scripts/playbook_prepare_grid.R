install.packages("sf", type = "binary")
library(sf)

basic_square_data <- st_read("C:\\Users\\finle\\Downloads\\cbs_vk100_2024_v1.gpkg")

#translating a row
library(deeplr)
my_key <- "59f55501-d037-4a25-abd6-0ec2b2e5609a:fx"
translated_names <- sapply(names(data), function(term) {
  Sys.sleep(0.5)  # wait 0.5 seconds between each call
  term_spaced <- gsub("_", " ", term)
  res <- POST("https://api-free.deepl.com/v2/translate",
              body = list(text = term, target_lang = "EN", source_lang = "NL", auth_key = my_key),
              encode = "form")
  content(res)$translations[[1]]$text
})
english_basic_square_data <- basic_square_data
names(english_basic_square_data) <- translated_names

#saving and reuploading key sets to then compare them

st_write(english_basic_square_data, "C:/Users/finle/Downloads/cbs_vk100_2024_english.gpkg")
onekm_square_data <- st_read("C:/Users/finle/Downloads/cbs_vk100_2024_english.gpkg")
untranslated_onekm_square_data <- st_read("C:\\Users\\finle\\Downloads\\cbs_vk100_2024_v1.gpkg")
fivekm_square_data <- st_read("C:\\Users\\finle\\Downloads\\cbs_vk500_2024_v1.gpkg")

setdiff(names(untranslated_onekm_square_data), names(fivekm_square_data))
setdiff(names(fivekm_square_data), names(untranslated_onekm_square_data))


#loading, editing, and sending to VScode the first dataset
install.packages(c("arrow", "dplyr"))  
install.packages("duckdb", repos = "https://duckdb.r-universe.dev")
install.packages("arrow", repos = "https://apache.r-universe.dev")

library(sf)
library(duckdb)
library(arrow)
library(dplyr)
library(arrow)

zip_path    <- "C:/Users/finle/Downloads/2025-cbs_vk100_2024_v1 (2).zip"
output_dir  <- "C:/NPRZ_project/first_data"

#creating folder to output to (first_data), unzipping file, importing & inspecting it
dir.create(output_dir, recursive = TRUE, showWarnings = FALSE) ##this actually did nothing because file in path under "output_dir" does already exist, would create it at this location otherwise
unzip(zip_path, exdir = file.path(output_dir, "cbs_raw"))


grid <- st_read("C:\\Users\\finle\\Downloads\\cbs_vk100_2024_v1.gpkg") #need to make sure this can pull out of the zip file

cat("\n--- Basic info ---\n")
cat("Rows (grid squares):", nrow(grid), "\n")
cat("Columns:", ncol(grid), "\n")
cat("CRS:", st_crs(grid)$input, "\n")
cat("\nColumn names:\n")
print(names(grid))

cat("\nFirst few rows:\n")
print(head(grid, 3))

#filtering empty columns
suppressed_cols <- names(grid)[
  sapply(st_drop_geometry(grid), function(col) {
    all(col == -99995, na.rm = TRUE)
  })
]

cat("Dropping", length(suppressed_cols), "fully suppressed columns:\n")
print(suppressed_cols)

grid_filtered <- grid %>%
  dplyr::select(-all_of(suppressed_cols))

#to inspect map
plot(st_geometry(grid_filtered),
     main = "CBS 100m grid - full Netherlands",
     col  = "steelblue",
     border = NA)

#saving in two different formats
duckdb_path <- file.path(output_dir, "basic_grid.duckdb") 

con <- dbConnect(duckdb(), duckdb_path) #establishes duckDB connection to my duckDB database (duck files in my project folder)
dbExecute(con, "INSTALL spatial; LOAD spatial;")

grid_for_db <- grid_filtered %>%
  dplyr::mutate(geom_wkt = st_as_text(geom)) %>%
  st_drop_geometry() #here we replace the gpkg geometry column in place of a format (as text, not type st type understood by sf) understood by duckDB as well 

dbWriteTable(con, "cbs_grid", grid_for_db, overwrite = TRUE) #applies created duck connection to selected/created table to create duckDB table, through using con which links this new duckDB connected/queried table to the duck db "filing cabinet" of "basic_data" saved within the project folder 

cat("\nRows in DuckDB:", dbGetQuery(con, "SELECT COUNT(*) FROM cbs_grid")[[1]], "\n") #this is actually running a simple fetch rows SQL query to my duck database(the file in my project folder with the earlier defined "duckdp_path" path), we can see its working. "cat" and [[1]] just means it prints one row and doesnt create an object, dbgetquery means it executes the SQL format query but retrieves using the duckDB engine (and way of receiving the data), which is in standard SQL format following con,(with con representing the path to our duck file)
dbDisconnect(con) #closes the connection to the file, while it is open the file is not accessible so make sure to close, opens when con is ran because we saved this function as a duckdb connection function
cat("DuckDB saved to:", duckdb_path, "\n") #prints file destination, this is stil basic_grid, we have now attached the cbs_grid table inside that duck file, can have multiple tables inside this DuckDB file



geoparquet_path <- file.path(output_dir, "basic_grid.parquet") ##this and below same as last, making a .parquet file path to save multiple tables potentially under this name, then mutating geomtery column to be understandble format for parquet file, then making sure its saved in right place

grid_parquet <- grid_filtered %>%
  dplyr::mutate(geometry_wkt = st_as_text(geom)) %>%
  st_drop_geometry() %>%
  as.data.frame()

write_parquet(grid_parquet, geoparquet_path)
cat("GeoParquet saved to:", geoparquet_path, "\n")


cat("\n=== Done ===\n")
cat("Grid cells:       ", nrow(grid_filtered), "\n")
cat("Variables kept:   ", ncol(grid_filtered) - 1, "(excl. geometry)\n")
cat("Variables dropped:", length(suppressed_cols), "(all -99995)\n")
cat("DuckDB:           ", duckdb_path, "\n")
cat("GeoParquet:       ", geoparquet_path, "\n")
cat("CRS:               EPSG:28992 (RD New)\n")



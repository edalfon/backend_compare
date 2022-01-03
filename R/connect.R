
connect_duckdb <- function(read_only = FALSE) {
  
  print("CONNECTING DuckDB ...")
  
  fs::dir_create(fs::path_dir("/backend/duckdb/backend.duckdb"))
  DBI::dbConnect(
    drv = duckdb::duckdb(), 
    dbdir = "/backend/duckdb/backend.duckdb",
    read_only = read_only
  )
}

connect_pg <- function() {
  
  DBI::dbConnect(
    odbc::odbc(),
    driver = "PostgreSQL Unicode(x64)",
    database = "backend",
    uid = "postgres",
    pwd = "postgres",
    host = NULL, #,"localhost",
    port = 5432,
    encoding = "WINDOWS-1252",
    bigint = "numeric"
  )
}

connect_arrow <- function(arrow_tbl) {
  
  arrow::open_dataset(
    sources = arrow_tbl,
    format = "arrow"
  )
}

connect_parquet <- function(parquet_tbl) {
  
  arrow::open_dataset(
    sources = parquet_tbl,
    format = "parquet"
  )
}


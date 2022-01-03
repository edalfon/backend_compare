
plan_enums <- function() { tar_plan(

  D_enums = "",
  
  tar_map(
    list(src_file = c(
      "D:/tiny_data.txt",
      "D:/small_data.txt",
      "D:/mid_data.txt",
      #"D:/year_data.txt",
      NULL
    )),
    
    tar_target(ingest_enum_tar, {
      D_enums
      ingest_enum(src_file)
    })
  )
    
)}



ingest_enum <- function(src_file = "/backend/tiny_data.txt") {
  
  tictoc::tic("Clean slate")
  duckdb_dir <- "/backend/duckdbtmp"
  fs::dir_create(duckdb_dir)
  fs::dir_delete(duckdb_dir)
  fs::dir_create(duckdb_dir)
  tictoc::toc()
  
  tictoc::tic("Connecting clean slate")
  duckdb_con <- DBI::dbConnect(
    duckdb::duckdb(), 
    paste0(duckdb_dir, "/backend.duckdb")
  )
  tictoc::toc()
  
  # 16244.74 secs ingesting auto, on inspiron 15 from 
  tictoc::tic("Ingesting auto")
  rows_auto <- DBI::dbExecute(duckdb_con, glue::glue_sql(.con = duckdb_con, "
    CREATE TABLE test_data AS
    SELECT *
    FROM read_csv_auto({src_file})
    ;
  "))
  ingest_time_auto <- tictoc::toc()

    
  tictoc::tic("Describing auto")
  desc_auto <- DBI::dbGetQuery(duckdb_con, "  
    DESCRIBE test_data;
  ")
  tictoc::toc()
  
  tictoc::tic("Database size auto")
  size_auto <- DBI::dbGetQuery(duckdb_con, "  
    PRAGMA database_size;
  ")
  tictoc::toc()
  
  

  create_enum_sql <- function(col) {
    
    tictoc::tic(paste0("Querying enum values for column: ", col))
    col_vls <- DBI::dbGetQuery(duckdb_con, glue::glue_sql(.con = duckdb_con, "
      SELECT DISTINCT({`col`}) AS vls FROM test_data;
    ")) |> 
      dplyr::pull(vls)
    tictoc::toc()
    
    glue::glue_sql(.con = duckdb_con, "
      CREATE TYPE {`col`} AS ENUM ({col_vls*});
    ")
  }

  
  tictoc::tic("Querying all enum values")
  text_cols <- c("sex", "firm", "id_type", "cat1", "cat2", "product", 
                 "prod_type", "prod_dept", "provider")
  enums_sql <- purrr::map(text_cols, ~create_enum_sql(.x))
  tictoc::toc()
  
  
  tictoc::tic("Disconnecting")
  DBI::dbDisconnect(duckdb_con, shutdown = TRUE)
  rm(duckdb_con)
  gc()
  tictoc::toc()
  
  tictoc::tic("Removing DuckDB")
  fs::dir_delete(duckdb_dir)
  fs::dir_create(duckdb_dir)
  tictoc::toc()
  
  
  tictoc::tic("Connecting again")
  duckdb_con <- DBI::dbConnect(
    duckdb::duckdb(), 
    paste0(duckdb_dir, "/backend.duckdb")
  )
  tictoc::toc()
  
  
  tictoc::tic("Creating enums")
  purrr::map(enums_sql, ~DBI::dbExecute(duckdb_con, .x))
  tictoc::toc()

    
  tictoc::tic("Ingesting enum")
  rows_enum <- DBI::dbExecute(duckdb_con, glue::glue_sql(.con = duckdb_con, "  
    CREATE TABLE test_data AS
    SELECT 
    firm::firm AS firm, 
    reg_type, 
    seq, 
    id_type::id_type AS id_type, 
    id_num AS id_num, 
    bdate, 
    sex::sex AS sex, 
    code, 
    cat1::cat1 AS cat1, 
    cat2::cat2 AS cat2, 
    sdate, 
    product::product AS product, 
    prod_type::prod_type AS prod_type, 
    prod_dept::prod_dept AS prod_dept, 
    prod_variants, 
    value, 
    value_add, 
    provider::provider AS provider, 
    age, 
    age_group, 
    age_group2, 
    seq2, 
    value2, 
    value3, 
    value_tot
    FROM read_csv_auto({src_file})
    ;
  "))
  ingest_time_enum <- tictoc::toc()
  
  
  tictoc::tic("Describing again")
  desc_enum <- DBI::dbGetQuery(duckdb_con, "  
    DESCRIBE test_data;
  ")
  tictoc::toc()
  
  
  tictoc::tic("Database size again")
  size_enum <- DBI::dbGetQuery(duckdb_con, "  
    PRAGMA database_size;
  ")
  tictoc::toc()
  
  tictoc::tic("Disconnecting ... again ...")
  DBI::dbDisconnect(duckdb_con, shutdown = TRUE)
  tictoc::toc()
  
  list(desc_auto, size_auto, rows_auto, ingest_time_auto,
       desc_enum, size_enum, rows_enum, ingest_time_enum)
}


plan_enums <- function() { tar_plan(

  D_enums = "",
  
  tar_map(
    list(src_file = c(
      "D:/tiny_data.txt",
      "D:/small_data.txt",
      "D:/mid_data.txt",
      "D:/year_data.txt",
      NULL
    )),
    
    tar_target(ingest_clean_slate_tar, {
      D_enums
      ingest_clean_slate(src_file)
    }),
    
    tar_target(query_all_enum_values_tar, {
      ingest_clean_slate_tar
      query_all_enum_values(src_file)
    }),
    
    tar_target(create_enums_tar, {
      create_enums(src_file, query_all_enum_values_tar)
    }),
    
    tar_target(ingest_enum_tar, {
      create_enums_tar
      ingest_enum(src_file)
    })
    
    
  )
    
)}



ingest_clean_slate <- function(src_file = "/backend/tiny_data.txt") {
  
  tictoc::tic("Clean slate")
  duckdb_dir <- paste0(
    "/backend/duckdbtmp_",
    fs::path_file(fs::path_ext_remove(src_file))
  )
  fs::dir_create(duckdb_dir)
  fs::dir_delete(duckdb_dir)
  fs::dir_create(duckdb_dir)
  tictoc::toc()
  
  tictoc::tic("Connecting clean slate")
  duckdb_tmp <- DBI::dbConnect(
    duckdb::duckdb(), 
    paste0(duckdb_dir, "/backend.duckdb")
  )
  tictoc::toc()
  on.exit({
    tictoc::tic("Disconnecting clean slate")
    DBI::dbDisconnect(duckdb_tmp, shutdown = TRUE)
    tictoc::toc()
  })
  
  
  # 16244.74 secs ingesting auto, on inspiron 15 from 
  # 16208.97 
  # 17749.86 
  # 16540.9 
  tictoc::tic("Ingesting auto")
  rows_auto <- ingest_duckdb_cp(src_file, duckdb_tmp, "test_data")
  ingest_time_auto <- tictoc::toc()
    
  tictoc::tic("Describing auto")
  desc_auto <- DBI::dbGetQuery(duckdb_tmp, "DESCRIBE test_data;")
  tictoc::toc()
  
  tictoc::tic("Database size auto")
  size_auto <- DBI::dbGetQuery(duckdb_tmp, "PRAGMA database_size;")
  tictoc::toc()
 
  list(size_auto, desc_auto, ingest_time_auto, rows_auto) 
}


query_col_distinct_values <- function(col, duckdb_tmp) {
  
  # just dealing with
  # Error: IO Error: IO Error: Could not read all bytes from file 
  # "/backend/duckdbtmp/backend.duckdb": wanted=262144 read=0
  # weird, but it seems this can help
  DBI::dbGetQuery(duckdb_tmp, glue::glue_sql(.con = duckdb_tmp, "
    SELECT DISTINCT({`col`}) AS vls
    FROM (SELECT * FROM test_data LIMIT 100) AS test_data;
  "))
  
  tictoc::tic(paste0("Querying enum values for column: ", col))
  col_vls <- DBI::dbGetQuery(duckdb_tmp, glue::glue_sql(.con = duckdb_tmp, "
      SELECT DISTINCT({`col`}) AS vls 
      FROM (SELECT * FROM test_data LIMIT ALL) AS test_data;
    ")) |> 
    dplyr::pull(vls)
  tictoc::toc()
  
  col_vls
}

query_all_enum_values <- function(src_file) {
 
  duckdb_dir <- paste0(
    "/backend/duckdbtmp_",
    fs::path_file(fs::path_ext_remove(src_file))
  )

  tictoc::tic("Connecting auto ingested")
  duckdb_tmp <- DBI::dbConnect(
    duckdb::duckdb(), 
    paste0(duckdb_dir, "/backend.duckdb")
  )
  tictoc::toc()
  on.exit({
    tictoc::tic("Disconnecting auto ingested")
    DBI::dbDisconnect(duckdb_tmp, shutdown = TRUE)
    tictoc::toc()
  })
  
  tictoc::tic("Querying all enum values")
  text_cols <- c("sex", "firm", "id_type", "cat1", "cat2", "product", 
                 "prod_type", "prod_dept", "provider")
  enums_vls <- purrr::map(
    .x = text_cols |> rlang::set_names(), 
    .f = ~query_col_distinct_values(.x, duckdb_tmp)
  )
  tictoc::toc()
  
  enums_vls
}




create_enums <- function(src_file = "/backend/tiny_data.txt",
                         query_all_enum_values_tar) {
  
  tictoc::tic("Removing DuckDB tmp")
  duckdb_dir <- paste0(
    "/backend/duckdbtmp_",
    fs::path_file(fs::path_ext_remove(src_file))
  )
  fs::dir_delete(duckdb_dir)
  fs::dir_create(duckdb_dir)
  tictoc::toc()
   
  tictoc::tic("Connecting enum")
  duckdb_tmp <- DBI::dbConnect(
    duckdb::duckdb(),
    paste0(duckdb_dir, "/backend.duckdb")
  )
  tictoc::toc()
  on.exit({
    tictoc::tic("Disconnecting enum")
    DBI::dbDisconnect(duckdb_tmp, shutdown = TRUE)
    tictoc::toc()
  })

  query_all_enum_values_tar$cat2 <- NULL
  enums_sql <- query_all_enum_values_tar |> 
    purrr::imap(~glue::glue_sql(.con = duckdb_tmp, "
      CREATE TYPE {`.y`} AS ENUM ({.x*});
    "))
  
  tictoc::tic("Creating enums")
  purrr::map(enums_sql, ~DBI::dbExecute(duckdb_tmp, .x))
  tictoc::toc()
  
  enums_sql
}


ingest_enum <- function(src_file = "/backend/tiny_data.txt") {

  duckdb_dir <- paste0(
    "/backend/duckdbtmp_",
    fs::path_file(fs::path_ext_remove(src_file))
  )

  tictoc::tic("Connecting enum")
  duckdb_tmp <- DBI::dbConnect(
    duckdb::duckdb(),
    paste0(duckdb_dir, "/backend.duckdb")
  )
  tictoc::toc()
  on.exit({
    tictoc::tic("Disconnecting enum")
    DBI::dbDisconnect(duckdb_tmp, shutdown = TRUE)
    tictoc::toc()
  })
  
  # 13279.12 
  tictoc::tic("Ingesting enum")
  rows_enum <- DBI::dbExecute(duckdb_tmp, glue::glue(.open = "[", .close = "]", "
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
    cat2 AS cat2, 
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
    FROM read_csv(
      '[src_file]', 
      delim = ';',
      header = True,
      columns = {
          'firm': 'TEXT',
          'reg_type': 'INTEGER',
          'seq': 'INTEGER',
          'id_type': 'TEXT',
          'id_num': 'TEXT',
          'bdate': 'TIMESTAMP',
          'sex': 'TEXT',
          'code': 'INTEGER',
          'cat1': 'TEXT',
          'cat2': 'TEXT',
          'sdate': 'TIMESTAMP',
          'product': 'TEXT',
          'prod_type': 'TEXT',
          'prod_dept': 'TEXT',
          'prod_variants': 'INTEGER',
          'value': 'INTEGER',
          'value_add': 'INTEGER',
          'provider': 'TEXT',
          'age': 'INTEGER',
          'age_group': 'INTEGER',
          'age_group2': 'INTEGER',
          'seq2': 'INTEGER',
          'value2': 'TEXT',
          'value3': 'DOUBLE',
          'value_tot': 'DOUBLE'
      }
    )
    ;
  "))
  ingest_time_enum <- tictoc::toc()
  
  
  tictoc::tic("Describing again")
  desc_enum <- DBI::dbGetQuery(duckdb_tmp, "DESCRIBE test_data;")
  tictoc::toc()
  
  
  tictoc::tic("Database size again")
  size_enum <- DBI::dbGetQuery(duckdb_tmp, "PRAGMA database_size;")
  tictoc::toc()
  
  list(desc_enum, size_enum, rows_enum, ingest_time_enum)
}








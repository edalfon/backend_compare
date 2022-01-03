plan_ingest <- function() { tarchetypes::tar_plan(
 
  A_ingest = "",
  A_ingest_duck = "",
  
  # want to test first using just small sample of the data, and then scale up 
  # so let's throw in here some static branching to run several targets
  # for different src_file
  tarchetypes::tar_map(
    values = list(src_file = c(
      tiny = "/backend/tiny_data.txt",
      small = "/backend/small_data.txt",
      mid = "/backend/mid_data.txt"
    )),
    
    # benchmark ingesting data into arrow, duckdb and postgres
    tar_target(bench_ingest_tar, cue = tar_cue("never"), {
      A_ingest # just to group them together in tar_visnetwork 
      bench_ingest(src_file, duckdb_con, pg_con)
    }),
    
    # having found: 
    # 1. duckdb getting slow ingesting larguish data
    # 2. duckdb also slowing down in querying columns stored as timestamp
    # I wonder if timestamp columns might be the underlying cause of 1. above
    # let's benchmark ingesting data, but reading the timestamp columns as text
    tar_target(bench_ingest_duckdb_date_tar, cue = tar_cue("never"), {
      A_ingest_duck
      bench_ingest_duckdb_date(src_file)
    })
  ),
  
  # well, given the times above, it does not seem sensible to benchmark 
  # year data ingestion, so let's ingest that only once
  A_ingest_all = "",
  tar_target(duckdb_all, cue = tar_cue("never"), {
    A_ingest_all
    ingest_duckdb_limit(
      src_file = "/backend/year_data.csv.gz",
      duckdb_con = duckdb_con,
      duckdb_tbl = "year_data",
      limit = 485530623
    )
  }),
  
  tar_target(pg_all, cue = tar_cue("never"), {
    A_ingest_all
    ingest_pg_fg_limit(
      src_file = "/backend/year_data.txt",
      pg_con = pg_con,
      pg_tbl = "year_data",
      limit = 485530624
    )
  }),
  
  tar_target(parquet_all, cue = tar_cue("never"), {
    A_ingest_all
    ingest_parquet(src_file = "/backend/year_data.csv.gz",
                   parquet_path = "/backend/parquet_year_data")
  }),
  
  tar_target(arrow_all, cue = tar_cue("never"), {
    A_ingest_all
    ingest_arrow(src_file = "/backend/year_data.csv.gz",
                 arrow_path = "/backend/arrow_year_data")
  }),
  
  # duck 031 was already available, and include changes that might be relevant
  # so I'll just try with this new version ingesting the same data 
  # (only once though) to see if the new version makes a difference
  A_ingest_duck031 = "",
  tarchetypes::tar_map(
    values = list(src_file = c(
      tiny = "/backend/tiny_data.txt",
      small = "/backend/small_data.txt",
      mid = "/backend/mid_data.txt",
      year = "/backend/year_data.csv.gz"
    )),
    
    # having found: 
    # 1. duckdb getting slow ingesting larguish data
    # 2. duckdb also slowing down in querying columns stored as timestamp
    # I wonder if timestamp columns might be the underlying cause of 1. above
    # let's benchmark ingesting data, but reading the timestamp columns as text
    tar_target(ingest_duck031, {
      A_ingest_duck031
      ingest_duckdb(src_file)
    })
  ),
  
  
  

)}

bench_ingest_duckdb_date <- function (src_test) {
  
  drop_query <- "DROP TABLE IF EXISTS tmp_table;"
  
  create_query_text <- glue::glue(.open = "[", .close = "]", "
    CREATE TABLE tmp_table AS
    SELECT * 
    FROM read_csv(
      '[src_test]', 
      delim = ';',
      header = True,
      columns = {
          'firm': 'TEXT',
          'reg_type': 'INTEGER',
          'seq': 'INTEGER',
          'id_type': 'TEXT',
          'id_num': 'TEXT',
          'bdate': 'TEXT',
          'sex': 'TEXT',
          'code': 'INTEGER',
          'cat1': 'TEXT',
          'cat2': 'TEXT',
          'sdate': 'TEXT',
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
  ")
  
  create_query_time <- glue::glue(.open = "[", .close = "]", "
    CREATE TABLE tmp_table AS
    SELECT * 
    FROM read_csv(
      '[src_test]', 
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
  ")

  create_query_auto <- glue::glue(.open = "[", .close = "]", "
    CREATE TABLE tmp_table AS
    SELECT * 
    FROM read_csv_auto('[src_test]')
    ;
  ")
  
  pg_auto <- glue::glue_sql(.con = pg_con, "
    COPY tmp_table 
    FROM {src_test} (FORMAT 'csv', DELIMITER ';', HEADER )
    ;
  ")
  
  # in this case using microbenchmark, for the `setup` convenience
  # to make sure the time dropping the table does not create too much noise
  mbm <- microbenchmark::microbenchmark(
    duckdb_text = DBI::dbExecute(duckdb_con, create_query_text),
    duckdb_timestamp = DBI::dbExecute(duckdb_con, create_query_time),
    duckdb_auto = DBI::dbExecute(duckdb_con, create_query_auto),
    pg_auto = {
      create_test_table(pg_con, "tmp_table")
      DBI::dbExecute(pg_con, pg_auto)
    },
    times = 10,
    setup = {
      DBI::dbExecute(duckdb_con, drop_query)
      DBI::dbExecute(pg_con, drop_query)
    }
  )
  
  mbm
}

ingest_arrow <- function(src_file, arrow_path) {
  
  csv_ds <- arrow::open_dataset(
    sources = src_file,
    format = "text",
    delimiter = ";"
  )
  
  arrow::write_dataset(
    dataset = csv_ds,
    path = arrow_path,
    format = "arrow"
  )
  
  arrow_path
}

ingest_parquet <- function (src_file, parquet_path) {
  
  csv_ds <- arrow::open_dataset(
    sources = src_file,
    format = "text",
    delimiter = ";"
  )
  
  arrow::write_dataset(
    dataset = csv_ds,
    path = parquet_path,
    format = "parquet"
  )
  
  parquet_path
}

ingest_duckdb_cp <- function (src_file, duckdb_con, duckdb_tbl) {
  
  drop_test_table(duckdb_con, duckdb_tbl)
  create_test_table(duckdb_con, duckdb_tbl)
  
  rows_affected <- DBI::dbExecute(duckdb_con, glue::glue_sql(.con = duckdb_con, "
    COPY {`duckdb_tbl`} 
    FROM {src_file} ( DELIMITER ';', HEADER )
    ;
  "))
  rows_affected
}

ingest_duckdb_cp2 <- function (src_file, duckdb_con, duclkdb_tbl) {
  
  drop_test_table(duckdb_con, duclkdb_tbl)
  create_test_table(duckdb_con, duclkdb_tbl)
  
  rows_affected <- DBI::dbExecute(duckdb_con, glue::glue_sql(.con = duckdb_con, "
    COPY {`duclkdb_tbl`} 
    FROM {src_file} ( AUTO_DETECT TRUE )
    ;
  "))
  duclkdb_tbl
}

ingest_duckdb <- function (src_file, duckdb_con, duckdb_tbl) {
  
  drop_test_table(duckdb_con, duckdb_tbl)
  
  rows_affected <- DBI::dbExecute(duckdb_con, glue::glue_sql(.con = duckdb_con, "
    CREATE TABLE {`duckdb_tbl`} AS
    SELECT * 
    FROM read_csv_auto({src_file})
    ;
  "))
  duckdb_tbl
}

ingest_duckdb_limit <- function (src_file, duckdb_con, duckdb_tbl, limit) {
  
  drop_test_table(duckdb_con, duckdb_tbl)

  rows_affected <- DBI::dbExecute(duckdb_con, glue::glue_sql(.con = duckdb_con, "
    CREATE TABLE {`duckdb_tbl`} AS
    SELECT * 
    FROM read_csv_auto({src_file})
    LIMIT {limit}
    ;
  "))
  duckdb_tbl
}

ingest_pg_fg <- function (src_file, pg_con, pg_tbl) {
  
  foreign_tbl <- paste0(pg_tbl, "_fg")
  
  create_fg_tbl_sql <- efun::pg_create_foreign_table(
    con = pg_con,
    file_path = src_file,
    table = foreign_tbl,
    sep = ";",
    drop_table = TRUE,
    execute = TRUE
  )
  
  rows_affected <- DBI::dbExecute(pg_con, glue::glue_sql(.con = pg_con, "
    DROP TABLE IF EXISTS {`pg_tbl`};
    CREATE TABLE {`pg_tbl`} AS
    SELECT * 
    FROM {`foreign_tbl`}
    ;
  "))
  paste0(pg_tbl)
}

ingest_pg_fg_limit <- function (src_file, pg_con, pg_tbl, limit) {
  
  foreign_tbl <- paste0(pg_tbl, "_fg")
  
  create_fg_tbl_sql <- efun::pg_create_foreign_table(
    con = pg_con,
    file_path = src_file,
    table = foreign_tbl,
    sep = ";",
    drop_table = TRUE,
    execute = TRUE
  )
  
  rows_affected <- DBI::dbExecute(pg_con, glue::glue_sql(.con = pg_con, "
    DROP TABLE IF EXISTS {`pg_tbl`};
    CREATE TABLE {`pg_tbl`} AS
    SELECT * 
    FROM {`foreign_tbl`}
    LIMIT {limit}
    ;
  "))
  paste0(pg_tbl)
}

ingest_pg_cp <- function (src_file, pg_con, pg_tbl) {
  
  drop_test_table(pg_con, pg_tbl)
  create_test_table(pg_con, pg_tbl)
  
  rows_affected <- DBI::dbExecute(pg_con, glue::glue_sql(.con = pg_con, "
    COPY {`pg_tbl`} 
    FROM {src_file} (FORMAT 'csv', DELIMITER ';', HEADER )
    ;
  "))
  rows_affected
}

drop_test_table <- function (con, tbl_con) {
  
  rows_affected <- DBI::dbExecute(con, glue::glue_sql(.con = con, "
    DROP TABLE IF EXISTS {`tbl_con`};
  "))
  tbl_con
}

create_test_table <- function (con, tbl_con) {
  
  rows_affected <- DBI::dbExecute(con, glue::glue_sql(.con = con, '
    CREATE TABLE {`tbl_con`} (
      "firm" TEXT,
      "reg_type" INTEGER,
      "seq" INTEGER,
      "id_type" TEXT,
      "id_num" TEXT,
      "bdate" TIMESTAMP,
      "sex" TEXT,
      "code" INTEGER,
      "cat1" TEXT,
      "cat2" TEXT,
      "sdate" TIMESTAMP,
      "product" TEXT,
      "prod_type" TEXT,
      "prod_dept" TEXT,
      "prod_variants" INTEGER,
      "value" INTEGER,
      "value_add" INTEGER,
      "provider" TEXT,
      "age" INTEGER,
      "age_group" INTEGER,
      "age_group2" INTEGER,
      "seq2" INTEGER,
      "value2" TEXT,
      "value3" DOUBLE PRECISION,
      "value_tot" DOUBLE PRECISION
    )
  '))
  rows_affected
}

bench_ingest <- function(src_file, duckdb_con, pg_con) {
  
  tbl_test <- fs::path_ext_remove(fs::path_file(src_test))
  arrow_tbl <- paste0(base_dir, "/arrow_", tbl_test)
  parquet_tbl <- paste0(base_dir, "/parquet_", tbl_test)
  
  bm <- bench::mark(
    arrow = ingest_arrow(src_file, arrow_tbl),
    parquet = ingest_parquet(src_file, parquet_tbl),
    duckdb = ingest_duckdb(src_file, duckdb_con, tbl_test),
    duckdb_copy = ingest_duckdb_cp(src_file, duckdb_con, tbl_test),
    duckdb_copy2 = ingest_duckdb_cp2(src_file, duckdb_con, tbl_test),
    pg_foreign = ingest_pg_fg(src_file, pg_con, tbl_test),
    pg_copy = ingest_pg_cp(src_file, pg_con, tbl_test),
    min_iterations = 10,
    check = FALSE,
    memory = FALSE
  )
  bm
}


plan_agg_count <- function() { tarchetypes::tar_plan(
  
  B_agg_count = "",
  B_agg_count_pgduck = "",
  
  tarchetypes::tar_map(
    values = list(src_file = c(
      tiny = "/backend/tiny_data.txt",
      small = "/backend/small_data.txt",
      mid = "/backend/mid_data.txt"
    )),
    
    tar_map(
      values = list(colname = c("seq", "product", "sex", "code", "reg_type", 
                                "id_num", "sdate")),
      tar_target(
        agg_count,
        cue = tar_cue("never"),
        command = {
          B_agg_count
          bench_agg_count(
            arrow_con, parquet_con, duckdb_con, pg_con, tbl_test, colname
          )
        }
      )
      
    )

  ),
  
  
  

  tar_map(
    values = list(colname = c("seq", "product", "sex", "code", "reg_type", 
                              "id_num", "sdate")),
    tar_target(
      agg_count_pgduck,
      command = {
        B_agg_count_pgduck
        bench_agg_count_pgduck(duckdb_con, pg_con, "year_data", colname)
      },
      cue = tar_cue("never")
    )
  ),
 

  
  
)}



agg_count <- function(tblref, colname, collect = FALSE, ...) {
  
  if (isTRUE(collect)) {
    tblout <- tblref |> 
      dplyr::count(.data[[colname]]) |> 
      dplyr::collect()
    
  } else {
    tblout <- tblref |> 
      dplyr::count(.data[[colname]]) |> 
      dplyr::compute()
  }
  
  tblout
}

agg_count_sql <- function(con, tbl_test, colname, collect = FALSE) {
  
  if (isTRUE(collect)) {
    tblout <- DBI::dbGetQuery(con, glue::glue_sql(.con = con, "
      SELECT COUNT(*) AS n, {`colname`}
      FROM {`tbl_test`}
      GROUP BY {`colname`}
      ;
    "))
  } else {
    rows_affected <- DBI::dbExecute(con, glue::glue_sql(.con = con, "
      DROP TABLE IF EXISTS temptbl;
    "))
    
    tblout <- DBI::dbExecute(con, glue::glue_sql(.con = con, "
      CREATE TABLE temptbl AS
      SELECT COUNT(*) AS n, {`colname`}
      FROM {`tbl_test`}
      GROUP BY {`colname`}
      ;
    "))
  }
  
  tblout
}

agg_count_sql_parquet <- function(parquet_tbl, duckdb_con, colname, collect = FALSE) {
  
  if (isTRUE(collect)) {
    tblout <- DBI::dbGetQuery(duckdb_con, glue::glue_sql(.con = duckdb_con, "
      SELECT COUNT(*) AS n, {`colname`}
      FROM parquet_scan({parquet_tbl})
      GROUP BY {`colname`}
      ;
    "))
  } else {
    rows_affected <- DBI::dbExecute(duckdb_con, glue::glue_sql(.con = duckdb_con, "
      DROP TABLE IF EXISTS temptbl;
    "))
    
    tblout <- DBI::dbExecute(duckdb_con, glue::glue_sql(.con = duckdb_con, "
      CREATE TABLE temptbl AS
      SELECT COUNT(*) AS n, {`colname`}
      FROM parquet_scan({parquet_tbl})
      GROUP BY {`colname`}
      ;
    "))
  }
  
  tblout
}


bench_agg_count <- function(arrow_con, parquet_con, duckdb_con, pg_con, 
                            tbl_test, colname) {

  bm <- bench::press(
    collect_data = c(TRUE, FALSE), {
    bench::mark(
      arrow_dplyr = agg_count(arrow_con, colname, collect_data),
      parquet_dplyr = agg_count(parquet_con, colname, collect_data),
      duckdb_dplyr = agg_count(tbl(duckdb_con, tbl_test), colname, collect_data),
      pg_dplyr = agg_count(tbl(pg_con, tbl_test), colname, collect_data),
      duckdb_sql = agg_count_sql(duckdb_con, tbl_test, colname, collect_data),
      pg_sql = agg_count_sql(pg_con, tbl_test, colname, collect_data),
      parquet_duckdb_sql = agg_count_sql_parquet(parquet_con$files, duckdb_con, 
                                                 colname, collect_data),
      min_iterations = 10,
      check = FALSE,
      memory = FALSE
    )}
  )

  bm
}



bench_agg_count_pgduck <- function(duckdb_con, pg_con, tbl_test, colname) {
  
  bm <- bench::press(
    collect_data = c(TRUE, FALSE), {
      bench::mark(
        duckdb_dplyr = agg_count(tbl(duckdb_con, tbl_test), colname, collect_data),
        pg_dplyr = agg_count(tbl(pg_con, tbl_test), colname, collect_data),
        duckdb_sql = agg_count_sql(duckdb_con, tbl_test, colname, collect_data),
        pg_sql = agg_count_sql(pg_con, tbl_test, colname, collect_data),
        min_iterations = 10,
        check = FALSE,
        memory = FALSE
      )}
  )
  
  bm
}



bench_count_distinct <- function(table_name = "tiny_data", 
                                 cols = c("firm", "product")) {
  
  bench::mark(
    pg = count_distinct_sql(pg_con, table_name, cols),
    duckdb = count_distinct_sql(duckdb_con, table_name, cols),
    check = FALSE,
    min_iterations = 10
  ) 
}

count_distinct_sql <- function(con, 
                               table_name = "tiny_data", 
                               cols = c("firm", "product")) {
  
  sql_query <- glue::glue_sql(.con = con, "
    SELECT COUNT(DISTINCT({`cols`*}))
    FROM {`table_name`}
    ;
  ")
  DBI::dbGetQuery(con, sql_query)
}

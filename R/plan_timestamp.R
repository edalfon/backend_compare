

plan_timestamp <- function() { tar_plan(
  
  data_prefix = c("tiny", "small", "mid", "year"),
  tar_target(
    name = cast_table, 
    command = {
      # create_table_cast(duckdb_con, data_prefix)
    },
    pattern = map(data_prefix)
  ),
  
  
  count_distinct_group = cast_table,
  agg_count_group = cast_table,
  count_distinct_vars = cast_table,
  tar_plan(
    bm_cdtc = {
      count_distinct_group
      bench_count_distinct_timestamp_cast(duckdb_con, prefix_i)
    },
    bm_actc = {
      agg_count_group
      bench_agg_count_timestamp_cast(duckdb_con, prefix_i)
    },
    bm_cdbs = {
      count_distinct_vars
      bench_count_distinct_bsdate(duckdb_con, prefix_i)
    }
  ) |> 
    tar_map(values = list(prefix_i = c("tiny", "small", "mid", "year"))),

  
)}


create_table_cast <- function(duckdb_con, data_prefix) {
  
  src_table <- paste0(data_prefix, "_data")
  new_table <- paste0(data_prefix, "_timestamp_cast")
  
  DBI::dbExecute(duckdb_con, glue::glue_sql(.con = duckdb_con, "
    DROP TABLE IF EXISTS {`new_table`};
  "))
  
  DBI::dbExecute(duckdb_con, glue::glue_sql(.con = duckdb_con, "
    CREATE TABLE {`new_table`} AS
    SELECT 
      bdate,
      sdate,
      bdate::DATE AS bdate_date,
      sdate::DATE AS sdate_date,
      bdate::TEXT AS bdate_txt, 
      sdate::TEXT AS sdate_txt
    FROM {`src_table`}
    ;
  "))
}

bench_count_distinct_timestamp_cast <- function(duckdb_con, data_prefix) {
  
  table_name <- paste0(data_prefix, "_timestamp_cast")
  
  bench::mark(
    # bdate = count_distinct_sql(duckdb_con, table_name, "bdate"),
    # sdate = count_distinct_sql(duckdb_con, table_name, "sdate"),
    bdate_date = count_distinct_sql(duckdb_con, table_name, "bdate_date"),
    sdate_date = count_distinct_sql(duckdb_con, table_name, "sdate_date"),
    bdate_txt = count_distinct_sql(duckdb_con, table_name, "bdate_txt"),
    sdate_txt = count_distinct_sql(duckdb_con, table_name, "sdate_txt"),
    check = FALSE,
    min_iterations = 10
  ) 
}

bench_count_distinct_bsdate <- function(duckdb_con, data_prefix) {
  
  table_name <- paste0(data_prefix, "_timestamp_cast")
  
  bench::mark(
    bdate = count_distinct_sql(duckdb_con, table_name, "bdate"),
    sdate = count_distinct_sql(duckdb_con, table_name, "sdate"),
    check = FALSE,
    min_iterations = 10
  ) 
}


bench_agg_count_timestamp_cast <- function(duckdb_con, data_prefix) {
  
  table_name <- paste0(data_prefix, "_timestamp_cast")
  
  bench::mark(
    # bdate = agg_count_sql(duckdb_con, table_name, "bdate"),
    # sdate = agg_count_sql(duckdb_con, table_name, "sdate"),
    bdate_date = agg_count_sql(duckdb_con, table_name, "bdate_date", TRUE),
    sdate_date = agg_count_sql(duckdb_con, table_name, "sdate_date", TRUE),
    bdate_txt = agg_count_sql(duckdb_con, table_name, "bdate_txt", TRUE),
    sdate_txt = agg_count_sql(duckdb_con, table_name, "sdate_txt", TRUE),
    check = FALSE,
    min_iterations = 10
  ) 
}

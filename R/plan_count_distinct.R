
plan_count_distinct <- function() { tar_plan(

  C_count_distinct_pgduck = "",
  
  tar_map(
    values = expand_grid(
      table_name = c("tiny_data", "small_data", "mid_data", "year_data"),
      cols = c("seq", "product", "sex", "code", "reg_type", "id_num", "sdate")
    ),
    tar_target(
      count_distinct_pgduck,
      command = {
        C_count_distinct_pgduck
        bench_count_distinct(table_name, cols)
      },
      cue = tar_cue("never")
    )
  ),
  
)}

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


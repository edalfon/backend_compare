
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

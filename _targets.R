library(conflicted)
conflicted::conflict_prefer("filter", "dplyr", quiet = TRUE)
conflicted::conflict_prefer("select", "dplyr", quiet = TRUE)
conflicted::conflict_prefer("expand", "tidyr", quiet = TRUE)


base_dir <- "/backend"

tar_plan(

  tar_target(siu, utils::sessionInfo()),


  plan_ingest(),
  plan_agg_count(),
  
  plan_count_distinct(),
  
  plan_timestamp(),
  
  plan_enums(),
  

) |>
  tar_hook_before(
    hook = {
      duckdb_con <- connect_duckdb()
      pg_con <- connect_pg()
      on.exit({
        print("DISCONNECTING DuckDB ...")
        DBI::dbDisconnect(duckdb_con, shutdown = TRUE)
        DBI::dbDisconnect(pg_con)
      })
    }
  ) |> 
  tar_hook_inner(
    hook = {
      arrow_con <- rlang::eval_tidy(.x)
      arrow_con
    },
    names_wrap = starts_with("arrow_con")
  ) |>
  tar_hook_inner(
    hook = {
      parquet_con <- rlang::eval_tidy(.x)
      parquet_con
    },
    names_wrap = starts_with("parquet_con")
  ) |>
  identity()

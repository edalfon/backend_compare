if (file.exists("~/.Rprofile")) source("~/.Rprofile")

if(!requireNamespace("pacman", quietly = TRUE)) {
  utils::install.packages("pacman")
}

pacman::p_load(
  magrittr,
  dplyr,
  tidyr,
  ggplot2,
  
  duckdb,
  arrow,
  DBI,
  odbc,

  targets,
  tarchetypes
)

library(conflicted)
conflicted::conflict_prefer("filter", "dplyr", quiet = TRUE)
conflicted::conflict_prefer("select", "dplyr", quiet = TRUE)
conflicted::conflict_prefer("expand", "tidyr", quiet = TRUE)

# make all fns available to {targets} (and interactively, via load_all)
if (rlang::is_interactive()) {
  devtools::load_all()
  # delayedAssign("duckdb_con", connect_duckdb(), assign.env = globalenv())
  # delayedAssign("pg_con", connect_pg(), assign.env = globalenv())
} else {
  invisible(
    lapply(list.files("./R", full.names = TRUE), source, encoding = "UTF-8")
  )
}

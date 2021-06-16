
library(dplyr)
library(dbplyr)

meudb_create()

meudb_get_rates <- function(name) {
  tbl(MeuDB$db_conn, "rates") |>
    filter(name == name) |>
    arrange(refdate) |>
    collect() |>
    mutate(refdate = as.Date(refdate))
}

meudb_get_indexes <- function(name) {
  tbl(MeuDB$db_conn, "indexes") |>
    filter(name == name) |>
    arrange(refdate) |>
    collect() |>
    mutate(refdate = as.Date(refdate))
}

library(ggplot2)

meudb_get_rates("CDI") |>
  ggplot(aes(x = refdate, y = value)) +
  geom_line()

meudb_get_indexes("IDI") |>
  ggplot(aes(x = refdate, y = value)) +
  geom_line()

dbDisconnect(MeuDB$db_conn)

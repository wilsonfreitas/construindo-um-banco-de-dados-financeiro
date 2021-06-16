
library(DBI)
library(stringr)

sql <- readLines("database.sql")

stmt <- str_c(sql, collapse = "\n") |>
  str_trim() |>
  str_split(";")

conn <- dbConnect(RSQLite::SQLite(), "meudb/meu.db")

lapply(stmt[[1]], function(q) {
  if (q != "")
    dbExecute(conn, q)
})

dbGetQuery(conn, "select * from sqlite_master")

dbDisconnect(conn)

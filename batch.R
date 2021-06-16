
meudb.options$set("meudb_path" = "meudb")
meudb_create()

cfg <- load_config_by_name("l1-b3-cdi")

fnames <- list.files(config_get_local_path(cfg), full.names = TRUE)
for (fname in fnames) {
  bname <- basename(fname)
  refdate <- tools::file_path_sans_ext(bname) |> ymd()
  x <- l1_b3_cdi_parser(cfg, refdate)
  try(l1_b3_cdi_insert_handler(x, cfg, refdate))
}

cfg2 <- load_config_by_name("l2-b3-idi")
for (fname in fnames) {
  cat(fname, "\n")
  bname <- basename(fname)
  refdate <- tools::file_path_sans_ext(bname) |> ymd()
  x <- l1_b3_cdi_parser(cfg, refdate)
  try(l2_b3_idi_insert_handler(x, cfg2, refdate))
}


dbDisconnect(MeuDB$db_conn)

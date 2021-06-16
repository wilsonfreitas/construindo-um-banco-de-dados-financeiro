
library(DBI)
library(glue)
library(httr)
library(yaml)
library(logger)
library(stringr)
library(lubridate)
library(jsonlite)
library(rbcb)
library(bizdays)
library(dplyr)
library(purrr)
library(xml2)
library(XML)

l1_b3_cdi_parser <- function(cfg, refdate) {
  rem <- check_local(cfg, refdate)
  txt <- readLines(rem$local_path, warn = FALSE)
  json <- fromJSON(txt)
  json$taxa <- json$taxa |> str_replace(",", ".") |> as.numeric()
  json$dataTaxa <- json$dataTaxa |> dmy()
  json$indice <- json$indice |>
    str_replace("\\.", "") |>
    str_replace(",", ".") |>
    as.numeric()
  json$dataIndice <- json$dataIndice |> dmy()
  json
}

l1_b3_cdi_insert_handler <- function(x, cfg, refdate) {
  sql <- glue_sql("insert into rates (refdate, name, value, source) values (?, ?, ?, ?)")
  dbExecute(MeuDB$db_conn, sql,
            params = list(format(x$dataTaxa, "%Y-%m-%d"), "CDI", x$taxa, "B3"))
}

l2_b3_idi_insert_handler <- function(x, cfg, refdate) {
  sql <- glue_sql("insert into indexes (refdate, name, value, source) values (?, ?, ?, ?)")
  dbExecute(MeuDB$db_conn, sql,
            params = list(format(x$dataIndice, "%Y-%m-%d"), "IDI", x$indice, "B3"))
}

handle_data <- function(cfg, refdate) {
  log_info("processing handle - {cfg$name}")
  # parse
  if (exists("requires", cfg)) {
    name <- cfg$requires
    cfg_req <- load_config_by_name(name)
    funcname <- name |> str_replace_all("-", "_") |> str_c("_parser")
    if (exists(funcname)) {
      func <- get(funcname)
      x <- try(func(cfg_req, refdate), silent = TRUE)
      if (is(x, "try-error")) {
        msg <- as.character(x) |> str_trim()
        log_info("processing handle - {cfg$name} - parse error <{msg}>")
        r <- structure(
          list(config = cfg, status = "handle-error", message = msg),
          class = "processed_data"
        )
        return(r)
      }
    } else {
      r <- structure(
        list(config = cfg, status = "handle-no-parser-has-require"),
        class = "processed_data"
      )
      log_info("processing handle - {cfg$name} - require has no parser")
      return(r)
    }
  } else {
    name <- cfg$name
    funcname <- name |> str_replace_all("-", "_") |> str_c("_parser")
    if (exists(funcname)) {
      func <- get(funcname)
      x <- try(func(cfg, refdate), silent = TRUE)
      if (is(x, "try-error")) {
        msg <- as.character(x) |> str_trim()
        log_info("processing handle - {cfg$name} - parse error <{msg}>")
        r <- structure(
          list(config = cfg, status = "handle-error", message = msg),
          class = "processed_data"
        )
        return(r)
      }
    } else {
      r <- structure(
        list(config = cfg, status = "handle-no-parser"),
        class = "processed_data"
      )
      log_info("processing handle - {cfg$name} - no parser")
      return(r)
    }
  }

  # insert
  funcname <- cfg$name |> str_replace_all("-", "_") |> str_c("_insert_handler")
  if (exists(funcname)) {
    func <- get(funcname)
    res <- try(func(x, cfg, refdate), silent = TRUE)
    if (is(res, "try-error")) {
      msg <- as.character(res) |> str_trim()
      log_info("processing handle - {cfg$name} - insert error <{msg}>")
      structure(
        list(config = cfg, status = "handle-error", message = msg),
        class = "processed_data"
      )
    } else {
      log_info("processing handle - {cfg$name} - ok")
      structure(
        list(config = cfg, status = "handle-ok"),
        class = "processed_data"
      )
    }
  } else {
    log_info("processing handle - {cfg$name} - no insert handler")
    structure(
      list(config = cfg, status = "handle-no-insert-handler"),
      class = "processed_data"
    )
  }
}

handle_download <- function(cfg, refdate) {
  # donwload
  funcname <- cfg$name |> str_replace_all("-", "_") |> str_c("_download")
  if (exists(funcname)) {
    func <- get(funcname)
    res <- try(func(cfg, refdate), silent = TRUE)
    res
  } else NULL
}

config_get_local_path <- function(cfg) {
  file.path(MeuDB$root_path, cfg$layer, cfg$output_dir)
}

load_config_by_name <- function(name) {
  fname <- file.path("config", str_c(name, ".yaml"))
  load_config(fname)
}

load_config <- function(fname) {
  bname <- basename(fname)
  name <- tools::file_path_sans_ext(bname)
  lx <- strsplit(bname, "-")[[1]][1]
  config <- yaml.load_file(fname)
  layer <- if (lx == "l1")
    "layer1"
  else if (lx == "l2")
    "layer2"
  else
    stop("Undefined LAYER: ", fname)
  config$layer <- layer
  config$name <- name
  structure(config, class = "config")
}

encoding <- function(cfg, ...) UseMethod("encoding", cfg)

encoding.config <- function(cfg, ...) {
  if (exists("encoding", cfg))
    cfg[["encoding"]]
  else
    "utf8"
}

type <- function(cfg, ...) UseMethod("type", cfg)

type.config <- function(cfg, ...) {
  if (exists("type", cfg))
    cfg[["type"]]
  else
    "NULL"
}

check_local <- function(cfg, refdate) {
  folder <- file.path(MeuDB$root_path, cfg$layer, cfg$output_dir)
  if (!dir.exists(folder))
    dir.create(folder, recursive = TRUE)
  rname <- glue("{format(refdate)}.{cfg$ext}")
  path <- glue("{folder}/{rname}")
  exists_ <- file.exists(path)
  list(local_file = rname,
       local_folder = folder,
       local_path = path,
       exists = exists_)
}

simple_download <- function(cfg) {
  simple_downloader(cfg$url, cfg$name, encoding(cfg))
}

format_date_url_download <- function(cfg, refdate) {
  if (exists("timedelta", cfg)) {
    date <- bizdays::offset(refdate, cfg$timedelta)
    log_info("Download date {date} with timedelta {cfg$timedelta}")
  } else {
    date <- refdate
    log_info("Download date {date}")
  }
  url <- strftime(date, cfg$url)
  log_info("Formated URL {url}")
  simple_downloader(url, cfg$name, encoding(cfg))
}

simple_downloader <- function(url, name, encoding) {
  res <- GET(url)
  if (status_code(res) != 200) {
    log_error("config {name} download error {status_code(res)}")
    return(NULL)
  }
  if (headers(res)[["content-type"]] == "application/octet-stream") {
    bin <- content(res, as = "raw")
    t <- tempfile()
    writeBin(bin, t)
  } else {
    text <- content(res, as = "text", encoding = encoding)
    t <- tempfile()
    writeChar(text, t)
  }
  t
}

download_data <- function(cfg, refdate) {
  log_info("processing download - {cfg$name}")
  if (exists("requires", cfg)) {
    r <- structure(
      list(config = cfg, status = "download-skip-has-required-task"),
      class = "processed_data"
    )
    log_info("processing download - {cfg$name} - download skipped has required task")
    return(r)
  }

  rem <- check_local(cfg, refdate)
  if (! meudb.options$get("meudb_layer1_overwrite") & rem$exists) {
    r <- structure(
      list(config = cfg, status = "download-skip-do-not-overwrite"),
      class = "processed_data"
    )
    log_info("processing download - {cfg$name} - download skipped do not overwrite")
    return(r)
  }

  # download
  tmp_file <- switch(
    type(cfg),
    datetime = format_date_url_download(cfg, refdate = refdate),
    simple = simple_download(cfg),
    handle_download(cfg, refdate))

  if (is.null(tmp_file)) {
    log_error("processing download - {cfg$name} - download fail")
    r <- structure(
      list(config = cfg, status = "download-fail"),
      class = "processed_data"
    )
    return(r)
  }

  log_info("processing download - {cfg$name} - temp file downloaded {tmp_file}")

  dv_file <- file.copy(tmp_file, rem$local_path)
  log_info("processing download - {cfg$name} - file saved {rem$local_path}")

  structure(
    list(config = cfg, local_file = rem$local_path, status = "ok"),
    class = "processed_data"
  )
}

# merge elements of y into x with the same names
merge_list = function(x, y) {
  x[names(y)] = y
  x
}

# new_defaults - creates a settings object
new_defaults <- function(value=list()) {
  defaults <- value

  get <- function(name, default=FALSE, drop=TRUE) {
    if (default)
      defaults <- value  # this is only a local version
    if (missing(name))
      defaults
    else {
      if (drop && length(name) == 1) {
        if (!exists(name, defaults))
          stop("Name (", name, ") not found in options.")
        defaults[[name]]
      } else
        defaults[name]
    }
  }
  set <- function(...) {
    dots <- list(...)
    if (length(dots) == 0) return()
    if (is.null(names(dots)) && length(dots) == 1 && is.list(dots[[1]]))
      if (length(dots <- dots[[1]]) == 0) return()
    defaults <<- merge(dots)
    invisible(NULL)
  }
  merge <- function(values) merge_list(defaults, values)
  restore <- function(target = value) defaults <<- target

  list(get=get, set=set, merge=merge, restore=restore)
}

meudb.options <- new_defaults()

meudb_create <- function() {
  rd_path <- meudb.options$get("meudb_path")

  if (! dir.exists(rd_path)) {
    dir.create(rd_path, recursive = TRUE)
    log_info("MeuDB root directory created at {rd_path}")
  } else {
    log_info("MeuDB directory {rd_path}")
  }

  layer1_path <- file.path(rd_path, "layer1")
  if (! dir.exists(layer1_path)) {
    dir.create(layer1_path, recursive = TRUE)
    log_info("MeuDB layer1 directory created at {layer1_path}")
  } else {
    log_info("MeuDB layer1 directory {layer1_path}")
  }

  layer2_path <- file.path(rd_path, "layer2")
  if (! dir.exists(layer2_path)) {
    dir.create(layer2_path, recursive = TRUE)
    log_info("MeuDB layer2 directory created at {layer2_path}")
  } else {
    log_info("MeuDB layer2 directory {layer2_path}")
  }

  conn <- dbConnect(RSQLite::SQLite(), file.path(rd_path, "meu.db"))

  (MeuDB <<- structure(list(
    root_path = rd_path,
    layer1_path = layer1_path,
    layer2_path = layer2_path,
    db_conn = conn
  ), class = "meudb"))
}

print.meudb <- function(x, ...) {
  cat("\n")
  cat("MeuDB - My private database\n\n")
  cat(glue(" - Root directory: {x$root_path}"), "\n")
  cat(glue(" - Layer1 directory: {x$layer1_path}"), "\n")
  cat(glue(" - Layer2 directory: {x$layer2_path}"), "\n")
  cat("\n")
  invisible(x)
}

# main ----

log_threshold(DEBUG)

meudb.options$set("meudb_path" = "meudb")
meudb.options$set("meudb_layer1_overwrite" = FALSE)
meudb_create()


refdate <- as.Date("2021-06-15")
log_info("session date {format(refdate)}")

config_dir <- file.path("config")
log_info("config dir: {config_dir}")

config_files <- list.files(config_dir, pattern = "*.yaml", full.names = TRUE)
configs <- map(config_files, load_config)

filter_ <- c()

if (length(filter_) > 0) {
  sel_confs <- keep(configs, function(x) x$name %in% filter_)
  download_statuses <- map(sel_confs, download_data, refdate = refdate)
  handle_statuses <- map(sel_confs, handle_data, refdate = refdate)
} else {
  download_statuses <- map(configs, download_data, refdate = refdate)
  handle_statuses <- map(configs, handle_data, refdate = refdate)
}

dbDisconnect(MeuDB$db_conn)

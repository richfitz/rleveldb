##' @useDynLib rleveldb, .registration = TRUE
leveldb_connect <- function(name,
                            create_if_missing = NULL,
                            error_if_exists = NULL,
                            paranoid_checks = NULL,
                            write_buffer_size = NULL,
                            max_open_files = NULL,
                            block_size = NULL,
                            use_compression = NULL,
                            cache_capacity = NULL,
                            bloom_filter_bits_per_key = NULL) {
  ptr <- .Call(Crleveldb_connect, name, create_if_missing, error_if_exists,
               paranoid_checks, write_buffer_size, max_open_files,
               block_size, use_compression,
               cache_capacity, bloom_filter_bits_per_key)
  attr(ptr, "options") <- list(name = name,
                               create_if_missing = create_if_missing,
                               error_if_exists = error_if_exists,
                               paranoid_checks = paranoid_checks,
                               write_buffer_size = write_buffer_size,
                               max_open_files = max_open_files,
                               block_size = block_size,
                               use_compression = use_compression,
                               cache_capacity = cache_capacity,
                               bloom_filter_bits_per_key =
                                 bloom_filter_bits_per_key)
  class(ptr) <- c("leveldb_connection", "leveldb_options")
  ptr
}

leveldb_close <- function(db, error_if_closed = FALSE) {
  .Call(Crleveldb_close, db, error_if_closed)
}

leveldb_destroy <- function(name) {
  .Call(Crleveldb_destroy, name)
}

leveldb_repair <- function(name) {
  .Call(Crleveldb_repair, name)
}

leveldb_property <- function(db, name, error_if_missing = FALSE) {
  .Call(Crleveldb_property, db, name, error_if_missing)
}

leveldb_get <- function(db, key, as_raw = NULL, error_if_missing = FALSE,
                        readoptions = NULL) {
  .Call(Crleveldb_get, db, key, as_raw, error_if_missing, readoptions)
}

leveldb_put <- function(db, key, value, writeoptions = NULL) {
  .Call(Crleveldb_put, db, key, value, writeoptions)
}

leveldb_delete <- function(db, key, writeoptions = NULL) {
  .Call(Crleveldb_delete, db, key, writeoptions)
}

leveldb_iter_create <- function(db, readoptions = NULL) {
  .Call(Crleveldb_iter_create, db, readoptions)
}

leveldb_iter_destroy <- function(it, error_if_destroyed = FALSE) {
  .Call(Crleveldb_iter_destroy, it, error_if_destroyed)
}

leveldb_iter_valid <- function(it) {
  .Call(Crleveldb_iter_valid, it)
}

leveldb_iter_seek_to_first <- function(it) {
  .Call(Crleveldb_iter_seek_to_first, it)
}

leveldb_iter_seek_to_last <- function(it) {
  .Call(Crleveldb_iter_seek_to_last, it)
}

leveldb_iter_seek <- function(it, key) {
  .Call(Crleveldb_iter_seek, it, key)
}

leveldb_iter_next <- function(it, error_if_invalid = FALSE) {
  .Call(Crleveldb_iter_next, it, error_if_invalid)
}

leveldb_iter_prev <- function(it, error_if_invalid = FALSE) {
  .Call(Crleveldb_iter_prev, it, error_if_invalid)
}

leveldb_iter_key <- function(it, as_raw = NULL, error_if_invalid = FALSE) {
  .Call(Crleveldb_iter_key, it, as_raw, error_if_invalid)
}

leveldb_iter_value <- function(it, as_raw = NULL,
                               error_if_invalid = FALSE) {
  .Call(Crleveldb_iter_value, it, as_raw, error_if_invalid)
}

leveldb_snapshot <- function(db) {
  ptr <- .Call(Crleveldb_snapshot_create, db)
  attr(ptr, "timestamp") <- Sys.time()
  class(ptr) <- "leveldb_snapshot"
  ptr
}

leveldb_writebatch_create <- function() {
  .Call(Crleveldb_writebatch_create)
}

leveldb_writebatch_destroy <- function(writebatch, error_if_destroyed = FALSE) {
  .Call(Crleveldb_writebatch_destroy, writebatch, error_if_destroyed)
}

leveldb_writebatch_clear <- function(writebatch) {
  .Call(Crleveldb_writebatch_clear, writebatch)
}

leveldb_writebatch_put <- function(writebatch, key, value) {
  .Call(Crleveldb_writebatch_put, writebatch, key, value)
}

leveldb_writebatch_delete <- function(writebatch, key) {
  .Call(Crleveldb_writebatch_delete, writebatch, key)
}

leveldb_write <- function(db, writebatch, writeoptions = NULL) {
  .Call(Crleveldb_write, db, writebatch, writeoptions)
}

leveldb_approximate_sizes <- function(db, start, limit) {
  .Call(Crleveldb_approximate_sizes, db, start, limit)
}

leveldb_compact_range <- function(db, start, limit) {
  .Call(Crleveldb_compact_range, db, start, limit)
}

leveldb_readoptions <- function(verify_checksums = NULL, fill_cache = NULL,
                                snapshot = NULL) {
  ptr <- .Call(Crleveldb_readoptions, verify_checksums, fill_cache, snapshot)
  attr(ptr, "options") <- list(verify_checksums = verify_checksums,
                               fill_cache = fill_cache,
                               snapshot = snapshot)
  class(ptr) <- c("leveldb_readoptions", "leveldb_options")
  ptr
}

leveldb_writeoptions <- function(sync = NULL) {
  ptr <- .Call(Crleveldb_writeoptions, sync)
  class(ptr) <- c("leveldb_writeoptions", "leveldb_options")
  attr(ptr, "options") <- list(sync = sync)
  ptr
}

leveldb_keys_len <- function(db, starts_with = NULL, readoptions = NULL) {
  .Call(Crleveldb_keys_len, db, starts_with, readoptions)
}

leveldb_keys <- function(db, starts_with = NULL, as_raw = FALSE,
                         readoptions = NULL) {
  .Call(Crleveldb_keys, db, starts_with, as_raw, readoptions)
}

leveldb_exists <- function(db, key, readoptions = NULL) {
  .Call(Crleveldb_exists, db, key, readoptions)
}

leveldb_version <- function() {
  ret <- list(.Call(Crleveldb_version))
  class(ret) <- "numeric_version"
  ret
}

##' @export
as.character.leveldb_snapshot <- function(x, ...) {
  sprintf("<leveldb_snapshot> @ %s", attr(x, "timestamp"))
}

##" @export
print.leveldb_snapshot <- function(x, ...) {
  cat(as.character(x), "\n")
  invisible(x)
}

##' @export
names.leveldb_options <- function(x, ...) {
  names(attr(x, "options", exact = TRUE))
}

##' @export
`$.leveldb_options` <- function(x, i) {
  attr(x, "options")[[i]]
}

##' @export
`[[.leveldb_options` <- function(x, i, ...) {
  attr(x, "options")[[i]]
}

##' @export
`$<-.leveldb_options` <- function(x, i, value) {
  stop(sprintf("%s objects are immutable", class(x)[[1L]]))
}

##' @export
`[[<-.leveldb_options` <- function(x, i, value, ...) {
  stop(sprintf("%s objects are immutable", class(x)[[1L]]))
}

##' @export
as.character.leveldb_options <- function(x, ...) {
  f <- function(x) {
    if (is.null(x)) {
      "<not set>"
    } else {
      as.character(x)
    }
  }
  value <- vapply(names(x), function(i) f(x[[i]]), character(1))
  txt <- c(sprintf("<%s>", class(x)[[1]]),
           sprintf("  - %s: %s", names(x), value))
  paste(txt, collapse = "\n")
}

##' @export
print.leveldb_options <- function(x, ...) {
  cat(as.character(x), "\n")
  invisible(x)
}

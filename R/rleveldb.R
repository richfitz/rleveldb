##' @useDynLib rleveldb, .registration = TRUE
leveldb_connect <- function(name,
                            create_if_missing = NULL,
                            error_if_exists = NULL,
                            paranoid_checks = NULL,
                            write_buffer_size = NULL,
                            max_open_files = NULL,
                            cache_capacity = NULL,
                            block_size = NULL,
                            use_compression = NULL,
                            bloom_filter_bits_per_key = NULL) {
  ## assert_scalar_character(name)
  .Call(Crleveldb_connect, name, create_if_missing, error_if_exists,
        paranoid_checks, write_buffer_size, max_open_files,
        cache_capacity, block_size, use_compression, bloom_filter_bits_per_key)
}

leveldb_close <- function(db, error_if_closed = FALSE) {
  .Call(Crleveldb_close, db, error_if_closed)
}

leveldb_destroy <- function(name) {
  ## assert_scalar_character(name)
  .Call(Crleveldb_destroy, name)
}

leveldb_property <- function(db, name, error_if_missing = FALSE) {
  .Call(Crleveldb_property, db, name, error_if_missing)
}

leveldb_get <- function(db, key, force_raw = FALSE, error_if_missing = FALSE,
                        readoptions = NULL) {
  ## assert_scalar_character_or_raw(key)
  .Call(Crleveldb_get, db, key, force_raw, error_if_missing, readoptions)
}

leveldb_put <- function(db, key, value, writeoptions = NULL) {
  ## assert_scalar_character_or_raw(key)
  ## assert_scalar_character_or_raw(value)
  .Call(Crleveldb_put, db, key, value, writeoptions)
}

leveldb_delete <- function(db, key, writeoptions = NULL) {
  ## assert_scalar_character_or_raw(key)
  .Call(Crleveldb_delete, db, key, writeoptions)
}

leveldb_iter_create <- function(db, readoptions = NULL) {
  .Call(Crleveldb_iter_create, db, readoptions)
}

leveldb_iter_destroy <- function(it, error_if_closed = FALSE) {
  .Call(Crleveldb_iter_destroy, it, error_if_closed)
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

leveldb_iter_next <- function(it) {
  .Call(Crleveldb_iter_next, it)
}

leveldb_iter_prev <- function(it) {
  .Call(Crleveldb_iter_prev, it)
}

leveldb_iter_key <- function(it, force_raw = FALSE, error_if_invalid = FALSE) {
  .Call(Crleveldb_iter_key, it, force_raw, error_if_invalid)
}

leveldb_iter_value <- function(it, force_raw = FALSE,
                               error_if_invalid = FALSE) {
  .Call(Crleveldb_iter_value, it, force_raw, error_if_invalid)
}

leveldb_snapshot <- function(db) {
  .Call(Crleveldb_snapshot_create, db)
}

leveldb_snapshot_release <- function(snapshot) {
  .Call(Crleveldb_snapshot_release, snapshot)
}

leveldb_readoptions <- function(verify_checksums = FALSE, fill_cache = NULL,
                                snapshot = NULL) {
  ptr <- .Call(Crleveldb_readoptions, verify_checksums, fill_cache, snapshot)
  class(ptr) <- "leveldb_readoptions"
  ptr
}

leveldb_writeoptions <- function(sync = NULL) {
  ptr <- .Call(Crleveldb_writeoptions, sync)
  class(ptr) <- "leveldb_writeoptions"
  ptr
}

leveldb_keys_len <- function(db, readoptions = NULL) {
  .Call(Crleveldb_keys_len, db, readoptions)
}

leveldb_keys <- function(db, as_raw = FALSE, readoptions = NULL) {
  .Call(Crleveldb_keys, db, as_raw, readoptions)
}

leveldb_exists <- function(db, key, readoptions = NULL) {
  ## assert_scalar_character_or_raw(key)
  .Call(Crleveldb_exists, db, key, readoptions)
}

leveldb_version <- function() {
  ret <- list(.Call(Crleveldb_version))
  class(ret) <- "numeric_version"
  ret
}

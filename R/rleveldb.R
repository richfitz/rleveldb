##' @useDynLib rleveldb, .registration = TRUE
leveldb_connect <- function(name) {
  ## assert_scalar_character(name)
  .Call(Crleveldb_connect, name)
}

leveldb_close <- function(db, error_if_closed = FALSE) {
  .Call(Crleveldb_close, db, error_if_closed)
}

leveldb_destroy <- function(name) {
  ## assert_scalar_character(name)
  .Call(Crleveldb_destroy, name)
}

leveldb_get <- function(db, key, force_raw = FALSE, error_if_missing = FALSE) {
  ## assert_scalar_character_or_raw(key)
  .Call(Crleveldb_get, db, key, force_raw, error_if_missing)
}

leveldb_put <- function(db, key, value) {
  ## assert_scalar_character_or_raw(key)
  ## assert_scalar_character_or_raw(value)
  .Call(Crleveldb_put, db, key, value)
}

leveldb_delete <- function(db, key) {
  ## assert_scalar_character_or_raw(key)
  .Call(Crleveldb_delete, db, key)
}

leveldb_iter_create <- function(db) {
  .Call(Crleveldb_iter_create, db)
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

leveldb_keys_len <- function(db) {
  .Call(Crleveldb_keys_len, db)
}

leveldb_keys <- function(db, as_raw = FALSE) {
  .Call(Crleveldb_keys, db, as_raw)
}

leveldb_exists <- function(db, key) {
  ## assert_scalar_character_or_raw(key)
  .Call(Crleveldb_exists, db, key)
}

leveldb_version <- function() {
  ret <- list(.Call(Crleveldb_version))
  class(ret) <- "numeric_version"
  ret
}

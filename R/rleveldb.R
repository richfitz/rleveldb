##' @useDynLib rleveldb, .registration = TRUE
rleveldb_connect <- function(name) {
  ## assert_scalar_character(name)
  .Call(Crleveldb_connect, name)
}

rleveldb_get <- function(db, key, force_raw = FALSE, error_if_missing = FALSE) {
  ## assert_scalar_character_or_raw(key)
  .Call(Crleveldb_get, db, key, force_raw, error_if_missing)
}

rleveldb_put <- function(db, key, value) {
  ## assert_scalar_character_or_raw(key)
  ## assert_scalar_character_or_raw(value)
  .Call(Crleveldb_put, db, key, value)
}

rleveldb_delete <- function(db, key) {
  ## assert_scalar_character_or_raw(key)
  .Call(Crleveldb_delete, db, key)
}

rleveldb_keys_len <- function(db) {
  .Call(Crleveldb_keys_len, db)
}

rleveldb_keys <- function(db, as_raw = FALSE) {
  .Call(Crleveldb_keys, db, as_raw)
}

rleveldb_exists <- function(db, key) {
  ## assert_scalar_character_or_raw(key)
  .Call(Crleveldb_exists, db, key)
}

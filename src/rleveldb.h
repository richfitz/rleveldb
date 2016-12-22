#include <R.h>
#include <Rinternals.h>

SEXP rleveldb_connect(SEXP r_name);
SEXP rleveldb_close(SEXP r_db, SEXP r_error_if_closed);
SEXP rleveldb_destroy(SEXP r_name);

SEXP rleveldb_get(SEXP r_db, SEXP key, SEXP r_force_raw,
                  SEXP r_error_if_missing);
SEXP rleveldb_put(SEXP r_db, SEXP key, SEXP value);
SEXP rleveldb_delete(SEXP r_db, SEXP key);

SEXP rleveldb_iter_create(SEXP r_db);
SEXP rleveldb_iter_destroy(SEXP r_it, SEXP r_error_if_closed);
SEXP rleveldb_iter_valid(SEXP r_it);
SEXP rleveldb_iter_seek_to_first(SEXP r_it);
SEXP rleveldb_iter_seek_to_last(SEXP r_it);
SEXP rleveldb_iter_seek(SEXP r_it, SEXP r_key);
SEXP rleveldb_iter_next(SEXP r_it);
SEXP rleveldb_iter_prev(SEXP r_it);
SEXP rleveldb_iter_key(SEXP r_it, SEXP r_force_raw, SEXP r_error_if_invalid);
SEXP rleveldb_iter_value(SEXP r_it, SEXP r_force_raw, SEXP r_error_if_invalid);

SEXP rleveldb_keys(SEXP r_db, SEXP r_as_raw);
SEXP rleveldb_keys_len(SEXP r_db);
SEXP rleveldb_exists(SEXP r_db, SEXP key);
SEXP rleveldb_version();

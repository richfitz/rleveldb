#include <R.h>
#include <Rinternals.h>

SEXP rleveldb_connect(SEXP r_name);
SEXP rleveldb_get(SEXP extptr, SEXP key, SEXP r_force_raw,
                  SEXP r_error_if_missing);
SEXP rleveldb_put(SEXP extptr, SEXP key, SEXP value);
SEXP rleveldb_delete(SEXP extptr, SEXP key);

SEXP rleveldb_keys(SEXP extptr, SEXP r_as_raw);
SEXP rleveldb_keys_len(SEXP extptr);
SEXP rleveldb_exists(SEXP extptr, SEXP key);

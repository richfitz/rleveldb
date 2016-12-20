#include <R.h>
#include <Rinternals.h>

SEXP rleveldb_connect(SEXP r_name);
SEXP rleveldb_get(SEXP extptr, SEXP key, SEXP r_force_raw);
SEXP rleveldb_put(SEXP extptr, SEXP key, SEXP value);
SEXP rleveldb_delete(SEXP extptr, SEXP key);

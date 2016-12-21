#include "rleveldb.h"
#include <R_ext/Rdynload.h>

static const R_CallMethodDef call_methods[] = {
  {"Crleveldb_connect",  (DL_FUNC) &rleveldb_connect,  1},
  {"Crleveldb_close",    (DL_FUNC) &rleveldb_close,    2},
  {"Crleveldb_destroy",  (DL_FUNC) &rleveldb_destroy,  1},

  {"Crleveldb_get",      (DL_FUNC) &rleveldb_get,      4},
  {"Crleveldb_put",      (DL_FUNC) &rleveldb_put,      3},
  {"Crleveldb_delete",   (DL_FUNC) &rleveldb_delete,   2},

  {"Crleveldb_keys_len", (DL_FUNC) &rleveldb_keys_len, 1},
  {"Crleveldb_keys",     (DL_FUNC) &rleveldb_keys,     2},
  {"Crleveldb_exists",   (DL_FUNC) &rleveldb_exists,   2},

  {NULL,                 NULL,                         0}
};

void R_init_rleveldb(DllInfo *info) {
  R_registerRoutines(info, NULL, call_methods, NULL, NULL);
}

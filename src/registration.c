#include "rleveldb.h"
#include <R_ext/Rdynload.h>

static const R_CallMethodDef call_methods[] = {
  {"Crleveldb_connect",  (DL_FUNC) &rleveldb_connect,  1},
  {"Crleveldb_close",    (DL_FUNC) &rleveldb_close,    2},
  {"Crleveldb_destroy",  (DL_FUNC) &rleveldb_destroy,  1},

  {"Crleveldb_get",      (DL_FUNC) &rleveldb_get,      4},
  {"Crleveldb_put",      (DL_FUNC) &rleveldb_put,      3},
  {"Crleveldb_delete",   (DL_FUNC) &rleveldb_delete,   2},

  {"Crleveldb_iter_create",        (DL_FUNC) &rleveldb_iter_create,        1},
  {"Crleveldb_iter_destroy",       (DL_FUNC) &rleveldb_iter_destroy,       2},
  {"Crleveldb_iter_valid",         (DL_FUNC) &rleveldb_iter_valid,         1},
  {"Crleveldb_iter_seek_to_first", (DL_FUNC) &rleveldb_iter_seek_to_first, 1},
  {"Crleveldb_iter_seek_to_last",  (DL_FUNC) &rleveldb_iter_seek_to_last,  1},
  {"Crleveldb_iter_seek",          (DL_FUNC) &rleveldb_iter_seek,          2},
  {"Crleveldb_iter_next",          (DL_FUNC) &rleveldb_iter_next,          1},
  {"Crleveldb_iter_prev",          (DL_FUNC) &rleveldb_iter_prev,          1},
  {"Crleveldb_iter_key",           (DL_FUNC) &rleveldb_iter_key,           3},
  {"Crleveldb_iter_value",         (DL_FUNC) &rleveldb_iter_value,         3},

  {"Crleveldb_keys_len", (DL_FUNC) &rleveldb_keys_len, 1},
  {"Crleveldb_keys",     (DL_FUNC) &rleveldb_keys,     2},
  {"Crleveldb_exists",   (DL_FUNC) &rleveldb_exists,   2},

  {NULL,                 NULL,                         0}
};

void R_init_rleveldb(DllInfo *info) {
  R_registerRoutines(info, NULL, call_methods, NULL, NULL);
}

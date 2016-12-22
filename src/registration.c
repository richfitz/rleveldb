#include "rleveldb.h"
#include <R_ext/Rdynload.h>
#include <leveldb/c.h>

static const R_CallMethodDef call_methods[] = {
  {"Crleveldb_connect",  (DL_FUNC) &rleveldb_connect,  10},
  {"Crleveldb_close",    (DL_FUNC) &rleveldb_close,    2},
  {"Crleveldb_destroy",  (DL_FUNC) &rleveldb_destroy,  1},
  {"Crleveldb_repair",   (DL_FUNC) &rleveldb_repair,   1},
  {"Crleveldb_property", (DL_FUNC) &rleveldb_property, 3},

  {"Crleveldb_get",      (DL_FUNC) &rleveldb_get,      5},
  {"Crleveldb_put",      (DL_FUNC) &rleveldb_put,      4},
  {"Crleveldb_delete",   (DL_FUNC) &rleveldb_delete,   3},

  {"Crleveldb_iter_create",        (DL_FUNC) &rleveldb_iter_create,        2},
  {"Crleveldb_iter_destroy",       (DL_FUNC) &rleveldb_iter_destroy,       2},
  {"Crleveldb_iter_valid",         (DL_FUNC) &rleveldb_iter_valid,         1},
  {"Crleveldb_iter_seek_to_first", (DL_FUNC) &rleveldb_iter_seek_to_first, 1},
  {"Crleveldb_iter_seek_to_last",  (DL_FUNC) &rleveldb_iter_seek_to_last,  1},
  {"Crleveldb_iter_seek",          (DL_FUNC) &rleveldb_iter_seek,          2},
  {"Crleveldb_iter_next",          (DL_FUNC) &rleveldb_iter_next,          1},
  {"Crleveldb_iter_prev",          (DL_FUNC) &rleveldb_iter_prev,          1},
  {"Crleveldb_iter_key",           (DL_FUNC) &rleveldb_iter_key,           3},
  {"Crleveldb_iter_value",         (DL_FUNC) &rleveldb_iter_value,         3},

  {"Crleveldb_snapshot_create",    (DL_FUNC) &rleveldb_snapshot_create,    1},
  {"Crleveldb_snapshot_release",   (DL_FUNC) &rleveldb_snapshot_release,   1},

  {"Crleveldb_approximate_sizes",  (DL_FUNC) &rleveldb_approximate_sizes,  3},
  {"Crleveldb_compact_range",      (DL_FUNC) &rleveldb_compact_range,      3},

  {"Crleveldb_readoptions",        (DL_FUNC) &rleveldb_readoptions,        3},
  {"Crleveldb_writeoptions",       (DL_FUNC) &rleveldb_writeoptions,       1},

  {"Crleveldb_keys_len", (DL_FUNC) &rleveldb_keys_len, 2},
  {"Crleveldb_keys",     (DL_FUNC) &rleveldb_keys,     3},
  {"Crleveldb_exists",   (DL_FUNC) &rleveldb_exists,   3},
  {"Crleveldb_version",  (DL_FUNC) &rleveldb_version,  0},

  {NULL,                 NULL,                         0}
};

extern leveldb_readoptions_t * default_readoptions;
extern leveldb_writeoptions_t * default_writeoptions;

void R_init_rleveldb(DllInfo *info) {
  R_registerRoutines(info, NULL, call_methods, NULL, NULL);
  default_readoptions = leveldb_readoptions_create();
  default_writeoptions = leveldb_writeoptions_create();
}

void R_unload_rleveldb(DllInfo *info) {
  leveldb_readoptions_destroy(default_readoptions);
  leveldb_writeoptions_destroy(default_writeoptions);
}

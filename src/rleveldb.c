#include "rleveldb.h"

#include <stdbool.h>
#include <leveldb/c.h>
#include "support.h"

// Internals:
leveldb_t* rleveldb_get_db(SEXP extptr, bool closed_error);
static void rleveldb_finalize(SEXP extptr);
void rleveldb_handle_error(char* err);

SEXP rleveldb_connect(SEXP r_name) {
  leveldb_options_t *options = leveldb_options_create();
  leveldb_options_set_create_if_missing(options, 1);
  char *err = NULL;
  const char *name = CHAR(STRING_ELT(r_name, 0));
  leveldb_t *db = leveldb_open(options, name, &err);
  rleveldb_handle_error(err);

  SEXP extPtr = PROTECT(R_MakeExternalPtr(db, r_name, R_NilValue));
  R_RegisterCFinalizer(extPtr, rleveldb_finalize);
  UNPROTECT(1);
  return extPtr;
}

SEXP rleveldb_get(SEXP extptr, SEXP key, SEXP r_force_raw) {
  leveldb_t *db = rleveldb_get_db(extptr, true);
  void *key_data = get_key_ptr(key);
  size_t key_len = get_key_len(key);
  bool force_raw = LOGICAL(r_force_raw)[0];

  char *err = NULL;
  leveldb_readoptions_t *options = leveldb_readoptions_create();
  size_t read_len;
  char* read = leveldb_get(db, options, key_data, key_len, &read_len, &err);
  rleveldb_handle_error(err);

  SEXP ret = raw_string_to_sexp(read, read_len, force_raw);
  leveldb_free(read);
  return ret;
}

SEXP rleveldb_put(SEXP extptr, SEXP key, SEXP value) {
  leveldb_t *db = rleveldb_get_db(extptr, true);
  void *key_data = get_key_ptr(key), *value_data = get_value_ptr(value);
  size_t key_len = get_key_len(key),   value_len = get_value_len(value);

  char *err = NULL;
  leveldb_writeoptions_t *options = leveldb_writeoptions_create();
  leveldb_put(db, options, key_data, key_len, value_data, value_len, &err);
  rleveldb_handle_error(err);

  return R_NilValue;
}

SEXP rleveldb_delete(SEXP extptr, SEXP key) {
  leveldb_t *db = rleveldb_get_db(extptr, true);
  void *key_data = get_key_ptr(key);
  size_t key_len = get_key_len(key);

  char *err = NULL;
  leveldb_writeoptions_t *options = leveldb_writeoptions_create();
  leveldb_delete(db, options, key_data, key_len, &err);
  rleveldb_handle_error(err);

  return R_NilValue;
}

// Internal function definitions:

void rleveldb_finalize(SEXP extptr) {
  leveldb_t* db = rleveldb_get_db(extptr, false);
  if (db) {
    leveldb_close(db);
    R_ClearExternalPtr(extptr);
  }
}

leveldb_t* rleveldb_get_db(SEXP extptr, bool closed_error) {
  void *db = NULL;
  if (TYPEOF(extptr) != EXTPTRSXP) {
    Rf_error("Expected an external pointer");
  }
  db = (leveldb_t*) R_ExternalPtrAddr(extptr);
  if (!db && closed_error) {
    Rf_error("Db is not connected");
  }
  return (leveldb_t*) db;
}

void rleveldb_handle_error(char* err) {
  if (err != NULL) {
    // Here, with the help of valgrind, work out how to get the error
    // string here safely into the actual guts of the error() call.
    REprintf("%s\n", err);
    leveldb_free(err);
    Rf_error("Unhandled error; need to print this nicely");
  }
}

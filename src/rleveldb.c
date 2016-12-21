#include "rleveldb.h"

#include <stdbool.h>
#include <leveldb/c.h>
#include "support.h"

// Internals:
leveldb_t* rleveldb_get_db(SEXP extptr, bool closed_error);
static void rleveldb_finalize(SEXP extptr);
void rleveldb_handle_error(char* err);
// Slightly different
size_t rleveldb_get_keys_len(leveldb_t *db);
bool rleveldb_get_exists(leveldb_t *db, const char *key_data, size_t key_len);

// Implementations:
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
  const char *key_data = get_key_ptr(key);
  size_t key_len = get_key_len(key);
  bool force_raw = scalar_logical(r_force_raw);

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
  const char *key_data = get_key_ptr(key), *value_data = get_value_ptr(value);
  size_t key_len = get_key_len(key),   value_len = get_value_len(value);

  char *err = NULL;
  leveldb_writeoptions_t *options = leveldb_writeoptions_create();
  leveldb_put(db, options, key_data, key_len, value_data, value_len, &err);
  rleveldb_handle_error(err);

  return R_NilValue;
}

SEXP rleveldb_delete(SEXP extptr, SEXP key) {
  leveldb_t *db = rleveldb_get_db(extptr, true);
  const char *key_data = get_key_ptr(key);
  size_t key_len = get_key_len(key);

  char *err = NULL;
  leveldb_writeoptions_t *options = leveldb_writeoptions_create();
  leveldb_delete(db, options, key_data, key_len, &err);
  rleveldb_handle_error(err);

  return R_NilValue;
}

// Built on top of the leveldb api:
SEXP rleveldb_keys(SEXP extptr, SEXP r_as_raw) {
  leveldb_t *db = rleveldb_get_db(extptr, true);
  bool as_raw = scalar_logical(r_as_raw);

  size_t n = rleveldb_get_keys_len(db);
  SEXP ret = PROTECT(allocVector(as_raw ? VECSXP : STRSXP, n));

  // TODO: at the moment this works with two passes; one computes the
  // number of keys and the other collects the keys.  That's not a bad
  // call, really, but it would be nice to be able to *collect*
  // things.  To do that efficiently we need a growing data structure.
  // It's possible that pairlists may accomplish that fairly
  // effectively.  This is going to be needed for a "match pattern"
  // find function, though the bigger hurdle is going to be doing any
  // sort of actual pattern matching aside from "starts with"

  leveldb_readoptions_t *options = leveldb_readoptions_create();
  leveldb_iterator_t *it = leveldb_create_iterator(db, options);
  leveldb_iter_seek_to_first(it);
  size_t key_len;
  for (size_t i = 0; i < n; ++i, leveldb_iter_next(it)) {
    const char *key_data = leveldb_iter_key(it, &key_len);
    if (as_raw) {
      SET_VECTOR_ELT(ret, i, allocVector(RAWSXP, key_len));
      memcpy(RAW(VECTOR_ELT(ret, i)), key_data, key_len);
    } else {
      // TODO: consider here checking that we do make a string of the
      // correct size?  If so we can throw an error and require that
      // bytes are used.
      //
      // TODO: if this throws an R error, do we leak the iterator?
      SET_STRING_ELT(ret, i, mkCharLen(key_data, key_len));
    }
  }

  UNPROTECT(1);
  return ret;
}

SEXP rleveldb_keys_len(SEXP extptr) {
  leveldb_t *db = rleveldb_get_db(extptr, true);
  return ScalarInteger(rleveldb_get_keys_len(db));
}

SEXP rleveldb_exists(SEXP extptr, SEXP key) {
  leveldb_t *db = rleveldb_get_db(extptr, true);
  const char *key_data = get_key_ptr(key);
  size_t key_len = get_key_len(key);
  return ScalarLogical(rleveldb_get_exists(db, key_data, key_len));
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

size_t rleveldb_get_keys_len(leveldb_t *db) {
  leveldb_readoptions_t *options = leveldb_readoptions_create();
  leveldb_iterator_t *it = leveldb_create_iterator(db, options);
  size_t n = 0;
  for (leveldb_iter_seek_to_first(it);
       leveldb_iter_valid(it);
       leveldb_iter_next(it)) {
    ++n;
  }
  return n;
}

bool rleveldb_get_exists(leveldb_t *db, const char *key_data, size_t key_len) {
  leveldb_readoptions_t *options = leveldb_readoptions_create();
  leveldb_iterator_t *it = leveldb_create_iterator(db, options);
  leveldb_iter_seek(it, key_data, key_len);

  bool found = false;
  if (leveldb_iter_valid(it)) {
    size_t it_key_len;
    const char *it_key_data = leveldb_iter_key(it, &it_key_len);
    if (it_key_len == key_len && memcmp(it_key_data, key_data, key_len) == 0) {
      found = true;
    }
  }
  return found;
}

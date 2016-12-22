#include "rleveldb.h"

#include <stdbool.h>
#include <leveldb/c.h>
#include "support.h"

// Internals:
leveldb_t* rleveldb_get_db(SEXP r_db, bool closed_error);
leveldb_iterator_t* rleveldb_get_iterator(SEXP r_it, bool closed_error);
static void rleveldb_finalize(SEXP r_db);
static void rleveldb_iter_finalize(SEXP r_db);
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
  leveldb_free(options);
  rleveldb_handle_error(err);

  SEXP r_db = PROTECT(R_MakeExternalPtr(db, r_name, R_NilValue));
  R_RegisterCFinalizer(r_db, rleveldb_finalize);
  UNPROTECT(1);
  return r_db;
}

SEXP rleveldb_close(SEXP r_db, SEXP r_error_if_closed) {
  leveldb_t *db = rleveldb_get_db(r_db, scalar_logical(r_error_if_closed));
  if (db != NULL) {
    leveldb_close(db);
    R_ClearExternalPtr(r_db);
  }
  return ScalarLogical(db != NULL);
}

SEXP rleveldb_destroy(SEXP r_name) {
  // TODO: on error, does leveldb_options_create cause a leak?
  leveldb_options_t *options = leveldb_options_create();
  leveldb_options_set_create_if_missing(options, 0);
  char *err = NULL;
  const char *name = CHAR(STRING_ELT(r_name, 0));
  leveldb_destroy_db(options, name, &err);
  leveldb_free(options);
  rleveldb_handle_error(err);
  return ScalarLogical(true);
}

SEXP rleveldb_get(SEXP r_db, SEXP r_key, SEXP r_force_raw,
                  SEXP r_error_if_missing) {
  leveldb_t *db = rleveldb_get_db(r_db, true);
  const char *key_data = get_key_ptr(r_key);
  size_t key_len = get_key_len(r_key);
  bool force_raw = scalar_logical(r_force_raw),
    error_if_missing = scalar_logical(r_error_if_missing);

  char *err = NULL;
  leveldb_readoptions_t *options = leveldb_readoptions_create();
  size_t read_len;
  char* read = leveldb_get(db, options, key_data, key_len, &read_len, &err);
  leveldb_free(options);
  rleveldb_handle_error(err);

  SEXP ret;
  if (read != NULL) {
    ret = raw_string_to_sexp(read, read_len, force_raw);
    leveldb_free(read);
  } else if (!error_if_missing) {
    ret = R_NilValue;
  } else if (TYPEOF(r_key) == STRSXP) {
    Rf_error("Key '%s' not found in database", key_data);
  } else {
    Rf_error("Key not found in database");
  }

  return ret;
}

SEXP rleveldb_put(SEXP r_db, SEXP r_key, SEXP r_value) {
  leveldb_t *db = rleveldb_get_db(r_db, true);
  const char *key_data = get_key_ptr(r_key),
    *value_data = get_value_ptr(r_value);
  size_t key_len = get_key_len(r_key), value_len = get_value_len(r_value);

  char *err = NULL;
  leveldb_writeoptions_t *options = leveldb_writeoptions_create();
  leveldb_put(db, options, key_data, key_len, value_data, value_len, &err);
  leveldb_free(options);
  rleveldb_handle_error(err);

  return R_NilValue;
}

SEXP rleveldb_delete(SEXP r_db, SEXP r_key) {
  leveldb_t *db = rleveldb_get_db(r_db, true);
  const char *key_data = get_key_ptr(r_key);
  size_t key_len = get_key_len(r_key);

  char *err = NULL;
  leveldb_writeoptions_t *options = leveldb_writeoptions_create();
  leveldb_delete(db, options, key_data, key_len, &err);
  leveldb_free(options);
  rleveldb_handle_error(err);

  return R_NilValue;
}

// Iterators
SEXP rleveldb_iter_create(SEXP r_db) {
  leveldb_t *db = rleveldb_get_db(r_db, true);
  leveldb_readoptions_t *options = leveldb_readoptions_create();
  leveldb_iterator_t *it = leveldb_create_iterator(db, options);

  // TODO: Consider using r_db as the tag here?
  SEXP r_it = PROTECT(R_MakeExternalPtr(it, R_NilValue, R_NilValue));
  R_RegisterCFinalizer(r_it, rleveldb_iter_finalize);
  UNPROTECT(1);
  return r_it;
}

SEXP rleveldb_iter_destroy(SEXP r_it, SEXP r_error_if_closed) {
  bool error_if_closed = scalar_logical(r_error_if_closed);
  leveldb_iterator_t *it = rleveldb_get_iterator(r_it, error_if_closed);
  if (it != NULL) {
    leveldb_iter_destroy(it);
    R_ClearExternalPtr(r_it);
  }
  return ScalarLogical(it != NULL);
}

SEXP rleveldb_iter_valid(SEXP r_it) {
  leveldb_iterator_t *it = rleveldb_get_iterator(r_it, true);
  return ScalarLogical(leveldb_iter_valid(it));
}

SEXP rleveldb_iter_seek_to_first(SEXP r_it) {
  leveldb_iterator_t *it = rleveldb_get_iterator(r_it, true);
  leveldb_iter_seek_to_first(it);
  return R_NilValue;
}

SEXP rleveldb_iter_seek_to_last(SEXP r_it) {
  leveldb_iterator_t *it = rleveldb_get_iterator(r_it, true);
  leveldb_iter_seek_to_last(it);
  return R_NilValue;
}

SEXP rleveldb_iter_seek(SEXP r_it, SEXP r_key) {
  leveldb_iterator_t *it = rleveldb_get_iterator(r_it, true);
  const char *key_data = get_key_ptr(r_key);
  size_t key_len = get_key_len(r_key);
  leveldb_iter_seek(it, key_data, key_len);
  return R_NilValue;
}

SEXP rleveldb_iter_next(SEXP r_it) {
  leveldb_iterator_t *it = rleveldb_get_iterator(r_it, true);
  leveldb_iter_next(it);
  return R_NilValue;
}

SEXP rleveldb_iter_prev(SEXP r_it) {
  leveldb_iterator_t *it = rleveldb_get_iterator(r_it, true);
  leveldb_iter_prev(it);
  return R_NilValue;
}

SEXP rleveldb_iter_key(SEXP r_it, SEXP r_force_raw, SEXP r_error_if_invalid) {
  leveldb_iterator_t *it = rleveldb_get_iterator(r_it, true);
  bool force_raw = scalar_logical(r_force_raw),
    error_if_invalid = scalar_logical(r_error_if_invalid);
  size_t len;
  if (!leveldb_iter_valid(it)) {
    if (error_if_invalid) {
      Rf_error("Iterator is not valid");
    } else {
      return R_NilValue;
    }
  }
  const char *data = leveldb_iter_key(it, &len);
  return raw_string_to_sexp(data, len, force_raw);
}

SEXP rleveldb_iter_value(SEXP r_it, SEXP r_force_raw, SEXP r_error_if_invalid) {
  leveldb_iterator_t *it = rleveldb_get_iterator(r_it, true);
  bool force_raw = scalar_logical(r_force_raw),
    error_if_invalid = scalar_logical(r_error_if_invalid);
  if (!leveldb_iter_valid(it)) {
    if (error_if_invalid) {
      Rf_error("Iterator is not valid");
    } else {
      return R_NilValue;
    }
  }
  size_t len;
  const char *data = leveldb_iter_value(it, &len);
  return raw_string_to_sexp(data, len, force_raw);
}

// Built on top of the leveldb api:
SEXP rleveldb_keys(SEXP r_db, SEXP r_as_raw) {
  leveldb_t *db = rleveldb_get_db(r_db, true);
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
  leveldb_free(options);
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
      // TODO: if this throws an R error, we leak iterators.  It would
      // be nice to put a wrapper around the iterator (creating a
      // reference) so that R will clean it up for us later.
      SET_STRING_ELT(ret, i, mkCharLen(key_data, key_len));
    }
  }
  leveldb_iter_destroy(it);

  UNPROTECT(1);
  return ret;
}

SEXP rleveldb_keys_len(SEXP r_db) {
  leveldb_t *db = rleveldb_get_db(r_db, true);
  return ScalarInteger(rleveldb_get_keys_len(db));
}

SEXP rleveldb_exists(SEXP r_db, SEXP r_key) {
  leveldb_t *db = rleveldb_get_db(r_db, true);
  const char *key_data = get_key_ptr(r_key);
  size_t key_len = get_key_len(r_key);
  return ScalarLogical(rleveldb_get_exists(db, key_data, key_len));
}

// Internal function definitions:
void rleveldb_finalize(SEXP r_db) {
  leveldb_t* db = rleveldb_get_db(r_db, false);
  if (db) {
    leveldb_close(db);
    R_ClearExternalPtr(r_db);
  }
}

void rleveldb_iter_finalize(SEXP r_it) {
  leveldb_iterator_t* it = rleveldb_get_iterator(r_it, false);
  if (it) {
    leveldb_iter_destroy(it);
    R_ClearExternalPtr(r_it);
  }
}

leveldb_t* rleveldb_get_db(SEXP r_db, bool closed_error) {
  void *db = NULL;
  if (TYPEOF(r_db) != EXTPTRSXP) {
    Rf_error("Expected an external pointer");
  }
  db = (leveldb_t*) R_ExternalPtrAddr(r_db);
  if (!db && closed_error) {
    Rf_error("leveldb handle is not open; can't connect");
  }
  return (leveldb_t*) db;
}

// TODO: distinguish here between an iterator and db handle by
// checking the SEXP on the tag?
leveldb_iterator_t* rleveldb_get_iterator(SEXP r_it, bool closed_error) {
  void *it = NULL;
  if (TYPEOF(r_it) != EXTPTRSXP) {
    Rf_error("Expected an external pointer");
  }
  it = (leveldb_iterator_t*) R_ExternalPtrAddr(r_it);
  if (!it && closed_error) {
    Rf_error("leveldb iterator is not open; can't connect");
  }
  return (leveldb_iterator_t*) it;
}

void rleveldb_handle_error(char* err) {
  if (err != NULL) {
    size_t len = strlen(err);
    char * msg = (char*) R_alloc(len + 1, sizeof(char));
    memcpy(msg, err, len + 1);
    leveldb_free(err);
    error(msg);
  }
}

size_t rleveldb_get_keys_len(leveldb_t *db) {
  leveldb_readoptions_t *options = leveldb_readoptions_create();
  leveldb_iterator_t *it = leveldb_create_iterator(db, options);
  leveldb_free(options);
  size_t n = 0;
  for (leveldb_iter_seek_to_first(it);
       leveldb_iter_valid(it);
       leveldb_iter_next(it)) {
    ++n;
  }
  leveldb_iter_destroy(it);
  return n;
}

bool rleveldb_get_exists(leveldb_t *db, const char *key_data, size_t key_len) {
  leveldb_readoptions_t *options = leveldb_readoptions_create();
  leveldb_iterator_t *it = leveldb_create_iterator(db, options);
  leveldb_free(options);
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

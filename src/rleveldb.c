#include "rleveldb.h"

#include <stdbool.h>
#include <leveldb/c.h>
#include "support.h"

leveldb_readoptions_t * default_readoptions;
leveldb_writeoptions_t * default_writeoptions;

// Internals:
leveldb_t* rleveldb_get_db(SEXP r_db, bool closed_error);
leveldb_iterator_t* rleveldb_get_iterator(SEXP r_it, bool closed_error);
leveldb_snapshot_t* rleveldb_get_snapshot(SEXP r_snapshot, bool closed_error);
leveldb_readoptions_t* rleveldb_get_readoptions(SEXP r_readoptions,
                                                bool closed_error);
leveldb_writeoptions_t* rleveldb_get_writeoptions(SEXP r_writeoptions,
                                                  bool closed_error);

// Finalisers
static void rleveldb_finalize(SEXP r_db);
static void rleveldb_iter_finalize(SEXP r_it);
static void rleveldb_snapshot_finalize(SEXP r_snapshot);
static void rleveldb_readoptions_finalize(SEXP r_readoptions);
static void rleveldb_writeoptions_finalize(SEXP r_writeoptions);

// Other internals
void rleveldb_handle_error(char* err);
leveldb_options_t* rleveldb_collect_options(SEXP r_create_if_missing,
                                            SEXP r_error_if_exists,
                                            SEXP r_paranoid_checks,
                                            SEXP r_write_buffer_size,
                                            SEXP r_max_open_files,
                                            SEXP r_cache_capacity,
                                            SEXP r_block_size,
                                            SEXP r_use_compression,
                                            SEXP r_bloom_filter_bits_per_key);
// Slightly different
size_t rleveldb_get_keys_len(leveldb_t *db, leveldb_readoptions_t *readoptions);
bool rleveldb_get_exists(leveldb_t *db, const char *key_data, size_t key_len,
                         leveldb_readoptions_t *readoptions);

// Implementations:
SEXP rleveldb_connect(SEXP r_name,
                      SEXP r_create_if_missing,
                      SEXP r_error_if_exists,
                      SEXP r_paranoid_checks,
                      SEXP r_write_buffer_size,
                      SEXP r_max_open_files,
                      SEXP r_cache_capacity,
                      SEXP r_block_size,
                      SEXP r_use_compression,
                      SEXP r_bloom_filter_bits_per_key) {
  // Unimplemented:
  // * general set_filter_policy
  // * set_env
  // * set_info_log
  // * set_comparator
  // * restart_interval
  leveldb_options_t *options =
    rleveldb_collect_options(r_create_if_missing, r_error_if_exists,
                             r_paranoid_checks, r_write_buffer_size,
                             r_max_open_files, r_cache_capacity,
                             r_block_size, r_use_compression,
                             r_bloom_filter_bits_per_key);
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

SEXP rleveldb_property(SEXP r_db, SEXP r_name, SEXP r_error_if_missing) {
  leveldb_t *db = rleveldb_get_db(r_db, true);
  const char *name = scalar_character(r_name);
  bool error_if_missing = scalar_logical(r_error_if_missing);
  char *value = leveldb_property_value(db, name);
  SEXP ret;
  if (value != NULL) {
    ret = mkString(value);
    leveldb_free(value);
  } else if (error_if_missing) {
    Rf_error("No such property '%s'", name);
  } else {
    ret = R_NilValue;
  }
  return ret;
}

SEXP rleveldb_get(SEXP r_db, SEXP r_key, SEXP r_force_raw,
                  SEXP r_error_if_missing, SEXP r_readoptions) {
  leveldb_t *db = rleveldb_get_db(r_db, true);
  const char *key_data = get_key_ptr(r_key);
  size_t key_len = get_key_len(r_key);
  bool force_raw = scalar_logical(r_force_raw),
    error_if_missing = scalar_logical(r_error_if_missing);
  leveldb_readoptions_t *readoptions =
    rleveldb_get_readoptions(r_readoptions, true);

  char *err = NULL;
  size_t read_len;
  char* read = leveldb_get(db, readoptions, key_data, key_len, &read_len, &err);
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

SEXP rleveldb_put(SEXP r_db, SEXP r_key, SEXP r_value, SEXP r_writeoptions) {
  leveldb_t *db = rleveldb_get_db(r_db, true);
  const char *key_data = get_key_ptr(r_key),
    *value_data = get_value_ptr(r_value);
  size_t key_len = get_key_len(r_key), value_len = get_value_len(r_value);
  leveldb_writeoptions_t *writeoptions =
    rleveldb_get_writeoptions(r_writeoptions, true);

  char *err = NULL;
  leveldb_put(db, writeoptions, key_data, key_len, value_data, value_len, &err);
  rleveldb_handle_error(err);

  return R_NilValue;
}

SEXP rleveldb_delete(SEXP r_db, SEXP r_key, SEXP r_writeoptions) {
  leveldb_t *db = rleveldb_get_db(r_db, true);
  const char *key_data = get_key_ptr(r_key);
  size_t key_len = get_key_len(r_key);
  leveldb_writeoptions_t *writeoptions =
    rleveldb_get_writeoptions(r_writeoptions, true);

  char *err = NULL;
  leveldb_delete(db, writeoptions, key_data, key_len, &err);
  rleveldb_handle_error(err);

  return R_NilValue;
}

// Iterators
SEXP rleveldb_iter_create(SEXP r_db, SEXP r_readoptions) {
  leveldb_t *db = rleveldb_get_db(r_db, true);
  leveldb_readoptions_t *readoptions =
    rleveldb_get_readoptions(r_readoptions, true);
  leveldb_iterator_t *it = leveldb_create_iterator(db, readoptions);

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

// Snapshots
SEXP rleveldb_snapshot_create(SEXP r_db) {
  leveldb_t *db = rleveldb_get_db(r_db, true);
  const leveldb_snapshot_t *snapshot = leveldb_create_snapshot(db);
  SEXP r_snapshot =
    PROTECT(R_MakeExternalPtr((void*) snapshot, r_db, R_NilValue));
  R_RegisterCFinalizer(r_snapshot, rleveldb_snapshot_finalize);
  UNPROTECT(1);
  return r_snapshot;
}

SEXP rleveldb_snapshot_release(SEXP r_snapshot, SEXP r_error_if_released) {
  bool error_if_relased = scalar_logical(r_error_if_released);
  const leveldb_snapshot_t *snapshot =
    rleveldb_get_snapshot(r_snapshot, error_if_relased);
  if (snapshot != NULL) {
    leveldb_t *db = rleveldb_get_db(R_ExternalPtrTag(r_snapshot), true);
    leveldb_release_snapshot(db, snapshot);
    R_ClearExternalPtr(r_snapshot);
  }
  return ScalarLogical(snapshot != NULL);
}

// Options
SEXP rleveldb_readoptions(SEXP r_verify_checksums, SEXP r_fill_cache,
                          SEXP r_snapshot) {
  leveldb_readoptions_t * options = leveldb_readoptions_create();
  SEXP tag = PROTECT(allocVector(VECSXP, 3));
  SET_VECTOR_ELT(tag, 0, r_verify_checksums);
  SET_VECTOR_ELT(tag, 1, r_fill_cache);
  SET_VECTOR_ELT(tag, 2, r_snapshot);
  SEXP ret = PROTECT(R_MakeExternalPtr(options, tag, R_NilValue));
  R_RegisterCFinalizer(ret, rleveldb_readoptions_finalize);
  if (r_verify_checksums != R_NilValue) {
    bool verify_checksums = scalar_logical(r_verify_checksums);
    leveldb_readoptions_set_verify_checksums(options, verify_checksums);
  }
  if (r_fill_cache != R_NilValue) {
    leveldb_readoptions_set_fill_cache(options, scalar_logical(r_fill_cache));
  }
  if (r_snapshot != R_NilValue) {
    leveldb_readoptions_set_snapshot(options,
                                     rleveldb_get_snapshot(r_snapshot, true));
  }

  UNPROTECT(2);
  return ret;
}

SEXP rleveldb_writeoptions(SEXP r_sync) {
  leveldb_writeoptions_t * options = leveldb_writeoptions_create();
  SEXP tag = PROTECT(allocVector(VECSXP, 1));
  SET_VECTOR_ELT(tag, 0, r_sync);
  SEXP ret = PROTECT(R_MakeExternalPtr(options, tag, R_NilValue));
  R_RegisterCFinalizer(ret, rleveldb_writeoptions_finalize);
  if (r_sync != R_NilValue) {
    leveldb_writeoptions_set_sync(options, scalar_logical(r_sync));
  }
  UNPROTECT(2);
  return ret;
}

// Built on top of the leveldb api:
SEXP rleveldb_keys(SEXP r_db, SEXP r_as_raw, SEXP r_readoptions) {
  leveldb_t *db = rleveldb_get_db(r_db, true);
  bool as_raw = scalar_logical(r_as_raw);
  leveldb_readoptions_t *readoptions =
    rleveldb_get_readoptions(r_readoptions, true);

  size_t n = rleveldb_get_keys_len(db, readoptions);
  SEXP ret = PROTECT(allocVector(as_raw ? VECSXP : STRSXP, n));

  // TODO: at the moment this works with two passes; one computes the
  // number of keys and the other collects the keys.  That's not a bad
  // call, really, but it would be nice to be able to *collect*
  // things.  To do that efficiently we need a growing data structure.
  // It's possible that pairlists may accomplish that fairly
  // effectively.  This is going to be needed for a "match pattern"
  // find function, though the bigger hurdle is going to be doing any
  // sort of actual pattern matching aside from "starts with"

  leveldb_iterator_t *it = leveldb_create_iterator(db, readoptions);
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

SEXP rleveldb_keys_len(SEXP r_db, SEXP r_readoptions) {
  leveldb_t *db = rleveldb_get_db(r_db, true);
  leveldb_readoptions_t *readoptions =
    rleveldb_get_readoptions(r_readoptions, true);
  return ScalarInteger(rleveldb_get_keys_len(db, readoptions));
}

SEXP rleveldb_exists(SEXP r_db, SEXP r_key, SEXP r_readoptions) {
  leveldb_t *db = rleveldb_get_db(r_db, true);
  const char *key_data = get_key_ptr(r_key);
  size_t key_len = get_key_len(r_key);
  leveldb_readoptions_t *readoptions =
    rleveldb_get_readoptions(r_readoptions, true);
  return ScalarLogical(rleveldb_get_exists(db, key_data, key_len, readoptions));
}

SEXP rleveldb_version() {
  SEXP ret = PROTECT(allocVector(INTSXP, 2));
  INTEGER(ret)[0] = leveldb_major_version();
  INTEGER(ret)[1] = leveldb_minor_version();
  UNPROTECT(1);
  return ret;
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

void rleveldb_snapshot_finalize(SEXP r_snapshot) {
  leveldb_snapshot_t* snapshot = rleveldb_get_snapshot(r_snapshot, false);
  if (snapshot) {
    leveldb_t *db = rleveldb_get_db(R_ExternalPtrTag(r_snapshot), false);
    if (db) {
      leveldb_release_snapshot(db, snapshot);
    }
    R_ClearExternalPtr(r_snapshot);
  }
}

void rleveldb_readoptions_finalize(SEXP r_readoptions) {
  leveldb_readoptions_t* readoptions =
    rleveldb_get_readoptions(r_readoptions, false);
  if (readoptions) {
    leveldb_readoptions_destroy(readoptions);
    R_ClearExternalPtr(r_readoptions);
  }
}

void rleveldb_writeoptions_finalize(SEXP r_writeoptions) {
  leveldb_writeoptions_t* writeoptions =
    rleveldb_get_writeoptions(r_writeoptions, false);
  if (writeoptions) {
    leveldb_writeoptions_destroy(writeoptions);
    R_ClearExternalPtr(r_writeoptions);
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

leveldb_snapshot_t* rleveldb_get_snapshot(SEXP r_snapshot, bool closed_error) {
  void *snapshot = NULL;
  if (TYPEOF(r_snapshot) != EXTPTRSXP) {
    Rf_error("Expected an external pointer");
  }
  snapshot = (leveldb_snapshot_t*) R_ExternalPtrAddr(r_snapshot);
  if (!snapshot && closed_error) {
    Rf_error("leveldb snapshot is not open; can't connect");
  }
  return (leveldb_snapshot_t*) snapshot;
}

leveldb_readoptions_t* rleveldb_get_readoptions(SEXP r_readoptions,
                                                bool closed_error) {
  if (r_readoptions == R_NilValue) {
    return default_readoptions;
  }
  void *readoptions = NULL;
  if (TYPEOF(r_readoptions) != EXTPTRSXP) {
    Rf_error("Expected an external pointer");
  }
  readoptions = (leveldb_readoptions_t*) R_ExternalPtrAddr(r_readoptions);
  if (!readoptions && closed_error) {
    Rf_error("leveldb readoptions is not open; can't connect");
  }
  return (leveldb_readoptions_t*) readoptions;
}

leveldb_writeoptions_t* rleveldb_get_writeoptions(SEXP r_writeoptions,
                                                  bool closed_error) {
  if (r_writeoptions == R_NilValue) {
    return default_writeoptions;
  }
  void *writeoptions = NULL;
  if (TYPEOF(r_writeoptions) != EXTPTRSXP) {
    Rf_error("Expected an external pointer");
  }
  writeoptions = (leveldb_writeoptions_t*) R_ExternalPtrAddr(r_writeoptions);
  if (!writeoptions && closed_error) {
    Rf_error("leveldb writeoptions is not open; can't connect");
  }
  return (leveldb_writeoptions_t*) writeoptions;
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

size_t rleveldb_get_keys_len(leveldb_t *db,
                             leveldb_readoptions_t *readoptions) {
  leveldb_iterator_t *it = leveldb_create_iterator(db, readoptions);
  size_t n = 0;
  for (leveldb_iter_seek_to_first(it);
       leveldb_iter_valid(it);
       leveldb_iter_next(it)) {
    ++n;
  }
  leveldb_iter_destroy(it);
  return n;
}

bool rleveldb_get_exists(leveldb_t *db, const char *key_data, size_t key_len,
                         leveldb_readoptions_t *readoptions) {
  leveldb_iterator_t *it = leveldb_create_iterator(db, readoptions);
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

leveldb_options_t* rleveldb_collect_options(SEXP r_create_if_missing,
                                            SEXP r_error_if_exists,
                                            SEXP r_paranoid_checks,
                                            SEXP r_write_buffer_size,
                                            SEXP r_max_open_files,
                                            SEXP r_cache_capacity,
                                            SEXP r_block_size,
                                            SEXP r_use_compression,
                                            SEXP r_bloom_filter_bits_per_key) {
  leveldb_options_t *options = leveldb_options_create();
  // TODO: put a finaliser on options so that we can error safely in
  // the scalar_logical commands
  if (r_create_if_missing != R_NilValue) {
    leveldb_options_set_create_if_missing(options,
                                          scalar_logical(r_create_if_missing));
  }
  if (r_error_if_exists != R_NilValue) {
    leveldb_options_set_error_if_exists(options,
                                        scalar_logical(r_error_if_exists));
  }
  if (r_paranoid_checks != R_NilValue) {
    leveldb_options_set_paranoid_checks(options,
                                        scalar_logical(r_paranoid_checks));
  }
  if (r_write_buffer_size != R_NilValue) {
    leveldb_options_set_write_buffer_size(options,
                                          scalar_size(r_write_buffer_size));
  }
  if (r_max_open_files != R_NilValue) {
    leveldb_options_set_max_open_files(options,
                                       scalar_size(r_max_open_files));
  }
  if (r_cache_capacity != R_NilValue) {
    // TODO: not clear when we have to delete this.  Will it be done
    // for us?  I think that we might have to do this when cleaning up
    // the options?
    size_t capacity = scalar_size(r_cache_capacity);
    leveldb_cache_t* cache = leveldb_cache_create_lru(capacity);
    leveldb_options_set_cache(options, cache);
  }
  if (r_block_size != R_NilValue) {
    leveldb_options_set_block_size(options,
                                   scalar_size(r_block_size));
  }
  if (r_use_compression != R_NilValue) {
    leveldb_options_set_compression(options,
                                    scalar_logical(r_use_compression));
  }
  if (r_bloom_filter_bits_per_key != R_NilValue) {
    size_t bits_per_key = scalar_size(r_bloom_filter_bits_per_key);
    leveldb_filterpolicy_t* filter =
      leveldb_filterpolicy_create_bloom(bits_per_key);
    leveldb_options_set_filter_policy(options, filter);
  }

  return options;
}

leveldb_readoptions_t * default_readoptions = NULL;
leveldb_writeoptions_t * default_writeoptions = NULL;

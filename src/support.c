#include "support.h"

bool is_raw_string(const char* str, size_t len) {
  if (len > 2) {
    if ((str[0] == 'X' || str[0] == 'B') && str[1] == '\n') {
      for (size_t i = 0; i < len; ++i) {
        if (str[i] == '\0') {
          return true;
        }
      }
    }
  }
  return false;
}

size_t get_data_len(SEXP data, const char* name) {
  switch (TYPEOF(data)) {
  case STRSXP:
    if (length(data) != 1) {
      Rf_error("%s must be a scalar character", name);
    }
    SEXP str = STRING_ELT(data, 0);
    return length(str);
    break;
  case RAWSXP:
    return length(data);
  default:
    Rf_error("Invalid data type for %s; expected string or raw", name);
  }
}

void* get_data_ptr(SEXP data, const char* name) {
  switch (TYPEOF(data)) {
  case STRSXP:
    if (length(data) != 1) {
      Rf_error("%s must be a scalar character", name);
    }
    SEXP str = STRING_ELT(data, 0);
    return (void*) CHAR(str);
  case RAWSXP:
    return RAW(data);
  default:
    Rf_error("Invalid data type for %s; expected string or raw", name);
  }
}

size_t get_key_len(SEXP key) {
  return get_data_len(key, "key");
}
void* get_key_ptr(SEXP key) {
  return get_data_ptr(key, "key");
}
size_t get_value_len(SEXP value) {
  return get_data_len(value, "value");
}
void* get_value_ptr(SEXP value) {
  return get_data_ptr(value, "value");
}

// This is the same strategy as redux.
SEXP raw_string_to_sexp(const char *str, size_t len, bool force_raw) {
  bool is_raw = force_raw || is_raw_string(str, len);
  SEXP ret;
  if (is_raw) {
    ret = PROTECT(allocVector(RAWSXP, len));
    memcpy(RAW(ret), str, len);
    UNPROTECT(1);
  } else {
    ret = PROTECT(mkString(str));
    const size_t slen = LENGTH(STRING_ELT(ret, 0));
    if (slen < len) {
      ret = PROTECT(allocVector(RAWSXP, len));
      memcpy(RAW(ret), str, len);
      UNPROTECT(2);
    } else {
      UNPROTECT(1);
    }
  }
  return ret;
}

// Same as from ring
bool scalar_logical(SEXP x) {
  if (TYPEOF(x) == LGLSXP && LENGTH(x) == 1) {
    int ret = INTEGER(x)[0];
    if (ret == NA_LOGICAL) {
      Rf_error("Expected a non-missing logical scalar");
    }
    return (bool)(ret);
  } else {
    Rf_error("Expected a logical scalar");
    return 0;
  }
}

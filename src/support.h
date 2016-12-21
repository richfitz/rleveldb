#include <stdbool.h>
#include <R.h>
#include <Rinternals.h>
bool is_raw_string(const char* str, size_t len);
size_t get_key_len(SEXP key);
const char* get_key_ptr(SEXP key);
size_t get_value_len(SEXP key);
const char* get_value_ptr(SEXP key);
SEXP raw_string_to_sexp(const char *str, size_t len, bool force_raw);
bool scalar_logical(SEXP x);

#include <stdbool.h>
#include <R.h>
#include <Rinternals.h>

size_t get_key(SEXP key, const char **key_data);
size_t get_value(SEXP value, const char **value_data);
size_t get_keys(SEXP keys, const char ***key_data, size_t **key_len);

bool is_raw_string(const char* str, size_t len);
SEXP raw_string_to_sexp(const char *str, size_t len, bool force_raw);
bool scalar_logical(SEXP x);
size_t scalar_size(SEXP x);
const char * scalar_character(SEXP x);

context("rleveldb")

## This is the low-level interface that requires a bit more care to
## use than we can expect users to have; passing naked pointers around
## really requires checking that they point at the right thing and
## that's really hard.
test_that("open, close", {
  db <- leveldb_connect(tempfile(), create_if_missing = TRUE)
  expect_true(leveldb_close(db))
  expect_false(leveldb_close(db))
  expect_error(leveldb_close(db, TRUE),
               "leveldb handle is not open")
  rm(db)
  gc()
})

test_that("destroy", {
  path <- tempfile()
  db <- leveldb_connect(path, create_if_missing = TRUE)
  expect_identical(leveldb_keys_len(db), 0L)

  expect_error(leveldb_destroy(path), "IO error: lock")
  leveldb_close(db)
  expect_true(leveldb_destroy(path))
  expect_false(file.exists(path))
  expect_true(leveldb_destroy(path))
  rm(db)
  gc()
})

test_that("CRUD", {
  db <- leveldb_connect(tempfile(), create_if_missing = TRUE)

  leveldb_put(db, "foo", "bar")
  expect_equal(leveldb_get(db, "foo"), "bar")
  expect_identical(leveldb_keys_len(db), 1L)

  leveldb_put(db, "foo", charToRaw("barbar"))
  expect_equal(leveldb_get(db, "foo"), "barbar")
  expect_equal(leveldb_get(db, "foo", TRUE), charToRaw("barbar"))

  expect_null(leveldb_delete(db, "foo"))
})

test_that("iterator", {
  db <- leveldb_connect(tempfile(), create_if_missing = TRUE)
  leveldb_put(db, "foo", "bar")
  it <- leveldb_iter_create(db)
  expect_false(leveldb_iter_valid(it))
  expect_null(leveldb_iter_seek_to_first(it))
  expect_true(leveldb_iter_valid(it))
  expect_equal(leveldb_iter_key(it), "foo")
  expect_equal(leveldb_iter_key(it, TRUE), charToRaw("foo"))
  expect_equal(leveldb_iter_value(it), "bar")
  expect_equal(leveldb_iter_value(it, TRUE), charToRaw("bar"))
  expect_null(leveldb_iter_next(it))
  expect_false(leveldb_iter_valid(it))

  expect_null(leveldb_iter_key(it))
  expect_null(leveldb_iter_key(it, TRUE))
  expect_error(leveldb_iter_key(it, error_if_invalid = TRUE),
               "Iterator is not valid")

  expect_null(leveldb_iter_value(it))
  expect_null(leveldb_iter_value(it, TRUE))
  expect_error(leveldb_iter_value(it, error_if_invalid = TRUE),
               "Iterator is not valid")
})

test_that("Get missing key", {
  db <- leveldb_connect(tempfile(), create_if_missing = TRUE)
  expect_null(leveldb_get(db, "foo"))
  expect_error(leveldb_get(db, "foo", error_if_missing = TRUE),
               "Key 'foo' not found in database")

  k <- charToRaw("foo")
  expect_null(leveldb_get(db, k))
  expect_error(leveldb_get(db, k, error_if_missing = TRUE),
               "Key not found in database")
})

test_that("keys", {
  db <- leveldb_connect(tempfile(), create_if_missing = TRUE)
  expect_identical(leveldb_keys_len(db), 0L)
  expect_identical(leveldb_keys(db, as_raw = TRUE), list())
  expect_identical(leveldb_keys(db, as_raw = NULL), list())
  expect_identical(leveldb_keys(db, as_raw = FALSE), character(0))

  leveldb_put(db, "foo", "bar")
  expect_identical(leveldb_keys_len(db), 1L)
  expect_equal(leveldb_keys(db, as_raw = TRUE), list(charToRaw("foo")))
  expect_equal(leveldb_keys(db, as_raw = NULL), list("foo"))
  expect_equal(leveldb_keys(db, as_raw = FALSE), "foo")

  expect_null(leveldb_delete(db, "foo"))
  expect_identical(leveldb_keys_len(db), 0L)
})

test_that("keys - starts_with", {
  db <- leveldb_connect(tempfile(), create_if_missing = TRUE)
  prefix <- rand_str()
  expect_identical(leveldb_keys_len(db, prefix), 0L)

  n <- 10L
  keys <- sprintf("%s:%s", prefix, replicate(n, rand_str()))
  dat <- rand_bytes(length(keys))

  for (i in seq_along(keys)) {
    leveldb_put(db, keys[[i]], dat[[i]])
  }

  expect_identical(leveldb_keys_len(db), n)
  expect_identical(leveldb_keys_len(db, prefix), n)

  ## add a key that does not fit the pattern:
  leveldb_put(db, "foo", "bar")

  expect_identical(leveldb_keys_len(db), n + 1L)
  expect_identical(leveldb_keys_len(db, prefix), n)

  expect_equal(sort(leveldb_keys(db)), sort(c(keys, "foo")))
  expect_equal(sort(leveldb_keys(db, prefix)), sort(keys))
})

test_that("exists", {
  db <- leveldb_connect(tempfile(), create_if_missing = TRUE)
  expect_false(leveldb_exists(db, "foo"))
  leveldb_put(db, "foo", "bar")
  expect_true(leveldb_exists(db, "foo"))
  expect_false(leveldb_exists(db, "bar"))
})

test_that("version", {
  v <- leveldb_version()
  expect_is(v, "numeric_version")
  expect_equal(length(v), 1L)
  expect_equal(length(unclass(v)[[1L]]), 2L)
})

test_that("properties", {
  db <- leveldb_connect(tempfile(), create_if_missing = TRUE)
  expect_is(leveldb_property(db, "leveldb.stats"), "character")
  expect_null(leveldb_property(db, "nosuchproperty"))
  expect_error(leveldb_property(db, "nosuchproperty", TRUE),
               "No such property 'nosuchproperty'")
})

test_that("repair", {
  path <- tempfile()
  db <- leveldb_connect(path, create_if_missing = TRUE)
  leveldb_put(db, "foo", "bar")
  leveldb_close(db)
  expect_true(leveldb_repair(path))
  db2 <- leveldb_connect(path)
  expect_equal(leveldb_get(db2, "foo"), "bar")
  leveldb_close(db2)
  unlink(path, recursive = TRUE)
})

test_that("raw detection -- serialized objects", {
  path <- tempfile()
  db <- leveldb_connect(path, create_if_missing = TRUE)

  x <- runif(10)
  sx <- serialize(x, NULL)
  y <- runif(10)
  sy <- serialize(y, NULL, xdr = FALSE)

  expect_null(leveldb_put(db, sx, sy))
  expect_true(leveldb_exists(db, sx))
  expect_false(leveldb_exists(db, sy))

  expect_identical(leveldb_get(db, sx), sy)
})

test_that("raw detection -- embedded nul", {
  path <- tempfile()
  db <- leveldb_connect(path, create_if_missing = TRUE)

  ## NOTE: this guarantees an _embedded_ nul and x != y
  x <- as.raw(c(1L, sample(0:255), 2L))
  y <- as.raw(c(3L, sample(0:255), 4L))

  expect_null(leveldb_put(db, x, y))
  expect_true(leveldb_exists(db, x))
  expect_false(leveldb_exists(db, y))

  ## I think that a better way here will be as_raw = TRUE, FALSE, NULL
  expect_identical(leveldb_get(db, x, as_raw = TRUE), y)
  expect_identical(leveldb_get(db, x, as_raw = NULL), y)
  expect_error(leveldb_get(db, x, as_raw = FALSE),
               "Value contains embedded nul bytes; cannot return string")

  expect_identical(leveldb_get(db, x), y)
})

test_that("simple options", {
  path <- tempfile()
  db <- leveldb_connect(path,
                        create_if_missing = TRUE,
                        error_if_exists = TRUE,
                        paranoid_checks = TRUE,
                        write_buffer_size = 100,
                        max_open_files = 5000,
                        block_size = 8192L,
                        use_compression = FALSE)

  expect_output(print(db), paste0("name: ", path))
  expect_output(print(db), "create_if_missing: TRUE")
  expect_output(print(db), "error_if_exists: TRUE")
  expect_output(print(db), "paranoid_checks: TRUE")
  expect_output(print(db), "write_buffer_size: 100")
  expect_output(print(db), "max_open_files: 5000")
  expect_output(print(db), "block_size: 8192")
  expect_output(print(db), "use_compression: FALSE")
})

test_that("error_if_exists", {
  path <- tempfile()
  db <- leveldb_connect(path, create_if_missing = TRUE)
  leveldb_close(db)
  expect_error(leveldb_connect(path, error_if_exists = TRUE),
               "exists (error_if_exists is true)", fixed = TRUE)
})

test_that("enable cache", {
  ## skip("not working")
  path <- tempfile()
  db <- leveldb_connect(path,
                        create_if_missing = TRUE,
                        cache_capacity = 1000000)
  expect_is(.Call(Crleveldb_tag, db)[[2]], "externalptr")

  leveldb_put(db, "foo", "bar")
  expect_equal(leveldb_get(db, "foo"), "bar")
  leveldb_close(db)
  rm(db)
  gc()
})

test_that("enable filter", {
  path <- tempfile()
  db <- leveldb_connect(path,
                        create_if_missing = TRUE,
                        bloom_filter_bits_per_key = 10)
  expect_is(.Call(Crleveldb_tag, db)[[3]], "externalptr")

  leveldb_put(db, "foo", "bar")
  expect_equal(leveldb_get(db, "foo"), "bar")
  leveldb_close(db)
  rm(db)
  gc()
})

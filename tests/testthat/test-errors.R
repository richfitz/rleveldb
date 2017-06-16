context("error handling")

test_that("as_raw", {
  path <- tempfile()
  db <- leveldb_open(path, create_if_missing = TRUE)

  expect_error(leveldb_get(db, "foo", NA),
               "Expected a non-missing logical scalar (or NULL)", fixed = TRUE)
  expect_error(leveldb_get(db, "foo", "x"),
               "Expected a logical scalar (or NULL)", fixed = TRUE)
})

test_that("key error handling", {
  path <- tempfile()
  db <- leveldb_open(path, create_if_missing = TRUE)
  expect_error(leveldb_get(db, c("a", "b")),
               "key must be a scalar character")
  expect_error(leveldb_get(db, NULL),
               "Invalid data type for key")
  expect_error(leveldb_get(db, 1),
               "Invalid data type for key")
})

test_that("logical error handling", {
  expect_error(leveldb_writeoptions(1), "Expected a logical scalar")
  expect_error(leveldb_writeoptions(c(1, 1)), "Expected a logical scalar")
  expect_error(leveldb_writeoptions(NA),
               "Expected a non-missing logical scalar")
})

test_that("name error handling", {
  expect_error(leveldb_open(NULL), "Expected a scalar string")
  expect_error(leveldb_open(letters), "Expected a scalar string")
  expect_error(leveldb_open(TRUE), "Expected a scalar string")
})

test_that("approximate_sizes error handling", {
  path <- tempfile()
  db <- leveldb_open(path, create_if_missing = TRUE)
  expect_error(leveldb_approximate_sizes(db, "a", character(0)),
               "Expected 'limit_key' to be a length 1 vector")
  expect_error(leveldb_approximate_sizes(db, "a", letters),
               "Expected 'limit_key' to be a length 1 vector")
  expect_error(leveldb_approximate_sizes(db, "a", 1),
               "Invalid type; expected a character or raw vector")
})
test_that("database handle handles safely", {
  expect_error(leveldb_get(NULL, "foo"), "Expected an external pointer")
  expect_error(leveldb_get(null_pointer(), "foo"),
               "leveldb handle is not open")
})

test_that("iterator handle handles safely", {
  expect_error(leveldb_iter_valid(NULL), "Expected an external pointer")
  expect_error(leveldb_iter_valid(null_pointer()),
               "leveldb iterator is not open")
  path <- tempfile()

  db <- leveldb_open(path, create_if_missing = TRUE)
  it <- leveldb_iter_create(db)
  leveldb_iter_destroy(it)
  expect_error(leveldb_iter_valid(it), "leveldb iterator is not open")
})

test_that("snapshot handle handles safely", {
  expect_error(leveldb_readoptions(snapshot = "a"),
               "Expected an external pointer")
  expect_error(leveldb_readoptions(snapshot = null_pointer()),
               "leveldb snapshot is not open")
})

test_that("writebatch handle handles safely", {
  expect_error(leveldb_writebatch_clear(NULL), "Expected an external pointer")
  expect_error(leveldb_writebatch_clear(null_pointer()),
               "leveldb writebatch is not open")
  wb <- leveldb_writebatch_create()
  leveldb_writebatch_destroy(wb)
  expect_error(leveldb_writebatch_clear(wb), "leveldb writebatch is not open")
})

test_that("readoptions handle handles safely", {
  path <- tempfile()
  db <- leveldb_open(path, create_if_missing = TRUE)
  expect_error(leveldb_iter_create(db, readoptions = 1),
               "Expected an external pointer")
  expect_error(leveldb_iter_create(db, readoptions = null_pointer()),
               "leveldb readoptions is not open")
})

test_that("writeoptions handle handles safely", {
  path <- tempfile()
  db <- leveldb_open(path, create_if_missing = TRUE)
  expect_error(leveldb_delete(db, "a", writeoptions = 1),
               "Expected an external pointer")
  expect_error(leveldb_delete(db, "a", writeoptions = null_pointer()),
               "leveldb writeoptions is not open")
})

test_that("scalar_size", {
  path <- tempfile()
  expect_error(leveldb_open(path, max_open_files = -1),
               "Expected a positive size")
  ## special values:
  expect_error(leveldb_open(path, max_open_files = NA_real_),
               "Expected a non-missing")
  expect_error(leveldb_open(path, max_open_files = Inf),
               "Expected a non-missing")
  expect_error(leveldb_open(path, max_open_files = NA_integer_),
               "Expected a non-missing")
  ## other
  expect_error(leveldb_open(path, max_open_files = "a"),
               "Expected a scalar size")
  expect_error(leveldb_open(path, max_open_files = c(10, 20)),
               "Expected a scalar size")
})

test_that("get_keys_data", {
  path <- tempfile()
  db <- leveldb_open(path, create_if_missing = TRUE)
  leveldb_approximate_sizes(db, raw(0), raw(255))

  expect_error(leveldb_approximate_sizes(db, "a", character(0)),
               "Expected 'limit_key' to be a length 1 vector")
  expect_error(leveldb_approximate_sizes(db, "a", letters),
               "Expected 'limit_key' to be a length 1 vector")
})

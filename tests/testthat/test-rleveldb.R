context("rleveldb")

test_that("open, close", {
  db <- leveldb_connect(tempfile())
  expect_true(leveldb_close(db))
  expect_false(leveldb_close(db))
  expect_error(leveldb_close(db, TRUE),
               "leveldb handle is not open")
  rm(db)
  gc()
})

test_that("destroy", {
  path <- tempfile()
  db <- leveldb_connect(path)
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
  db <- leveldb_connect(tempfile())

  leveldb_put(db, "foo", "bar")
  expect_equal(leveldb_get(db, "foo"), "bar")
  expect_identical(leveldb_keys_len(db), 1L)

  leveldb_put(db, "foo", charToRaw("barbar"))
  expect_equal(leveldb_get(db, "foo"), "barbar")
  expect_equal(leveldb_get(db, "foo", TRUE), charToRaw("barbar"))

  expect_null(leveldb_delete(db, "foo"))
})

test_that("Get missing key", {
  db <- leveldb_connect(tempfile())
  expect_null(leveldb_get(db, "foo"))
  expect_error(leveldb_get(db, "foo", error_if_missing = TRUE),
               "Key 'foo' not found in database")

  k <- charToRaw("foo")
  expect_null(leveldb_get(db, k))
  expect_error(leveldb_get(db, k, error_if_missing = TRUE),
               "Key not found in database")
})

test_that("keys", {
  db <- leveldb_connect(tempfile())
  expect_identical(leveldb_keys_len(db), 0L)
  expect_identical(leveldb_keys(db, TRUE), list())
  expect_identical(leveldb_keys(db, FALSE), character(0))

  leveldb_put(db, "foo", "bar")
  expect_identical(leveldb_keys_len(db), 1L)
  expect_equal(leveldb_keys(db, TRUE), list(charToRaw("foo")))
  expect_equal(leveldb_keys(db, FALSE), "foo")

  expect_null(leveldb_delete(db, "foo"))
  expect_identical(leveldb_keys_len(db), 0L)
})

test_that("exists", {
  db <- leveldb_connect(tempfile())
  expect_false(leveldb_exists(db, "foo"))
  leveldb_put(db, "foo", "bar")
  expect_true(leveldb_exists(db, "foo"))
  expect_false(leveldb_exists(db, "bar"))
})

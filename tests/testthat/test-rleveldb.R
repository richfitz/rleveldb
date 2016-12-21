context("rleveldb")

test_that("open, close", {
  db <- rleveldb_connect(tempfile())
  expect_true(rleveldb_close(db))
  expect_false(rleveldb_close(db))
  expect_error(rleveldb_close(db, TRUE),
               "leveldb handle is not open")
  rm(db)
  gc()
})

test_that("destroy", {
  path <- tempfile()
  db <- rleveldb_connect(path)
  expect_identical(rleveldb_keys_len(db), 0L)

  expect_error(rleveldb_destroy(path), "IO error: lock")
  rleveldb_close(db)
  expect_true(rleveldb_destroy(path))
  expect_false(file.exists(path))
  expect_true(rleveldb_destroy(path))
  rm(db)
  gc()
})

test_that("CRUD", {
  db <- rleveldb_connect(tempfile())

  rleveldb_put(db, "foo", "bar")
  expect_equal(rleveldb_get(db, "foo"), "bar")
  expect_identical(rleveldb_keys_len(db), 1L)

  rleveldb_put(db, "foo", charToRaw("barbar"))
  expect_equal(rleveldb_get(db, "foo"), "barbar")
  expect_equal(rleveldb_get(db, "foo", TRUE), charToRaw("barbar"))

  expect_null(rleveldb_delete(db, "foo"))
})

test_that("Get missing key", {
  db <- rleveldb_connect(tempfile())
  expect_null(rleveldb_get(db, "foo"))
  expect_error(rleveldb_get(db, "foo", error_if_missing = TRUE),
               "Key 'foo' not found in database")

  k <- charToRaw("foo")
  expect_null(rleveldb_get(db, k))
  expect_error(rleveldb_get(db, k, error_if_missing = TRUE),
               "Key not found in database")
})

test_that("keys", {
  db <- rleveldb_connect(tempfile())
  expect_identical(rleveldb_keys_len(db), 0L)
  expect_identical(rleveldb_keys(db, TRUE), list())
  expect_identical(rleveldb_keys(db, FALSE), character(0))

  rleveldb_put(db, "foo", "bar")
  expect_identical(rleveldb_keys_len(db), 1L)
  expect_equal(rleveldb_keys(db, TRUE), list(charToRaw("foo")))
  expect_equal(rleveldb_keys(db, FALSE), "foo")

  expect_null(rleveldb_delete(db, "foo"))
  expect_identical(rleveldb_keys_len(db), 0L)
})

test_that("exists", {
  db <- rleveldb_connect(tempfile())
  expect_false(rleveldb_exists(db, "foo"))
  rleveldb_put(db, "foo", "bar")
  expect_true(rleveldb_exists(db, "foo"))
  expect_false(rleveldb_exists(db, "bar"))
})

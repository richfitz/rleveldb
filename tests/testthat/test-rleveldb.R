context("rleveldb")

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
